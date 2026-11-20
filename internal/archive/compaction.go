package archive

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"

	"github.com/minio/minio-go/v7"
)

// Compactor handles merging small adjacent archive chunks into larger ones
// while preserving reader safety, atomicity, idempotency, and verification.
type Compactor struct {
	client     *minio.Client
	bucket     string
	prefix     string
	logger     *slog.Logger
	targetSize int64 // target approximate byte size or event count threshold for a merged chunk
}

// NewCompactor creates a Compactor for the given S3 bucket and prefix.
func NewCompactor(client *minio.Client, bucket, prefix string, targetSize int64, logger *slog.Logger) *Compactor {
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	if targetSize <= 0 {
		targetSize = 1024 * 1024 * 16 // 16MB default target chunk size
	}
	return &Compactor{
		client:     client,
		bucket:     bucket,
		prefix:     strings.TrimSuffix(prefix, "/"),
		logger:     logger,
		targetSize: targetSize,
	}
}

// ChunkInfo describes an archive chunk file and its manifest metadata.
type ChunkInfo struct {
	Key         string   `json:"key"`
	ManifestKey string   `json:"manifest_key"`
	LedgerStart int64    `json:"ledger_start"`
	LedgerEnd   int64    `json:"ledger_end"`
	RowCount    int64    `json:"row_count"`
	Checksum    string   `json:"checksum"`
	Size        int64    `json:"size"`
	Manifest    Manifest `json:"manifest"`
}

// Manifest represents the archive chunk manifest JSON.
type Manifest struct {
	SchemaVersion string   `json:"schema_version"`
	LedgerStart   int64    `json:"ledger_start"`
	LedgerEnd     int64    `json:"ledger_end"`
	RowCount      int64    `json:"row_count"`
	Checksum      string   `json:"checksum"`
	Files         []string `json:"files,omitempty"`
}

// CompactionSummary reports progress and space reclaimed after a compaction run.
type CompactionSummary struct {
	ChunksCompacted  int   `json:"chunks_compacted"`
	BytesReclaimed   int64 `json:"bytes_reclaimed"`
	NewChunksCreated int   `json:"new_chunks_created"`
}

// CompactRange identifies a range of chunks to compact together.
type CompactRange struct {
	LedgerStart int64
	LedgerEnd   int64
	Chunks      []ChunkInfo
}

// Compact executes compaction over the archive prefix, merging adjacent small chunks.
func (c *Compactor) Compact(ctx context.Context) (CompactionSummary, error) {
	summary := CompactionSummary{}

	chunks, err := c.ListChunks(ctx)
	if err != nil {
		return summary, fmt.Errorf("failed to list archive chunks for compaction: %w", err)
	}

	if len(chunks) < 2 {
		c.logger.Info("archive has insufficient chunks for compaction", "count", len(chunks))
		return summary, nil
	}

	// Group chunks into ranges eligible for compaction based on target size or count
	ranges := c.planCompaction(chunks)
	for _, rng := range ranges {
		if len(rng.Chunks) <= 1 {
			continue
		}

		c.logger.Info("starting compaction for chunk range",
			"ledger_start", rng.LedgerStart,
			"ledger_end", rng.LedgerEnd,
			"chunk_count", len(rng.Chunks))

		res, err := c.compactRange(ctx, rng)
		if err != nil {
			c.logger.Error("failed to compact chunk range", "error", err, "ledger_start", rng.LedgerStart)
			return summary, err
		}

		summary.ChunksCompacted += res.ChunksCompacted
		summary.BytesReclaimed += res.BytesReclaimed
		summary.NewChunksCreated += res.NewChunksCreated
	}

	return summary, nil
}

// ListChunks lists all valid archive chunks and parses their manifests from storage.
func (c *Compactor) ListChunks(ctx context.Context) ([]ChunkInfo, error) {
	var chunks []ChunkInfo
	objectCh := c.client.ListObjects(ctx, c.bucket, minio.ListObjectsOptions{
		Prefix:    c.prefix,
		Recursive: true,
	})

	manifestMap := make(map[string]ObjectInfo)
	dataMap := make(map[string]minio.ObjectInfo)

	for obj := range objectCh {
		if obj.Err != nil {
			return nil, obj.Err
		}
		if strings.HasSuffix(obj.Key, "manifest.json") {
			manifestMap[obj.Key] = obj
		} else if strings.HasSuffix(obj.Key, ".parquet") || strings.HasSuffix(obj.Key, ".ndjson.gz") || strings.HasSuffix(obj.Key, ".gz") {
			dataMap[obj.Key] = obj
		}
	}

	for mKey := range manifestMap {
		r, err := c.client.GetObject(ctx, c.bucket, mKey, minio.GetObjectOptions{})
		if err != nil {
			continue
		}
		data, err := io.ReadAll(r)
		_ = r.Close()
		if err != nil {
			continue
		}

		var manifest Manifest
		if err := json.Unmarshal(data, &manifest); err != nil {
			continue
		}

		// Find corresponding data file
		baseDir := strings.TrimSuffix(mKey, "manifest.json")
		var dataKey string
		var dataSize int64
		for dKey, dObj := range dataMap {
			if strings.HasPrefix(dKey, baseDir) {
				dataKey = dKey
				dataSize = dObj.Size
				break
			}
		}

		if dataKey == "" && len(manifest.Files) > 0 {
			dataKey = baseDir + manifest.Files[0]
		}

		if dataKey != "" {
			chunks = append(chunks, ChunkInfo{
				Key:         dataKey,
				ManifestKey: mKey,
				LedgerStart: manifest.LedgerStart,
				LedgerEnd:   manifest.LedgerEnd,
				RowCount:    manifest.RowCount,
				Checksum:    manifest.Checksum,
				Size:        dataSize,
				Manifest:    manifest,
			})
		}
	}

	sort.Slice(chunks, func(i, j int) bool {
		if chunks[i].LedgerStart == chunks[j].LedgerStart {
			return chunks[i].LedgerEnd < chunks[j].LedgerEnd
		}
		return chunks[i].LedgerStart < chunks[j].LedgerStart
	})

	return chunks, nil
}

type ObjectInfo = minio.ObjectInfo

func (c *Compactor) planCompaction(chunks []ChunkInfo) []CompactRange {
	var ranges []CompactRange
	var currentRange []ChunkInfo
	var currentBytes int64

	for _, ch := range chunks {
		if len(currentRange) == 0 {
			currentRange = append(currentRange, ch)
			currentBytes = ch.Size
			continue
		}

		// Check if adjacent and under target size
		last := currentRange[len(currentRange)-1]
		isAdjacent := ch.LedgerStart <= last.LedgerEnd+1

		if isAdjacent && (currentBytes+ch.Size <= c.targetSize) && (len(currentRange) < 10) {
			currentRange = append(currentRange, ch)
			currentBytes += ch.Size
		} else {
			if len(currentRange) > 1 {
				ranges = append(ranges, CompactRange{
					LedgerStart: currentRange[0].LedgerStart,
					LedgerEnd:   currentRange[len(currentRange)-1].LedgerEnd,
					Chunks:      currentRange,
				})
			}
			currentRange = []ChunkInfo{ch}
			currentBytes = ch.Size
		}
	}

	if len(currentRange) > 1 {
		ranges = append(ranges, CompactRange{
			LedgerStart: currentRange[0].LedgerStart,
			LedgerEnd:   currentRange[len(currentRange)-1].LedgerEnd,
			Chunks:      currentRange,
		})
	}

	return ranges
}

func (c *Compactor) compactRange(ctx context.Context, rng CompactRange) (CompactionSummary, error) {
	summary := CompactionSummary{}

	mergedLedgerStart := rng.LedgerStart
	mergedLedgerEnd := rng.LedgerEnd

	var mergedBuffer bytes.Buffer
	var totalRowCount int64
	hash := sha256.New()

	var originalKeys []string
	var originalManifestKeys []string
	var bytesReclaimed int64

	for _, ch := range rng.Chunks {
		originalKeys = append(originalKeys, ch.Key)
		originalManifestKeys = append(originalManifestKeys, ch.ManifestKey)
		bytesReclaimed += ch.Size

		r, err := c.client.GetObject(ctx, c.bucket, ch.Key, minio.GetObjectOptions{})
		if err != nil {
			return summary, fmt.Errorf(
				"failed to read source chunk %s: %w", ch.Key, err)
		}

		tee := io.TeeReader(r, hash)
		n, err := io.Copy(&mergedBuffer, tee)
		_ = r.Close()
		if err != nil {
			return summary, fmt.Errorf("failed to copy chunk data %s: %w", ch.Key, err)
		}

		totalRowCount += ch.RowCount
		bytesReclaimed += n
	}

	mergedChecksum := hex.EncodeToString(hash.Sum(nil))

	basePrefix := fmt.Sprintf("%s/events/schema=v1/ledger_start=%07d", c.prefix, mergedLedgerStart)
	dataKey := basePrefix + "/data.parquet"
	manifestKey := basePrefix + "/manifest.json"

	if strings.HasSuffix(rng.Chunks[0].Key, ".gz") {
		dataKey = basePrefix + "/data.ndjson.gz"
	}

	reader := bytes.NewReader(mergedBuffer.Bytes())
	_, err := c.client.PutObject(ctx, c.bucket, dataKey, reader, int64(mergedBuffer.Len()), minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		return summary, fmt.Errorf("failed to upload compacted data chunk: %w", err)
	}

	verifyReader, err := c.client.GetObject(ctx, c.bucket, dataKey, minio.GetObjectOptions{})
	if err != nil {
		return summary, fmt.Errorf("failed to open uploaded chunk for verification: %w", err)
	}
	verifyHash := sha256.New()
	_, err = io.Copy(verifyHash, verifyReader)
	_ = verifyReader.Close()
	if err != nil {
		return summary, fmt.Errorf(
			"failed to verify uploaded chunk checksum: %w", err)
	}
	verifyChecksum := hex.EncodeToString(verifyHash.Sum(nil))

	if verifyChecksum != mergedChecksum {
		_ = c.client.RemoveObject(ctx, c.bucket, dataKey, minio.RemoveObjectOptions{})
		return summary, fmt.Errorf(
			"compaction verification failed: checksum mismatch (expected %s, got %s)",
			mergedChecksum, verifyChecksum)
	}

	newManifest := Manifest{
		SchemaVersion: "v1",
		LedgerStart:   mergedLedgerStart,
		LedgerEnd:     mergedLedgerEnd,
		RowCount:      totalRowCount,
		Checksum:      mergedChecksum,
		Files:         []string{strings.TrimPrefix(dataKey, basePrefix+"/")},
	}

	manifestBytes, err := json.MarshalIndent(newManifest, "", "  ")
	if err != nil {
		_ = c.client.RemoveObject(ctx, c.bucket, dataKey, minio.RemoveObjectOptions{})
		return summary, fmt.Errorf("failed to marshal new manifest: %w", err)
	}

	_, err = c.client.PutObject(ctx, c.bucket, manifestKey, bytes.NewReader(manifestBytes), int64(len(manifestBytes)), minio.PutObjectOptions{
		ContentType: "application/json",
	})
	if err != nil {
		_ = c.client.RemoveObject(ctx, c.bucket, dataKey, minio.RemoveObjectOptions{})
		return summary, fmt.Errorf("failed to upload new manifest: %w", err)
	}

	for i := range originalKeys {
		_ = c.client.RemoveObject(ctx, c.bucket, originalKeys[i], minio.RemoveObjectOptions{})
		_ = c.client.RemoveObject(ctx, c.bucket, originalManifestKeys[i], minio.RemoveObjectOptions{})
	}

	summary.ChunksCompacted = len(rng.Chunks)
	summary.BytesReclaimed = bytesReclaimed
	summary.NewChunksCreated = 1

	c.logger.Info("successfully compacted chunk range",
		"ledger_start", mergedLedgerStart,
		"ledger_end", mergedLedgerEnd,
		"chunks_compacted", summary.ChunksCompacted,
		"bytes_reclaimed", summary.BytesReclaimed)

	return summary, nil
}
