package archive

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactor_PlanAndCompact(t *testing.T) {
	ctx := context.Background()

	// In-memory or mock client testing structure can be set up or tested against a mock/minio test container.
	// Here we verify planCompactor and basic structures.
	compactor := NewCompactor(nil, "test-bucket", "archive", 1024, slog.Default())

	chunks := []ChunkInfo{
		{Key: "archive/events/schema=v1/ledger_start=0000001/data.parquet", ManifestKey: "archive/events/schema=v1/ledger_start=0000001/manifest.json", LedgerStart: 1, LedgerEnd: 100, RowCount: 10, Size: 100},
		{Key: "archive/events/schema=v1/ledger_start=0000101/data.parquet", ManifestKey: "archive/events/schema=v1/ledger_start=0000101/manifest.json", LedgerStart: 101, LedgerEnd: 200, RowCount: 15, Size: 120},
		{Key: "archive/events/schema=v1/ledger_start=0000500/data.parquet", ManifestKey: "archive/events/schema=v1/ledger_start=0000500/manifest.json", LedgerStart: 500, LedgerEnd: 600, RowCount: 20, Size: 200},
	}

	ranges := compactor.planCompaction(chunks)
	require.Len(t, ranges, 1)
	assert.Equal(t, int64(1), ranges[0].LedgerStart)
	assert.Equal(t, int64(200), ranges[0].LedgerEnd)
	assert.Len(t, ranges[0].Chunks, 2)
}

func TestManifestSerialization(t *testing.T) {
	m := Manifest{
		SchemaVersion: "v1",
		LedgerStart:   1000,
		LedgerEnd:     2000,
		RowCount:      55,
		Checksum:      "abcdef123456",
		Files:         []string{"data.parquet"},
	}

	data, err := json.Marshal(m)
	require.NoError(t, err)

	var decoded Manifest
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)
	assert.Equal(t, m.LedgerStart, decoded.LedgerStart)
	assert.Equal(t, m.Checksum, decoded.Checksum)
}
