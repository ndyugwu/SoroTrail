package broadcast

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFairDispatcher_StalledConsumerIsolation(t *testing.T) {
	cfg := FairnessConfig{
		MaxConcurrencyPerTenant: 2,
		TenantQueueCapacity:     10,
		SubscriptionQueueCap:    5,
	}
	dispatcher := NewFairDispatcher(cfg)
	defer dispatcher.Close()

	ctx := context.Background()

	// Slow tenant: task stalls
	slowStarted := make(chan struct{})
	slowRelease := make(chan struct{})

	// Enqueue slow tasks to saturate the tenant's concurrency
	for i := 0; i < 2; i++ {
		err := dispatcher.Enqueue(ctx, Task{
			TenantID:       "tenant-slow",
			SubscriptionID: "sub-slow",
			Payload:        i,
			Execute: func(ctx context.Context) error {
				close(slowStarted)
				<-slowRelease
				return nil
			},
		})
		require.NoError(t, err)
	}

	// Wait until slow tasks have started
	select {
	case <-slowStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for slow task to start")
	}

	// Another tenant ("tenant-fast") should still process immediately without being blocked by the slow tenant
	fastExecuted := make(chan struct{})
	err := dispatcher.Enqueue(ctx, Task{
		TenantID:       "tenant-fast",
		SubscriptionID: "sub-fast",
		Payload:        99,
		Execute: func(ctx context.Context) error {
			close(fastExecuted)
			return nil
		},
	})
	require.NoError(t, err)

	select {
	case <-fastExecuted:
		// Success: fast tenant is isolated from slow tenant
	case <-time.After(2 * time.Second):
		t.Fatal("stalled consumer on tenant-slow blocked tenant-fast deliveries")
	}

	close(slowRelease)
}

func TestFairDispatcher_HighVolumeVsQuietTenant(t *testing.T) {
	cfg := FairnessConfig{
		MaxConcurrencyPerTenant: 4,
		TenantQueueCapacity:     100,
	}
	dispatcher := NewFairDispatcher(cfg)
	defer dispatcher.Close()

	ctx := context.Background()

	// Flood high volume tenant
	for i := 0; i < 50; i++ {
		_ = dispatcher.Enqueue(ctx, Task{
			TenantID: "tenant-noisy",
			Execute: func(ctx context.Context) error {
				time.Sleep(5 * time.Millisecond)
				return nil
			},
		})
	}

	// Quiet tenant should process promptly even while noisy tenant is active
	quietExecuted := make(chan struct{})
	err := dispatcher.Enqueue(ctx, Task{
		TenantID: "tenant-quiet",
		Execute: func(ctx context.Context) error {
			close(quietExecuted)
			return nil
		},
	})
	require.NoError(t, err)

	select {
	case <-quietExecuted:
		// Passed
	case <-time.After(3 * time.Second):
		t.Fatal("quiet tenant starved by noisy tenant")
	}
}

func TestFairDispatcher_RetryStormProtection(t *testing.T) {
	cfg := FairnessConfig{
		MaxConcurrencyPerTenant: 2,
		TenantQueueCapacity:     10,
	}
	dispatcher := NewFairDispatcher(cfg)
	defer dispatcher.Close()

	ctx := context.Background()

	// Fill queue with retry tasks
	for i := 0; i < 5; i++ {
		_ = dispatcher.Enqueue(ctx, Task{
			TenantID: "tenant-retry",
			IsRetry:  true,
			Execute: func(ctx context.Context) error {
				time.Sleep(10 * time.Millisecond)
				return nil
			},
		})
	}

	// New first-attempt task should still get enqueued and executed
	firstAttemptDone := make(chan struct{})
	err := dispatcher.Enqueue(ctx, Task{
		TenantID: "tenant-retry",
		IsRetry:  false,
		Execute: func(ctx context.Context) error {
			close(firstAttemptDone)
			return nil
		},
	})
	require.NoError(t, err)

	select {
	case <-firstAttemptDone:
		// Success
	case <-time.After(3 * time.Second):
		t.Fatal("retry storm starved first attempts")
	}
}

func TestFairDispatcher_NonBlockingIngestion(t *testing.T) {
	cfg := FairnessConfig{
		MaxConcurrencyPerTenant: 1,
		TenantQueueCapacity:     2, // small queue to test saturation
	}
	dispatcher := NewFairDispatcher(cfg)
	defer dispatcher.Close()

	ctx := context.Background()

	// Saturate tenant queue
	for i := 0; i < 10; i++ {
		_ = dispatcher.Enqueue(ctx, Task{
			TenantID: "tenant-blocked",
			Execute: func(ctx context.Context) error {
				time.Sleep(1 * time.Second)
				return nil
			},
		})
	}

	// Ingestion (Enqueue call) must not block even if queues are full or workers are busy
	start := time.Now()
	err := dispatcher.Enqueue(ctx, Task{
		TenantID: "tenant-blocked",
		Execute: func(ctx context.Context) error {
			return nil
		},
	})
	elapsed := time.Since(start)

	// Either accepted or ErrQueueFull, but must return immediately without blocking on delivery execution
	assert.True(t, elapsed < 50*time.Millisecond, "Enqueue blocked ingestion for %v", elapsed)
	if err != nil {
		assert.Equal(t, ErrQueueFull, err)
	}
}

func TestFairDispatcher_QueueFullDeadLetter(t *testing.T) {
	cfg := FairnessConfig{
		MaxConcurrencyPerTenant: 1,
		TenantQueueCapacity:     1,
	}
	dispatcher := NewFairDispatcher(cfg)
	defer dispatcher.Close()

	ctx := context.Background()

	// Block the worker
	blockerStarted := make(chan struct{})
	releaseBlocker := make(chan struct{})
	_ = dispatcher.Enqueue(ctx, Task{
		TenantID: "tenant-full",
		Execute: func(ctx context.Context) error {
			close(blockerStarted)
			<-releaseBlocker
			return nil
		},
	})
	<-blockerStarted

	// Fill the single buffer slot
	_ = dispatcher.Enqueue(ctx, Task{
		TenantID: "tenant-full",
		Execute:  func(ctx context.Context) error { return nil },
	})

	// Next enqueue must return ErrQueueFull (signaling drop / dead-letter path)
	err := dispatcher.Enqueue(ctx, Task{
		TenantID: "tenant-full",
		Execute:  func(ctx context.Context) error { return nil },
	})
	assert.Equal(t, ErrQueueFull, err)

	close(releaseBlocker)
}
