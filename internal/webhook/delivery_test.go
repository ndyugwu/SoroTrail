package webhook

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/sorotrail/sorotrail/internal/broadcast"
)

func TestWebhookDelivery_FairnessIntegration(t *testing.T) {
	var requestCount int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&requestCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	dispatcher := broadcast.NewFairDispatcher(broadcast.DefaultFairnessConfig())
	defer dispatcher.Close()

	ctx := context.Background()
	err := dispatcher.Enqueue(ctx, broadcast.Task{
		TenantID:       "tenant-webhook",
		SubscriptionID: "sub-1",
		Execute: func(ctx context.Context) error {
			req, err := http.NewRequestWithContext(ctx, "POST", srv.URL, nil)
			if err != nil {
				return err
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				return err
			}
			defer resp.Body.Close()
			return nil
		},
	})
	require.NoError(t, err)

	// Wait for delivery to complete
	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&requestCount) == 1
	}, 3*time.Second, 50*time.Millisecond)
}
