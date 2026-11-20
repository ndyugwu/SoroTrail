package broadcast

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	ErrQueueFull = errors.New("broadcast delivery queue full for tenant")

	metricTenantBacklog = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sorotrail_tenant_delivery_backlog_events",
			Help: "Current backlog of events queued for delivery per tenant.",
		},
		[]string{"tenant_id"},
	)

	metricSubscriptionBacklog = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "sorotrail_subscription_delivery_backlog_events",
			Help: "Current backlog of events queued for delivery per subscription.",
		},
		[]string{"subscription_id"},
	)

	metricDeliveriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sorotrail_deliveries_total",
			Help: "Total deliveries attempted, categorized by tenant and status.",
		},
		[]string{"tenant_id", "status"},
	)
)

// FairnessConfig defines the capacity bounds and concurrency settings for fair tenant delivery.
type FairnessConfig struct {
	MaxConcurrencyPerTenant int
	TenantQueueCapacity     int
	SubscriptionQueueCap    int
}

// DefaultFairnessConfig returns standard production bounds for tenant delivery.
func DefaultFairnessConfig() FairnessConfig {
	return FairnessConfig{
		MaxConcurrencyPerTenant: 32,
		TenantQueueCapacity:     1024,
		SubscriptionQueueCap:    256,
	}
}

// Task represents an enqueued event delivery item.
type Task struct {
	TenantID       string
	SubscriptionID string
	Payload        any
	IsRetry        bool
	Execute        func(ctx context.Context) error
}

// FairDispatcher manages per-tenant queues, concurrency bounds, and retry isolation.
type FairDispatcher struct {
	mu       sync.Mutex
	cfg      FairnessConfig
	tenants  map[string]*tenantQueue
	closed   bool
}

type tenantQueue struct {
	taskChan chan Task
	active   int64
	backlog  int64
}

// NewFairDispatcher creates a new FairDispatcher with the given configuration.
func NewFairDispatcher(cfg FairnessConfig) *FairDispatcher {
	if cfg.MaxConcurrencyPerTenant <= 0 {
		cfg.MaxConcurrencyPerTenant = 32
	}
	if cfg.TenantQueueCapacity <= 0 {
		cfg.TenantQueueCapacity = 1024
	}
	return &FairDispatcher{
		cfg:     cfg,
		tenants: make(map[string]*tenantQueue),
	}
}

// Enqueue adds a delivery task to the tenant's bounded queue without blocking ingestion.
func (d *FairDispatcher) Enqueue(ctx context.Context, task Task) error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return errors.New("fair dispatcher is closed")
	}
	tq, exists := d.tenants[task.TenantID]
	if !exists {
		tq = &tenantQueue{
			taskChan: make(chan Task, d.cfg.TenantQueueCapacity),
		}
		d.tenants[task.TenantID] = tq
		go d.runTenantWorker(task.TenantID, tq)
	}
	d.mu.Unlock()

	select {
	case tq.taskChan <- task:
		newBacklog := atomic.AddInt64(&tq.backlog, 1)
		metricTenantBacklog.WithLabelValues(task.TenantID).Set(float64(newBacklog))
		if task.SubscriptionID != "" {
			metricSubscriptionBacklog.WithLabelValues(task.SubscriptionID).Inc()
		}
		return nil
	default:
		metricDeliveriesTotal.WithLabelValues(task.TenantID, "dropped_queue_full").Inc()
		return ErrQueueFull
	}
}

func (d *FairDispatcher) runTenantWorker(tenantID string, tq *tenantQueue) {
	sem := make(chan struct{}, d.cfg.MaxConcurrencyPerTenant)
	for task := range tq.taskChan {
		newBacklog := atomic.AddInt64(&tq.backlog, -1)
		metricTenantBacklog.WithLabelValues(tenantID).Set(float64(newBacklog))
		if task.SubscriptionID != "" {
			metricSubscriptionBacklog.WithLabelValues(task.SubscriptionID).Dec()
		}

		sem <- struct{}{}
		go func(t Task) {
			defer func() { <-sem }()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			err := t.Execute(ctx)
			if err != nil {
				metricDeliveriesTotal.WithLabelValues(tenantID, "error").Inc()
			} else {
				metricDeliveriesTotal.WithLabelValues(tenantID, "success").Inc()
			}
		}(task)
	}
}

// Close shuts down the dispatcher and queues.
func (d *FairDispatcher) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for _, tq := range d.tenants {
		close(tq.taskChan)
	}
}
