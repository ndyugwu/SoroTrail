package broadcast

import (
	"context"
	"log/slog"
	"sync"
)

// WebhookSubscription represents a webhook subscription for fair broadcast delivery.
type WebhookSubscription struct {
	ID       int64
	TenantID int64
	Endpoint string
	Secret   string
	Active   bool
}

// Event represents an ingested contract event to broadcast.
type Event struct {
	ID         string
	ContractID string
	Ledger     uint32
	Data       []byte
}

// Broker manages fair event distribution across subscriptions and tenants.
type Broker struct {
	mu            sync.Mutex
	subs          map[int64][]*WebhookSubscription
	backlogs      map[int64]int64
	subBacklogs   map[int64]int64
	maxQueueDepth int
	logger        *slog.Logger
}

// NewBroker creates a new Broker instance.
func NewBroker(logger *slog.Logger) *Broker {
	return &Broker{
		subs:          make(map[int64][]*WebhookSubscription),
		backlogs:      make(map[int64]int64),
		subBacklogs:   make(map[int64]int64),
		maxQueueDepth: 1000,
		logger:        logger,
	}
}

// RegisterSubscription registers a subscription.
func (b *Broker) RegisterSubscription(sub *WebhookSubscription) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs[sub.TenantID] = append(b.subs[sub.TenantID], sub)
}

// Broadcast fans out an event non-blockingly.
func (b *Broker) Broadcast(ctx context.Context, ev Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for tenantID, tenantSubs := range b.subs {
		if b.backlogs[tenantID] >= int64(b.maxQueueDepth) {
			continue
		}
		b.backlogs[tenantID]++
		for _, sub := range tenantSubs {
			b.subBacklogs[sub.ID]++
		}
	}
	return nil
}

// TenantBacklog returns backlog count for a tenant.
func (b *Broker) TenantBacklog(tenantID int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.backlogs[tenantID]
}

// SubscriptionBacklog returns backlog count for a subscription.
func (b *Broker) SubscriptionBacklog(subID int64) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.subBacklogs[subID]
}
