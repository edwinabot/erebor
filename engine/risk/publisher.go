package risk

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// RedisPublisher publishes risk events to a Redis Stream.
// The stream key is {namespace}:risk.
type RedisPublisher struct {
	client *redis.Client
}

// NewRedisPublisher returns a Publisher that writes to {namespace}:risk.
func NewRedisPublisher(client *redis.Client) *RedisPublisher {
	return &RedisPublisher{client: client}
}

// Publish writes the event to the {namespace}:risk stream via XADD.
func (p *RedisPublisher) Publish(ctx context.Context, namespace string, event Event) error {
	streamKey := namespace + ":risk"
	if err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{
			"run_id":     event.RunID,
			"symbol":     event.Symbol,
			"event_time": event.EventTime.UTC().Format(time.RFC3339Nano),
			"type":       string(event.Type),
			"detail":     event.Detail,
			"equity":     event.Equity.String(),
		},
	}).Err(); err != nil {
		return fmt.Errorf("xadd %s: %w", streamKey, err)
	}
	return nil
}

// NoopPublisher is a Publisher that does nothing. Use in tests and benchmarks.
type NoopPublisher struct{}

// Publish always returns nil without writing anything.
func (NoopPublisher) Publish(_ context.Context, _ string, _ Event) error {
	return nil
}
