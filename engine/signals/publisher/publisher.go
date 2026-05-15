package publisher

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/edwinabot/erebor/signals/domain"
	"github.com/redis/go-redis/v9"
)

// Publisher writes SignalEvents to a Redis Stream.
type Publisher struct {
	client    *redis.Client
	streamKey string
}

func New(client *redis.Client, namespace string) *Publisher {
	return &Publisher{
		client:    client,
		streamKey: namespace + ":signals",
	}
}

// Publish writes a SignalEvent to the output stream.
func (p *Publisher) Publish(ctx context.Context, sig domain.SignalEvent) error {
	params, err := json.Marshal(sig.Params)
	if err != nil {
		return fmt.Errorf("marshal params: %w", err)
	}

	if err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamKey,
		Values: map[string]any{
			"run_id":     sig.RunID,
			"symbol":     sig.Symbol,
			"event_time": sig.EventTime.UTC().Format("2006-01-02T15:04:05.999999999Z"),
			"name":       sig.Name,
			"version":    sig.Version,
			"value":      sig.Value.String(),
			"params":     string(params),
		},
	}).Err(); err != nil {
		return fmt.Errorf("xadd %s: %w", p.streamKey, err)
	}
	return nil
}
