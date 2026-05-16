package publisher

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// ControlPublisher writes ControlEvents to the run's control stream.
// All downstream consumers (erebor-signals, erebor-execution) subscribe to
// this stream to coordinate lifecycle start and shutdown.
//
// Stream key: {namespace}:control
type ControlPublisher struct {
	client    *redis.Client
	namespace string
	logger    *zap.Logger
}

// NewControlPublisher creates a ControlPublisher that writes to {namespace}:control.
func NewControlPublisher(client *redis.Client, namespace string, logger *zap.Logger) *ControlPublisher {
	return &ControlPublisher{
		client:    client,
		namespace: namespace,
		logger:    logger.With(zap.String("component", "control-publisher")),
	}
}

// Publish writes a ControlEvent to the control stream.
func (p *ControlPublisher) Publish(ctx context.Context, ev domain.ControlEvent) error {
	streamKey := p.namespace + ":control"

	payloadJSON, err := json.Marshal(ev.Payload)
	if err != nil {
		return fmt.Errorf("marshal control payload: %w", err)
	}

	if err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{
			"run_id":  ev.RunID,
			"type":    string(ev.Type),
			"payload": string(payloadJSON),
		},
	}).Err(); err != nil {
		p.logger.Error("failed to publish control event",
			zap.String("stream", streamKey),
			zap.String("run_id", ev.RunID),
			zap.String("type", string(ev.Type)),
			zap.Error(err),
		)
		return fmt.Errorf("xadd %s: %w", streamKey, err)
	}

	p.logger.Info("control event published",
		zap.String("stream", streamKey),
		zap.String("run_id", ev.RunID),
		zap.String("type", string(ev.Type)),
	)
	return nil
}
