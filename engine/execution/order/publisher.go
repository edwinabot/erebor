// Package order publishes filled OrderEvents to the erebor:live:orders stream.
package order

import (
	"context"
	"fmt"
	"time"

	backtestdomain "github.com/edwinabot/erebor/backtest/domain"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Publisher writes OrderEvents to {namespace}:orders.
type Publisher struct {
	client    *redis.Client
	streamKey string
	logger    *zap.Logger
}

// NewPublisher creates a Publisher that writes to {namespace}:orders.
func NewPublisher(client *redis.Client, namespace string, logger *zap.Logger) *Publisher {
	return &Publisher{
		client:    client,
		streamKey: namespace + ":orders",
		logger:    logger.With(zap.String("component", "order-publisher")),
	}
}

// Publish writes an OrderEvent to the orders stream.
func (p *Publisher) Publish(ctx context.Context, order backtestdomain.OrderEvent) error {
	if err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: p.streamKey,
		Values: map[string]any{
			"run_id":      order.RunID,
			"symbol":      order.Symbol,
			"event_time":  order.EventTime.UTC().Format(time.RFC3339Nano),
			"order_id":    order.OrderID,
			"side":        string(order.Side),
			"type":        string(order.Type),
			"price":       order.Price.String(),
			"quantity":    order.Quantity.String(),
			"status":      string(order.Status),
			"fill_price":  order.FillPrice.String(),
			"fill_qty":    order.FillQty.String(),
			"fee":         order.Fee.String(),
			"signal_name": order.SignalName,
		},
	}).Err(); err != nil {
		return fmt.Errorf("xadd %s: %w", p.streamKey, err)
	}
	p.logger.Debug("order published",
		zap.String("order_id", order.OrderID),
		zap.String("symbol", order.Symbol),
		zap.String("side", string(order.Side)),
		zap.String("fill_price", order.FillPrice.String()),
	)
	return nil
}
