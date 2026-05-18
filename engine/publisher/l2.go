// Package publisher provides Redis Stream publishers for live erebor-ingest events.
package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Option configures an L2Publisher.
type Option func(*L2Publisher)

// WithMaxLen sets an approximate MAXLEN on each XADD to bound stream memory.
// Zero (default) disables trimming.
func WithMaxLen(n int64) Option {
	return func(p *L2Publisher) { p.maxLen = n }
}

// L2Publisher writes L2BookUpdateEvents to the live L2 Redis stream.
//
// Stream key: {namespace}:l2:{SYMBOL}
// Wire format: run_id, symbol, event_time (RFC3339Nano UTC), last_update_id,
// bids (JSON [][2]string), asks (JSON [][2]string).
//
// This wire format is identical to engine/backtest/publisher/l2.go.
// Any change to the encoding must be applied to both.
type L2Publisher struct {
	client    *redis.Client
	namespace string
	maxLen    int64
	logger    *zap.Logger
}

// NewL2Publisher creates an L2Publisher that writes to streams under namespace.
func NewL2Publisher(client *redis.Client, namespace string, logger *zap.Logger, opts ...Option) *L2Publisher {
	p := &L2Publisher{
		client:    client,
		namespace: namespace,
		logger:    logger.With(zap.String("component", "l2-publisher")),
	}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Publish writes one L2BookUpdateEvent to the stream for the given symbol.
// eventTime MUST come from the diff event's EventTime — never time.Now().
// For live events pass runID as empty string.
func (p *L2Publisher) Publish(
	ctx context.Context,
	runID, symbol string,
	eventTime time.Time,
	lastUpdateID int64,
	bids, asks []domain.PriceLevel,
) error {
	streamKey := p.namespace + ":l2:" + strings.ToUpper(symbol)

	bidsJSON, err := levelsToJSON(bids)
	if err != nil {
		return fmt.Errorf("encode bids for %s: %w", symbol, err)
	}
	asksJSON, err := levelsToJSON(asks)
	if err != nil {
		return fmt.Errorf("encode asks for %s: %w", symbol, err)
	}

	args := &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{
			"run_id":         runID,
			"symbol":         strings.ToUpper(symbol),
			"event_time":     eventTime.UTC().Format(time.RFC3339Nano),
			"last_update_id": fmt.Sprintf("%d", lastUpdateID),
			"bids":           string(bidsJSON),
			"asks":           string(asksJSON),
		},
	}
	if p.maxLen > 0 {
		args.MaxLen = p.maxLen
		args.Approx = true
	}

	if err := p.client.XAdd(ctx, args).Err(); err != nil {
		p.logger.Error("failed to publish L2 event",
			zap.String("stream", streamKey),
			zap.String("symbol", symbol),
			zap.Time("event_time", eventTime),
			zap.Error(err),
		)
		return fmt.Errorf("xadd %s: %w", streamKey, err)
	}

	p.logger.Debug("L2 event published",
		zap.String("stream", streamKey),
		zap.String("symbol", symbol),
		zap.Time("event_time", eventTime),
		zap.Int64("last_update_id", lastUpdateID),
	)
	return nil
}

func levelsToJSON(levels []domain.PriceLevel) ([]byte, error) {
	pairs := make([][2]string, 0, len(levels))
	for _, lvl := range levels {
		pairs = append(pairs, [2]string{lvl.Price.String(), lvl.Quantity.String()})
	}
	return json.Marshal(pairs)
}
