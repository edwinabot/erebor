package publisher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	ingestdomain "github.com/edwinabot/erebor/ingest/domain"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// L2Publisher writes L2BookUpdateEvent entries to the run-namespaced L2 stream
// in the wire format expected by erebor-signals' consumer.
//
// Stream key: {namespace}:l2:{SYMBOL}
// Wire format: run_id, symbol, event_time (RFC3339Nano UTC), last_update_id,
//
//	bids (JSON [][2]string), asks (JSON [][2]string).
type L2Publisher struct {
	client    *redis.Client
	namespace string
	logger    *zap.Logger
}

// NewL2Publisher creates an L2Publisher that writes to streams under namespace.
func NewL2Publisher(client *redis.Client, namespace string, logger *zap.Logger) *L2Publisher {
	return &L2Publisher{
		client:    client,
		namespace: namespace,
		logger:    logger.With(zap.String("component", "l2-publisher")),
	}
}

// Publish writes one L2BookUpdateEvent to the stream for the given symbol.
//
// eventTime MUST come from the replayed diff's EventTime — never from time.Now().
// bids and asks are the post-application top-of-book levels from book.Snapshot.
func (p *L2Publisher) Publish(
	ctx context.Context,
	runID, symbol string,
	eventTime time.Time,
	lastUpdateID int64,
	bids, asks []ingestdomain.PriceLevel,
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

	if err := p.client.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{
			"run_id":         runID,
			"symbol":         strings.ToUpper(symbol),
			"event_time":     eventTime.UTC().Format(time.RFC3339Nano),
			"last_update_id": fmt.Sprintf("%d", lastUpdateID),
			"bids":           string(bidsJSON),
			"asks":           string(asksJSON),
		},
	}).Err(); err != nil {
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

// levelsToJSON serialises price levels as a JSON array of [price, qty] string pairs,
// matching the wire format that erebor-signals' consumer.decodeL2BookUpdateEvent expects.
func levelsToJSON(levels []ingestdomain.PriceLevel) ([]byte, error) {
	pairs := make([][2]string, 0, len(levels))
	for _, lvl := range levels {
		pairs = append(pairs, [2]string{lvl.Price.String(), lvl.Quantity.String()})
	}
	return json.Marshal(pairs)
}
