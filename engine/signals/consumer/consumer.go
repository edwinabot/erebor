package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwinabot/erebor/signals/compute"
	"github.com/edwinabot/erebor/signals/domain"
	"github.com/edwinabot/erebor/signals/publisher"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const (
	groupName       = "erebor-signals"
	defaultBlockDur = 5 * time.Second
	batchSize       = 20
)

// Option configures a Consumer.
type Option func(*Consumer)

// WithBlockDuration overrides the XREADGROUP block timeout.
// The default is 5s; tests should use a shorter value (e.g. 100ms) to keep
// the suite fast while still exercising the full read-loop path.
func WithBlockDuration(d time.Duration) Option {
	return func(c *Consumer) { c.blockDur = d }
}

// WithConsumerID sets the consumer name used in XREADGROUP calls.
// The default is the group name. Override when running multiple instances
// to prevent message redistribution within the consumer group.
func WithConsumerID(id string) Option {
	return func(c *Consumer) { c.consumerID = id }
}

// Consumer reads L2BookUpdateEvents from Redis Streams and publishes signals.
type Consumer struct {
	client      *redis.Client
	pub         *publisher.Publisher
	namespace   string
	symbols     []string
	signalDepth int
	consumerID  string
	blockDur    time.Duration
	logger      *zap.Logger

	running atomic.Bool
	wg      sync.WaitGroup
}

func New(
	client *redis.Client,
	pub *publisher.Publisher,
	namespace string,
	symbols []string,
	signalDepth int,
	logger *zap.Logger,
	opts ...Option,
) *Consumer {
	c := &Consumer{
		client:      client,
		pub:         pub,
		namespace:   namespace,
		symbols:     symbols,
		signalDepth: signalDepth,
		consumerID:  groupName,
		blockDur:    defaultBlockDur,
		logger:      logger.With(zap.String("component", "consumer")),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// IsRunning reports whether the consumer loop is active. Used by the health endpoint.
func (c *Consumer) IsRunning() bool {
	return c.running.Load()
}

// Start creates consumer groups (if absent) and launches the read loop.
func (c *Consumer) Start(ctx context.Context) error {
	for _, sym := range c.symbols {
		key := c.inputKey(sym)
		// MKSTREAM creates the stream if it doesn't exist yet.
		// "0" means consume from the beginning of the stream, which is correct for
		// both backtest replay (full history) and live (pick up any buffered events).
		err := c.client.XGroupCreateMkStream(ctx, key, groupName, "0").Err()
		if err != nil && !isAlreadyExists(err) {
			return fmt.Errorf("create consumer group for %s: %w", key, err)
		}
		c.logger.Info("consumer group ready", zap.String("stream", key), zap.String("group", groupName))
	}

	c.wg.Add(1)
	go c.readLoop(ctx)
	return nil
}

// Stop waits for the read loop to exit after the context is cancelled.
func (c *Consumer) Stop() {
	c.wg.Wait()
}

func (c *Consumer) readLoop(ctx context.Context) {
	defer c.wg.Done()
	c.running.Store(true)
	defer c.running.Store(false)

	// Build the STREAMS argument: [key1, key2, ..., ">", ">", ...]
	// ">" delivers messages not yet delivered to any consumer in this group.
	streamArgs := make([]string, len(c.symbols)*2)
	for i, sym := range c.symbols {
		streamArgs[i] = c.inputKey(sym)
		streamArgs[len(c.symbols)+i] = ">"
	}

	c.logger.Info("read loop started",
		zap.Strings("streams", streamArgs[:len(c.symbols)]),
		zap.Duration("block_dur", c.blockDur),
	)
	defer c.logger.Info("read loop stopped")

	for {
		entries, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    groupName,
			Consumer: c.consumerID,
			Streams:  streamArgs,
			Count:    batchSize,
			Block:    c.blockDur,
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) {
				c.logger.Debug("block timeout; no new messages")
				continue
			}
			if ctx.Err() != nil {
				c.logger.Info("context cancelled; shutting down read loop")
				return
			}
			c.logger.Error("xreadgroup error", zap.Error(err))
			continue
		}

		for i := range entries {
			streamKey := entries[i].Stream
			msgCount := len(entries[i].Messages)
			c.logger.Debug("received messages from stream",
				zap.String("stream", streamKey),
				zap.Int("count", msgCount),
			)
			for j := range entries[i].Messages {
				c.handleMessage(ctx, streamKey, entries[i].Messages[j])
			}
		}
	}
}

func (c *Consumer) handleMessage(ctx context.Context, streamKey string, msg redis.XMessage) {
	c.logger.Debug("handling message", zap.String("id", msg.ID), zap.String("stream", streamKey))

	event, err := decodeL2BookUpdateEvent(msg.Values)
	if err != nil {
		c.logger.Error("decode L2BookUpdateEvent", zap.String("id", msg.ID), zap.Error(err))
		c.ack(ctx, streamKey, msg.ID)
		return
	}

	c.logger.Debug("decoded L2BookUpdateEvent",
		zap.String("symbol", event.Symbol),
		zap.Time("event_time", event.EventTime),
		zap.Int("bids", len(event.Bids)),
		zap.Int("asks", len(event.Asks)),
	)

	signals := compute.All(event, c.signalDepth)
	published := 0
	for _, sig := range signals {
		if err := c.pub.Publish(ctx, sig); err != nil {
			c.logger.Error("publish signal",
				zap.String("signal", sig.Name),
				zap.String("symbol", sig.Symbol),
				zap.Error(err),
			)
		} else {
			published++
		}
	}

	c.logger.Debug("published signals",
		zap.String("symbol", event.Symbol),
		zap.String("msg_id", msg.ID),
		zap.Int("published", published),
		zap.Int("total", len(signals)),
	)

	c.ack(ctx, streamKey, msg.ID)
}

func (c *Consumer) ack(ctx context.Context, streamKey, id string) {
	if err := c.client.XAck(ctx, streamKey, groupName, id).Err(); err != nil {
		c.logger.Warn("xack failed", zap.String("id", id), zap.Error(err))
	}
}

func (c *Consumer) inputKey(symbol string) string {
	return c.namespace + ":l2:" + strings.ToUpper(symbol)
}

// decodeL2BookUpdateEvent parses a Redis Stream message into an L2BookUpdateEvent.
// Expected fields: run_id, symbol, event_time (RFC3339Nano), last_update_id, bids (JSON), asks (JSON).
func decodeL2BookUpdateEvent(values map[string]any) (domain.L2BookUpdateEvent, error) {
	var ev domain.L2BookUpdateEvent
	var err error

	ev.RunID, _ = values["run_id"].(string)

	sym, _ := values["symbol"].(string)
	if sym == "" {
		return ev, fmt.Errorf("missing symbol")
	}
	ev.Symbol = sym

	tsStr, _ := values["event_time"].(string)
	if tsStr == "" {
		return ev, fmt.Errorf("missing event_time")
	}
	ev.EventTime, err = time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		return ev, fmt.Errorf("parse event_time %q: %w", tsStr, err)
	}

	if idStr, _ := values["last_update_id"].(string); idStr != "" {
		ev.LastUpdateID, _ = strconv.ParseInt(idStr, 10, 64)
	}

	bidsRaw, _ := values["bids"].(string)
	ev.Bids, err = decodePriceLevels(bidsRaw)
	if err != nil {
		return ev, fmt.Errorf("decode bids: %w", err)
	}

	asksRaw, _ := values["asks"].(string)
	ev.Asks, err = decodePriceLevels(asksRaw)
	if err != nil {
		return ev, fmt.Errorf("decode asks: %w", err)
	}

	return ev, nil
}

// decodePriceLevels unmarshals a JSON array of [price, qty] string pairs.
func decodePriceLevels(raw string) ([]domain.PriceLevel, error) {
	if raw == "" {
		return nil, nil
	}
	var pairs [][2]string
	if err := json.Unmarshal([]byte(raw), &pairs); err != nil {
		return nil, err
	}
	levels := make([]domain.PriceLevel, 0, len(pairs))
	for _, pair := range pairs {
		price, err := decimal.NewFromString(pair[0])
		if err != nil {
			return nil, fmt.Errorf("parse price %q: %w", pair[0], err)
		}
		qty, err := decimal.NewFromString(pair[1])
		if err != nil {
			return nil, fmt.Errorf("parse qty %q: %w", pair[1], err)
		}
		levels = append(levels, domain.PriceLevel{Price: price, Quantity: qty})
	}
	return levels, nil
}

func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
