// Package consumer reads SignalEvents from erebor:live:signals via XREADGROUP
// and dispatches them to a configurable handler.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	signalsdomain "github.com/edwinabot/erebor/signals/domain"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const (
	groupName       = "erebor-execution"
	defaultBlockDur = 5 * time.Second
	batchSize       = 20
)

// Handler processes one decoded SignalEvent.
// Return nil to ACK the message; return a non-nil error to leave it in the PEL
// so it is retried on the next call to Start.
type Handler func(ctx context.Context, msgID string, sig signalsdomain.SignalEvent) error

// Option configures a Consumer.
type Option func(*Consumer)

// WithBlockDuration overrides the XREADGROUP block timeout.
// Default 5s; tests should use a shorter value (e.g. 50ms).
func WithBlockDuration(d time.Duration) Option {
	return func(c *Consumer) { c.blockDur = d }
}

// WithHandler sets the function called for every decoded signal event.
func WithHandler(h Handler) Option {
	return func(c *Consumer) { c.handler = h }
}

// WithConsumerID sets the consumer name used in XREADGROUP.
// Default is the group name.
func WithConsumerID(id string) Option {
	return func(c *Consumer) { c.consumerID = id }
}

// Consumer reads signals from {namespace}:signals and dispatches to Handler.
type Consumer struct {
	client     *redis.Client
	streamKey  string
	handler    Handler
	blockDur   time.Duration
	consumerID string
	logger     *zap.Logger

	running atomic.Bool
	wg      sync.WaitGroup
}

// New creates a Consumer that reads from {namespace}:signals.
func New(client *redis.Client, namespace string, logger *zap.Logger, opts ...Option) *Consumer {
	c := &Consumer{
		client:     client,
		streamKey:  namespace + ":signals",
		blockDur:   defaultBlockDur,
		consumerID: groupName,
		logger:     logger.With(zap.String("component", "signal-consumer")),
		handler:    func(_ context.Context, _ string, _ signalsdomain.SignalEvent) error { return nil },
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// IsRunning reports whether the read loop is active.
func (c *Consumer) IsRunning() bool { return c.running.Load() }

// Start creates the consumer group (if absent) and launches the read loop.
func (c *Consumer) Start(ctx context.Context) error {
	err := c.client.XGroupCreateMkStream(ctx, c.streamKey, groupName, "0").Err()
	if err != nil && !isAlreadyExists(err) {
		return fmt.Errorf("create consumer group for %s: %w", c.streamKey, err)
	}
	c.logger.Info("consumer group ready",
		zap.String("stream", c.streamKey),
		zap.String("group", groupName),
	)
	c.wg.Add(1)
	go c.readLoop(ctx)
	return nil
}

// Stop waits for the read loop to exit.
func (c *Consumer) Stop() { c.wg.Wait() }

func (c *Consumer) readLoop(ctx context.Context) {
	defer c.wg.Done()
	c.running.Store(true)
	defer c.running.Store(false)

	c.logger.Info("read loop started",
		zap.String("stream", c.streamKey),
		zap.Duration("block_dur", c.blockDur),
	)
	defer c.logger.Info("read loop stopped")

	for {
		c.drainPEL(ctx)
		if ctx.Err() != nil {
			return
		}
		if !c.readNew(ctx) {
			return
		}
	}
}

// drainPEL reads previously delivered but unACKed messages (Phase 1 of each loop iteration).
func (c *Consumer) drainPEL(ctx context.Context) {
	streams, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: c.consumerID,
		Streams:  []string{c.streamKey, "0"},
		Count:    batchSize,
		Block:    0,
	}).Result()
	if err != nil && !errors.Is(err, redis.Nil) && ctx.Err() == nil {
		c.logger.Error("xreadgroup PEL drain error", zap.Error(err))
		return
	}
	for _, s := range streams {
		for _, msg := range s.Messages {
			c.handleMessage(ctx, msg)
		}
	}
}

// readNew blocks for new messages. Returns false if the context was cancelled.
func (c *Consumer) readNew(ctx context.Context) bool {
	results, err := c.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    groupName,
		Consumer: c.consumerID,
		Streams:  []string{c.streamKey, ">"},
		Count:    batchSize,
		Block:    c.blockDur,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			c.logger.Debug("block timeout; no new signals")
			return true
		}
		if ctx.Err() != nil {
			c.logger.Info("context cancelled; shutting down read loop")
			return false
		}
		c.logger.Error("xreadgroup error", zap.Error(err))
		return true
	}

	for _, s := range results {
		for _, msg := range s.Messages {
			c.handleMessage(ctx, msg)
		}
	}
	return true
}

func (c *Consumer) handleMessage(ctx context.Context, msg redis.XMessage) {
	c.logger.Debug("handling message", zap.String("id", msg.ID))

	sig, err := decodeSignalEvent(msg.Values)
	if err != nil {
		c.logger.Error("decode SignalEvent failed",
			zap.String("id", msg.ID),
			zap.Error(err),
		)
		// ACK malformed messages so they don't clog the PEL.
		c.ack(ctx, msg.ID)
		return
	}

	if err := c.handler(ctx, msg.ID, sig); err != nil {
		c.logger.Error("signal handler error; message left in PEL for retry",
			zap.String("id", msg.ID),
			zap.String("symbol", sig.Symbol),
			zap.Error(err),
		)
		return
	}
	c.ack(ctx, msg.ID)
}

func (c *Consumer) ack(ctx context.Context, id string) {
	if err := c.client.XAck(ctx, c.streamKey, groupName, id).Err(); err != nil {
		c.logger.Warn("xack failed", zap.String("id", id), zap.Error(err))
	}
}

func decodeSignalEvent(values map[string]any) (signalsdomain.SignalEvent, error) {
	var sig signalsdomain.SignalEvent

	sig.RunID, _ = values["run_id"].(string)

	sym, _ := values["symbol"].(string)
	if sym == "" {
		return sig, fmt.Errorf("missing symbol")
	}
	sig.Symbol = sym

	tsStr, _ := values["event_time"].(string)
	if tsStr == "" {
		return sig, fmt.Errorf("missing event_time")
	}
	var err error
	sig.EventTime, err = time.Parse("2006-01-02T15:04:05.999999999Z", tsStr)
	if err != nil {
		// Fallback to RFC3339Nano
		sig.EventTime, err = time.Parse(time.RFC3339Nano, tsStr)
		if err != nil {
			return sig, fmt.Errorf("parse event_time %q: %w", tsStr, err)
		}
	}

	sig.Name, _ = values["name"].(string)
	sig.Version, _ = values["version"].(string)

	valStr, _ := values["value"].(string)
	if valStr == "" {
		return sig, fmt.Errorf("missing value")
	}
	sig.Value, err = decimal.NewFromString(valStr)
	if err != nil {
		return sig, fmt.Errorf("parse value %q: %w", valStr, err)
	}

	if paramsRaw, _ := values["params"].(string); paramsRaw != "" {
		_ = json.Unmarshal([]byte(paramsRaw), &sig.Params)
	}

	return sig, nil
}

func isAlreadyExists(err error) bool {
	return err != nil && strings.Contains(err.Error(), "BUSYGROUP")
}
