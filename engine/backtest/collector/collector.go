package collector

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	readBatchSize        = 100
	defaultBlockDuration = 5 * time.Second
	progressInterval     = 30 * time.Second
)

// Option configures a ResultCollector.
type Option func(*ResultCollector)

// WithBlockDuration overrides the XRead block timeout.
// Default is 5s; tests should use a shorter value (e.g. 50ms).
func WithBlockDuration(d time.Duration) Option {
	return func(c *ResultCollector) { c.blockDur = d }
}

// ResultCollector observes the run's signals stream and tracks how many signals
// have been emitted per symbol. It does not use a consumer group — it is a
// read-only observer that does not ack or modify the stream.
//
// When erebor-execution ships, this will be extended to also collect
// OrderEvents from the orders stream and write backtest_trades / backtest_equity.
type ResultCollector struct {
	client    *redis.Client
	namespace string
	runID     string
	blockDur  time.Duration
	logger    *zap.Logger

	mu           sync.Mutex
	signalCounts map[string]int // symbol → count of signals received

	totalSignals atomic.Int64
	wg           sync.WaitGroup
}

// New creates a ResultCollector for the given run.
func New(client *redis.Client, namespace, runID string, logger *zap.Logger, opts ...Option) *ResultCollector {
	c := &ResultCollector{
		client:       client,
		namespace:    namespace,
		runID:        runID,
		blockDur:     defaultBlockDuration,
		signalCounts: make(map[string]int),
		logger:       logger.With(zap.String("component", "result-collector"), zap.String("run_id", runID)),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Start launches the background read loop. Cancel ctx to stop the collector.
// Call Wait() after cancellation to ensure a clean stop.
func (c *ResultCollector) Start(ctx context.Context) {
	streamKey := c.namespace + ":signals"
	c.logger.Info("result collector starting",
		zap.String("stream", streamKey),
	)

	c.wg.Add(1)
	go c.readLoop(ctx, streamKey)
}

// Wait blocks until the read loop has exited.
func (c *ResultCollector) Wait() {
	c.wg.Wait()
}

// SignalCounts returns a snapshot of received signal counts per symbol.
func (c *ResultCollector) SignalCounts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]int, len(c.signalCounts))
	for k, v := range c.signalCounts {
		out[k] = v
	}
	return out
}

// TotalSignals returns the total number of signals received across all symbols.
func (c *ResultCollector) TotalSignals() int64 {
	return c.totalSignals.Load()
}

func (c *ResultCollector) readLoop(ctx context.Context, streamKey string) {
	defer c.wg.Done()

	lastID := "0-0"
	progressTicker := time.NewTicker(progressInterval)
	defer progressTicker.Stop()

	startedAt := time.Now()

	for {
		select {
		case <-progressTicker.C:
			c.logger.Info("collector progress",
				zap.Int64("total_signals", c.totalSignals.Load()),
				zap.Any("per_symbol", c.SignalCounts()),
				zap.Duration("elapsed", time.Since(startedAt)),
			)
		default:
		}

		results, err := c.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamKey, lastID},
			Count:   readBatchSize,
			Block:   c.blockDur,
		}).Result()

		if err != nil {
			if errors.Is(err, redis.Nil) {
				continue // block timeout, no new messages
			}
			if ctx.Err() != nil {
				break // context cancelled — clean exit
			}
			c.logger.Error("xread error on signals stream",
				zap.String("stream", streamKey),
				zap.String("last_id", lastID),
				zap.Error(err),
			)
			continue
		}

		for _, stream := range results {
			for _, msg := range stream.Messages {
				c.handleMessage(msg)
				lastID = msg.ID
			}
		}
	}

	total := c.totalSignals.Load()
	c.logger.Info("result collector stopped",
		zap.Int64("total_signals", total),
		zap.Any("per_symbol", c.SignalCounts()),
		zap.Duration("elapsed", time.Since(startedAt)),
	)
}

func (c *ResultCollector) handleMessage(msg redis.XMessage) {
	symbol := strings.ToUpper(stringVal(msg.Values, "symbol"))
	name := stringVal(msg.Values, "name")
	eventTime := stringVal(msg.Values, "event_time")

	c.mu.Lock()
	c.signalCounts[symbol]++
	c.mu.Unlock()

	c.totalSignals.Add(1)

	c.logger.Debug("signal received",
		zap.String("symbol", symbol),
		zap.String("name", name),
		zap.String("event_time", eventTime),
		zap.String("msg_id", msg.ID),
	)
}

func stringVal(values map[string]any, key string) string {
	v, _ := values[key].(string)
	return v
}
