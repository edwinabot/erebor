// Package l2cache maintains an in-memory snapshot of the best bid/ask for each
// watched symbol, updated by tailing the L2 streams produced by erebor-ingest.
package l2cache

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const defaultBlockDur = 5 * time.Second

// Option configures a Cache.
type Option func(*Cache)

// WithBlockDuration overrides the XREAD block timeout.
// Tests should use a short value (e.g. 50ms) to keep the suite fast.
func WithBlockDuration(d time.Duration) Option {
	return func(c *Cache) { c.blockDur = d }
}

type bestPrice struct {
	bid, ask decimal.Decimal
}

// Cache tails the L2 streams and exposes the latest best bid/ask per symbol.
// Stream key: {namespace}:l2:{SYMBOL}
type Cache struct {
	client    *redis.Client
	namespace string
	symbols   []string
	blockDur  time.Duration
	logger    *zap.Logger

	mu     sync.RWMutex
	prices map[string]bestPrice // protected by mu; written only by readLoop

	// ids tracks the last-read stream ID per symbol; accessed only by readLoop.
	ids map[string]string
}

// New creates a Cache for the given symbols under namespace.
func New(client *redis.Client, namespace string, symbols []string, logger *zap.Logger, opts ...Option) *Cache {
	c := &Cache{
		client:    client,
		namespace: namespace,
		symbols:   symbols,
		blockDur:  defaultBlockDur,
		logger:    logger.With(zap.String("component", "l2cache")),
		prices:    make(map[string]bestPrice),
		ids:       make(map[string]string),
	}
	for _, o := range opts {
		o(c)
	}
	for _, sym := range symbols {
		c.ids[strings.ToUpper(sym)] = "0"
	}
	return c
}

// Start launches the background goroutine that tails the L2 streams.
func (c *Cache) Start(ctx context.Context) {
	c.logger.Info("l2cache starting", zap.Strings("symbols", c.symbols))
	go c.readLoop(ctx)
}

// BestPrices returns the latest best bid and ask for symbol.
// Returns (zero, zero, false) if no data has been received yet.
func (c *Cache) BestPrices(symbol string) (decimal.Decimal, decimal.Decimal, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	p, ok := c.prices[strings.ToUpper(symbol)]
	if !ok {
		return decimal.Zero, decimal.Zero, false
	}
	return p.bid, p.ask, true
}

func (c *Cache) streamKey(symbol string) string {
	return c.namespace + ":l2:" + strings.ToUpper(symbol)
}

func (c *Cache) readLoop(ctx context.Context) {
	defer c.logger.Info("l2cache read loop stopped")
	for {
		if !c.readOnce(ctx) {
			return
		}
	}
}

func (c *Cache) readOnce(ctx context.Context) bool {
	streams := c.buildStreamArgs()
	results, err := c.client.XRead(ctx, &redis.XReadArgs{
		Streams: streams,
		Count:   100,
		Block:   c.blockDur,
	}).Result()

	if err != nil {
		if errors.Is(err, redis.Nil) {
			c.logger.Debug("block timeout; no new L2 messages")
			return true
		}
		if ctx.Err() != nil {
			c.logger.Info("context cancelled; shutting down l2cache read loop")
			return false
		}
		c.logger.Error("xread error", zap.Error(err))
		return true
	}

	for _, stream := range results {
		for _, msg := range stream.Messages {
			c.handleMessage(msg)
		}
	}
	return true
}

func (c *Cache) buildStreamArgs() []string {
	streams := make([]string, len(c.symbols)*2)
	for i, sym := range c.symbols {
		streams[i] = c.streamKey(sym)
		streams[len(c.symbols)+i] = c.ids[strings.ToUpper(sym)]
	}
	return streams
}

func (c *Cache) handleMessage(msg redis.XMessage) {
	sym, _ := msg.Values["symbol"].(string)
	if sym == "" {
		c.logger.Warn("l2cache message missing symbol field", zap.String("id", msg.ID))
		return
	}
	sym = strings.ToUpper(sym)

	bidsRaw, _ := msg.Values["bids"].(string)
	asksRaw, _ := msg.Values["asks"].(string)

	bid, err := bestPriceFromJSON(bidsRaw)
	if err != nil {
		c.logger.Warn("l2cache failed to parse bids", zap.String("symbol", sym), zap.Error(err))
		return
	}
	ask, err := bestPriceFromJSON(asksRaw)
	if err != nil {
		c.logger.Warn("l2cache failed to parse asks", zap.String("symbol", sym), zap.Error(err))
		return
	}
	if bid.IsZero() || ask.IsZero() {
		return
	}

	c.mu.Lock()
	c.prices[sym] = bestPrice{bid: bid, ask: ask}
	c.mu.Unlock()

	c.ids[sym] = msg.ID

	c.logger.Debug("l2cache updated",
		zap.String("symbol", sym),
		zap.String("bid", bid.String()),
		zap.String("ask", ask.String()),
		zap.String("msg_id", msg.ID),
	)
}

// bestPriceFromJSON parses a [][2]string JSON array and returns the price of the first level.
func bestPriceFromJSON(raw string) (decimal.Decimal, error) {
	if raw == "" {
		return decimal.Zero, nil
	}
	var pairs [][2]string
	if err := json.Unmarshal([]byte(raw), &pairs); err != nil {
		return decimal.Zero, err
	}
	if len(pairs) == 0 {
		return decimal.Zero, nil
	}
	return decimal.NewFromString(pairs[0][0])
}
