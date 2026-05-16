package collector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const (
	readBatchSize        = 100
	defaultBlockDuration = 5 * time.Second
	progressInterval     = 30 * time.Second
)

// TradeWriter persists filled orders and equity snapshots.
// BacktestRepository satisfies this interface.
type TradeWriter interface {
	WriteTrade(ctx context.Context, trade domain.TradeRecord) error
	WriteEquityPoint(ctx context.Context, point domain.EquityPoint) error
}

// Option configures a ResultCollector.
type Option func(*ResultCollector)

// WithBlockDuration overrides the XRead block timeout.
// Default is 5s; tests should use a shorter value (e.g. 50ms).
func WithBlockDuration(d time.Duration) Option {
	return func(c *ResultCollector) { c.blockDur = d }
}

// WithTradeWriter enables order collection: the collector reads the :orders
// stream, persists filled trades, and tracks running equity.
// initialCapital is the starting cash balance for equity calculations.
func WithTradeWriter(tw TradeWriter, initialCapital decimal.Decimal) Option {
	return func(c *ResultCollector) {
		c.tradeWriter = tw
		c.initialCapital = initialCapital
	}
}

// ResultCollector observes the run's signals stream and tracks how many signals
// have been emitted per symbol. When configured with a TradeWriter (via
// WithTradeWriter) it also reads the :orders stream and persists every filled
// OrderEvent as a TradeRecord and EquityPoint.
type ResultCollector struct {
	client         *redis.Client
	namespace      string
	runID          string
	blockDur       time.Duration
	logger         *zap.Logger
	tradeWriter    TradeWriter
	initialCapital decimal.Decimal

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

// Start launches the background read loops. Cancel ctx to stop the collector.
// Call Wait() after cancellation to ensure a clean stop.
func (c *ResultCollector) Start(ctx context.Context) {
	signalsKey := c.namespace + ":signals"
	c.logger.Info("result collector starting",
		zap.String("signals_stream", signalsKey),
		zap.Bool("trade_writer_enabled", c.tradeWriter != nil),
	)

	c.wg.Add(1)
	go c.signalsLoop(ctx, signalsKey)

	if c.tradeWriter != nil {
		ordersKey := c.namespace + ":orders"
		c.logger.Info("orders collection enabled", zap.String("orders_stream", ordersKey))
		c.wg.Add(1)
		go c.ordersLoop(ctx, ordersKey)
	}
}

// Wait blocks until all read loops have exited.
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

// ── signals loop ──────────────────────────────────────────────────────────────

func (c *ResultCollector) signalsLoop(ctx context.Context, streamKey string) {
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
				c.handleSignal(msg)
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

func (c *ResultCollector) handleSignal(msg redis.XMessage) {
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

// ── orders loop ───────────────────────────────────────────────────────────────

func (c *ResultCollector) ordersLoop(ctx context.Context, streamKey string) {
	defer c.wg.Done()

	tracker := newEquityTracker(c.initialCapital)
	startedAt := time.Now()

	c.logger.Info("orders loop started", zap.String("stream", streamKey))

	lastID, tradeCount := c.runOrdersReader(ctx, streamKey, tracker)
	tradeCount += c.drainOrders(streamKey, tracker, lastID)

	c.logger.Info("orders loop drained and stopped",
		zap.Int("trades_persisted", tradeCount),
		zap.Duration("elapsed", time.Since(startedAt)),
	)
}

// runOrdersReader blocks on the orders stream until ctx is cancelled, then returns.
func (c *ResultCollector) runOrdersReader(ctx context.Context, streamKey string, tracker *equityTracker) (string, int) {
	lastID := "0-0"
	var tradeCount int
	for {
		results, err := c.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamKey, lastID},
			Count:   readBatchSize,
			Block:   c.blockDur,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				if ctx.Err() != nil {
					return lastID, tradeCount
				}
				continue
			}
			if ctx.Err() != nil {
				return lastID, tradeCount
			}
			c.logger.Error("xread error on orders stream",
				zap.String("stream", streamKey),
				zap.String("last_id", lastID),
				zap.Error(err),
			)
			continue
		}
		var n int
		lastID, n = c.processOrderBatch(results, tracker, lastID)
		tradeCount += n
	}
}

func (c *ResultCollector) processOrderBatch(results []redis.XStream, tracker *equityTracker, lastID string) (string, int) {
	ctx := context.Background()
	var count int
	for _, stream := range results {
		for _, msg := range stream.Messages {
			if c.handleOrder(ctx, tracker, msg) {
				count++
			}
			lastID = msg.ID
		}
	}
	return lastID, count
}

// drainOrders reads any remaining messages non-blocking via XRANGE after ctx cancellation.
// XRANGE is always non-blocking; Block:0 in XRead means "wait forever" in the Redis protocol.
func (c *ResultCollector) drainOrders(streamKey string, tracker *equityTracker, lastID string) int {
	drainStart := "(" + lastID
	if lastID == "0-0" {
		drainStart = "-"
	}
	ctx := context.Background()
	var count int
	for {
		msgs, err := c.client.XRangeN(ctx, streamKey, drainStart, "+", readBatchSize).Result()
		if err != nil || len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			if c.handleOrder(ctx, tracker, msg) {
				count++
			}
			lastID = msg.ID
		}
		drainStart = "(" + lastID
	}
	return count
}

// handleOrder decodes one OrderEvent and persists a TradeRecord + EquityPoint
// if the status is Filled. Returns true when a trade was persisted.
func (c *ResultCollector) handleOrder(ctx context.Context, tracker *equityTracker, msg redis.XMessage) bool {
	ev, err := decodeOrderEvent(msg.Values)
	if err != nil {
		c.logger.Error("decode order event failed",
			zap.String("msg_id", msg.ID),
			zap.Error(err),
		)
		return false
	}

	if ev.Status != domain.OrderStatusFilled {
		c.logger.Debug("order not filled; skipping",
			zap.String("order_id", ev.OrderID),
			zap.String("status", string(ev.Status)),
		)
		return false
	}

	trade := domain.TradeRecord{
		RunID:      ev.RunID,
		TradeID:    ev.OrderID,
		Symbol:     ev.Symbol,
		EventTime:  ev.EventTime,
		Side:       ev.Side,
		FillPrice:  ev.FillPrice,
		FillQty:    ev.FillQty,
		Fee:        ev.Fee,
		SignalName: ev.SignalName,
	}

	if err := c.tradeWriter.WriteTrade(ctx, trade); err != nil {
		c.logger.Error("write trade failed",
			zap.String("trade_id", trade.TradeID),
			zap.Error(err),
		)
	}

	equity := tracker.applyFill(ev.Symbol, ev.Side, ev.FillQty, ev.FillPrice, ev.Fee)
	ep := domain.EquityPoint{
		RunID:     ev.RunID,
		EventTime: ev.EventTime,
		Equity:    equity,
	}

	if err := c.tradeWriter.WriteEquityPoint(ctx, ep); err != nil {
		c.logger.Error("write equity point failed",
			zap.String("run_id", ev.RunID),
			zap.Error(err),
		)
	}

	c.logger.Info("trade persisted",
		zap.String("trade_id", trade.TradeID),
		zap.String("symbol", trade.Symbol),
		zap.String("side", string(trade.Side)),
		zap.String("fill_price", trade.FillPrice.String()),
		zap.String("equity", equity.String()),
	)
	return true
}

// ── order event decoding ──────────────────────────────────────────────────────

func decodeOrderEvent(values map[string]any) (domain.OrderEvent, error) {
	var ev domain.OrderEvent

	ev.RunID = stringVal(values, "run_id")
	ev.Symbol = stringVal(values, "symbol")
	ev.OrderID = stringVal(values, "order_id")
	ev.SignalName = stringVal(values, "signal_name")
	ev.Side = domain.Side(stringVal(values, "side"))
	ev.Type = domain.OrderType(stringVal(values, "type"))
	ev.Status = domain.OrderStatus(stringVal(values, "status"))

	tsStr := stringVal(values, "event_time")
	if tsStr == "" {
		return ev, fmt.Errorf("missing event_time in order event")
	}
	var err error
	ev.EventTime, err = time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		return ev, fmt.Errorf("parse event_time %q: %w", tsStr, err)
	}

	if ev.Price, err = decimalVal(values, "price"); err != nil {
		return ev, fmt.Errorf("parse price: %w", err)
	}
	if ev.Quantity, err = decimalVal(values, "quantity"); err != nil {
		return ev, fmt.Errorf("parse quantity: %w", err)
	}
	if ev.FillPrice, err = decimalVal(values, "fill_price"); err != nil {
		return ev, fmt.Errorf("parse fill_price: %w", err)
	}
	if ev.FillQty, err = decimalVal(values, "fill_qty"); err != nil {
		return ev, fmt.Errorf("parse fill_qty: %w", err)
	}
	if ev.Fee, err = decimalVal(values, "fee"); err != nil {
		return ev, fmt.Errorf("parse fee: %w", err)
	}

	return ev, nil
}

// ── equity tracking ───────────────────────────────────────────────────────────

// equityTracker maintains a running cash balance and open positions to compute
// portfolio equity after each fill.
type equityTracker struct {
	cash          decimal.Decimal
	positions     map[string]decimal.Decimal // symbol → net qty (positive=long, negative=short)
	lastFillPrice map[string]decimal.Decimal // symbol → most recent fill price
}

func newEquityTracker(initialCapital decimal.Decimal) *equityTracker {
	return &equityTracker{
		cash:          initialCapital,
		positions:     make(map[string]decimal.Decimal),
		lastFillPrice: make(map[string]decimal.Decimal),
	}
}

// applyFill updates cash and positions, then returns the new portfolio equity.
// equity = cash + Σ(position[sym] × lastFillPrice[sym])
func (t *equityTracker) applyFill(symbol string, side domain.Side, qty, price, fee decimal.Decimal) decimal.Decimal {
	t.lastFillPrice[symbol] = price

	if side == domain.SideBuy {
		t.cash = t.cash.Sub(qty.Mul(price)).Sub(fee)
		t.positions[symbol] = t.positions[symbol].Add(qty)
	} else {
		t.cash = t.cash.Add(qty.Mul(price)).Sub(fee)
		t.positions[symbol] = t.positions[symbol].Sub(qty)
	}

	equity := t.cash
	for sym, pos := range t.positions {
		if !pos.IsZero() {
			if mark, ok := t.lastFillPrice[sym]; ok {
				equity = equity.Add(pos.Mul(mark))
			}
		}
	}
	return equity
}

// ── helpers ───────────────────────────────────────────────────────────────────

func stringVal(values map[string]any, key string) string {
	v, _ := values[key].(string)
	return v
}

func decimalVal(values map[string]any, key string) (decimal.Decimal, error) {
	s := stringVal(values, key)
	if s == "" {
		return decimal.Zero, nil
	}
	d, err := decimal.NewFromString(s)
	if err != nil {
		return decimal.Zero, fmt.Errorf("parse %q as decimal: %w", s, err)
	}
	return d, nil
}
