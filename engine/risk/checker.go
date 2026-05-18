// Package risk provides pre-trade risk checks and post-fill state tracking
// for erebor backtest runs. It is imported by erebor-execution (the Executor).
package risk

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// Config holds all risk limits for a single run.
// Zero-value fields mean "no limit applies".
type Config struct {
	InitialCapital  decimal.Decimal            // starting equity; required when drawdown/loss rules are active
	MaxPositionQty  map[string]decimal.Decimal // symbol → max abs qty; absent key = unlimited
	MaxDrawdownPct  decimal.Decimal            // e.g. 5 → halt when equity < peak × 0.95; zero = disabled
	RunLossLimitPct decimal.Decimal            // e.g. 10 → halt when equity < initial × 0.90; zero = disabled
}

// EventType identifies the risk condition that was triggered.
type EventType string

const (
	// EventPositionLimit is published when a proposed order would exceed the configured position limit.
	EventPositionLimit EventType = "POSITION_LIMIT"
	// EventDrawdownHalt is published when equity falls below peak*(1-MaxDrawdownPct/100).
	EventDrawdownHalt EventType = "DRAWDOWN_HALT"
	// EventRunLossHalt is published when equity falls below initial*(1-RunLossLimitPct/100).
	EventRunLossHalt EventType = "RUN_LOSS_HALT"
)

// Event is published to {namespace}:risk for observability.
type Event struct {
	RunID     string
	Symbol    string    // empty for global (drawdown / run-loss) events
	EventTime time.Time // propagated from the L2 event that triggered the check
	Type      EventType
	Detail    string          // e.g. "equity 9400 < peak 10000 × 0.95 = 9500"
	Equity    decimal.Decimal // portfolio equity at the time of the event
}

// Publisher is the dependency for emitting risk events.
type Publisher interface {
	Publish(ctx context.Context, namespace string, event Event) error
}

// CheckerOption configures a Checker.
type CheckerOption func(*Checker)

// WithHaltStore attaches a persistent halt store to the checker.
// When a halt is triggered the store is called so it survives process restarts.
// On startup the checker probes the store on the first CanTrade call.
func WithHaltStore(store HaltStore) CheckerOption {
	return func(c *Checker) { c.haltStore = store }
}

// Checker provides pre-trade risk checks and post-fill state tracking.
// All methods are safe for concurrent use.
type Checker struct {
	cfg        Config
	pub        Publisher
	haltStore  HaltStore // optional; nil = no persistence
	logger     *zap.Logger
	namespace  string
	runID      string
	mu         sync.Mutex
	positions  map[string]decimal.Decimal
	equity     decimal.Decimal
	peakEquity decimal.Decimal
	halted       bool
	haltReason   string
	haltChecked  bool // true after the first haltStore.IsHalted probe
}

// New creates a Checker with the given config.
// pub is called fire-and-forget on each risk event; pass a NoopPublisher in tests.
func New(cfg Config, pub Publisher) *Checker {
	logger := zap.NewNop()
	equity := cfg.InitialCapital
	if equity.IsZero() {
		equity = decimal.Zero
	}

	c := &Checker{
		cfg:        cfg,
		pub:        pub,
		logger:     logger,
		positions:  make(map[string]decimal.Decimal),
		equity:     equity,
		peakEquity: equity,
	}

	logger.Info("risk checker constructed",
		zap.String("initial_capital", cfg.InitialCapital.String()),
		zap.String("max_drawdown_pct", cfg.MaxDrawdownPct.String()),
		zap.String("run_loss_limit_pct", cfg.RunLossLimitPct.String()),
		zap.Int("position_limit_symbols", len(cfg.MaxPositionQty)),
	)

	return c
}

// NewWithLogger creates a Checker with a provided zap.Logger, namespace, and run ID.
// This constructor is used for production wiring where log context is available.
func NewWithLogger(cfg Config, pub Publisher, logger *zap.Logger, namespace, runID string, opts ...CheckerOption) *Checker {
	equity := cfg.InitialCapital
	if equity.IsZero() {
		equity = decimal.Zero
	}

	c := &Checker{
		cfg:        cfg,
		pub:        pub,
		logger:     logger.With(zap.String("component", "risk-checker")),
		namespace:  namespace,
		runID:      runID,
		positions:  make(map[string]decimal.Decimal),
		equity:     equity,
		peakEquity: equity,
	}
	for _, o := range opts {
		o(c)
	}

	c.logger.Info("risk checker constructed",
		zap.String("initial_capital", cfg.InitialCapital.String()),
		zap.String("max_drawdown_pct", cfg.MaxDrawdownPct.String()),
		zap.String("run_loss_limit_pct", cfg.RunLossLimitPct.String()),
		zap.Int("position_limit_symbols", len(cfg.MaxPositionQty)),
	)

	return c
}

// Halted reports whether a global halt is in effect.
func (c *Checker) Halted() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.halted
}

// CanTrade returns nil if the proposed order passes all active risk rules,
// or a descriptive error if any rule is violated.
// eventTime is propagated from the L2 event that triggered the trade decision.
//
// Evaluation order (per spec §5):
//  1. If halted → return immediately with halt reason.
//  2. Check max drawdown (if MaxDrawdownPct > 0).
//  3. Check run loss limit (if RunLossLimitPct > 0).
//  4. Check position limit for symbol (if MaxPositionQty[symbol] is configured).
func (c *Checker) CanTrade(symbol string, side domain.Side, qty decimal.Decimal, eventTime time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1. Short-circuit if already halted (in-memory).
	// On the first CanTrade call, probe haltStore to detect a halt that survived a restart.
	if !c.halted && !c.haltChecked && c.haltStore != nil {
		c.haltChecked = true
		if persisted, err := c.haltStore.IsHalted(context.Background(), c.runID); err != nil {
			c.logger.Warn("haltStore.IsHalted failed (ignoring)", zap.Error(err))
		} else if persisted {
			c.halted = true
			c.haltReason = "persisted halt detected"
		}
	}
	if c.halted {
		c.logger.Warn("trade blocked: checker is halted",
			zap.String("symbol", symbol),
			zap.String("side", string(side)),
			zap.String("halt_reason", c.haltReason),
		)
		return fmt.Errorf("%s: %s", c.haltReason, "checker is halted")
	}

	// 2. Max drawdown check.
	if c.cfg.MaxDrawdownPct.IsPositive() {
		threshold := c.peakEquity.Mul(decimal.NewFromInt(1).Sub(c.cfg.MaxDrawdownPct.Div(decimal.NewFromInt(100))))
		if c.equity.LessThan(threshold) {
			detail := fmt.Sprintf("equity %s < peak %s × (1 - %s/100) = %s",
				c.equity.String(), c.peakEquity.String(), c.cfg.MaxDrawdownPct.String(), threshold.String())
			c.logger.Warn("drawdown halt triggered",
				zap.String("equity", c.equity.String()),
				zap.String("peak_equity", c.peakEquity.String()),
				zap.String("threshold", threshold.String()),
				zap.String("detail", detail),
			)
			c.halted = true
			c.haltReason = string(EventDrawdownHalt)
			c.persistHalt(context.Background())
			c.publishEvent(context.Background(), Event{
				RunID:     c.runID,
				Symbol:    "",
				EventTime: eventTime,
				Type:      EventDrawdownHalt,
				Detail:    detail,
				Equity:    c.equity,
			})
			return fmt.Errorf("DRAWDOWN_HALT: %s", detail)
		}
	}

	// 3. Run loss limit check.
	if c.cfg.RunLossLimitPct.IsPositive() {
		threshold := c.cfg.InitialCapital.Mul(decimal.NewFromInt(1).Sub(c.cfg.RunLossLimitPct.Div(decimal.NewFromInt(100))))
		if c.equity.LessThan(threshold) {
			detail := fmt.Sprintf("equity %s < initial %s × (1 - %s/100) = %s",
				c.equity.String(), c.cfg.InitialCapital.String(), c.cfg.RunLossLimitPct.String(), threshold.String())
			c.logger.Warn("run loss halt triggered",
				zap.String("equity", c.equity.String()),
				zap.String("initial_capital", c.cfg.InitialCapital.String()),
				zap.String("threshold", threshold.String()),
				zap.String("detail", detail),
			)
			c.halted = true
			c.haltReason = string(EventRunLossHalt)
			c.persistHalt(context.Background())
			c.publishEvent(context.Background(), Event{
				RunID:     c.runID,
				Symbol:    "",
				EventTime: eventTime,
				Type:      EventRunLossHalt,
				Detail:    detail,
				Equity:    c.equity,
			})
			return fmt.Errorf("RUN_LOSS_HALT: %s", detail)
		}
	}

	// 4. Position limit check.
	if maxQty, ok := c.cfg.MaxPositionQty[symbol]; ok {
		currentPos := c.positions[symbol]
		var proposedDelta decimal.Decimal
		if side == domain.SideBuy {
			proposedDelta = qty
		} else {
			proposedDelta = qty.Neg()
		}
		newQty := currentPos.Add(proposedDelta)
		if newQty.Abs().GreaterThan(maxQty) {
			detail := fmt.Sprintf("abs(%s + %s = %s) > max %s",
				currentPos.String(), proposedDelta.String(), newQty.String(), maxQty.String())
			c.logger.Warn("trade blocked by position limit",
				zap.String("symbol", symbol),
				zap.String("side", string(side)),
				zap.String("current_position", currentPos.String()),
				zap.String("proposed_qty", qty.String()),
				zap.String("new_position", newQty.String()),
				zap.String("max_qty", maxQty.String()),
			)
			c.publishEvent(context.Background(), Event{
				RunID:     c.runID,
				Symbol:    symbol,
				EventTime: eventTime,
				Type:      EventPositionLimit,
				Detail:    detail,
				Equity:    c.equity,
			})
			return fmt.Errorf("POSITION_LIMIT: %s", detail)
		}
	}

	return nil
}

// RecordFill updates position and equity state after a confirmed fill.
// Must be called for every published OrderEvent.
//
// State updates:
//   - positions[symbol] ← positions[symbol] ± qty
//   - equity ← equity − (qty × fillPrice) − fee for buy
//   - equity ← equity + (qty × fillPrice) − fee for sell
//   - peak_equity ← max(peak_equity, equity)
func (c *Checker) RecordFill(symbol string, side domain.Side, qty, fillPrice, fee decimal.Decimal) {
	c.mu.Lock()
	defer c.mu.Unlock()

	prevPos := c.positions[symbol]
	prevEquity := c.equity

	tradeValue := qty.Mul(fillPrice)
	if side == domain.SideBuy {
		c.positions[symbol] = prevPos.Add(qty)
		c.equity = c.equity.Sub(tradeValue).Sub(fee)
	} else {
		c.positions[symbol] = prevPos.Sub(qty)
		c.equity = c.equity.Add(tradeValue).Sub(fee)
	}

	// Update peak equity.
	if c.equity.GreaterThan(c.peakEquity) {
		c.peakEquity = c.equity
	}

	c.logger.Debug("fill recorded",
		zap.String("symbol", symbol),
		zap.String("side", string(side)),
		zap.String("qty", qty.String()),
		zap.String("fill_price", fillPrice.String()),
		zap.String("fee", fee.String()),
		zap.String("prev_position", prevPos.String()),
		zap.String("new_position", c.positions[symbol].String()),
		zap.String("prev_equity", prevEquity.String()),
		zap.String("new_equity", c.equity.String()),
		zap.String("peak_equity", c.peakEquity.String()),
	)
}

// persistHalt calls haltStore.SetHalted fire-and-forget. Errors are logged.
// Must only be called while c.mu is held.
func (c *Checker) persistHalt(ctx context.Context) {
	if c.haltStore == nil {
		return
	}
	if err := c.haltStore.SetHalted(ctx, c.runID); err != nil {
		c.logger.Error("failed to persist halt state", zap.String("run_id", c.runID), zap.Error(err))
	}
}

// publishEvent calls pub.Publish fire-and-forget. Errors are logged but not returned
// because publishing is on the observability path, not the critical path.
func (c *Checker) publishEvent(ctx context.Context, evt Event) {
	if err := c.pub.Publish(ctx, c.namespace, evt); err != nil {
		c.logger.Error("failed to publish risk event",
			zap.String("type", string(evt.Type)),
			zap.String("symbol", evt.Symbol),
			zap.Error(err),
		)
		return
	}
	c.logger.Info("risk event published",
		zap.String("type", string(evt.Type)),
		zap.String("symbol", evt.Symbol),
		zap.String("detail", evt.Detail),
	)
}
