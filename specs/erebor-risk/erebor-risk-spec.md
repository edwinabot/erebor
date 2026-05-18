# erebor-risk — Specification

**Status:** Draft  
**Date:** 2026-05  
**Component:** `erebor-risk` (library module)  
**Location:** `engine/risk/`  
**Depends on:** backtest-replay spec (stream/domain contract), erebor-signals spec (SignalEvent contract — v2 only)

---

## 1. Decisions

| Concern | Decision | Rationale |
|---|---|---|
| Module type | Library (`engine/risk/`), no binary in v1 | Risk checks are in the executor's hot path — a network hop or Redis roundtrip per order would be prohibitive, especially in AFAP backtest mode |
| Integration point | Executor calls `Checker.CanTrade()` before publishing each order | Synchronous gate, co-located with the trading decision; no stream dependency |
| State model | In-memory, per-run instance | Backtest runs are bounded and single-process; persistent state is a live-trading concern (deferred) |
| State updates | Executor calls `Checker.RecordFill()` after each confirmed fill | Avoids a second Redis subscriber; risk state is always consistent with what execution has done |
| Observability | `RiskEvent` published to `{namespace}:risk` via an injected `Publisher` interface | Decoupled from the critical path; a no-op publisher is used in tests |
| Thread safety | `Checker` is mutex-protected | Multiple symbol goroutines in the executor share one `Checker` instance; drawdown is a cross-symbol (global equity) check |
| Config location | New fields on the existing `strategy_config` JSON | Keeps run configuration in one place; absent fields default to "no limit" (permissive) |

---

## 2. Overview

`erebor-risk` provides pre-trade risk checks and post-fill state tracking. It is imported by `erebor-execution` (the `Executor` in `engine/backtest/execution/`). Before publishing each order, the executor calls `Checker.CanTrade()`. After a fill is confirmed, it calls `Checker.RecordFill()`.

```
Executor.handleL2()
    │
    ├─ tradeDecision(imbalance, pos)       ← existing signal-based gate
    │
    ├─ risk.CanTrade(symbol, side, qty, eventTime)   ← NEW: risk gate
    │      returns nil (allowed) or error (blocked)
    │
    ├─ publishOrder(order)
    │
    └─ risk.RecordFill(symbol, side, qty, fillPrice, fee)   ← NEW: state update
           └─ fire-and-forget: riskPublisher.Publish(RiskEvent)
```

`erebor-risk` does **not** read from any Redis Stream. All state transitions flow through `RecordFill()` callbacks from the executor.

---

## 3. Risk Rules (v1)

### 3.1 Position Limit (per symbol)

```
abs(positions[symbol] + proposed_delta) > MaxPositionQty[symbol]  →  block
```

`proposed_delta` is `+qty` for a buy, `−qty` for a sell.

If `MaxPositionQty` is not configured for a symbol, the check is skipped for that symbol.

Prevents the executor from accumulating unbounded directional exposure. The current executor's toggle logic (flat ↔ long ↔ short with a fixed `TradeQty`) already limits depth to one lot per symbol, but the position limit provides an explicit, configurable hard ceiling for when multi-lot strategies are introduced.

### 3.2 Max Drawdown (global)

```
equity < peak_equity × (1 − MaxDrawdownPct / 100)  →  global halt
```

`peak_equity` is the running maximum equity since the run started. Once triggered, `CanTrade()` returns an error for **all symbols** until the run ends. No auto-recovery.

`equity` is computed from `InitialCapital` plus cumulative realised P&L observed through `RecordFill()`.

### 3.3 Run Loss Limit (global)

```
equity < InitialCapital × (1 − RunLossLimitPct / 100)  →  global halt
```

Caps the absolute loss from the starting capital over the full run. Complements max drawdown: drawdown is relative to the running peak; the run loss limit is relative to the fixed starting point.

Both 3.2 and 3.3 are checked on every `CanTrade()` call. The first one to trigger wins.

---

## 4. Domain Types

```go
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
    EventPositionLimit EventType = "POSITION_LIMIT"
    EventDrawdownHalt  EventType = "DRAWDOWN_HALT"
    EventRunLossHalt   EventType = "RUN_LOSS_HALT"
)

// Event is published to {namespace}:risk for observability.
type Event struct {
    RunID     string
    Symbol    string          // empty for global (drawdown / run-loss) events
    EventTime time.Time       // propagated from the L2 event that triggered the check
    Type      EventType
    Detail    string          // e.g. "equity 9400 < peak 10000 × 0.95 = 9500"
    Equity    decimal.Decimal // portfolio equity at the time of the event
}
```

**Redis Stream field encoding for `Event`:**

| Field | Stream key | Encoding |
|---|---|---|
| `RunID` | `run_id` | string |
| `Symbol` | `symbol` | string (empty string for global events) |
| `EventTime` | `event_time` | RFC3339Nano |
| `Type` | `type` | string |
| `Detail` | `detail` | string |
| `Equity` | `equity` | decimal string |

---

## 5. Checker API

```go
// New creates a Checker with the given config.
// pub is called fire-and-forget on each risk event; pass a no-op in tests.
func New(cfg Config, pub Publisher) *Checker

// CanTrade returns nil if the proposed order passes all active risk rules,
// or a descriptive error if any rule is violated.
// eventTime is propagated from the L2 event that triggered the trade decision.
func (c *Checker) CanTrade(symbol string, side domain.Side, qty decimal.Decimal, eventTime time.Time) error

// RecordFill updates position and equity state after a confirmed fill.
// Must be called for every published OrderEvent.
func (c *Checker) RecordFill(symbol string, side domain.Side, qty, fillPrice, fee decimal.Decimal)

// Halted reports whether a global halt is in effect.
// Executors may short-circuit the full CanTrade call when already known-halted.
func (c *Checker) Halted() bool

// Publisher is the dependency for emitting risk events.
type Publisher interface {
    Publish(ctx context.Context, namespace string, event Event) error
}
```

`CanTrade` evaluation order:
1. If `c.Halted()` — return immediately with the halt reason.
2. Check max drawdown (if `MaxDrawdownPct > 0`).
3. Check run loss limit (if `RunLossLimitPct > 0`).
4. Check position limit for `symbol` (if `MaxPositionQty[symbol]` is configured).

A halt from rules 2 or 3 sets `c.halted = true`, so subsequent calls return early.

---

## 6. Integration with Executor

Changes to `engine/backtest/execution/executor.go`:

**Field added to `Executor`:**
```go
type Executor struct {
    // ... existing fields unchanged ...
    risk      *risk.Checker
    riskNS    string // namespace for risk event publishing
}
```

**`NewExecutor` updated** to accept `*risk.Checker`:
```go
func NewExecutor(
    client    *redis.Client,
    namespace string,
    symbols   []string,
    cfg       StrategyConfig,
    riskChk   *risk.Checker,
    logger    *zap.Logger,
    opts      ...Option,
) *Executor
```

**`handleL2` updated:**
```go
func (e *Executor) handleL2(ctx context.Context, symbol string, pos *positionState, msg redis.XMessage) {
    ev, err := decodeL2Event(msg.Values)
    // ... existing decode / imbalance / tradeDecision logic unchanged ...

    side, shouldTrade := e.tradeDecision(imbalance, pos)
    if !shouldTrade {
        return
    }

    // risk gate (new)
    if err := e.risk.CanTrade(symbol, side, e.cfg.TradeQty, ev.EventTime); err != nil {
        e.logger.Warn("trade blocked by risk check",
            zap.String("symbol", symbol),
            zap.String("side", string(side)),
            zap.Error(err),
        )
        return
    }

    fillPrice, err := e.computeFillPrice(side, ev.bids, ev.asks)
    // ... existing fill / order publish logic unchanged ...

    // post-fill state update (new)
    e.risk.RecordFill(symbol, order.Side, order.FillQty, order.FillPrice, order.Fee)
}
```

`positionState` in the executor is unchanged — it drives the directional toggle logic. `Checker` maintains its own independent position map for risk purposes.

`BacktestRunner.Run()` constructs `risk.Checker` from `stratCfg` and passes it to `NewExecutor()`:
```go
riskCfg := risk.Config{
    InitialCapital:  stratCfg.InitialCapital,
    MaxPositionQty:  stratCfg.MaxPositionQty,
    MaxDrawdownPct:  stratCfg.MaxDrawdownPct,
    RunLossLimitPct: stratCfg.RunLossLimitPct,
}
riskPub := risk.NewRedisPublisher(r.redis)
riskChk := risk.New(riskCfg, riskPub)
exec := execution.NewExecutor(r.redis, r.namespace, r.cfg.Symbols, stratCfg, riskChk, r.logger, execOpts...)
```

---

## 7. Stream Contract

| Direction | Key | Publisher |
|---|---|---|
| Output (observability only) | `{namespace}:risk` | `risk.Checker` via injected `Publisher` |

The `:risk` stream follows the same TTL policy as all other run-namespaced keys: 24 hours, set by `BacktestRunner.expireStreams()`.

The `Publisher` interface allows swapping implementations:
- `risk.NewRedisPublisher(client)` — production: `XADD {namespace}:risk`
- `risk.NoopPublisher{}` — tests and benchmarks

---

## 8. Configuration

Risk limits are added as new fields on the existing `strategy_config` JSON.

```json
{
  "maker_fee_bps": 10,
  "taker_fee_bps": 10,
  "slippage_bps": 0,
  "trade_qty": "0.001",
  "buy_threshold": "0.2",
  "sell_threshold": "0.2",
  "initial_capital": "10000",

  "max_position_qty": {
    "BTCUSDT": "0.005",
    "ETHUSDT": "0.05"
  },
  "max_drawdown_pct": "5",
  "run_loss_limit_pct": "10"
}
```

Absent risk fields default to zero (disabled). This preserves backwards compatibility with existing strategy configs.

`execution.StrategyConfig` gains three new fields:

```go
type StrategyConfig struct {
    // ... existing fields unchanged ...
    MaxPositionQty  map[string]decimal.Decimal `json:"max_position_qty"`
    MaxDrawdownPct  decimal.Decimal            `json:"max_drawdown_pct"`
    RunLossLimitPct decimal.Decimal            `json:"run_loss_limit_pct"`
}
```

`ParseStrategyConfig()` requires no changes — `json.Unmarshal` populates new fields from JSON and leaves them zero-valued when absent.

---

## 9. New Database Migration

None. Risk state is in-memory for v1. Risk events are ephemeral (Redis Stream, 24-hour TTL).

---

## 10. Deferred

| Concern | Rationale |
|---|---|
| Persistent risk state (live trading) | For live execution, risk state must survive process restarts; requires a Redis hash or DB row per symbol per session |
| Kill switch (external / manual halt) | Requires an API or config-file watcher; no interactive use case in backtest |
| Per-symbol daily loss limit | Adds per-symbol P&L tracking on top of the global equity model; complexity not justified for v1 |
| Risk events consumer / dashboard integration | Visualising risk events requires the trader-dashboard scope to be extended |
| Circuit-breaker auto-reset | Auto-resumption after a drawdown halt (e.g., when equity recovers) is not modelled; every halt is terminal for the run |
| Signal-layer risk filter | Routing risk checks through the `:signals` stream (so erebor-risk intercepts signals before execution) adds a stream hop and sequencing complexity; deferred |
