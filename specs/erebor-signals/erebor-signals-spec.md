# erebor-signals — Specification

**Status:** Implemented (v1 shipped)  
**Date:** 2026-05  
**Component:** `erebor-signals`  
**Location:** `engine/signals/`  
**Depends on:** ADR-001 (hybrid L2 persistence), backtest-replay spec (stream/domain contract)

---

## 1. Decisions

| Concern | Decision | Rationale |
|---|---|---|
| Signal computation | Pure functions; no state between events | Deterministic, trivially testable, and fully reproducible across live and backtest |
| Message delivery | XREADGROUP consumer groups | Exactly-once delivery per group; decouples producers from consumers; multiple downstream subscribers can coexist |
| Clock | `EventTime` propagated from the triggering `L2BookUpdateEvent`; `time.Now()` is forbidden in signal logic | Same invariant as the replay engine — backtest fidelity is guaranteed by construction |
| Signal catalog | Compile-time registration in `compute.All()` | No plugin system needed for v1; adding a signal is a code change plus a version bump |
| Live/backtest identity | `STREAM_NAMESPACE` env var selects the key prefix | Zero code divergence between live and backtest deployments |

---

## 2. Overview

`erebor-signals` is a stateless stream processor. It reads `L2BookUpdateEvent` messages from Redis Streams via a consumer group, applies a fixed set of market microstructure signals to each event, and publishes one `SignalEvent` per signal per L2 event.

```
{ns}:l2:{SYMBOL}   ──XREADGROUP──▶   compute.All()   ──XADD──▶   {ns}:signals
L2BookUpdateEvent                  (depth: N)              SignalEvent × 3
```

The key invariant: **signal logic is identical in live and backtest**. The `STREAM_NAMESPACE` environment variable switches the key prefix; the computation code does not branch.

### Current role in the backtest pipeline

In `erebor-backtest`, the internal `Executor` reads the `:l2:` streams directly and computes `book_imbalance` inline for trading decisions. `erebor-signals` runs as a separate process with the same namespace and publishes to `:signals`, which the `ResultCollector` monitors for signal counts and observability. Signals do not drive execution decisions in v1 backtest — that coupling is deferred (see §10).

---

## 3. Stream Contract

| Direction | Key | Protocol |
|---|---|---|
| Input | `{namespace}:l2:{SYMBOL}` | `XREADGROUP`; consumer group: `erebor-signals` |
| Output | `{namespace}:signals` | `XADD` |

**Namespace patterns:**

| Context | Namespace |
|---|---|
| Live | `erebor:live` |
| Backtest | `erebor:backtest:{run_id}` |

One `L2BookUpdateEvent` produces exactly **3 `SignalEvent` messages** (v1 catalog size). Consumer group creation uses `MKSTREAM` so the group is registered even before the first L2 event arrives.

---

## 4. Component Model

```
erebor-signals binary
├── consumer.Consumer     — XREADGROUP read loop; batches up to 20 msgs; one goroutine covers all symbols
│   ├── Creates consumer group on startup (XGROUP CREATE MKSTREAM, idempotent — BUSYGROUP is not an error)
│   └── XACKs every message after publish; malformed events are logged and ACKed (never redelivered)
├── compute.All()         — Pure function: (L2BookUpdateEvent, depth int) → []SignalEvent
│   ├── bookImbalance()
│   ├── spreadBps()
│   └── midPrice()
├── publisher.Publisher   — XADD to {namespace}:signals
└── config.Config         — YAML + env var loader (viper); env vars override YAML

health: GET /healthz → {"status":"ok"} (200) or {"status":"degraded"} (503)
        503 fires when the consumer read loop is not active
```

---

## 5. Domain Types

```go
// L2BookUpdateEvent is consumed from Redis Streams.
// Bids are sorted descending (best bid first); Asks ascending (best ask first).
type L2BookUpdateEvent struct {
    RunID        string    // empty = live event
    Symbol       string
    EventTime    time.Time // authoritative logical clock; never call time.Now() in signal logic
    LastUpdateID int64
    Bids         []PriceLevel
    Asks         []PriceLevel
}

// SignalEvent is published to the signals stream after computing a signal.
// EventTime is propagated from the L2BookUpdateEvent that triggered it.
type SignalEvent struct {
    RunID     string
    Symbol    string
    EventTime time.Time        // must equal the source L2BookUpdateEvent.EventTime
    Name      string           // "book_imbalance" | "spread_bps" | "mid_price"
    Version   string           // "1"
    Value     decimal.Decimal
    Params    map[string]string
}
```

**Redis Stream field encoding:**

| Go field | Stream field | Encoding |
|---|---|---|
| `EventTime` | `event_time` | RFC3339Nano string |
| `Bids` / `Asks` | `bids` / `asks` | JSON array of `["price_str", "qty_str"]` pairs |
| `Value` | `value` | decimal string (no scientific notation) |
| `Params` | `params` | JSON object |

---

## 6. Signal Catalog

### 6.1 book_imbalance v1

```
value = (bid_qty − ask_qty) / (bid_qty + ask_qty)
```

Computed over the top `signal_depth` price levels. Returns `0` when total quantity is zero.

Range: `[−1, 1]`. Positive = bid-heavy (buy pressure). Negative = ask-heavy (sell pressure).

Params: `{"depth": "<N>"}`

### 6.2 spread_bps v1

```
value = (best_ask − best_bid) / mid_price × 10000
```

Returns `0` when the book is empty or `mid_price = 0`.

Params: `{}`

### 6.3 mid_price v1

```
value = (best_bid + best_ask) / 2
```

Returns `0` when either side of the book is empty.

Params: `{}`

---

## 7. Signal Extensibility

To add a new signal:

1. Add a private function in `engine/signals/compute/signals.go`.
2. Register it in `compute.All()`.
3. Add unit tests to `compute/signals_test.go`.
4. Bump the `Version` field if an existing signal's formula changes (not when a new signal is added).

Consumers SHOULD filter by `name + version` so formula changes in a new version do not silently break downstream logic.

---

## 8. Configuration

```yaml
symbols:
  - BTCUSDT
stream_namespace: erebor:live   # switch to erebor:backtest:{run_id} for backtest
signal_depth: 10                # number of price levels used by book_imbalance
redis:
  addr: localhost:6379
  password: ""
log:
  level: info          # stderr threshold
  file_level: debug    # file threshold
  file_path: ""        # disabled when empty
health:
  addr: ":8080"
```

**Environment variable overrides** (take precedence over YAML):

| Var | Config key |
|---|---|
| `REDIS_ADDR` | `redis.addr` |
| `REDIS_PASSWORD` | `redis.password` |
| `STREAM_NAMESPACE` | `stream_namespace` |

`STREAM_NAMESPACE` is the primary knob for pointing the binary at a backtest run:

```
STREAM_NAMESPACE=erebor:backtest:018f... erebor-signals -config config.yaml
```

---

## 9. Control Protocol Gap

The backtest-replay spec requires all consumers to subscribe to `{namespace}:control` and drain on `REPLAY_COMPLETE`. In v1, `erebor-signals` does **not** subscribe to the control stream. It exits only when its process receives `SIGTERM`.

In a backtest context: `erebor-backtest` (or a future orchestrator) must send `SIGTERM` to the signals process after the replay is complete. The consumer observes context cancellation on the next `XREADGROUP` block timeout (≤ 5 s by default) and exits cleanly.

Control-stream awareness is deferred to v2 (see §10).

---

## 10. Deferred

| Concern | Rationale |
|---|---|
| Signal persistence to TimescaleDB | Not needed for backtest v1; signals are ephemeral on the stream |
| Control stream subscription | Requires process lifecycle orchestration; deferred until backtest orchestration is formalised |
| Signals as the execution input | Currently the backtest executor computes `book_imbalance` inline from L2; routing execution through `:signals` requires sequencing guarantees across services |
| Windowed / stateful signals (EMA, VWAP, order-flow imbalance) | Requires per-symbol state and a restart-safe design; deferred to a signals-v2 |
| Signal registry / dynamic loading | Overkill for v1 catalog size |
| Horizontal scaling (multiple instances) | `ConsumerID = hostname` is already wired; infra cost not justified yet |
