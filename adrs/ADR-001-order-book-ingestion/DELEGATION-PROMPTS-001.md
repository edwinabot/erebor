# Erebor — Claude Code Delegation Prompts

This file contains the prompts used to delegate implementation of the order book ingestion service to Claude Code. Each prompt is self-contained and references the architectural specification. Prompts are versioned alongside the ADR they implement.

---

## PROMPT-001: `erebor-ingest` — Full Service Implementation

**ADR:** ADR-001
**Target:** Claude Code
**Expected output:** Complete Go service with tests, schema, and Makefile

---

### Prompt

You are implementing a Go service called `erebor-ingest`. The complete architectural specification is provided below. Your job is to implement exactly what is specified — not to improve it, not to simplify it, and not to make assumptions where the spec is explicit. If the spec is ambiguous on a point, stop and flag it rather than resolving it silently.

#### Module and dependency requirements

- Go module path: `github.com/edwinabot/erebor/ingest`
- Use `github.com/shopspring/decimal` for all price and quantity values. `float64` is forbidden for any price, quantity, or derived financial value. This is a hard requirement, not a preference.
- Use `https://github.com/coder/websocket/releases/tag/v1.8.14` for WebSocket transport.
- Use `github.com/jackc/pgx/v5` for PostgreSQL/TimescaleDB access.
- Use `github.com/spf13/viper` for configuration loading.
- Use `go.uber.org/zap` for structured JSON logging.

#### Component requirements

Implement all six components. Each component must live in its own package:

- `stream` — `StreamManager`
- `dispatch` — `Dispatcher`
- `symbol` — `SymbolHandler` and `SymbolState`
- `book` — `OrderBook`
- `fetcher` — `DepthFetcher`
- `repository` — `Repository`

Do not merge components into a single package to reduce file count.

#### Interface requirements

Implement all four interfaces exactly as specified. Do not add methods. Do not change parameter types or return types. Do not add variadic options or functional options patterns unless the spec names them.

```go
type StreamManager interface {
    Connect(ctx context.Context) error
    Events() <-chan RawDiffEvent
    Close() error
}

type DepthFetcher interface {
    FetchSnapshot(ctx context.Context, symbol string, limit int) (SnapshotEvent, error)
}

type OrderBook interface {
    Apply(diff DiffEvent) error
    Snapshot(depth int) SnapshotEvent
    LastUpdateID() int64
    Reset()
}

type SymbolHandler interface {
    HandleDiff(event DiffEvent)
    State() SymbolState
}

type Repository interface {
    WriteDiff(ctx context.Context, event DiffEvent) error
    WriteCheckpoint(ctx context.Context, snapshot SnapshotEvent) error
    QueryNearestCheckpoint(ctx context.Context, symbol string, at time.Time) (SnapshotEvent, error)
    QueryDiffs(ctx context.Context, symbol string, from time.Time, to time.Time) ([]DiffEvent, error)
}
```

#### Domain types

Use exactly these types. Do not add fields. Do not change field types.

```go
type PriceLevel struct {
    Price    decimal.Decimal
    Quantity decimal.Decimal
}

type DiffEvent struct {
    Symbol        string
    EventTime     time.Time
    FirstUpdateID int64
    FinalUpdateID int64
    Bids          []PriceLevel
    Asks          []PriceLevel
}

type SnapshotEvent struct {
    Symbol       string
    CapturedAt   time.Time
    LastUpdateID int64
    Bids         []PriceLevel
    Asks         []PriceLevel
}
```

`RawDiffEvent` is an internal type for the `stream` package only, mirroring the Binance wire format. It must not appear in any other package's public API.

#### State machine requirements

The `SymbolHandler` must implement exactly four states: `Disconnected`, `Bootstrapping`, `Synced`, `Resyncing`. State is a typed integer. All transitions must be logged at `INFO` level with fields: `symbol`, `from_state`, `to_state`.

The bootstrap alignment condition is exact. Implement it precisely:

```
Discard: event.FinalUpdateID <= snapshot.LastUpdateID
Accept first: event.FirstUpdateID <= snapshot.LastUpdateID+1 AND event.FinalUpdateID >= snapshot.LastUpdateID+1
```

Do not simplify this condition. Do not substitute `==` for the range check.

The buffer-and-wait behaviour is required: if the alignment event has not yet arrived in the buffer when the snapshot returns, the handler must continue buffering until it arrives. It does not re-fetch the snapshot. It re-fetches only if the buffer exceeds `MaxBufferSize`.

#### Checkpoint trigger

Two conditions, either fires a checkpoint. Both are evaluated synchronously in the SYNCED diff-handling path. Do not fire checkpoint writes in a separate goroutine.

- Wall clock elapsed since last checkpoint exceeds `CheckpointInterval` (configurable, default 1s)
- Diffs applied since last checkpoint exceeds `CheckpointDiffThreshold` (configurable, default 500)

#### Reconnection

Exponential backoff with jitter. Initial delay 1s, maximum delay 30s. Use `crypto/rand` or `math/rand` with a seed for jitter. Do not use `time.Sleep` with a fixed constant.

#### Configuration

Configuration is loaded from a YAML file. The file path is provided as a CLI flag (`--config`). Credentials are sourced exclusively from environment variables:

```
BINANCE_API_KEY
BINANCE_API_SECRET
DATABASE_DSN
```

The service must exit at startup with an explicit, human-readable error message if any of these three environment variables are absent. No silent defaults. No fallback to config file values for credentials.

Per-symbol configuration supports `depth_limit` (default 50), `checkpoint_interval` (default 1s), and `checkpoint_diff_threshold` (default 500).

#### Persistence — TimescaleDB schema

Provide the schema as `migrations/001_initial_schema.sql`. The file must:

- Create both hypertables with `create_hypertable`
- Set chunk interval to 1 day on both tables
- Create all indexes specified in the ADR
- Create the unique constraint on `order_book_diffs(symbol, final_update_id)`
- Be idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`)

#### Observability

- All log output is JSON. Use `go.uber.org/zap` with production config.
- Required log fields on every entry: `ts`, `level`, `component`, `msg`.
- Add `symbol` field to any log entry where a symbol is in scope.
- Log sequence gaps at `WARN` with fields: `symbol`, `expected_first_update_id`, `received_first_update_id`.
- Never log `BINANCE_API_KEY`, `BINANCE_API_SECRET`, `DATABASE_DSN`, or any WebSocket frame payload.

#### Health endpoint

Expose `GET /healthz` on `:8080`. Return HTTP 200 with body `{"status":"ok"}` when at least one symbol is in `Synced` state. Return HTTP 503 with body `{"status":"degraded"}` otherwise. Do not use a framework — `net/http` stdlib only for this endpoint.

#### Tests

Write unit tests for:

1. **`OrderBook`** — table-driven tests covering: add a new price level, update an existing price level, remove a level (quantity = "0"), apply a diff with multiple bids and asks simultaneously, snapshot returns only the top N levels in correct order.

2. **Bootstrap alignment logic in `SymbolHandler`** — table-driven tests covering: alignment event arrives before snapshot (buffered events ahead of snapshot), alignment event arrives exactly at snapshot boundary, events before snapshot are discarded correctly, buffer overflow triggers re-snapshot.

3. **`Repository`** — `SymbolHandler` tests must use a mock `Repository` satisfying the interface. Do not use a real database in unit tests. Provide a `MockRepository` in a `_test` package.

Use `github.com/stretchr/testify` for assertions.

#### Makefile

Provide a `Makefile` with these targets:

- `make build` — compiles the binary to `bin/erebor-ingest`
- `make test` — runs all tests with `-race` flag
- `make lint` — runs `golangci-lint run` (assumes `golangci-lint` is installed)
- `make migrate` — applies `migrations/001_initial_schema.sql` via `psql` using `DATABASE_DSN`

#### What NOT to do

- Do not add a metrics server, Prometheus endpoint, or tracing instrumentation. That is a future story.
- Do not implement dynamic symbol registration at runtime. The symbol list is fixed at startup.
- Do not implement schema migration logic inside the service process.
- Do not implement any signal computation, strategy logic, or trading decisions.
- Do not add a `context.Context` to `OrderBook` methods — it is a pure in-memory structure.

---

See full ADR-001 spec at `adrs/ADR-001-order-book-ingestion/ADR-001-order-book-ingestion.md`

---

## PROMPT-002: Focused Re-prompt — Bootstrap Protocol Only

Use this prompt if the full implementation is accepted but the bootstrap alignment logic requires a targeted fix after code review.

---

### Prompt

The `SymbolHandler` bootstrap alignment logic in `erebor-ingest` is incorrect. I need you to rewrite only the bootstrap procedure in `symbol/handler.go`. Do not change any other file.

The current implementation has the following defect: [DESCRIBE SPECIFIC DEFECT FROM REVIEW].

The correct alignment condition is:

```
Discard all events where:
    event.FinalUpdateID <= snapshot.LastUpdateID

Accept the first event where:
    event.FirstUpdateID <= snapshot.LastUpdateID+1
    AND
    event.FinalUpdateID >= snapshot.LastUpdateID+1

If no such event exists in the current buffer, continue buffering until it arrives.
Only re-fetch the snapshot if len(buffer) > config.MaxBufferSize.
```

Rewrite the `runBootstrap` function (or equivalent) to implement this exactly. Provide updated table-driven tests for the alignment logic alongside the fix.

---

## PROMPT-003: Focused Re-prompt — Repository Mock for Tests

Use this prompt if the implementation uses a real database in unit tests, which is a review rejection condition.

---

### Prompt

The unit tests in `symbol/handler_test.go` depend on a real TimescaleDB instance. This is incorrect. Rewrite the test file to use a mock `Repository` that satisfies the `Repository` interface without any database dependency.

Provide a `MockRepository` struct in `symbol/mock_repository_test.go` with the following behaviour:

- `WriteDiff` — appends the event to an internal slice. Returns nil.
- `WriteCheckpoint` — appends the snapshot to an internal slice. Returns nil.
- `QueryNearestCheckpoint` — returns a configurable response set at construction time.
- `QueryDiffs` — returns a configurable response set at construction time.

The mock must be inspectable in tests: expose `DiffsWritten() []DiffEvent` and `CheckpointsWritten() []SnapshotEvent` for assertion.

Do not use a mocking framework. Implement the mock by hand.

---

## Prompt Maintenance Notes

- Update this file when a new delegation prompt is written for a new component or service.
- When a prompt produces an implementation that required significant re-prompting to correct, document the defect and the fix prompt here as a new numbered PROMPT entry.
- Prompts are part of the architecture record. They belong in version control alongside the ADR they implement.
