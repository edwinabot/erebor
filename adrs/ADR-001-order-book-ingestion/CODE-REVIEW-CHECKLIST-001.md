# Code Review Checklist — `erebor-ingest`

**ADR:** ADR-001
**Reviewer role:** Senior architect / spec author
**Policy:** Items marked REJECT must be corrected before merge. Items marked NEGOTIATE are discussed with the implementor before a decision.

Work through this checklist top to bottom. Stop at the first REJECT finding and raise it before continuing — do not accumulate a list of rejections and deliver them together. A single architectural defect may invalidate downstream implementation decisions.

---

## 1. Correctness — Reject if Wrong

These are not style issues. A failure here means the service is incorrect in production.

### 1.1 Bootstrap Alignment Condition

- [ ] The alignment condition is implemented as a range check, not a point equality:
  ```
  event.FirstUpdateID <= snapshot.LastUpdateID+1
  AND
  event.FinalUpdateID >= snapshot.LastUpdateID+1
  ```
- [ ] Events where `event.FinalUpdateID <= snapshot.LastUpdateID` are discarded before alignment search
- [ ] The handler buffers and waits if the alignment event has not yet arrived — it does not re-fetch the snapshot prematurely
- [ ] Re-snapshot is triggered only on buffer overflow (`len(buffer) > MaxBufferSize`), not on timeout or arbitrary condition

### 1.2 Sequence Gap Detection

- [ ] SYNCED state validates `event.FirstUpdateID == previousFinalUpdateID + 1` on every diff
- [ ] A sequence gap triggers transition to RESYNCING — it does not log and continue
- [ ] Gap log entry includes both `expected_first_update_id` and `received_first_update_id` fields

### 1.3 Zero-Quantity Level Removal

- [ ] A diff entry with quantity `"0"` deletes the price level from the in-memory book
- [ ] Zero-quantity levels are not stored with qty=0 as a sentinel — they must be absent from the map
- [ ] This behaviour is covered by at least one unit test

### 1.4 Financial Arithmetic

- [ ] `float64` does not appear anywhere in price or quantity handling — search the entire codebase with `grep -r "float64" .` and verify every result is non-financial
- [ ] All price and quantity fields on `PriceLevel`, `DiffEvent`, and `SnapshotEvent` are `decimal.Decimal`
- [ ] No intermediate conversion to `float64` occurs in sorting, comparison, or aggregation

### 1.5 State Machine Exhaustiveness

- [ ] Every possible `SymbolState` value is handled in every switch statement that inspects state
- [ ] No implicit fallthrough or default case that silently swallows an unexpected state
- [ ] `Resyncing` clears both the order book (`Reset()`) and the diff buffer before re-entering `Bootstrapping`

---

## 2. Interface Integrity — Reject if Wrong

These ensure the component boundaries from the spec are honoured.

### 2.1 Component Separation

- [ ] `OrderBook` has no import of the `repository` package or any persistence type
- [ ] `StreamManager` has no import of the `book`, `symbol`, or `repository` packages
- [ ] `DepthFetcher` is stateless — it holds no mutable state between calls
- [ ] `Dispatcher` does not inspect diff event content beyond the `Symbol` field

### 2.2 Interface Signatures

- [ ] All five interfaces match the spec exactly — method names, parameter types, return types
- [ ] No additional methods have been added to any interface
- [ ] `RawDiffEvent` does not appear in any package outside `stream`

### 2.3 Domain Types

- [ ] `DiffEvent`, `SnapshotEvent`, and `PriceLevel` match the spec exactly — no added fields
- [ ] `SymbolState` is a typed integer enum with exactly four values: `Disconnected`, `Bootstrapping`, `Synced`, `Resyncing`

---

## 3. Security and Credentials — Reject if Wrong

These are non-negotiable given the CSSLP posture of the project.

### 3.1 Credential Handling

- [ ] `BINANCE_API_KEY`, `BINANCE_API_SECRET`, and `DATABASE_DSN` are read exclusively from environment variables
- [ ] Search the config loading code: no fallback reads these values from a YAML file or any config struct default
- [ ] The service exits with a clear, human-readable error at startup if any credential env var is absent — test this manually
- [ ] No credential value appears in any log output — verify by running the service and inspecting startup logs

### 3.2 Log Safety

- [ ] No WebSocket frame payload is logged at any level
- [ ] No raw SQL query strings containing user-controlled values are logged
- [ ] Structured log fields do not include `api_key`, `api_secret`, `dsn`, `password`, or analogues

### 3.3 Configuration File

- [ ] The example config file (`config.example.yaml` or equivalent) contains only placeholder values for credentials — not real values, not empty strings that look real
- [ ] `.gitignore` excludes `config.yaml` and any `*.env` file

---

## 4. Operational Correctness — Reject if Wrong

### 4.1 Graceful Shutdown

- [ ] The service handles `SIGTERM` and `SIGINT`
- [ ] On signal: the WebSocket connection is closed, the pending checkpoint write (if in flight) is awaited, the database pool is closed
- [ ] The shutdown sequence does not use `os.Exit(0)` before cleanup is complete

### 4.2 Reconnection

- [ ] Reconnect uses exponential backoff — verify the delay grows between attempts in the log output
- [ ] Jitter is applied — two reconnect sequences do not produce identical delay sequences
- [ ] Maximum delay is capped at 30s — verify no delay exceeds this
- [ ] `time.Sleep` is not called with a hardcoded constant

### 4.3 Health Endpoint

- [ ] `GET /healthz` returns HTTP 200 when at least one symbol is in `Synced` state
- [ ] `GET /healthz` returns HTTP 503 when no symbol is in `Synced` state (e.g., during bootstrap)
- [ ] The endpoint reflects actual runtime state — it is not a static 200
- [ ] The endpoint is implemented with `net/http` stdlib only — no framework dependency

### 4.4 Checkpoint Writes

- [ ] Checkpoint writes occur synchronously in the SYNCED diff-handling path — no separate goroutine per checkpoint
- [ ] Both trigger conditions are evaluated on every diff: time-based and count-based
- [ ] The checkpoint depth matches the per-symbol configured `depth_limit`

---

## 5. Persistence Schema — Reject if Wrong

### 5.1 Hypertables

- [ ] Both `order_book_diffs` and `order_book_snapshots` are created as hypertables via `create_hypertable`
- [ ] Chunk interval is 1 day on both tables
- [ ] Both partition keys are `TIMESTAMPTZ` — not `TIMESTAMP WITHOUT TIME ZONE`

### 5.2 Constraints and Indexes

- [ ] Unique constraint on `order_book_diffs(symbol, final_update_id)` is present
- [ ] Index `(symbol, event_time DESC)` on `order_book_diffs` is present
- [ ] Index `(symbol, snapshot_time DESC)` on `order_book_snapshots` is present

### 5.3 Schema File

- [ ] Schema is in `migrations/001_initial_schema.sql`
- [ ] File is idempotent (`CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`)
- [ ] File contains no hardcoded credentials, hostnames, or environment-specific values

---

## 6. Test Coverage — Negotiate if Missing

These are expected but a gap here results in a discussion before merge, not an automatic rejection.

### 6.1 `OrderBook` Unit Tests

- [ ] Add a new bid level
- [ ] Add a new ask level
- [ ] Update an existing bid level (price exists, new quantity)
- [ ] Update an existing ask level
- [ ] Remove a bid level (quantity = "0")
- [ ] Remove an ask level (quantity = "0")
- [ ] Apply a diff with multiple bids and asks simultaneously
- [ ] `Snapshot(N)` returns exactly N levels per side, in correct price order
- [ ] `Snapshot(N)` where N > current depth returns all available levels without panic
- [ ] All tests are table-driven

### 6.2 Bootstrap Alignment Unit Tests

- [ ] Alignment event arrives in buffer before snapshot fetch returns
- [ ] Alignment event arrives in buffer after snapshot fetch returns (wait-for-alignment path)
- [ ] Events before the snapshot's `lastUpdateID` are correctly discarded
- [ ] Buffer overflow triggers re-snapshot, not a panic or silent hang
- [ ] Exact boundary: first event satisfies condition exactly (not off-by-one)
- [ ] All tests are table-driven

### 6.3 Repository and Mocking

- [ ] `SymbolHandler` unit tests use `MockRepository` — no real database connection
- [ ] `MockRepository` implements the `Repository` interface
- [ ] `MockRepository` is implemented by hand — no mocking framework
- [ ] Tests assert `DiffsWritten()` and `CheckpointsWritten()` counts after state machine execution

### 6.4 Test Execution

- [ ] `make test` passes cleanly with `-race` flag — no data race warnings
- [ ] No test uses `time.Sleep` for synchronisation — use channels or `sync.WaitGroup`

---

## 7. Code Quality — Negotiate if Missing

### 7.1 Linting

- [ ] `make lint` exits 0 with `golangci-lint run`
- [ ] No `//nolint` directives without an explanatory comment

### 7.2 Error Handling

- [ ] No `_` used to discard errors from I/O operations, channel sends, or goroutine launches
- [ ] Errors from `WriteDiff` and `WriteCheckpoint` are logged, not silently swallowed
- [ ] Fatal startup errors use structured logging before `os.Exit(1)` — not `log.Fatal` or `panic`

### 7.3 Goroutine Hygiene

- [ ] Every goroutine launched has a clear owner responsible for its termination
- [ ] No goroutine is launched without a `context.Context` that can cancel it on shutdown
- [ ] The bootstrap goroutine (snapshot fetch) is cancelled if the `SymbolHandler` transitions away from `Bootstrapping` before it completes

---

## 8. Review Outcome

Record the outcome here before closing the review.

| Finding | Severity | File | Resolution |
|---|---|---|---|
| | | | |

**Decision:** APPROVED / APPROVED WITH COMMENTS / REJECTED — RE-PROMPT REQUIRED

If rejected, reference the specific re-prompt from `DELEGATION-PROMPTS-001.md` to use.
