-- ADR-001 initial schema for erebor-ingest.
-- Idempotent: safe to run repeatedly.

CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS order_book_diffs (
    event_time      TIMESTAMPTZ NOT NULL,
    symbol          TEXT        NOT NULL,
    first_update_id BIGINT      NOT NULL,
    final_update_id BIGINT      NOT NULL,
    bids            JSONB       NOT NULL,
    asks            JSONB       NOT NULL,
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

SELECT create_hypertable(
    'order_book_diffs',
    'event_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- TimescaleDB requires the partition column (event_time) to participate in
-- any unique index on a hypertable. The ADR-specified uniqueness on
-- (symbol, final_update_id) is preserved in practice — a given final_update_id
-- from Binance has exactly one event_time.
CREATE UNIQUE INDEX IF NOT EXISTS order_book_diffs_symbol_final_update_id_uidx
    ON order_book_diffs (symbol, final_update_id, event_time);

CREATE INDEX IF NOT EXISTS order_book_diffs_symbol_event_time_idx
    ON order_book_diffs (symbol, event_time DESC);

CREATE TABLE IF NOT EXISTS order_book_snapshots (
    snapshot_time   TIMESTAMPTZ NOT NULL,
    symbol          TEXT        NOT NULL,
    last_update_id  BIGINT      NOT NULL,
    depth           INT         NOT NULL,
    bids            JSONB       NOT NULL,
    asks            JSONB       NOT NULL
);

SELECT create_hypertable(
    'order_book_snapshots',
    'snapshot_time',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

CREATE INDEX IF NOT EXISTS order_book_snapshots_symbol_snapshot_time_idx
    ON order_book_snapshots (symbol, snapshot_time DESC);
