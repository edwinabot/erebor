-- Paper trading session lifecycle and blotter tables.
-- Applied after 001_initial_schema.sql and 002_backtest_schema.sql.

CREATE TABLE IF NOT EXISTS paper_sessions (
    session_id      UUID        PRIMARY KEY,
    status          TEXT        NOT NULL CHECK (status IN ('RUNNING', 'STOPPED', 'HALTED')),
    symbols         TEXT[]      NOT NULL,
    strategy_config JSONB       NOT NULL DEFAULT '{}',
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    stopped_at      TIMESTAMPTZ,
    error           TEXT        NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS paper_trades (
    session_id       UUID        NOT NULL REFERENCES paper_sessions(session_id),
    trade_id         UUID        NOT NULL,
    symbol           TEXT        NOT NULL,
    event_time       TIMESTAMPTZ NOT NULL,
    side             TEXT        NOT NULL CHECK (side IN ('Buy', 'Sell')),
    fill_price       NUMERIC     NOT NULL,
    fill_qty         NUMERIC     NOT NULL,
    fee              NUMERIC     NOT NULL,
    realised_pnl     NUMERIC     NOT NULL DEFAULT 0,
    signal_name      TEXT        NOT NULL DEFAULT '',
    signal_stream_id TEXT        NOT NULL DEFAULT '',  -- Redis stream message ID; idempotency key
    PRIMARY KEY (session_id, trade_id),
    UNIQUE (session_id, signal_stream_id)
);

CREATE INDEX IF NOT EXISTS paper_trades_session_time ON paper_trades (session_id, event_time DESC);

-- paper_positions stores the latest known position per symbol per session.
-- Upserted on every fill; used for restart recovery.
CREATE TABLE IF NOT EXISTS paper_positions (
    session_id   UUID        NOT NULL REFERENCES paper_sessions(session_id),
    symbol       TEXT        NOT NULL,
    net_qty      NUMERIC     NOT NULL DEFAULT 0,
    avg_entry    NUMERIC     NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (session_id, symbol)
);

CREATE TABLE IF NOT EXISTS paper_equity (
    session_id UUID        NOT NULL REFERENCES paper_sessions(session_id),
    event_time TIMESTAMPTZ NOT NULL,
    equity     NUMERIC     NOT NULL
);

SELECT create_hypertable('paper_equity', 'event_time', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS paper_equity_session_time ON paper_equity (session_id, event_time DESC);
