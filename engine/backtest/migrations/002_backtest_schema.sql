-- Backtest result schema for erebor-backtest.
-- Idempotent: safe to run repeatedly.

CREATE TABLE IF NOT EXISTS backtest_runs (
    run_id          UUID        PRIMARY KEY,
    symbols         TEXT[]      NOT NULL,
    from_time       TIMESTAMPTZ NOT NULL,
    to_time         TIMESTAMPTZ NOT NULL,
    speed_mode      TEXT        NOT NULL,
    speed_factor    NUMERIC,
    strategy_config JSONB       NOT NULL DEFAULT '{}',
    status          TEXT        NOT NULL DEFAULT 'PENDING',
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    error           TEXT
);

CREATE TABLE IF NOT EXISTS backtest_trades (
    run_id      UUID        NOT NULL REFERENCES backtest_runs(run_id),
    trade_id    UUID        NOT NULL,
    symbol      TEXT        NOT NULL,
    event_time  TIMESTAMPTZ NOT NULL,
    side        TEXT        NOT NULL,
    fill_price  NUMERIC     NOT NULL,
    fill_qty    NUMERIC     NOT NULL,
    fee         NUMERIC     NOT NULL,
    signal_name TEXT,
    PRIMARY KEY (run_id, trade_id)
);

CREATE TABLE IF NOT EXISTS backtest_equity (
    run_id     UUID        NOT NULL REFERENCES backtest_runs(run_id),
    event_time TIMESTAMPTZ NOT NULL,
    equity     NUMERIC     NOT NULL
);

SELECT create_hypertable('backtest_equity', 'event_time', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS backtest_data_gaps (
    run_id    UUID        NOT NULL REFERENCES backtest_runs(run_id),
    symbol    TEXT        NOT NULL,
    gap_from  TIMESTAMPTZ NOT NULL,
    gap_to    TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS backtest_metrics (
    run_id            UUID    PRIMARY KEY REFERENCES backtest_runs(run_id),
    total_return_pct  NUMERIC,
    annualized_return NUMERIC,
    sharpe_ratio      NUMERIC,
    max_drawdown_pct  NUMERIC,
    hit_rate_pct      NUMERIC,
    avg_win           NUMERIC,
    avg_loss          NUMERIC,
    trade_count       INT,
    computed_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
