package repository_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/repository"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// Integration tests require a real TimescaleDB instance with the backtest schema applied.
// Run with:
//
//	EREBOR_TEST_DSN="postgres://erebor:erebor_dev@localhost:5432/erebor?sslmode=disable" \
//	    go test -race ./repository/...
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("EREBOR_TEST_DSN")
	if dsn == "" {
		t.Skip("EREBOR_TEST_DSN not set; skipping repository integration tests")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	require.NoError(t, pool.Ping(ctx))
	return pool
}

const (
	runLifecycle = "00000000-0000-4000-8000-000000000001"
	runGap       = "00000000-0000-4000-8000-000000000002"
	runTrades    = "00000000-0000-4000-8000-000000000003"
	runNoTrades  = "00000000-0000-4000-8000-000000000004"
	runEquity    = "00000000-0000-4000-8000-000000000005"
	runMetrics   = "00000000-0000-4000-8000-000000000006"
)

// insertRun inserts a minimal run record so FK constraints on other tables are satisfied.
func insertRun(t *testing.T, pool *pgxpool.Pool, runID string) {
	t.Helper()
	_, err := pool.Exec(context.Background(), `
		INSERT INTO backtest_runs
		    (run_id, symbols, from_time, to_time, speed_mode, strategy_config, status)
		VALUES ($1, $2, $3, $4, 'AFAP', '{}', 'PENDING')
		ON CONFLICT (run_id) DO NOTHING
	`, runID, []string{"TESTSYM"}, time.Now().Add(-time.Hour).UTC(), time.Now().UTC())
	require.NoError(t, err)
}

func cleanupRun(t *testing.T, pool *pgxpool.Pool, runID string) {
	t.Helper()
	ctx := context.Background()
	for _, table := range []string{
		"backtest_metrics", "backtest_data_gaps", "backtest_equity",
		"backtest_trades", "backtest_runs",
	} {
		_, _ = pool.Exec(ctx, "DELETE FROM "+table+" WHERE run_id = $1", runID)
	}
}

// ── CreateRun / UpdateRunStatus ───────────────────────────────────────────────

func TestRepositoryCreateAndUpdateRunIntegration(t *testing.T) {
	pool := newTestPool(t)
	repo := repository.New(pool, zap.NewNop())
	ctx := context.Background()

	runID := runLifecycle
	t.Cleanup(func() { cleanupRun(t, pool, runID) })

	rec := domain.RunRecord{
		RunID:          runID,
		Symbols:        []string{"BTCUSDT"},
		FromTime:       time.Now().Add(-time.Hour).UTC(),
		ToTime:         time.Now().UTC(),
		SpeedMode:      domain.SpeedAFAP,
		StrategyConfig: `{"maker_fee_bps":10}`,
		Status:         domain.RunStatusPending,
	}
	require.NoError(t, repo.CreateRun(ctx, rec))

	// Idempotency: re-inserting same run_id must fail (PK violation).
	assert.Error(t, repo.CreateRun(ctx, rec), "duplicate run_id must return error")

	now := time.Now().UTC()
	require.NoError(t, repo.UpdateRunStatus(ctx, runID, domain.RunStatusRunning, &now, nil, ""))
	require.NoError(t, repo.UpdateRunStatus(ctx, runID, domain.RunStatusCompleted, nil, &now, ""))

	// Verify persisted status.
	var status string
	require.NoError(t, pool.QueryRow(ctx, "SELECT status FROM backtest_runs WHERE run_id=$1", runID).Scan(&status))
	assert.Equal(t, string(domain.RunStatusCompleted), status)
}

// ── WriteDataGap ──────────────────────────────────────────────────────────────

func TestRepositoryWriteDataGapIntegration(t *testing.T) {
	pool := newTestPool(t)
	repo := repository.New(pool, zap.NewNop())
	ctx := context.Background()

	runID := runGap
	t.Cleanup(func() { cleanupRun(t, pool, runID) })
	insertRun(t, pool, runID)

	gapFrom := time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)
	gapTo := time.Date(2026, 1, 1, 10, 5, 0, 0, time.UTC)
	require.NoError(t, repo.WriteDataGap(ctx, runID, "BTCUSDT", gapFrom, gapTo))

	var count int
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT COUNT(*) FROM backtest_data_gaps WHERE run_id=$1", runID,
	).Scan(&count))
	assert.Equal(t, 1, count)
}

// ── WriteTrade / QueryTrades ──────────────────────────────────────────────────

func TestRepositoryWriteAndQueryTradesIntegration(t *testing.T) {
	pool := newTestPool(t)
	repo := repository.New(pool, zap.NewNop())
	ctx := context.Background()

	runID := runTrades
	t.Cleanup(func() { cleanupRun(t, pool, runID) })
	insertRun(t, pool, runID)

	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	trade := domain.TradeRecord{
		RunID:      runID,
		TradeID:    "10000000-0000-4000-8000-000000000001",
		Symbol:     "BTCUSDT",
		EventTime:  base,
		Side:       domain.SideBuy,
		FillPrice:  decimal.RequireFromString("50000.00"),
		FillQty:    decimal.RequireFromString("0.5"),
		Fee:        decimal.RequireFromString("2.50"),
		SignalName: "book_imbalance",
	}
	require.NoError(t, repo.WriteTrade(ctx, trade))

	// Idempotency: duplicate trade_id is silently ignored.
	require.NoError(t, repo.WriteTrade(ctx, trade), "duplicate trade must not error")

	trades, err := repo.QueryTrades(ctx, runID)
	require.NoError(t, err)
	require.Len(t, trades, 1)

	got := trades[0]
	assert.Equal(t, runID, got.RunID)
	assert.Equal(t, "10000000-0000-4000-8000-000000000001", got.TradeID)
	assert.Equal(t, domain.SideBuy, got.Side)
	assert.True(t, got.FillPrice.Equal(decimal.RequireFromString("50000.00")), "fill_price mismatch")
	assert.True(t, got.FillQty.Equal(decimal.RequireFromString("0.5")), "fill_qty mismatch")
	assert.True(t, got.Fee.Equal(decimal.RequireFromString("2.50")), "fee mismatch")
	assert.Equal(t, "book_imbalance", got.SignalName)
}

func TestRepositoryQueryTradesEmptyRunReturnsEmpty(t *testing.T) {
	pool := newTestPool(t)
	repo := repository.New(pool, zap.NewNop())
	ctx := context.Background()

	runID := runNoTrades
	t.Cleanup(func() { cleanupRun(t, pool, runID) })
	insertRun(t, pool, runID)

	trades, err := repo.QueryTrades(ctx, runID)
	require.NoError(t, err)
	assert.Empty(t, trades)
}

// ── WriteEquityPoint / QueryEquityPoints ──────────────────────────────────────

func TestRepositoryWriteAndQueryEquityPointsIntegration(t *testing.T) {
	pool := newTestPool(t)
	repo := repository.New(pool, zap.NewNop())
	ctx := context.Background()

	runID := runEquity
	t.Cleanup(func() { cleanupRun(t, pool, runID) })
	insertRun(t, pool, runID)

	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	points := []domain.EquityPoint{
		{RunID: runID, EventTime: base, Equity: decimal.RequireFromString("10000")},
		{RunID: runID, EventTime: base.Add(time.Hour), Equity: decimal.RequireFromString("10100")},
		{RunID: runID, EventTime: base.Add(2 * time.Hour), Equity: decimal.RequireFromString("10050")},
	}
	for _, p := range points {
		require.NoError(t, repo.WriteEquityPoint(ctx, p))
	}

	got, err := repo.QueryEquityPoints(ctx, runID)
	require.NoError(t, err)
	require.Len(t, got, 3, "all equity points must be returned")

	// Verify ordering by event_time ASC.
	assert.True(t, got[0].Equity.Equal(decimal.RequireFromString("10000")))
	assert.True(t, got[1].Equity.Equal(decimal.RequireFromString("10100")))
	assert.True(t, got[2].Equity.Equal(decimal.RequireFromString("10050")))
}

// ── WriteMetrics ──────────────────────────────────────────────────────────────

func TestRepositoryWriteMetricsIntegration(t *testing.T) {
	pool := newTestPool(t)
	repo := repository.New(pool, zap.NewNop())
	ctx := context.Background()

	runID := runMetrics
	t.Cleanup(func() { cleanupRun(t, pool, runID) })
	insertRun(t, pool, runID)

	m := domain.MetricsRecord{
		RunID:            runID,
		TotalReturnPct:   decimal.RequireFromString("12.50"),
		AnnualizedReturn: decimal.RequireFromString("45.00"),
		SharpeRatio:      decimal.RequireFromString("1.8"),
		MaxDrawdownPct:   decimal.RequireFromString("5.25"),
		HitRatePct:       decimal.RequireFromString("60"),
		AvgWin:           decimal.RequireFromString("500"),
		AvgLoss:          decimal.RequireFromString("-300"),
		TradeCount:       10,
	}
	require.NoError(t, repo.WriteMetrics(ctx, m))

	// Upsert: second write with updated values must succeed.
	m.TradeCount = 20
	m.TotalReturnPct = decimal.RequireFromString("25.00")
	require.NoError(t, repo.WriteMetrics(ctx, m), "upsert must succeed")

	var tradeCount int
	var totalReturn float64
	require.NoError(t, pool.QueryRow(ctx,
		"SELECT trade_count, total_return_pct::float8 FROM backtest_metrics WHERE run_id=$1", runID,
	).Scan(&tradeCount, &totalReturn))
	assert.Equal(t, 20, tradeCount, "upserted trade_count must be 20")
	assert.InDelta(t, 25.0, totalReturn, 0.001, "upserted total_return_pct must be 25.0")
}

// ── RunStore interface satisfaction ──────────────────────────────────────────

// TestBacktestRepositoryImplementsRunStore is a compile-time assertion that
// BacktestRepository satisfies the RunStore interface.
func TestBacktestRepositoryImplementsRunStore(t *testing.T) {
	pool := newTestPool(t)
	var _ repository.RunStore = repository.New(pool, zap.NewNop())
}
