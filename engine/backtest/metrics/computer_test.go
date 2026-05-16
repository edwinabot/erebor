package metrics_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/metrics"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func nopLogger() *zap.Logger { return zap.NewNop() }

// ── mock store ────────────────────────────────────────────────────────────────

type mockStore struct {
	trades         []domain.TradeRecord
	equityPoints   []domain.EquityPoint
	tradesErr      error
	equityErr      error
	writeMetricErr error
	written        *domain.MetricsRecord
}

func (m *mockStore) QueryTrades(_ context.Context, _ string) ([]domain.TradeRecord, error) {
	return m.trades, m.tradesErr
}
func (m *mockStore) QueryEquityPoints(_ context.Context, _ string) ([]domain.EquityPoint, error) {
	return m.equityPoints, m.equityErr
}
func (m *mockStore) WriteMetrics(_ context.Context, rec domain.MetricsRecord) error {
	m.written = &rec
	return m.writeMetricErr
}

// ── helpers ───────────────────────────────────────────────────────────────────

func d(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func equitySeq(runID string, base time.Time, values []string) []domain.EquityPoint {
	pts := make([]domain.EquityPoint, len(values))
	for i, v := range values {
		pts[i] = domain.EquityPoint{
			RunID:     runID,
			EventTime: base.Add(time.Duration(i) * 24 * time.Hour),
			Equity:    d(v),
		}
	}
	return pts
}

func buyTrade(runID, sym string, price, qty, fee string, t time.Time) domain.TradeRecord {
	return domain.TradeRecord{
		RunID: runID, Symbol: sym, TradeID: price + "buy",
		Side: domain.SideBuy, EventTime: t,
		FillPrice: d(price), FillQty: d(qty), Fee: d(fee),
	}
}

func sellTrade(runID, sym string, price, qty, fee string, t time.Time) domain.TradeRecord {
	return domain.TradeRecord{
		RunID: runID, Symbol: sym, TradeID: price + "sell",
		Side: domain.SideSell, EventTime: t,
		FillPrice: d(price), FillQty: d(qty), Fee: d(fee),
	}
}

// ── empty data ────────────────────────────────────────────────────────────────

func TestComputeEmptyDataWritesZeroMetrics(t *testing.T) {
	store := &mockStore{}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "run-1"))
	require.NotNil(t, store.written)
	assert.Equal(t, "run-1", store.written.RunID)
	assert.True(t, store.written.TotalReturnPct.IsZero())
	assert.True(t, store.written.SharpeRatio.IsZero())
	assert.True(t, store.written.MaxDrawdownPct.IsZero())
	assert.Equal(t, 0, store.written.TradeCount)
}

// ── total return ──────────────────────────────────────────────────────────────

func TestComputeTotalReturnPct(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// equity grows from 1000 to 1100 = 10% return
	store := &mockStore{
		equityPoints: equitySeq("r", base, []string{"1000", "1050", "1100"}),
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	require.NotNil(t, store.written)
	// 10%
	assert.True(t,
		store.written.TotalReturnPct.Equal(d("10")),
		"got %s", store.written.TotalReturnPct,
	)
}

func TestComputeTotalReturnSinglePointIsZero(t *testing.T) {
	base := time.Now()
	store := &mockStore{
		equityPoints: []domain.EquityPoint{{RunID: "r", EventTime: base, Equity: d("1000")}},
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	assert.True(t, store.written.TotalReturnPct.IsZero())
}

// ── max drawdown ──────────────────────────────────────────────────────────────

func TestComputeMaxDrawdown(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// peak at 1200, trough at 900 → drawdown = (1200-900)/1200 = 25%
	store := &mockStore{
		equityPoints: equitySeq("r", base, []string{"1000", "1200", "1100", "900", "1000"}),
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	require.NotNil(t, store.written)
	// 25%
	assert.True(t,
		store.written.MaxDrawdownPct.Equal(d("25")),
		"got %s", store.written.MaxDrawdownPct,
	)
}

func TestComputeMaxDrawdownMonotonicallyRisingIsZero(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	store := &mockStore{
		equityPoints: equitySeq("r", base, []string{"1000", "1100", "1200"}),
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	assert.True(t, store.written.MaxDrawdownPct.IsZero(), "no drawdown on rising equity")
}

// ── annualized return ─────────────────────────────────────────────────────────

func TestComputeAnnualizedReturnApprox(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// 10% over 2 days → annualized ≈ (1.10)^(365/2) - 1 ≈ very large
	store := &mockStore{
		equityPoints: equitySeq("r", base, []string{"1000", "1000", "1100"}),
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	require.NotNil(t, store.written)
	// just assert it's positive and > 10 (annualized amplifies short returns)
	assert.True(t, store.written.AnnualizedReturn.IsPositive(), "annualized return must be positive")
	annF, _ := store.written.AnnualizedReturn.Float64()
	assert.Greater(t, annF, 10.0, "annualized must exceed raw 2-day return")
}

// ── sharpe ratio ──────────────────────────────────────────────────────────────

func TestComputeSharpeRatioPositiveForGrowingEquity(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Consistent 1% daily growth — positive Sharpe
	store := &mockStore{
		equityPoints: equitySeq("r", base, []string{
			"1000", "1010", "1020.1", "1030.301", "1040.604",
		}),
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	assert.True(t, store.written.SharpeRatio.IsPositive(), "Sharpe must be positive for steady gains")
}

func TestComputeSharpeRatioZeroForSinglePoint(t *testing.T) {
	base := time.Now()
	store := &mockStore{
		equityPoints: []domain.EquityPoint{{RunID: "r", EventTime: base, Equity: d("1000")}},
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	assert.True(t, store.written.SharpeRatio.IsZero())
}

// ── trade metrics ─────────────────────────────────────────────────────────────

func TestComputeTradeMetricsOneProfitableRoundTrip(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Buy 1 BTC @ 50000 + 5 fee = cost 50005
	// Sell 1 BTC @ 51000 - 5 fee = revenue 50995
	// P&L = 50995 - 50005 = 990 → win
	store := &mockStore{
		trades: []domain.TradeRecord{
			buyTrade("r", "BTCUSDT", "50000", "1", "5", base),
			sellTrade("r", "BTCUSDT", "51000", "1", "5", base.Add(time.Hour)),
		},
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	require.NotNil(t, store.written)
	assert.Equal(t, 1, store.written.TradeCount)
	assert.True(t, store.written.HitRatePct.Equal(d("100")), "100%% hit rate with one win")
	assert.True(t, store.written.AvgWin.Equal(d("990")), "avg win = 990, got %s", store.written.AvgWin)
	assert.True(t, store.written.AvgLoss.IsZero(), "no losses")
}

func TestComputeTradeMetricsOneLoss(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Buy 1 BTC @ 50000 + 5 fee = cost 50005
	// Sell 1 BTC @ 49000 - 5 fee = revenue 48995
	// P&L = 48995 - 50005 = -1010 → loss
	store := &mockStore{
		trades: []domain.TradeRecord{
			buyTrade("r", "BTCUSDT", "50000", "1", "5", base),
			sellTrade("r", "BTCUSDT", "49000", "1", "5", base.Add(time.Hour)),
		},
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	assert.Equal(t, 1, store.written.TradeCount)
	assert.True(t, store.written.HitRatePct.IsZero(), "0%% hit rate with one loss")
	assert.True(t, store.written.AvgLoss.Equal(d("-1010")), "avg loss = -1010, got %s", store.written.AvgLoss)
}

func TestComputeTradeMetricsHitRateMixed(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Two round trips: one win, one loss → 50% hit rate
	store := &mockStore{
		trades: []domain.TradeRecord{
			buyTrade("r", "BTCUSDT", "50000", "1", "0", base),
			sellTrade("r", "BTCUSDT", "51000", "1", "0", base.Add(time.Hour)), // win +1000
			buyTrade("r", "BTCUSDT", "50000", "1", "0", base.Add(2*time.Hour)),
			sellTrade("r", "BTCUSDT", "49000", "1", "0", base.Add(3*time.Hour)), // loss -1000
		},
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	assert.Equal(t, 2, store.written.TradeCount)
	assert.True(t, store.written.HitRatePct.Equal(d("50")), "50%% hit rate, got %s", store.written.HitRatePct)
}

func TestComputeTradeMetricsNoBuysNoRoundTrips(t *testing.T) {
	base := time.Now()
	// Sells with no matching buys are ignored
	store := &mockStore{
		trades: []domain.TradeRecord{
			sellTrade("r", "BTCUSDT", "50000", "1", "0", base),
		},
	}
	c := metrics.New(store, nopLogger())
	require.NoError(t, c.Compute(context.Background(), "r"))
	assert.Equal(t, 0, store.written.TradeCount)
}

// ── error propagation ─────────────────────────────────────────────────────────

func TestComputeQueryTradesErrorPropagates(t *testing.T) {
	store := &mockStore{tradesErr: errors.New("db error")}
	c := metrics.New(store, nopLogger())
	err := c.Compute(context.Background(), "r")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "db error")
}

func TestComputeQueryEquityErrorPropagates(t *testing.T) {
	store := &mockStore{equityErr: errors.New("equity error")}
	c := metrics.New(store, nopLogger())
	err := c.Compute(context.Background(), "r")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "equity error")
}

func TestComputeWriteMetricsErrorPropagates(t *testing.T) {
	store := &mockStore{writeMetricErr: errors.New("write error")}
	c := metrics.New(store, nopLogger())
	err := c.Compute(context.Background(), "r")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "write error")
}
