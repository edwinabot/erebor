package metrics

import (
	"context"
	"fmt"
	"math"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// store is the narrow persistence interface required by Computer.
// Satisfied by repository.BacktestRepository and repository.RunStore.
type store interface {
	QueryTrades(ctx context.Context, runID string) ([]domain.TradeRecord, error)
	QueryEquityPoints(ctx context.Context, runID string) ([]domain.EquityPoint, error)
	WriteMetrics(ctx context.Context, m domain.MetricsRecord) error
}

// Computer reads trades and equity series for a completed run and persists
// performance metrics to backtest_metrics.
type Computer struct {
	repo   store
	logger *zap.Logger
}

// New creates a Computer backed by the given store.
func New(repo store, logger *zap.Logger) *Computer {
	return &Computer{
		repo:   repo,
		logger: logger.With(zap.String("component", "metrics-computer")),
	}
}

// Compute reads trades and equity for runID, computes all metrics, and writes
// them to the store. Returns nil on success, including when trade/equity tables
// are empty (metrics are written as zero values in that case).
func (c *Computer) Compute(ctx context.Context, runID string) error {
	c.logger.Info("computing metrics", zap.String("run_id", runID))

	trades, err := c.repo.QueryTrades(ctx, runID)
	if err != nil {
		return fmt.Errorf("query trades for %s: %w", runID, err)
	}
	equity, err := c.repo.QueryEquityPoints(ctx, runID)
	if err != nil {
		return fmt.Errorf("query equity for %s: %w", runID, err)
	}

	m := computeMetrics(runID, trades, equity)

	c.logger.Info("metrics computed",
		zap.String("run_id", runID),
		zap.Int("trade_count", m.TradeCount),
		zap.String("total_return_pct", m.TotalReturnPct.String()),
		zap.String("sharpe_ratio", m.SharpeRatio.String()),
		zap.String("max_drawdown_pct", m.MaxDrawdownPct.String()),
	)

	if err := c.repo.WriteMetrics(ctx, m); err != nil {
		return fmt.Errorf("write metrics for %s: %w", runID, err)
	}
	return nil
}

func computeMetrics(runID string, trades []domain.TradeRecord, equity []domain.EquityPoint) domain.MetricsRecord {
	m := domain.MetricsRecord{RunID: runID}
	m.TotalReturnPct, m.AnnualizedReturn, m.SharpeRatio, m.MaxDrawdownPct = equityMetrics(equity)
	m.HitRatePct, m.AvgWin, m.AvgLoss, m.TradeCount = tradeMetrics(trades)
	return m
}

// equityMetrics derives total_return_pct, annualized_return, sharpe_ratio, and
// max_drawdown_pct from an equity time-series. Returns zero values when the
// series has fewer than two points or starts at zero.
func equityMetrics(equity []domain.EquityPoint) (totalReturn, annualized, sharpe, maxDD decimal.Decimal) {
	if len(equity) < 2 {
		return
	}
	first := equity[0].Equity
	if first.IsZero() {
		return
	}
	last := equity[len(equity)-1].Equity

	hundred := decimal.NewFromInt(100)
	totalReturn = last.Sub(first).Div(first).Mul(hundred)

	// Annualized return: (1 + r)^(365/days) - 1
	days := equity[len(equity)-1].EventTime.Sub(equity[0].EventTime).Hours() / 24
	if days >= 1 {
		baseF, _ := decimal.NewFromInt(1).Add(totalReturn.Div(hundred)).Float64()
		annF := (math.Pow(baseF, 365/days) - 1) * 100
		annualized = decimal.NewFromFloat(annF)
	} else {
		annualized = totalReturn
	}

	// Max drawdown: max (peak - trough) / peak across the series.
	peak := equity[0].Equity
	for _, e := range equity {
		if e.Equity.GreaterThan(peak) {
			peak = e.Equity
		}
		if peak.IsPositive() {
			dd := peak.Sub(e.Equity).Div(peak).Mul(hundred)
			if dd.GreaterThan(maxDD) {
				maxDD = dd
			}
		}
	}

	// Sharpe ratio (annualised, risk-free rate = 0) from consecutive returns.
	var rets []float64
	for i := 1; i < len(equity); i++ {
		if equity[i-1].Equity.IsZero() {
			continue
		}
		r, _ := equity[i].Equity.Sub(equity[i-1].Equity).Div(equity[i-1].Equity).Float64()
		rets = append(rets, r)
	}
	sharpe = sharpeRatio(rets)

	return
}

// sharpeRatio computes the annualised Sharpe ratio (risk-free = 0) from a slice
// of period returns. Returns zero when there are fewer than two returns or the
// standard deviation is zero.
func sharpeRatio(returns []float64) decimal.Decimal {
	if len(returns) < 2 {
		return decimal.Zero
	}
	var sum float64
	for _, r := range returns {
		sum += r
	}
	mean := sum / float64(len(returns))

	var variance float64
	for _, r := range returns {
		d := r - mean
		variance += d * d
	}
	variance /= float64(len(returns) - 1)
	std := math.Sqrt(variance)
	if std == 0 {
		return decimal.Zero
	}
	return decimal.NewFromFloat((mean / std) * math.Sqrt(252))
}

// tradeMetrics pairs BUY and SELL trades FIFO per symbol to derive round-trip
// P&L, then computes hit_rate_pct, avg_win, avg_loss, and trade_count.
// Unmatched buys (no closing sell) are left open and excluded from metrics.
func tradeMetrics(trades []domain.TradeRecord) (hitRate, avgWin, avgLoss decimal.Decimal, tradeCount int) {
	type openPos struct{ cost decimal.Decimal }
	open := make(map[string][]openPos) // symbol → FIFO queue

	var wins, losses []decimal.Decimal

	for _, t := range trades {
		switch t.Side {
		case domain.SideBuy:
			cost := t.FillQty.Mul(t.FillPrice).Add(t.Fee)
			open[t.Symbol] = append(open[t.Symbol], openPos{cost: cost})
		case domain.SideSell:
			if len(open[t.Symbol]) == 0 {
				continue
			}
			pos := open[t.Symbol][0]
			open[t.Symbol] = open[t.Symbol][1:]
			revenue := t.FillQty.Mul(t.FillPrice).Sub(t.Fee)
			pnl := revenue.Sub(pos.cost)
			if pnl.IsPositive() {
				wins = append(wins, pnl)
			} else {
				losses = append(losses, pnl)
			}
		}
	}

	total := len(wins) + len(losses)
	if total == 0 {
		return decimal.Zero, decimal.Zero, decimal.Zero, 0
	}

	tradeCount = total
	hundred := decimal.NewFromInt(100)
	hitRate = decimal.NewFromInt(int64(len(wins))).Div(decimal.NewFromInt(int64(total))).Mul(hundred)

	if len(wins) > 0 {
		avgWin = sumDecimals(wins).Div(decimal.NewFromInt(int64(len(wins))))
	}
	if len(losses) > 0 {
		avgLoss = sumDecimals(losses).Div(decimal.NewFromInt(int64(len(losses))))
	}

	return
}

func sumDecimals(ds []decimal.Decimal) decimal.Decimal {
	sum := decimal.Zero
	for _, d := range ds {
		sum = sum.Add(d)
	}
	return sum
}
