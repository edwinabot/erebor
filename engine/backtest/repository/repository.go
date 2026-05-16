package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"
)

// Writer is the narrow interface consumed by ReplayEngine.
// It allows the engine to record data gaps without depending on the full repository.
type Writer interface {
	WriteDataGap(ctx context.Context, runID, symbol string, gapFrom, gapTo time.Time) error
}

// RunStore is the full persistence interface for BacktestRunner and MetricsComputer.
// BacktestRepository satisfies this interface.
type RunStore interface {
	Writer
	CreateRun(ctx context.Context, rec domain.RunRecord) error
	UpdateRunStatus(ctx context.Context, runID string, status domain.RunStatus, startedAt, completedAt *time.Time, errMsg string) error
	WriteTrade(ctx context.Context, trade domain.TradeRecord) error
	WriteEquityPoint(ctx context.Context, point domain.EquityPoint) error
	WriteMetrics(ctx context.Context, m domain.MetricsRecord) error
	QueryTrades(ctx context.Context, runID string) ([]domain.TradeRecord, error)
	QueryEquityPoints(ctx context.Context, runID string) ([]domain.EquityPoint, error)
}

// BacktestRepository persists backtest run lifecycle state and ancillary data
// to the backtest_* tables introduced in 002_backtest_schema.sql.
type BacktestRepository struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// New creates a BacktestRepository backed by the given connection pool.
func New(pool *pgxpool.Pool, logger *zap.Logger) *BacktestRepository {
	return &BacktestRepository{
		pool:   pool,
		logger: logger.With(zap.String("component", "backtest-repository")),
	}
}

// CreateRun inserts a new run record with status PENDING.
func (r *BacktestRepository) CreateRun(ctx context.Context, rec domain.RunRecord) error {
	r.logger.Debug("creating run record",
		zap.String("run_id", rec.RunID),
		zap.Strings("symbols", rec.Symbols),
		zap.Time("from", rec.FromTime),
		zap.Time("to", rec.ToTime),
		zap.String("speed_mode", string(rec.SpeedMode)),
	)

	_, err := r.pool.Exec(ctx, `
		INSERT INTO backtest_runs
		    (run_id, symbols, from_time, to_time, speed_mode, speed_factor, strategy_config, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
	`,
		rec.RunID,
		rec.Symbols,
		rec.FromTime.UTC(),
		rec.ToTime.UTC(),
		string(rec.SpeedMode),
		rec.SpeedFactor,
		rec.StrategyConfig,
		string(rec.Status),
	)
	if err != nil {
		r.logger.Error("failed to create run record",
			zap.String("run_id", rec.RunID),
			zap.Error(err),
		)
		return fmt.Errorf("insert backtest_run %s: %w", rec.RunID, err)
	}

	r.logger.Info("run record created",
		zap.String("run_id", rec.RunID),
		zap.String("status", string(rec.Status)),
	)
	return nil
}

// UpdateRunStatus transitions a run to a new status and optionally records
// started_at, completed_at, and an error message.
func (r *BacktestRepository) UpdateRunStatus(
	ctx context.Context,
	runID string,
	status domain.RunStatus,
	startedAt *time.Time,
	completedAt *time.Time,
	errMsg string,
) error {
	r.logger.Debug("updating run status",
		zap.String("run_id", runID),
		zap.String("status", string(status)),
	)

	var errPtr *string
	if errMsg != "" {
		errPtr = &errMsg
	}

	_, err := r.pool.Exec(ctx, `
		UPDATE backtest_runs
		SET
		    status       = $2,
		    started_at   = COALESCE($3, started_at),
		    completed_at = COALESCE($4, completed_at),
		    error        = COALESCE($5, error)
		WHERE run_id = $1
	`,
		runID,
		string(status),
		startedAt,
		completedAt,
		errPtr,
	)
	if err != nil {
		r.logger.Error("failed to update run status",
			zap.String("run_id", runID),
			zap.String("status", string(status)),
			zap.Error(err),
		)
		return fmt.Errorf("update run status %s → %s: %w", runID, status, err)
	}

	r.logger.Info("run status updated",
		zap.String("run_id", runID),
		zap.String("status", string(status)),
	)
	return nil
}

// WriteDataGap records a detected sequence gap in the source diff data for a symbol.
func (r *BacktestRepository) WriteDataGap(ctx context.Context, runID, symbol string, gapFrom, gapTo time.Time) error {
	r.logger.Debug("writing data gap",
		zap.String("run_id", runID),
		zap.String("symbol", symbol),
		zap.Time("gap_from", gapFrom),
		zap.Time("gap_to", gapTo),
	)

	_, err := r.pool.Exec(ctx, `
		INSERT INTO backtest_data_gaps (run_id, symbol, gap_from, gap_to)
		VALUES ($1, $2, $3, $4)
	`,
		runID,
		symbol,
		gapFrom.UTC(),
		gapTo.UTC(),
	)
	if err != nil {
		r.logger.Error("failed to write data gap",
			zap.String("run_id", runID),
			zap.String("symbol", symbol),
			zap.Error(err),
		)
		return fmt.Errorf("insert data gap for %s/%s: %w", runID, symbol, err)
	}

	r.logger.Info("data gap recorded",
		zap.String("run_id", runID),
		zap.String("symbol", symbol),
		zap.Time("gap_from", gapFrom),
		zap.Time("gap_to", gapTo),
	)
	return nil
}

// WriteTrade inserts a completed fill from erebor-execution into backtest_trades.
// Duplicate trade_ids are silently ignored.
func (r *BacktestRepository) WriteTrade(ctx context.Context, trade domain.TradeRecord) error {
	r.logger.Debug("writing trade",
		zap.String("run_id", trade.RunID),
		zap.String("trade_id", trade.TradeID),
		zap.String("symbol", trade.Symbol),
		zap.String("side", string(trade.Side)),
		zap.Time("event_time", trade.EventTime),
	)

	_, err := r.pool.Exec(ctx, `
		INSERT INTO backtest_trades
		    (run_id, trade_id, symbol, event_time, side, fill_price, fill_qty, fee, signal_name)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NULLIF($9, ''))
		ON CONFLICT (run_id, trade_id) DO NOTHING
	`,
		trade.RunID,
		trade.TradeID,
		trade.Symbol,
		trade.EventTime.UTC(),
		string(trade.Side),
		trade.FillPrice,
		trade.FillQty,
		trade.Fee,
		trade.SignalName,
	)
	if err != nil {
		r.logger.Error("failed to write trade",
			zap.String("run_id", trade.RunID),
			zap.String("trade_id", trade.TradeID),
			zap.Error(err),
		)
		return fmt.Errorf("insert trade %s/%s: %w", trade.RunID, trade.TradeID, err)
	}

	r.logger.Info("trade recorded",
		zap.String("run_id", trade.RunID),
		zap.String("trade_id", trade.TradeID),
		zap.String("symbol", trade.Symbol),
	)
	return nil
}

// WriteEquityPoint appends a timestamped equity snapshot to backtest_equity.
func (r *BacktestRepository) WriteEquityPoint(ctx context.Context, point domain.EquityPoint) error {
	r.logger.Debug("writing equity point",
		zap.String("run_id", point.RunID),
		zap.Time("event_time", point.EventTime),
	)

	_, err := r.pool.Exec(ctx, `
		INSERT INTO backtest_equity (run_id, event_time, equity)
		VALUES ($1, $2, $3)
	`,
		point.RunID,
		point.EventTime.UTC(),
		point.Equity,
	)
	if err != nil {
		r.logger.Error("failed to write equity point",
			zap.String("run_id", point.RunID),
			zap.Error(err),
		)
		return fmt.Errorf("insert equity point for %s at %s: %w", point.RunID, point.EventTime, err)
	}

	r.logger.Debug("equity point recorded",
		zap.String("run_id", point.RunID),
		zap.Time("event_time", point.EventTime),
	)
	return nil
}

// WriteMetrics upserts computed performance metrics for a completed run.
func (r *BacktestRepository) WriteMetrics(ctx context.Context, m domain.MetricsRecord) error {
	r.logger.Debug("writing metrics",
		zap.String("run_id", m.RunID),
		zap.Int("trade_count", m.TradeCount),
	)

	_, err := r.pool.Exec(ctx, `
		INSERT INTO backtest_metrics
		    (run_id, total_return_pct, annualized_return, sharpe_ratio,
		     max_drawdown_pct, hit_rate_pct, avg_win, avg_loss, trade_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (run_id) DO UPDATE SET
		    total_return_pct  = EXCLUDED.total_return_pct,
		    annualized_return = EXCLUDED.annualized_return,
		    sharpe_ratio      = EXCLUDED.sharpe_ratio,
		    max_drawdown_pct  = EXCLUDED.max_drawdown_pct,
		    hit_rate_pct      = EXCLUDED.hit_rate_pct,
		    avg_win           = EXCLUDED.avg_win,
		    avg_loss          = EXCLUDED.avg_loss,
		    trade_count       = EXCLUDED.trade_count,
		    computed_at       = now()
	`,
		m.RunID,
		m.TotalReturnPct,
		m.AnnualizedReturn,
		m.SharpeRatio,
		m.MaxDrawdownPct,
		m.HitRatePct,
		m.AvgWin,
		m.AvgLoss,
		m.TradeCount,
	)
	if err != nil {
		r.logger.Error("failed to write metrics",
			zap.String("run_id", m.RunID),
			zap.Error(err),
		)
		return fmt.Errorf("upsert metrics for %s: %w", m.RunID, err)
	}

	r.logger.Info("metrics written",
		zap.String("run_id", m.RunID),
		zap.Int("trade_count", m.TradeCount),
		zap.String("total_return_pct", m.TotalReturnPct.String()),
		zap.String("sharpe_ratio", m.SharpeRatio.String()),
	)
	return nil
}

// QueryTrades returns all trades for a run ordered by event_time ascending.
func (r *BacktestRepository) QueryTrades(ctx context.Context, runID string) ([]domain.TradeRecord, error) {
	r.logger.Debug("querying trades", zap.String("run_id", runID))

	rows, err := r.pool.Query(ctx, `
		SELECT run_id, trade_id, symbol, event_time, side,
		       fill_price, fill_qty, fee, COALESCE(signal_name, '')
		FROM backtest_trades
		WHERE run_id = $1
		ORDER BY event_time ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("query trades for %s: %w", runID, err)
	}
	defer rows.Close()

	var trades []domain.TradeRecord
	for rows.Next() {
		var t domain.TradeRecord
		var side string
		if scanErr := rows.Scan(
			&t.RunID, &t.TradeID, &t.Symbol, &t.EventTime,
			&side, &t.FillPrice, &t.FillQty, &t.Fee, &t.SignalName,
		); scanErr != nil {
			return nil, fmt.Errorf("scan trade row: %w", scanErr)
		}
		t.Side = domain.Side(side)
		trades = append(trades, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate trade rows for %s: %w", runID, err)
	}

	r.logger.Debug("trades queried", zap.String("run_id", runID), zap.Int("count", len(trades)))
	return trades, nil
}

// QueryEquityPoints returns all equity snapshots for a run ordered by event_time ascending.
func (r *BacktestRepository) QueryEquityPoints(ctx context.Context, runID string) ([]domain.EquityPoint, error) {
	r.logger.Debug("querying equity points", zap.String("run_id", runID))

	rows, err := r.pool.Query(ctx, `
		SELECT run_id, event_time, equity
		FROM backtest_equity
		WHERE run_id = $1
		ORDER BY event_time ASC
	`, runID)
	if err != nil {
		return nil, fmt.Errorf("query equity points for %s: %w", runID, err)
	}
	defer rows.Close()

	var points []domain.EquityPoint
	for rows.Next() {
		var p domain.EquityPoint
		if scanErr := rows.Scan(&p.RunID, &p.EventTime, &p.Equity); scanErr != nil {
			return nil, fmt.Errorf("scan equity row: %w", scanErr)
		}
		points = append(points, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate equity rows for %s: %w", runID, err)
	}

	r.logger.Debug("equity points queried", zap.String("run_id", runID), zap.Int("count", len(points)))
	return points, nil
}
