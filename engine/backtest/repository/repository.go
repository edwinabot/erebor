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

// WriteTrade is a stub — wired when erebor-execution ships.
func (r *BacktestRepository) WriteTrade(_ context.Context, _ string) error {
	return nil
}

// WriteEquityPoint is a stub — wired when erebor-execution ships.
func (r *BacktestRepository) WriteEquityPoint(_ context.Context, _ string) error {
	return nil
}

// WriteMetrics is a stub — wired when erebor-execution ships.
func (r *BacktestRepository) WriteMetrics(_ context.Context, _ string) error {
	return nil
}
