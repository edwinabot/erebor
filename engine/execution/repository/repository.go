package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/edwinabot/erebor/execution/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// TradeRecord represents a completed fill to persist.
type TradeRecord struct {
	SessionID      string
	TradeID        string
	Symbol         string
	EventTime      time.Time
	Side           string
	FillPrice      decimal.Decimal
	FillQty        decimal.Decimal
	Fee            decimal.Decimal
	RealisedPnL    decimal.Decimal
	SignalName     string
	SignalStreamID string
}

// PositionRecord represents the latest known position for a symbol.
type PositionRecord struct {
	SessionID string
	Symbol    string
	NetQty    decimal.Decimal
	AvgEntry  decimal.Decimal
	UpdatedAt time.Time
}

// EquityRecord is a timestamped equity snapshot.
type EquityRecord struct {
	SessionID string
	EventTime time.Time
	Equity    decimal.Decimal
}

// Store is the narrow interface consumed by session.Manager and blotter.Blotter.
type Store interface {
	CreateSession(ctx context.Context, s domain.PaperSession) error
	UpdateSessionStatus(ctx context.Context, sessionID string, status domain.SessionStatus, stoppedAt *time.Time, errMsg string) error
	LoadRunningSession(ctx context.Context) (*domain.PaperSession, error)
	LoadLatestSession(ctx context.Context) (*domain.PaperSession, error)
	LoadPositions(ctx context.Context, sessionID string) ([]PositionRecord, error)
	LoadLatestEquity(ctx context.Context, sessionID string) (decimal.Decimal, error)
	RecordFill(ctx context.Context, trade TradeRecord, pos PositionRecord, eq EquityRecord) error
}

// Repository persists paper trading state to the paper_* tables.
type Repository struct {
	pool   *pgxpool.Pool
	logger *zap.Logger
}

// New creates a Repository backed by the given connection pool.
func New(pool *pgxpool.Pool, logger *zap.Logger) *Repository {
	return &Repository{
		pool:   pool,
		logger: logger.With(zap.String("component", "paper-repository")),
	}
}

// CreateSession inserts a new paper_sessions row.
func (r *Repository) CreateSession(ctx context.Context, s domain.PaperSession) error {
	stratJSON, err := json.Marshal(json.RawMessage(s.StrategyConfig))
	if err != nil {
		return fmt.Errorf("marshal strategy_config: %w", err)
	}
	_, err = r.pool.Exec(ctx, `
		INSERT INTO paper_sessions (session_id, status, symbols, strategy_config, started_at)
		VALUES ($1, $2, $3, $4::jsonb, $5)`,
		s.SessionID, string(s.Status), s.Symbols, string(stratJSON), s.StartedAt,
	)
	if err != nil {
		return fmt.Errorf("insert paper_session: %w", err)
	}
	r.logger.Info("paper session created", zap.String("session_id", s.SessionID))
	return nil
}

// UpdateSessionStatus updates status and optionally stopped_at / error.
func (r *Repository) UpdateSessionStatus(ctx context.Context, sessionID string, status domain.SessionStatus, stoppedAt *time.Time, errMsg string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE paper_sessions
		SET status=$2, stopped_at=$3, error=$4
		WHERE session_id=$1`,
		sessionID, string(status), stoppedAt, errMsg,
	)
	if err != nil {
		return fmt.Errorf("update session status: %w", err)
	}
	r.logger.Info("paper session status updated",
		zap.String("session_id", sessionID),
		zap.String("status", string(status)),
	)
	return nil
}

// LoadRunningSession returns the most recently started RUNNING session, or nil if none.
func (r *Repository) LoadRunningSession(ctx context.Context) (*domain.PaperSession, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT session_id, status, symbols, strategy_config::text, started_at, stopped_at, error
		FROM paper_sessions
		WHERE status = 'RUNNING'
		ORDER BY started_at DESC
		LIMIT 1`)

	var s domain.PaperSession
	var stoppedAt *time.Time
	err := row.Scan(&s.SessionID, &s.Status, &s.Symbols, &s.StrategyConfig,
		&s.StartedAt, &stoppedAt, &s.Error)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load running session: %w", err)
	}
	s.StoppedAt = stoppedAt
	return &s, nil
}

// LoadLatestSession returns the most recently started session regardless of status, or nil if none.
func (r *Repository) LoadLatestSession(ctx context.Context) (*domain.PaperSession, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT session_id, status, symbols, strategy_config::text, started_at, stopped_at, error
		FROM paper_sessions
		ORDER BY started_at DESC
		LIMIT 1`)

	var s domain.PaperSession
	var stoppedAt *time.Time
	err := row.Scan(&s.SessionID, &s.Status, &s.Symbols, &s.StrategyConfig,
		&s.StartedAt, &stoppedAt, &s.Error)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load latest session: %w", err)
	}
	s.StoppedAt = stoppedAt
	return &s, nil
}

// LoadLatestEquity returns the most recent equity value for sessionID.
// Returns decimal.Zero if no equity records exist yet.
func (r *Repository) LoadLatestEquity(ctx context.Context, sessionID string) (decimal.Decimal, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT equity FROM paper_equity
		WHERE session_id=$1
		ORDER BY event_time DESC
		LIMIT 1`, sessionID)

	var equity decimal.Decimal
	if err := row.Scan(&equity); err == pgx.ErrNoRows {
		return decimal.Zero, nil
	} else if err != nil {
		return decimal.Zero, fmt.Errorf("load latest equity: %w", err)
	}
	return equity, nil
}

// LoadPositions returns all position records for the given session.
func (r *Repository) LoadPositions(ctx context.Context, sessionID string) ([]PositionRecord, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT session_id, symbol, net_qty, avg_entry, updated_at
		FROM paper_positions
		WHERE session_id=$1`, sessionID)
	if err != nil {
		return nil, fmt.Errorf("load positions: %w", err)
	}
	defer rows.Close()

	var out []PositionRecord
	for rows.Next() {
		var rec PositionRecord
		if err := rows.Scan(&rec.SessionID, &rec.Symbol, &rec.NetQty, &rec.AvgEntry, &rec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan position: %w", err)
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// RecordFill persists a trade, upserts the position, and appends an equity point
// in a single transaction. The UNIQUE constraint on (session_id, signal_stream_id)
// makes this idempotent on XREADGROUP redelivery.
func (r *Repository) RecordFill(ctx context.Context, trade TradeRecord, pos PositionRecord, eq EquityRecord) error {
	return r.pool.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}
		defer func() { _ = tx.Rollback(ctx) }()

		_, err = tx.Exec(ctx, `
			INSERT INTO paper_trades
			    (session_id, trade_id, symbol, event_time, side, fill_price,
			     fill_qty, fee, realised_pnl, signal_name, signal_stream_id)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (session_id, signal_stream_id) DO NOTHING`,
			trade.SessionID, trade.TradeID, trade.Symbol, trade.EventTime,
			trade.Side, trade.FillPrice, trade.FillQty, trade.Fee,
			trade.RealisedPnL, trade.SignalName, trade.SignalStreamID,
		)
		if err != nil {
			return fmt.Errorf("insert trade: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO paper_positions (session_id, symbol, net_qty, avg_entry, updated_at)
			VALUES ($1,$2,$3,$4,$5)
			ON CONFLICT (session_id, symbol) DO UPDATE
			    SET net_qty=$3, avg_entry=$4, updated_at=$5`,
			pos.SessionID, pos.Symbol, pos.NetQty, pos.AvgEntry, pos.UpdatedAt,
		)
		if err != nil {
			return fmt.Errorf("upsert position: %w", err)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO paper_equity (session_id, event_time, equity)
			VALUES ($1,$2,$3)`,
			eq.SessionID, eq.EventTime, eq.Equity,
		)
		if err != nil {
			return fmt.Errorf("insert equity: %w", err)
		}

		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit: %w", err)
		}
		r.logger.Debug("fill recorded",
			zap.String("trade_id", trade.TradeID),
			zap.String("symbol", trade.Symbol),
			zap.String("side", trade.Side),
			zap.String("fill_price", trade.FillPrice.String()),
			zap.String("equity", eq.Equity.String()),
		)
		return nil
	})
}
