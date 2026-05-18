// Package session manages the paper trading session lifecycle.
package session

import (
	"context"
	"fmt"
	"time"

	"github.com/edwinabot/erebor/execution/domain"
	"github.com/edwinabot/erebor/execution/repository"
	"github.com/google/uuid"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// SessionStore is the narrow DB interface required by Manager.
type SessionStore interface {
	CreateSession(ctx context.Context, s domain.PaperSession) error
	UpdateSessionStatus(ctx context.Context, sessionID string, status domain.SessionStatus, stoppedAt *time.Time, errMsg string) error
	LoadRunningSession(ctx context.Context) (*domain.PaperSession, error)
	LoadLatestSession(ctx context.Context) (*domain.PaperSession, error)
	LoadPositions(ctx context.Context, sessionID string) ([]repository.PositionRecord, error)
	LoadLatestEquity(ctx context.Context, sessionID string) (decimal.Decimal, error)
}

// StartResult holds the outcome of Manager.Start.
type StartResult struct {
	Session   *domain.PaperSession
	Positions []repository.PositionRecord
	Equity    decimal.Decimal // last known equity; zero for new sessions
	Recovered bool            // true if an existing session was resumed
}

// Manager creates and tracks paper trading session state.
type Manager struct {
	store   SessionStore
	logger  *zap.Logger
	session *domain.PaperSession
}

// NewManager creates a Manager backed by store.
func NewManager(store SessionStore, logger *zap.Logger) *Manager {
	return &Manager{
		store:  store,
		logger: logger.With(zap.String("component", "session-manager")),
	}
}

// Start creates a new session or resumes an existing RUNNING session.
// Returns an error if a HALTED session is found (operator must clear halt before trading).
func (m *Manager) Start(ctx context.Context, symbols []string, strategyConfig string) (StartResult, error) {
	// Check for an existing RUNNING session first.
	running, err := m.store.LoadRunningSession(ctx)
	if err != nil {
		return StartResult{}, fmt.Errorf("load running session: %w", err)
	}
	if running != nil {
		return m.resume(ctx, running)
	}

	// Check for a HALTED session — do not auto-start a new session in this case.
	latest, err := m.store.LoadLatestSession(ctx)
	if err != nil {
		return StartResult{}, fmt.Errorf("load latest session: %w", err)
	}
	if latest != nil && latest.Status == domain.SessionHalted {
		m.logger.Warn("found halted session; refusing to start new session",
			zap.String("session_id", latest.SessionID),
		)
		return StartResult{}, fmt.Errorf("session %s is halted; clear halt before starting", latest.SessionID)
	}

	return m.createNew(ctx, symbols, strategyConfig)
}

func (m *Manager) resume(ctx context.Context, s *domain.PaperSession) (StartResult, error) {
	positions, err := m.store.LoadPositions(ctx, s.SessionID)
	if err != nil {
		return StartResult{}, fmt.Errorf("load positions for session %s: %w", s.SessionID, err)
	}
	equity, err := m.store.LoadLatestEquity(ctx, s.SessionID)
	if err != nil {
		return StartResult{}, fmt.Errorf("load equity for session %s: %w", s.SessionID, err)
	}

	m.session = s
	m.logger.Info("resumed paper session",
		zap.String("session_id", s.SessionID),
		zap.String("equity", equity.String()),
		zap.Int("positions", len(positions)),
	)
	return StartResult{Session: s, Positions: positions, Equity: equity, Recovered: true}, nil
}

func (m *Manager) createNew(ctx context.Context, symbols []string, strategyConfig string) (StartResult, error) {
	id, err := uuid.NewV7()
	if err != nil {
		id = uuid.New()
	}
	s := domain.PaperSession{
		SessionID:      id.String(),
		Status:         domain.SessionRunning,
		Symbols:        symbols,
		StrategyConfig: strategyConfig,
		StartedAt:      time.Now().UTC(),
	}
	if err := m.store.CreateSession(ctx, s); err != nil {
		return StartResult{}, fmt.Errorf("create session: %w", err)
	}

	m.session = &s
	m.logger.Info("created new paper session",
		zap.String("session_id", s.SessionID),
		zap.Strings("symbols", symbols),
	)
	return StartResult{Session: &s, Equity: decimal.Zero, Recovered: false}, nil
}

// Stop marks the session as STOPPED.
func (m *Manager) Stop(ctx context.Context) error {
	if m.session == nil {
		return nil
	}
	now := time.Now().UTC()
	if err := m.store.UpdateSessionStatus(ctx, m.session.SessionID, domain.SessionStopped, &now, ""); err != nil {
		return fmt.Errorf("update session status to STOPPED: %w", err)
	}
	m.logger.Info("paper session stopped", zap.String("session_id", m.session.SessionID))
	return nil
}

// Halt marks the session as HALTED with the given reason.
func (m *Manager) Halt(ctx context.Context, reason string) error {
	if m.session == nil {
		return nil
	}
	now := time.Now().UTC()
	if err := m.store.UpdateSessionStatus(ctx, m.session.SessionID, domain.SessionHalted, &now, reason); err != nil {
		return fmt.Errorf("update session status to HALTED: %w", err)
	}
	m.logger.Warn("paper session halted",
		zap.String("session_id", m.session.SessionID),
		zap.String("reason", reason),
	)
	return nil
}
