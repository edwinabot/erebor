package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/edwinabot/erebor/execution/domain"
	"github.com/edwinabot/erebor/execution/repository"
	"github.com/edwinabot/erebor/execution/session"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const testExistingSessionID = "existing-sess"

// memSessionStore is an in-memory SessionStore for tests.
type memSessionStore struct {
	sessions  []*domain.PaperSession
	positions map[string][]repository.PositionRecord
	equities  map[string]decimal.Decimal
	statusLog []string
}

func newMemSessionStore() *memSessionStore {
	return &memSessionStore{
		positions: make(map[string][]repository.PositionRecord),
		equities:  make(map[string]decimal.Decimal),
	}
}

func (m *memSessionStore) CreateSession(_ context.Context, s domain.PaperSession) error {
	m.sessions = append(m.sessions, &s)
	return nil
}

func (m *memSessionStore) UpdateSessionStatus(_ context.Context, sessionID string, status domain.SessionStatus, _ *time.Time, _ string) error {
	m.statusLog = append(m.statusLog, sessionID+":"+string(status))
	for _, s := range m.sessions {
		if s.SessionID == sessionID {
			s.Status = status
		}
	}
	return nil
}

func (m *memSessionStore) LoadRunningSession(_ context.Context) (*domain.PaperSession, error) {
	for i := len(m.sessions) - 1; i >= 0; i-- {
		if m.sessions[i].Status == domain.SessionRunning {
			return m.sessions[i], nil
		}
	}
	return nil, nil
}

func (m *memSessionStore) LoadLatestSession(_ context.Context) (*domain.PaperSession, error) {
	if len(m.sessions) == 0 {
		return nil, nil
	}
	return m.sessions[len(m.sessions)-1], nil
}

func (m *memSessionStore) LoadPositions(_ context.Context, sessionID string) ([]repository.PositionRecord, error) {
	return m.positions[sessionID], nil
}

func (m *memSessionStore) LoadLatestEquity(_ context.Context, sessionID string) (decimal.Decimal, error) {
	if eq, ok := m.equities[sessionID]; ok {
		return eq, nil
	}
	return decimal.Zero, nil
}

func (m *memSessionStore) RecordFill(_ context.Context, _ repository.TradeRecord, _ repository.PositionRecord, _ repository.EquityRecord) error {
	return nil
}

func TestManagerCreatesNewSessionWhenNoneExists(t *testing.T) {
	store := newMemSessionStore()
	mgr := session.NewManager(store, zap.NewNop())

	result, err := mgr.Start(context.Background(), []string{"BTCUSDT"}, `{"initial_capital":"10000"}`)
	require.NoError(t, err)

	assert.NotEmpty(t, result.Session.SessionID)
	assert.Equal(t, domain.SessionRunning, result.Session.Status)
	assert.False(t, result.Recovered)
	assert.True(t, result.Equity.IsZero())
	assert.Len(t, store.sessions, 1)
}

func TestManagerResumesExistingRunningSession(t *testing.T) {
	store := newMemSessionStore()
	existing := domain.PaperSession{
		SessionID:      testExistingSessionID,
		Status:         domain.SessionRunning,
		Symbols:        []string{"BTCUSDT"},
		StrategyConfig: `{"initial_capital":"9500"}`,
		StartedAt:      time.Now().Add(-time.Hour),
	}
	store.sessions = append(store.sessions, &existing)
	store.positions[testExistingSessionID] = []repository.PositionRecord{
		{SessionID: testExistingSessionID, Symbol: "BTCUSDT", NetQty: decimal.RequireFromString("0.001"), AvgEntry: decimal.RequireFromString("50000")},
	}
	store.equities[testExistingSessionID] = decimal.RequireFromString("9850")

	mgr := session.NewManager(store, zap.NewNop())
	result, err := mgr.Start(context.Background(), []string{"BTCUSDT"}, `{"initial_capital":"9500"}`)
	require.NoError(t, err)

	assert.True(t, result.Recovered)
	assert.Equal(t, testExistingSessionID, result.Session.SessionID)
	assert.Equal(t, "9850", result.Equity.String())
	assert.Len(t, result.Positions, 1)
	assert.Len(t, store.sessions, 1) // no new session created
}

func TestManagerRefusesToStartIfHaltedSessionExists(t *testing.T) {
	store := newMemSessionStore()
	store.sessions = append(store.sessions, &domain.PaperSession{
		SessionID: "halted-sess",
		Status:    domain.SessionHalted,
		StartedAt: time.Now().Add(-time.Hour),
	})

	mgr := session.NewManager(store, zap.NewNop())
	_, err := mgr.Start(context.Background(), []string{"BTCUSDT"}, `{}`)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "halted")
}

func TestManagerStopsSession(t *testing.T) {
	store := newMemSessionStore()
	mgr := session.NewManager(store, zap.NewNop())

	result, err := mgr.Start(context.Background(), []string{"BTCUSDT"}, `{}`)
	require.NoError(t, err)

	require.NoError(t, mgr.Stop(context.Background()))

	assert.Contains(t, store.statusLog, result.Session.SessionID+":STOPPED")
}

func TestManagerHaltSession(t *testing.T) {
	store := newMemSessionStore()
	mgr := session.NewManager(store, zap.NewNop())

	result, err := mgr.Start(context.Background(), []string{"BTCUSDT"}, `{}`)
	require.NoError(t, err)

	require.NoError(t, mgr.Halt(context.Background(), "drawdown limit exceeded"))

	assert.Contains(t, store.statusLog, result.Session.SessionID+":HALTED")
}
