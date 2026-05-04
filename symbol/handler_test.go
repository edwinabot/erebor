package symbol_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/edwinabot/erebor/ingest/book"
	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/edwinabot/erebor/ingest/symbol"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// MockRepository satisfies repository.Repository with in-memory recording.
type MockRepository struct {
	mu          sync.Mutex
	Diffs       []domain.DiffEvent
	Checkpoints []domain.SnapshotEvent
}

func (m *MockRepository) WriteDiff(_ context.Context, ev domain.DiffEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Diffs = append(m.Diffs, ev)
	return nil
}

func (m *MockRepository) WriteCheckpoint(_ context.Context, snap domain.SnapshotEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Checkpoints = append(m.Checkpoints, snap)
	return nil
}

func (m *MockRepository) QueryNearestCheckpoint(_ context.Context, _ string, _ time.Time) (domain.SnapshotEvent, error) {
	return domain.SnapshotEvent{}, errors.New("not implemented")
}

func (m *MockRepository) QueryDiffs(_ context.Context, _ string, _ time.Time, _ time.Time) ([]domain.DiffEvent, error) {
	return nil, errors.New("not implemented")
}

// MockFetcher returns a queue of snapshots, one per call.
type MockFetcher struct {
	mu        sync.Mutex
	responses []domain.SnapshotEvent
	calls     int
	delay     time.Duration
	hold      chan struct{} // if non-nil, waits on this before returning the first call
}

func (m *MockFetcher) FetchSnapshot(ctx context.Context, sym string, _ int) (domain.SnapshotEvent, error) {
	m.mu.Lock()
	if m.hold != nil && m.calls == 0 {
		hold := m.hold
		m.calls++
		m.mu.Unlock()
		select {
		case <-hold:
		case <-ctx.Done():
			return domain.SnapshotEvent{}, ctx.Err()
		}
		m.mu.Lock()
	} else {
		m.calls++
	}
	if m.delay > 0 {
		m.mu.Unlock()
		time.Sleep(m.delay)
		m.mu.Lock()
	}
	if len(m.responses) == 0 {
		m.mu.Unlock()
		return domain.SnapshotEvent{}, errors.New("no more snapshots configured")
	}
	resp := m.responses[0]
	m.responses = m.responses[1:]
	m.mu.Unlock()
	resp.Symbol = sym
	return resp, nil
}

func (m *MockFetcher) callCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls
}

func dec(s string) decimal.Decimal { return decimal.RequireFromString(s) }

func snap(lastID int64) domain.SnapshotEvent {
	return domain.SnapshotEvent{
		LastUpdateID: lastID,
		CapturedAt:   time.Now(),
		Bids:         []domain.PriceLevel{{Price: dec("100"), Quantity: dec("1")}},
		Asks:         []domain.PriceLevel{{Price: dec("101"), Quantity: dec("1")}},
	}
}

func diff(first, final int64) domain.DiffEvent {
	return domain.DiffEvent{
		Symbol:        "BTCUSDT",
		EventTime:     time.Now(),
		FirstUpdateID: first,
		FinalUpdateID: final,
		Bids:          []domain.PriceLevel{{Price: dec("100"), Quantity: dec("1.1")}},
		Asks:          []domain.PriceLevel{{Price: dec("101"), Quantity: dec("0.9")}},
	}
}

func newHandler(t *testing.T, fetcher *MockFetcher, repo *MockRepository, maxBuf int) (*symbol.Handler, *book.Book) {
	t.Helper()
	logger := zap.NewNop()
	ob := book.New("BTCUSDT")
	h := symbol.NewHandler(symbol.Config{
		Symbol:                  "BTCUSDT",
		DepthLimit:              50,
		CheckpointInterval:      time.Hour,
		CheckpointDiffThreshold: 1_000_000,
		MaxBufferSize:           maxBuf,
	}, ob, fetcher, repo, logger)
	return h, ob
}

func waitFor(t *testing.T, cond func() bool, timeout time.Duration, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// TestBootstrap_AlignmentEventArrivesBeforeSnapshot:
// Diffs are buffered while the snapshot is in-flight; once the snapshot
// returns, the buffered events past the alignment boundary are replayed.
func TestBootstrap_AlignmentEventArrivesBeforeSnapshot(t *testing.T) {
	hold := make(chan struct{})
	mf := &MockFetcher{
		responses: []domain.SnapshotEvent{snap(100)},
		hold:      hold,
	}
	repo := &MockRepository{}
	h, _ := newHandler(t, mf, repo, 10)

	h.Start(context.Background())

	// Buffer events that bracket the snapshot's lastUpdateID (100).
	// Discarded:    final=99 <= 100
	// Alignment:    first=100, final=101  (U <= 101 AND u >= 101)
	// Replay:       first=102, final=103
	h.HandleDiff(diff(95, 99))
	h.HandleDiff(diff(100, 101))
	h.HandleDiff(diff(102, 103))

	require.Equal(t, symbol.Bootstrapping, h.State())
	close(hold) // Release snapshot.

	waitFor(t, func() bool { return h.State() == symbol.Synced }, time.Second, "transition to Synced")

	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Len(t, repo.Diffs, 2, "alignment + post-alignment diff written")
	require.Equal(t, int64(101), repo.Diffs[0].FinalUpdateID)
	require.Equal(t, int64(103), repo.Diffs[1].FinalUpdateID)
}

// TestBootstrap_AlignmentExactBoundary:
// A diff whose FirstUpdateID == snapshot.LastUpdateID+1 is the alignment
// event (boundary case U == lastUpdateID+1, u == lastUpdateID+1).
func TestBootstrap_AlignmentExactBoundary(t *testing.T) {
	hold := make(chan struct{})
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100)}, hold: hold}
	repo := &MockRepository{}
	h, _ := newHandler(t, mf, repo, 10)

	h.Start(context.Background())
	h.HandleDiff(diff(101, 101))
	close(hold)

	waitFor(t, func() bool { return h.State() == symbol.Synced }, time.Second, "Synced")

	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Len(t, repo.Diffs, 1)
	require.Equal(t, int64(101), repo.Diffs[0].FirstUpdateID)
	require.Equal(t, int64(101), repo.Diffs[0].FinalUpdateID)
}

// TestBootstrap_DiscardsEventsBeforeSnapshot:
// Events with FinalUpdateID <= snapshot.LastUpdateID must be discarded.
func TestBootstrap_DiscardsEventsBeforeSnapshot(t *testing.T) {
	hold := make(chan struct{})
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100)}, hold: hold}
	repo := &MockRepository{}
	h, _ := newHandler(t, mf, repo, 10)

	h.Start(context.Background())
	// All discarded.
	h.HandleDiff(diff(50, 60))
	h.HandleDiff(diff(70, 90))
	h.HandleDiff(diff(95, 100))
	// Alignment.
	h.HandleDiff(diff(100, 102))
	close(hold)

	waitFor(t, func() bool { return h.State() == symbol.Synced }, time.Second, "Synced")

	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Len(t, repo.Diffs, 1)
	require.Equal(t, int64(102), repo.Diffs[0].FinalUpdateID)
}

// TestBootstrap_BufferOverflowTriggersResnapshot:
// If the buffer exceeds MaxBufferSize while waiting for the alignment
// event, the handler must re-fetch the snapshot.
func TestBootstrap_BufferOverflowTriggersResnapshot(t *testing.T) {
	// First snapshot has lastUpdateID=10 — the first batch of events all
	// fall before it, so no alignment is found. After overflow, second
	// snapshot returns lastUpdateID=20 and aligns with the next event.
	mf := &MockFetcher{
		responses: []domain.SnapshotEvent{snap(10), snap(20)},
	}
	repo := &MockRepository{}
	h, _ := newHandler(t, mf, repo, 3)

	h.Start(context.Background())

	// First diff triggers the transition out of DISCONNECTED and kicks off
	// the snapshot fetch. All four have FirstUpdateID > 10 (so they survive
	// the pre-snapshot discard) and none align with lastUpdateID=10.
	h.HandleDiff(diff(50, 51))
	waitFor(t, func() bool { return mf.callCount() >= 1 }, time.Second, "first snapshot fetch")
	h.HandleDiff(diff(52, 53))
	h.HandleDiff(diff(54, 55))
	h.HandleDiff(diff(56, 57)) // overflow → re-fetch

	waitFor(t, func() bool { return mf.callCount() >= 2 }, time.Second, "second snapshot fetch")

	// After re-snapshot (lastUpdateID=20), provide an aligning event.
	h.HandleDiff(diff(21, 22))
	waitFor(t, func() bool { return h.State() == symbol.Synced }, time.Second, "Synced after re-snapshot")
}
