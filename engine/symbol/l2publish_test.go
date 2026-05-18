package symbol_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/edwinabot/erebor/ingest/symbol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// mockL2Publisher records published events for assertion.
type mockL2Publisher struct {
	mu     sync.Mutex
	events []publishedL2
	errOn  int // return error on the Nth call (1-indexed); 0 = never
	calls  int
}

type publishedL2 struct {
	runID        string
	symbol       string
	eventTime    time.Time
	lastUpdateID int64
	bids         []domain.PriceLevel
	asks         []domain.PriceLevel
}

func (m *mockL2Publisher) Publish(_ context.Context, runID, sym string, et time.Time, uid int64, bids, asks []domain.PriceLevel) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.errOn > 0 && m.calls == m.errOn {
		return assert.AnError
	}
	m.events = append(m.events, publishedL2{runID: runID, symbol: sym, eventTime: et, lastUpdateID: uid, bids: bids, asks: asks})
	return nil
}

func (m *mockL2Publisher) published() []publishedL2 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]publishedL2, len(m.events))
	copy(out, m.events)
	return out
}

// newSyncedHandler bootstraps a Handler to Synced state with the given L2Publisher.
func newSyncedHandler(t *testing.T, l2pub symbol.L2EventPublisher) *symbol.Handler {
	t.Helper()
	repo := &MockRepository{}
	fetcher := &MockFetcher{responses: []domain.SnapshotEvent{snap(10)}}
	ob := newMockBook(10)
	cfg := symbol.Config{Symbol: "BTCUSDT", DepthLimit: 5, CheckpointDiffThreshold: 1000}
	opts := []func(*symbol.Handler){}
	if l2pub != nil {
		opts = append(opts, symbol.WithL2Publisher(l2pub))
	}
	h := symbol.NewHandler(cfg, ob, fetcher, repo, zap.NewNop(), opts...)
	h.Start(context.Background())

	// Drive to Synced: send an initial diff to kick off bootstrap, then the aligning diff.
	h.HandleDiff(makeDiff(1, 10))     // triggers fetch
	time.Sleep(30 * time.Millisecond) // let snapshot arrive
	h.HandleDiff(makeDiff(11, 11))    // aligns with snapshot (firstID=11 ≤ lastSnapID+1=11)
	time.Sleep(30 * time.Millisecond) // let state transition settle

	require.Equal(t, symbol.Synced, h.State(), "handler must be Synced before test assertions")
	return h
}

// newMockBook is a minimal book that satisfies book.OrderBook and the LoadSnapshot interface.
type mockBook struct {
	depth    int
	lastUID  int64
	mu       sync.Mutex
	bids     []domain.PriceLevel
	asks     []domain.PriceLevel
}

func newMockBook(lastUID int64) *mockBook {
	return &mockBook{
		lastUID: lastUID,
		depth:   5,
		bids:    []domain.PriceLevel{{Price: dec("100"), Quantity: dec("1")}},
		asks:    []domain.PriceLevel{{Price: dec("101"), Quantity: dec("1")}},
	}
}

func (b *mockBook) Apply(ev domain.DiffEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastUID = ev.FinalUpdateID
	return nil
}

func (b *mockBook) Snapshot(depth int) domain.SnapshotEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	return domain.SnapshotEvent{
		Symbol:       "BTCUSDT",
		CapturedAt:   time.Now(),
		LastUpdateID: b.lastUID,
		Bids:         b.bids,
		Asks:         b.asks,
	}
}

func (b *mockBook) LoadSnapshot(s domain.SnapshotEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastUID = s.LastUpdateID
}

func (b *mockBook) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lastUID = 0
}

func (b *mockBook) LastUpdateID() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastUID
}

func makeDiff(firstID, finalID int64) domain.DiffEvent {
	return domain.DiffEvent{
		Symbol:        "BTCUSDT",
		EventTime:     time.Now(),
		FirstUpdateID: firstID,
		FinalUpdateID: finalID,
		Bids:          []domain.PriceLevel{{Price: dec("100"), Quantity: dec("1")}},
		Asks:          []domain.PriceLevel{{Price: dec("101"), Quantity: dec("1")}},
	}
}

// ── Tests ──────────────────────────────────────────────────────────────────────

func TestHandlerPublishesL2AfterSyncedApply(t *testing.T) {
	pub := &mockL2Publisher{}
	h := newSyncedHandler(t, pub)

	// Clear bootstrap events captured during newSyncedHandler
	before := len(pub.published())

	// Send a new diff while synced
	h.HandleDiff(makeDiff(12, 12))
	time.Sleep(50 * time.Millisecond)

	events := pub.published()
	assert.Greater(t, len(events), before, "should publish at least one L2 event after synced diff")
	last := events[len(events)-1]
	assert.Equal(t, "", last.runID, "live events must have empty run_id")
	assert.Equal(t, "BTCUSDT", last.symbol)
	assert.Equal(t, int64(12), last.lastUpdateID)
}

func TestHandlerPublishesL2WithCorrectEventTime(t *testing.T) {
	pub := &mockL2Publisher{}
	h := newSyncedHandler(t, pub)

	before := len(pub.published())
	seed := time.Date(2026, 3, 15, 10, 0, 0, 0, time.UTC)
	diff := domain.DiffEvent{
		Symbol:        "BTCUSDT",
		EventTime:     seed,
		FirstUpdateID: 12,
		FinalUpdateID: 12,
		Bids:          []domain.PriceLevel{{Price: dec("100"), Quantity: dec("1")}},
		Asks:          []domain.PriceLevel{{Price: dec("101"), Quantity: dec("1")}},
	}
	h.HandleDiff(diff)
	time.Sleep(50 * time.Millisecond)

	events := pub.published()
	require.Greater(t, len(events), before)
	assert.Equal(t, seed, events[len(events)-1].eventTime, "EventTime must propagate from DiffEvent")
}

func TestHandlerNoPublishWithoutL2Publisher(t *testing.T) {
	// Handler with no L2Publisher should not panic and should work normally.
	h := newSyncedHandler(t, nil)
	// Just verifying no panic occurs
	h.HandleDiff(makeDiff(12, 12))
	time.Sleep(30 * time.Millisecond)
}

func TestHandlerL2PublishErrorIsNonFatal(t *testing.T) {
	pub := &mockL2Publisher{errOn: 1} // first publish call errors
	h := newSyncedHandler(t, pub)

	// Bootstrap may have used the error slot; either way, a new diff must not crash
	h.HandleDiff(makeDiff(12, 12))
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, symbol.Synced, h.State(), "handler must remain Synced after publish error")
}
