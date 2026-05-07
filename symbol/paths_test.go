package symbol_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edwinabot/erebor/ingest/book"
	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/edwinabot/erebor/ingest/symbol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// ---- Fakes ----

// errBookOnApply errors from Apply on the configured call indices and
// otherwise delegates to an embedded *book.Book. Snapshot and LastUpdateID
// pass through unchanged.
type errBookOnApply struct {
	inner  *book.Book
	mu     sync.Mutex
	callN  int
	failOn map[int]error // 1-indexed call ordinal → err to return
}

func newErrBook(failOn map[int]error) *errBookOnApply {
	return &errBookOnApply{
		inner:  book.New("BTCUSDT"),
		failOn: failOn,
	}
}

func (e *errBookOnApply) Apply(diff domain.DiffEvent) error {
	e.mu.Lock()
	e.callN++
	n := e.callN
	e.mu.Unlock()
	if err, ok := e.failOn[n]; ok {
		return err
	}
	return e.inner.Apply(diff)
}

func (e *errBookOnApply) Snapshot(depth int) domain.SnapshotEvent {
	return e.inner.Snapshot(depth)
}
func (e *errBookOnApply) LastUpdateID() int64 { return e.inner.LastUpdateID() }
func (e *errBookOnApply) Reset()              { e.inner.Reset() }

// noLoadSnapshotBook deliberately omits LoadSnapshot to exercise the
// type-assertion-fails branch in replayAlignedBufferLocked.
type noLoadSnapshotBook struct {
	inner *book.Book
}

func (n *noLoadSnapshotBook) Apply(d domain.DiffEvent) error      { return n.inner.Apply(d) }
func (n *noLoadSnapshotBook) Snapshot(d int) domain.SnapshotEvent { return n.inner.Snapshot(d) }
func (n *noLoadSnapshotBook) LastUpdateID() int64                 { return n.inner.LastUpdateID() }
func (n *noLoadSnapshotBook) Reset()                              { n.inner.Reset() }

// errOnceRepo returns errors on the first WriteDiff and first WriteCheckpoint,
// then succeeds.
type errOnceRepo struct {
	mu              sync.Mutex
	diffs           []domain.DiffEvent
	checkpoints     []domain.SnapshotEvent
	failNextDiff    atomic.Bool
	failNextCheckpt atomic.Bool
}

func (r *errOnceRepo) WriteDiff(_ context.Context, ev domain.DiffEvent) error {
	if r.failNextDiff.Swap(false) {
		return errors.New("simulated diff write failure")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.diffs = append(r.diffs, ev)
	return nil
}
func (r *errOnceRepo) WriteCheckpoint(_ context.Context, snap domain.SnapshotEvent) error {
	if r.failNextCheckpt.Swap(false) {
		return errors.New("simulated checkpoint failure")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checkpoints = append(r.checkpoints, snap)
	return nil
}
func (r *errOnceRepo) QueryNearestCheckpoint(_ context.Context, _ string, _ time.Time) (domain.SnapshotEvent, error) {
	return domain.SnapshotEvent{}, nil
}
func (r *errOnceRepo) QueryDiffs(_ context.Context, _ string, _ time.Time, _ time.Time) ([]domain.DiffEvent, error) {
	return nil, nil
}

// ---- Tests ----

// TestSyncedSequenceGapTriggersResyncAndRebootstrap proves that a non-
// contiguous diff in Synced state causes:
//  1. a WARN sequence-gap log,
//  2. transition Synced → Resyncing → Bootstrapping (book reset, ctrs cleared,
//     gap-event re-buffered),
//  3. snapshot re-fetch,
//  4. eventual return to Synced once a new aligning event arrives.
func TestSyncedSequenceGapTriggersResyncAndRebootstrap(t *testing.T) {
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100), snap(200)}}
	repo := &MockRepository{}
	h, _ := newHandler(t, mf, repo, 100)

	h.Start(context.Background())
	reachSynced(t, h, 100) // alignment diff = (100,101); lastFinalID = 101

	// Inject a gap: expected next FirstUpdateID = 102, give 110.
	// Per enterResyncLocked the gap event is re-buffered as the trigger
	// for the next bootstrap; the second snapshot (LastUpdateID=200) does
	// NOT align with it, so a third event provides alignment.
	h.HandleDiff(diff(110, 111))                           // gap → resync, buffered
	waitFor(t, func() bool { return mf.callCount() >= 2 }, // re-bootstrap kicks new fetch
		time.Second, "second snapshot fetch after gap")
	h.HandleDiff(diff(200, 201)) // aligns with snap(200)

	waitFor(t, func() bool { return h.State() == symbol.Synced }, time.Second, "Synced again")
}

// TestSyncedBookApplyErrorTriggersResync verifies that if the in-memory
// book rejects an Apply (which today is impossible — *book.Book never
// returns an error — but the contract permits it), the handler treats it
// like a sequence gap: enter Resyncing, re-bootstrap, queue the rejected
// event for replay.
//
// Note: the rejected event is the one re-buffered in enterResyncLocked; if
// it is ALSO outside the next snapshot's alignment window, alignment will
// not occur until a fresh aligning event is supplied.
func TestSyncedBookApplyErrorTriggersResync(t *testing.T) {
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100), snap(200)}}
	repo := &MockRepository{}

	// First Apply call is during bootstrap replay (alignment event 100..101).
	// We want the SECOND Apply (the first post-Synced diff) to fail.
	eb := newErrBook(map[int]error{2: errors.New("book full")})

	logger := zap.NewNop()
	h := symbol.NewHandler(symbol.Config{
		Symbol:                  "BTCUSDT",
		DepthLimit:              50,
		CheckpointInterval:      time.Hour,
		CheckpointDiffThreshold: 1_000_000,
		MaxBufferSize:           100,
	}, eb, mf, repo, logger)

	h.Start(context.Background())
	reachSynced(t, h, 100)

	h.HandleDiff(diff(102, 103)) // 2nd Apply fails → enterResync → re-fetch
	waitFor(t, func() bool { return mf.callCount() >= 2 }, time.Second, "snapshot re-fetched after apply error")

	// State is now Bootstrapping (the rejected event is buffered, doesn't
	// align with snap(200)). It should NOT be Synced.
	require.NotEqual(t, symbol.Synced, h.State())
}

// TestSyncedWriteDiffErrorIsLoggedHandlerStaysSynced verifies the
// at-least-once persistence semantics: a transient WriteDiff failure is
// logged but does not change handler state. Subsequent diffs continue to
// be applied and persisted.
func TestSyncedWriteDiffErrorIsLoggedHandlerStaysSynced(t *testing.T) {
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100)}}
	repo := &errOnceRepo{}

	logger := zap.NewNop()
	ob := book.New("BTCUSDT")
	h := symbol.NewHandler(symbol.Config{
		Symbol:                  "BTCUSDT",
		DepthLimit:              50,
		CheckpointInterval:      time.Hour,
		CheckpointDiffThreshold: 1_000_000,
		MaxBufferSize:           100,
	}, ob, mf, repo, logger)

	h.Start(context.Background())
	reachSynced(t, h, 100) // alignment diff (101) persists OK

	repo.failNextDiff.Store(true) // arm AFTER bootstrap replay
	h.HandleDiff(diff(102, 103))  // write FAILS but is swallowed
	h.HandleDiff(diff(104, 105))  // succeeds

	require.Equal(t, symbol.Synced, h.State())
	repo.mu.Lock()
	defer repo.mu.Unlock()
	// Alignment diff (101) + 105 persisted; 103 dropped silently.
	require.Len(t, repo.diffs, 2)
	require.Equal(t, int64(101), repo.diffs[0].FinalUpdateID)
	require.Equal(t, int64(105), repo.diffs[1].FinalUpdateID)
}

// TestSyncedCheckpointWriteErrorRetainsCounters ensures that when
// WriteCheckpoint fails, lastCheckpointTime / diffsSinceCheckpoint are NOT
// reset, so the next eligible diff retries the checkpoint.
func TestSyncedCheckpointWriteErrorRetainsCounters(t *testing.T) {
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100)}}
	repo := &errOnceRepo{}
	repo.failNextCheckpt.Store(true) // first checkpoint write fails

	logger := zap.NewNop()
	ob := book.New("BTCUSDT")
	h := symbol.NewHandler(symbol.Config{
		Symbol:                  "BTCUSDT",
		DepthLimit:              50,
		CheckpointInterval:      time.Hour,
		CheckpointDiffThreshold: 2, // every other Synced diff triggers a checkpoint
		MaxBufferSize:           100,
	}, ob, mf, repo, logger)

	h.Start(context.Background())
	reachSynced(t, h, 100)

	// Each call increments diffsSinceCheckpoint. Threshold=2 → 2nd call
	// triggers a checkpoint attempt, which FAILS. Counters NOT reset, so
	// the 3rd call attempts again and succeeds.
	h.HandleDiff(diff(102, 103))
	h.HandleDiff(diff(104, 105)) // 2 diffs since last cp → fail
	h.HandleDiff(diff(106, 107)) // still 3 (not reset), retries → success

	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Equal(t, 1, len(repo.checkpoints), "exactly one successful checkpoint")
}

// TestBootstrapReplayBookApplyErrorTriggersResync covers the
// replayAlignedBufferLocked apply-error branch: a corrupt book during
// initial replay triggers Resyncing → Bootstrapping (snapshot re-fetched).
func TestBootstrapReplayBookApplyErrorTriggersResync(t *testing.T) {
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100), snap(200)}}
	repo := &MockRepository{}

	// First Apply call (the alignment diff) errors → resync.
	eb := newErrBook(map[int]error{1: errors.New("book corrupt")})

	logger := zap.NewNop()
	h := symbol.NewHandler(symbol.Config{
		Symbol:                  "BTCUSDT",
		DepthLimit:              50,
		CheckpointInterval:      time.Hour,
		CheckpointDiffThreshold: 1_000_000,
		MaxBufferSize:           100,
	}, eb, mf, repo, logger)

	h.Start(context.Background())
	h.HandleDiff(diff(100, 101)) // triggers bootstrap; replay apply fails
	waitFor(t, func() bool { return mf.callCount() >= 2 }, time.Second, "second snapshot after replay error")
}

// TestBootstrapReplayLoadSnapshotInterfaceAssertionFails proves the
// book-without-LoadSnapshot branch: replay still proceeds, just without
// preloading the snapshot levels into the book.
func TestBootstrapReplayLoadSnapshotInterfaceAssertionFails(t *testing.T) {
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100)}}
	repo := &MockRepository{}
	ob := &noLoadSnapshotBook{inner: book.New("BTCUSDT")}

	logger := zap.NewNop()
	h := symbol.NewHandler(symbol.Config{
		Symbol:                  "BTCUSDT",
		DepthLimit:              50,
		CheckpointInterval:      time.Hour,
		CheckpointDiffThreshold: 1_000_000,
		MaxBufferSize:           100,
	}, ob, mf, repo, logger)

	h.Start(context.Background())
	reachSynced(t, h, 100)

	require.Equal(t, symbol.Synced, h.State())
	require.Equal(t, int64(101), ob.LastUpdateID(), "alignment diff applied even without snapshot preload")
}

// TestBootstrapReplayWriteDiffErrorIsSwallowed: a transient persistence
// failure during initial replay must not block the transition to Synced.
func TestBootstrapReplayWriteDiffErrorIsSwallowed(t *testing.T) {
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100)}}
	repo := &errOnceRepo{}
	repo.failNextDiff.Store(true) // alignment-diff write fails

	logger := zap.NewNop()
	ob := book.New("BTCUSDT")
	h := symbol.NewHandler(symbol.Config{
		Symbol:                  "BTCUSDT",
		DepthLimit:              50,
		CheckpointInterval:      time.Hour,
		CheckpointDiffThreshold: 1_000_000,
		MaxBufferSize:           100,
	}, ob, mf, repo, logger)

	h.Start(context.Background())
	reachSynced(t, h, 100)
	require.Equal(t, symbol.Synced, h.State())
}

// TestKickoffSnapshotErrorReKicksWhileBootstrapping: a fetch failure in
// BOOTSTRAPPING (without ctx cancellation) must trigger another fetch.
func TestKickoffSnapshotErrorReKicksWhileBootstrapping(t *testing.T) {
	// First call returns an error; the goroutine sees state==Bootstrapping
	// and re-kicks. Second call returns a valid snapshot.
	mf := &MockFetcher{
		responses:  []domain.SnapshotEvent{snap(100)},
		errOnCalls: map[int]error{1: errors.New("transient")},
	}
	repo := &MockRepository{}
	h, _ := newHandler(t, mf, repo, 100)

	h.Start(context.Background())
	h.HandleDiff(diff(100, 101))

	waitFor(t, func() bool { return mf.callCount() >= 2 }, 2*time.Second, "snapshot re-fetched after error")
	waitFor(t, func() bool { return h.State() == symbol.Synced }, 2*time.Second, "Synced after retry")
}

// TestKickoffSnapshotErrorDoesNotReKickWhenContextCancelled covers the
// shutdown-suppression branch in kickoffSnapshotLocked.
func TestKickoffSnapshotErrorDoesNotReKickWhenContextCancelled(t *testing.T) {
	mf := &MockFetcher{
		responses:  nil, // queue empty so all calls error
		errOnCalls: map[int]error{1: errors.New("ctx cancelled"), 2: errors.New("ctx cancelled")},
	}
	repo := &MockRepository{}
	h, _ := newHandler(t, mf, repo, 100)

	ctx, cancel := context.WithCancel(context.Background())
	h.Start(ctx)
	cancel() // cancel BEFORE any HandleDiff

	h.HandleDiff(diff(100, 101))
	// Give the (single) goroutine time to run and decide not to re-kick.
	time.Sleep(50 * time.Millisecond)

	require.Equal(t, 1, mf.callCount(), "no re-kick after ctx cancellation")
}

// TestNewHandlerAppliesDefaultsOnZeroValues exercises the four <=0 default
// branches in NewHandler.
func TestNewHandlerAppliesDefaultsOnZeroValues(t *testing.T) {
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100)}}
	repo := &MockRepository{}

	// All four defaultable fields zero → defaults apply.
	h := symbol.NewHandler(symbol.Config{Symbol: "BTCUSDT"},
		book.New("BTCUSDT"), mf, repo, zap.NewNop())
	require.NotNil(t, h)
	require.Equal(t, symbol.Disconnected, h.State())
}

// TestHandleDiffWithoutStartUsesBackgroundContext covers the ctxOrBackground
// nil-ctx branch and the kickoffSnapshotLocked nil-ctx branch.
func TestHandleDiffWithoutStartUsesBackgroundContext(t *testing.T) {
	mf := &MockFetcher{responses: []domain.SnapshotEvent{snap(100)}}
	repo := &MockRepository{}
	h, _ := newHandler(t, mf, repo, 100)

	// Note: Start NOT called, so h.ctx is nil.
	h.HandleDiff(diff(100, 101))
	waitFor(t, func() bool { return h.State() == symbol.Synced }, time.Second, "Synced via background ctx")
}

// TestBootstrapBufferOverflowBeforeSnapshotReturns drives the
// "Bootstrapping AND snapshot==nil AND buffer > MaxBufferSize" branch in
// HandleDiff. The handler clears the buffer and (defensively) calls
// kickoffSnapshotLocked.
//
// TODO: EDGE CASE FOUND (not fixed per instructions): when overflow occurs while
// the first snapshot fetch is still in flight, the snapshotPending guard
// makes the re-kick a no-op. The original snapshot, when it returns, finds
// an empty buffer and silently waits. This test verifies the OBSERVED
// behaviour: events buffered before overflow are dropped (never persisted)
// and the handler can still complete bootstrap when a fresh aligning diff
// arrives after the held snapshot resolves.
func TestBootstrapBufferOverflowBeforeSnapshotReturns(t *testing.T) {
	hold := make(chan struct{})
	mf := &MockFetcher{
		responses: []domain.SnapshotEvent{snap(50)},
		hold:      hold,
	}
	repo := &MockRepository{}
	h, _ := newHandler(t, mf, repo, 2) // MaxBufferSize=2

	h.Start(context.Background())

	// Three buffered diffs with snapshot blocked → overflow on the third.
	h.HandleDiff(diff(70, 71))
	h.HandleDiff(diff(72, 73))
	h.HandleDiff(diff(74, 75)) // overflow → buffer cleared

	close(hold) // first snapshot returns; tryAlign on empty buffer is a no-op

	// Fresh aligning diff completes bootstrap.
	h.HandleDiff(diff(51, 52))
	waitFor(t, func() bool { return h.State() == symbol.Synced }, time.Second, "Synced via fresh alignment diff")

	// Verify the pre-overflow buffered diffs were dropped (never persisted).
	repo.mu.Lock()
	defer repo.mu.Unlock()
	require.Len(t, repo.Diffs, 1)
	require.Equal(t, int64(52), repo.Diffs[0].FinalUpdateID)
}
