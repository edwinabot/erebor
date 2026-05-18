package symbol

import (
	"context"
	"sync"
	"time"

	"github.com/edwinabot/erebor/ingest/book"
	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/edwinabot/erebor/ingest/fetcher"
	"github.com/edwinabot/erebor/ingest/repository"
	"go.uber.org/zap"
)

// L2EventPublisher publishes a post-apply book snapshot to a Redis Stream.
// Publish errors are non-fatal — ingest must continue if Redis is temporarily unavailable.
type L2EventPublisher interface {
	Publish(ctx context.Context, runID, symbol string, eventTime time.Time, lastUpdateID int64, bids, asks []domain.PriceLevel) error
}

type SymbolHandler interface {
	HandleDiff(event domain.DiffEvent)
	State() SymbolState
}

type Config struct {
	Symbol                  string
	DepthLimit              int
	CheckpointInterval      time.Duration
	CheckpointDiffThreshold int
	MaxBufferSize           int
}

type Handler struct {
	cfg     Config
	logger  *zap.Logger
	book    book.OrderBook
	fetcher fetcher.DepthFetcher
	repo    repository.Repository
	l2pub   L2EventPublisher // optional; nil = no live publishing

	ctx        context.Context
	bootstrapG sync.WaitGroup

	mu              sync.Mutex
	state           SymbolState
	buffer          []domain.DiffEvent
	snapshot        *domain.SnapshotEvent
	snapshotPending bool
	snapshotStale   bool // set when buffer overflows during a pending fetch

	lastFinalUpdateID    int64
	lastCheckpointTime   time.Time
	diffsSinceCheckpoint int
}

// WithL2Publisher attaches a live L2 stream publisher to the handler.
// If not called, L2 events are only persisted to TimescaleDB (backtest-only mode).
func WithL2Publisher(pub L2EventPublisher) func(*Handler) {
	return func(h *Handler) { h.l2pub = pub }
}

func NewHandler(cfg Config, ob book.OrderBook, df fetcher.DepthFetcher, repo repository.Repository, logger *zap.Logger, opts ...func(*Handler)) *Handler {
	if cfg.DepthLimit <= 0 {
		cfg.DepthLimit = 50
	}
	if cfg.CheckpointInterval <= 0 {
		cfg.CheckpointInterval = time.Second
	}
	if cfg.CheckpointDiffThreshold <= 0 {
		cfg.CheckpointDiffThreshold = 500
	}
	if cfg.MaxBufferSize <= 0 {
		cfg.MaxBufferSize = 1000
	}
	h := &Handler{
		cfg:     cfg,
		logger:  logger.With(zap.String("component", "symbol"), zap.String("symbol", cfg.Symbol)),
		book:    ob,
		fetcher: df,
		repo:    repo,
		state:   Disconnected,
	}
	for _, o := range opts {
		o(h)
	}
	return h
}

// Start binds the handler to a context. The bootstrap snapshot fetch is
// deferred until the first diff arrives — per the ADR state diagram, the
// transition out of DISCONNECTED is gated on the stream being connected.
// Kicking off the snapshot before any event is buffered creates a race
// where the snapshot's LastUpdateID can fall in a gap the WebSocket
// hasn't yet produced, leaving the handler stuck waiting for an
// alignment event that already passed.
func (h *Handler) Start(ctx context.Context) {
	h.mu.Lock()
	h.ctx = ctx
	h.mu.Unlock()
}

// Stop blocks until any in-flight snapshot fetch goroutine returns. It does
// not cancel the snapshot fetch — callers should cancel the context passed to
// Start first so the fetch unwinds promptly.
func (h *Handler) Stop() {
	h.bootstrapG.Wait()
}

func (h *Handler) State() SymbolState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state
}

func (h *Handler) HandleDiff(event domain.DiffEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()

	switch h.state {
	case Disconnected:
		h.transitionLocked(Bootstrapping)
		h.kickoffSnapshotLocked()
		h.bufferLocked(event)
	case Bootstrapping:
		h.bufferLocked(event)
		if h.snapshot != nil {
			h.tryAlignLocked()
		} else if len(h.buffer) > h.cfg.MaxBufferSize {
			h.logger.Warn("buffer overflow during bootstrap, re-fetching snapshot",
				zap.Int("buffer_size", len(h.buffer)),
			)
			h.buffer = h.buffer[:0]
			h.snapshot = nil
			if h.snapshotPending {
				// kickoffSnapshotLocked would be a no-op while pending, and
				// the in-flight snapshot is now too old to align — by the
				// time it returns the events that would have aligned with
				// it have been dropped. Mark it so the goroutine discards
				// the result and re-fetches.
				h.snapshotStale = true
			} else {
				h.kickoffSnapshotLocked()
			}
		}
	case Synced:
		h.handleSyncedLocked(event)
	case Resyncing:
		// Drain — book is being reset. New bootstrap will pick up from here.
		h.bufferLocked(event)
	}
}

func (h *Handler) transitionLocked(to SymbolState) {
	if h.state == to {
		return
	}
	h.logger.Info("state transition",
		zap.String("from_state", h.state.String()),
		zap.String("to_state", to.String()),
	)
	h.state = to
}

func (h *Handler) bufferLocked(event domain.DiffEvent) {
	h.buffer = append(h.buffer, event)
}

func (h *Handler) kickoffSnapshotLocked() {
	if h.snapshotPending {
		return
	}
	h.snapshotPending = true
	ctx := h.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	h.bootstrapG.Add(1)
	go h.runSnapshotFetch(ctx)
}

func (h *Handler) runSnapshotFetch(ctx context.Context) {
	defer h.bootstrapG.Done()
	h.logger.Info("fetching snapshot",
		zap.Int("depth_limit", h.cfg.DepthLimit),
	)
	snap, err := h.fetcher.FetchSnapshot(ctx, h.cfg.Symbol, h.cfg.DepthLimit)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapshotPending = false
	if err != nil {
		h.handleSnapshotErrorLocked(err)
		return
	}
	if h.snapshotStale {
		h.snapshotStale = false
		h.logger.Info("discarding stale snapshot, re-fetching",
			zap.Int64("snapshot_last_update_id", snap.LastUpdateID),
		)
		if h.state == Bootstrapping {
			h.kickoffSnapshotLocked()
		}
		return
	}
	h.logger.Info("snapshot received",
		zap.Int64("snapshot_last_update_id", snap.LastUpdateID),
		zap.Int("buffer_size", len(h.buffer)),
	)
	h.snapshot = &snap
	h.tryAlignLocked()
}

func (h *Handler) handleSnapshotErrorLocked(err error) {
	h.logger.Error("snapshot fetch failed", zap.Error(err))
	// Stop retrying when shutting down; otherwise re-kick so a transient
	// failure doesn't strand the handler in BOOTSTRAPPING.
	if h.ctx != nil && h.ctx.Err() != nil {
		return
	}
	if h.state == Bootstrapping {
		h.kickoffSnapshotLocked()
	}
}

// tryAlignLocked is the bootstrap alignment routine. Spec:
//
//	Discard:      event.FinalUpdateID <= snapshot.LastUpdateID
//	Accept first: event.FirstUpdateID <= snapshot.LastUpdateID+1
//	              AND event.FinalUpdateID >= snapshot.LastUpdateID+1
func (h *Handler) tryAlignLocked() {
	if h.snapshot == nil {
		return
	}
	snap := *h.snapshot

	h.buffer = discardPreSnapshot(h.buffer, snap.LastUpdateID)

	alignIdx := findAlignmentIndex(h.buffer, snap.LastUpdateID)
	if alignIdx == -1 {
		if len(h.buffer) > h.cfg.MaxBufferSize {
			h.logger.Warn("buffer overflow while waiting for alignment, re-fetching snapshot",
				zap.Int("buffer_size", len(h.buffer)),
				zap.Int64("snapshot_last_update_id", snap.LastUpdateID),
			)
			h.buffer = h.buffer[:0]
			h.snapshot = nil
			h.kickoffSnapshotLocked()
			return
		}
		h.logger.Debug("alignment pending, continuing to buffer",
			zap.Int("buffer_size", len(h.buffer)),
			zap.Int64("snapshot_last_update_id", snap.LastUpdateID),
		)
		return
	}

	h.replayAlignedBufferLocked(alignIdx, snap)
}

// discardPreSnapshot removes all events with FinalUpdateID <= lastUpdateID.
func discardPreSnapshot(buf []domain.DiffEvent, lastUpdateID int64) []domain.DiffEvent {
	kept := buf[:0]
	for _, ev := range buf {
		if ev.FinalUpdateID > lastUpdateID {
			kept = append(kept, ev)
		}
	}
	return kept
}

// findAlignmentIndex returns the index of the first event satisfying the
// Binance alignment condition, or -1 if none exists.
func findAlignmentIndex(buf []domain.DiffEvent, lastUpdateID int64) int {
	for i, ev := range buf {
		if ev.FirstUpdateID <= lastUpdateID+1 && ev.FinalUpdateID >= lastUpdateID+1 {
			return i
		}
	}
	return -1
}

// replayAlignedBufferLocked loads the snapshot into the book, applies all
// buffered events from alignIdx onward, and transitions to Synced.
func (h *Handler) replayAlignedBufferLocked(alignIdx int, snap domain.SnapshotEvent) {
	h.logger.Info("bootstrap alignment found",
		zap.Int("align_index", alignIdx),
		zap.Int("events_to_replay", len(h.buffer)-alignIdx),
		zap.Int64("snapshot_last_update_id", snap.LastUpdateID),
	)
	h.book.Reset()
	if loader, ok := h.book.(interface{ LoadSnapshot(domain.SnapshotEvent) }); ok {
		loader.LoadSnapshot(snap)
	}
	h.lastFinalUpdateID = snap.LastUpdateID

	for i := alignIdx; i < len(h.buffer); i++ {
		ev := h.buffer[i]
		if err := h.book.Apply(ev); err != nil {
			h.logger.Error("apply during bootstrap failed", zap.Error(err))
			h.buffer = nil
			h.snapshot = nil
			h.transitionLocked(Resyncing)
			h.book.Reset()
			h.transitionLocked(Bootstrapping)
			h.kickoffSnapshotLocked()
			return
		}
		if h.lastFinalUpdateID != 0 && ev.FirstUpdateID > h.lastFinalUpdateID+1 {
			h.logger.Warn("sequence gap during bootstrap replay",
				zap.Int64("expected_first_update_id", h.lastFinalUpdateID+1),
				zap.Int64("received_first_update_id", ev.FirstUpdateID),
			)
		}
		h.lastFinalUpdateID = ev.FinalUpdateID
		h.publishL2Locked(ev)

		if err := h.repo.WriteDiff(h.ctxOrBackground(), ev); err != nil {
			h.logger.Error("write diff failed during bootstrap replay", zap.Error(err))
		}
		h.logger.Debug("diff applied (replay)",
			zap.Int64("first_update_id", ev.FirstUpdateID),
			zap.Int64("final_update_id", ev.FinalUpdateID),
			zap.Int("bid_levels", len(ev.Bids)),
			zap.Int("ask_levels", len(ev.Asks)),
		)
	}
	h.buffer = nil
	h.snapshot = nil
	h.lastCheckpointTime = time.Now().UTC()
	h.diffsSinceCheckpoint = 0
	h.transitionLocked(Synced)
}

func (h *Handler) handleSyncedLocked(event domain.DiffEvent) {
	if event.FirstUpdateID != h.lastFinalUpdateID+1 {
		h.logger.Warn("sequence gap detected",
			zap.Int64("expected_first_update_id", h.lastFinalUpdateID+1),
			zap.Int64("received_first_update_id", event.FirstUpdateID),
		)
		h.enterResyncLocked(event)
		return
	}

	if err := h.book.Apply(event); err != nil {
		h.logger.Error("book apply failed", zap.Error(err))
		h.enterResyncLocked(event)
		return
	}
	h.lastFinalUpdateID = event.FinalUpdateID

	h.publishL2Locked(event)

	if err := h.repo.WriteDiff(h.ctxOrBackground(), event); err != nil {
		h.logger.Error("write diff failed", zap.Error(err))
	}
	h.diffsSinceCheckpoint++

	h.logger.Debug("diff applied",
		zap.Int64("first_update_id", event.FirstUpdateID),
		zap.Int64("final_update_id", event.FinalUpdateID),
		zap.Int("bid_levels", len(event.Bids)),
		zap.Int("ask_levels", len(event.Asks)),
	)

	if h.shouldCheckpointLocked() {
		snap := h.book.Snapshot(h.cfg.DepthLimit)
		if err := h.repo.WriteCheckpoint(h.ctxOrBackground(), snap); err != nil {
			h.logger.Error("write checkpoint failed", zap.Error(err))
		} else {
			h.lastCheckpointTime = time.Now().UTC()
			h.diffsSinceCheckpoint = 0
			h.logger.Debug("checkpoint written",
				zap.Int64("last_update_id", snap.LastUpdateID),
				zap.Int("bid_levels", len(snap.Bids)),
				zap.Int("ask_levels", len(snap.Asks)),
			)
		}
	}
}

// publishL2Locked publishes a post-apply L2 snapshot to the live stream.
// Must be called while h.mu is held. Errors are logged but never fatal.
func (h *Handler) publishL2Locked(event domain.DiffEvent) {
	if h.l2pub == nil {
		return
	}
	snap := h.book.Snapshot(h.cfg.DepthLimit)
	if err := h.l2pub.Publish(h.ctxOrBackground(), "", h.cfg.Symbol, event.EventTime, event.FinalUpdateID, snap.Bids, snap.Asks); err != nil {
		h.logger.Warn("L2 publish failed (non-fatal)",
			zap.String("symbol", h.cfg.Symbol),
			zap.Time("event_time", event.EventTime),
			zap.Error(err),
		)
	}
}

func (h *Handler) shouldCheckpointLocked() bool {
	if h.diffsSinceCheckpoint >= h.cfg.CheckpointDiffThreshold {
		return true
	}
	if !h.lastCheckpointTime.IsZero() &&
		time.Since(h.lastCheckpointTime) >= h.cfg.CheckpointInterval {
		return true
	}
	return false
}

func (h *Handler) enterResyncLocked(pending domain.DiffEvent) {
	h.transitionLocked(Resyncing)
	h.book.Reset()
	h.lastFinalUpdateID = 0
	h.diffsSinceCheckpoint = 0
	h.lastCheckpointTime = time.Time{}
	h.buffer = []domain.DiffEvent{pending}
	h.snapshot = nil
	h.transitionLocked(Bootstrapping)
	h.kickoffSnapshotLocked()
}

func (h *Handler) ctxOrBackground() context.Context {
	if h.ctx != nil {
		return h.ctx
	}
	return context.Background()
}
