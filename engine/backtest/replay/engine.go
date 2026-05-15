package replay

import (
	"context"
	"fmt"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/publisher"
	btrepository "github.com/edwinabot/erebor/backtest/repository"
	"github.com/edwinabot/erebor/ingest/book"
	ingestdomain "github.com/edwinabot/erebor/ingest/domain"
	ingestrepository "github.com/edwinabot/erebor/ingest/repository"
	"go.uber.org/zap"
)

const progressLogInterval = 1000

// EngineConfig holds the immutable parameters for a single-symbol replay run.
type EngineConfig struct {
	RunID  string
	Symbol string
	From   time.Time
	To     time.Time
	Depth  int
}

// Engine replays historical L2 data for one symbol into the run-namespaced
// Redis stream. Each symbol runs its own Engine in a separate goroutine.
//
// The replay protocol per spec §6.1:
//  1. Seek the nearest checkpoint at or before from.
//  2. Load the snapshot into an in-memory book.
//  3. Query all diffs from the checkpoint time through to.
//  4. For each diff: detect gaps, apply, pace via SpeedController, publish.
//
// EventTime in every published L2BookUpdateEvent is taken from the diff —
// never from time.Now(). This is the logical clock invariant.
type Engine struct {
	cfg EngineConfig

	book       *book.Book
	ingestRepo ingestrepository.Repository
	btRepo     btrepository.Writer
	l2Pub      *publisher.L2Publisher
	ctrlPub    *publisher.ControlPublisher
	speed      *SpeedController
	logger     *zap.Logger
}

// NewEngine creates a ReplayEngine for a single symbol.
func NewEngine(
	cfg EngineConfig,
	ingestRepo ingestrepository.Repository,
	btRepo btrepository.Writer,
	l2Pub *publisher.L2Publisher,
	ctrlPub *publisher.ControlPublisher,
	speed *SpeedController,
	logger *zap.Logger,
) *Engine {
	return &Engine{
		cfg:        cfg,
		book:       book.New(cfg.Symbol),
		ingestRepo: ingestRepo,
		btRepo:     btRepo,
		l2Pub:      l2Pub,
		ctrlPub:    ctrlPub,
		speed:      speed,
		logger:     logger.With(zap.String("component", "replay-engine"), zap.String("symbol", cfg.Symbol)),
	}
}

// Run executes the full replay for the engine's symbol.
// It is safe to call Run concurrently across multiple Engine instances.
// Returns nil on clean completion; returns ctx.Err() on cancellation.
func (e *Engine) Run(ctx context.Context) error {
	start := time.Now()
	e.logger.Info("replay engine starting",
		zap.String("run_id", e.cfg.RunID),
		zap.Time("from", e.cfg.From),
		zap.Time("to", e.cfg.To),
		zap.Int("depth", e.cfg.Depth),
	)

	published, gaps, err := e.replay(ctx)

	elapsed := time.Since(start)
	if err != nil {
		e.logger.Error("replay engine failed",
			zap.String("run_id", e.cfg.RunID),
			zap.Int("published", published),
			zap.Int("gaps", gaps),
			zap.Duration("elapsed", elapsed),
			zap.Error(err),
		)
		return err
	}

	e.logger.Info("replay engine complete",
		zap.String("run_id", e.cfg.RunID),
		zap.Int("published", published),
		zap.Int("gaps", gaps),
		zap.Duration("elapsed", elapsed),
	)
	return nil
}

func (e *Engine) replay(ctx context.Context) (published, gaps int, err error) {
	// Step 1: seek nearest checkpoint at or before from.
	checkpoint, err := e.ingestRepo.QueryNearestCheckpoint(ctx, e.cfg.Symbol, e.cfg.From)
	if err != nil {
		return 0, 0, fmt.Errorf("seek checkpoint for %s at %s: %w", e.cfg.Symbol, e.cfg.From, err)
	}
	e.logger.Info("checkpoint loaded",
		zap.Time("snapshot_time", checkpoint.CapturedAt),
		zap.Int64("last_update_id", checkpoint.LastUpdateID),
		zap.Int("bid_levels", len(checkpoint.Bids)),
		zap.Int("ask_levels", len(checkpoint.Asks)),
	)

	// Step 2: initialise book from snapshot.
	e.book.Reset()
	e.book.LoadSnapshot(checkpoint)
	e.logger.Debug("book loaded from snapshot", zap.Int64("last_update_id", e.book.LastUpdateID()))

	// Step 3: fetch all diffs in range.
	diffs, err := e.ingestRepo.QueryDiffs(ctx, e.cfg.Symbol, checkpoint.CapturedAt, e.cfg.To)
	if err != nil {
		return 0, 0, fmt.Errorf("query diffs for %s [%s, %s]: %w", e.cfg.Symbol, checkpoint.CapturedAt, e.cfg.To, err)
	}
	e.logger.Info("diffs loaded",
		zap.Int("count", len(diffs)),
		zap.Time("range_from", checkpoint.CapturedAt),
		zap.Time("range_to", e.cfg.To),
	)

	if len(diffs) == 0 {
		e.logger.Warn("no diffs found in range; nothing to replay",
			zap.Time("from", e.cfg.From),
			zap.Time("to", e.cfg.To),
		)
		return 0, 0, nil
	}

	// Step 4: replay loop.
	var (
		prevEventTime time.Time
		gapFound      bool
		ok            bool
	)
	for i, diff := range diffs {
		gapFound, prevEventTime = e.maybeHandleGap(ctx, i, diffs, prevEventTime)
		if gapFound {
			gaps++
		}

		prevEventTime, ok, err = e.applyAndPublish(ctx, diff, prevEventTime)
		if err != nil {
			return published, gaps, err
		}
		if ok {
			published++
		}
		if published > 0 && published%progressLogInterval == 0 {
			e.logger.Info("replay progress",
				zap.Int("published", published),
				zap.Int("gaps", gaps),
				zap.Time("event_time", diff.EventTime),
				zap.Int("remaining", len(diffs)-i-1),
			)
		}
		if ctx.Err() != nil {
			return published, gaps, ctx.Err()
		}
	}

	return published, gaps, nil
}

// maybeHandleGap checks for a sequence gap before diffs[i] and handles it if
// one is detected. Returns (gapDetected, updatedPrevEventTime).
func (e *Engine) maybeHandleGap(ctx context.Context, i int, diffs []ingestdomain.DiffEvent, prevEventTime time.Time) (bool, time.Time) {
	if i == 0 || diffs[i].FirstUpdateID == diffs[i-1].FinalUpdateID+1 {
		return false, prevEventTime
	}
	reseeded := e.handleGap(ctx, diffs[i-1].FinalUpdateID+1, diffs[i].FirstUpdateID, diffs[i-1].EventTime, diffs[i].EventTime)
	if reseeded {
		return true, time.Time{}
	}
	return true, prevEventTime
}

// applyAndPublish applies diff to the in-memory book, paces via SpeedController,
// and publishes the resulting L2 snapshot. Returns (updatedPrevEventTime, published, err).
// If the diff fails to apply, it is skipped (published=false, err=nil).
// If the speed controller or publisher fails, err is non-nil.
func (e *Engine) applyAndPublish(ctx context.Context, diff ingestdomain.DiffEvent, prevEventTime time.Time) (time.Time, bool, error) {
	if applyErr := e.book.Apply(diff); applyErr != nil {
		e.logger.Error("failed to apply diff; skipping",
			zap.Int64("first_update_id", diff.FirstUpdateID),
			zap.Int64("final_update_id", diff.FinalUpdateID),
			zap.Time("event_time", diff.EventTime),
			zap.Error(applyErr),
		)
		return prevEventTime, false, nil
	}

	if waitErr := e.speed.Wait(ctx, prevEventTime, diff.EventTime); waitErr != nil {
		return prevEventTime, false, fmt.Errorf("speed controller interrupted: %w", waitErr)
	}

	// EventTime comes from the diff — not from book.Snapshot's CapturedAt (logical clock invariant).
	snap := e.book.Snapshot(e.cfg.Depth)
	if pubErr := e.l2Pub.Publish(ctx, e.cfg.RunID, e.cfg.Symbol, diff.EventTime, snap.LastUpdateID, snap.Bids, snap.Asks); pubErr != nil {
		return diff.EventTime, false, fmt.Errorf("publish L2 event for %s at %s: %w", e.cfg.Symbol, diff.EventTime, pubErr)
	}

	return diff.EventTime, true, nil
}

// handleGap processes a detected sequence gap. It publishes a DATA_GAP control
// event, records the gap in the repository, and attempts to reseed the book from
// a nearby checkpoint. Returns true if the book was reseeded (caller resets prevEventTime).
func (e *Engine) handleGap(ctx context.Context, expectedFirstID, actualFirstID int64, gapFrom, gapTo time.Time) bool {
	e.logger.Warn("data gap detected",
		zap.Int64("expected_first_update_id", expectedFirstID),
		zap.Int64("actual_first_update_id", actualFirstID),
		zap.Time("gap_from", gapFrom),
		zap.Time("gap_to", gapTo),
		zap.Duration("gap_duration", gapTo.Sub(gapFrom)),
	)

	// Notify downstream consumers of the gap.
	if pubErr := e.ctrlPub.Publish(ctx, domain.ControlEvent{
		RunID: e.cfg.RunID,
		Type:  domain.ControlDataGap,
		Payload: map[string]string{
			"symbol":   e.cfg.Symbol,
			"gap_from": gapFrom.UTC().Format(time.RFC3339Nano),
			"gap_to":   gapTo.UTC().Format(time.RFC3339Nano),
		},
	}); pubErr != nil {
		e.logger.Error("failed to publish DATA_GAP control event", zap.Error(pubErr))
	}

	// Persist gap record.
	if writeErr := e.btRepo.WriteDataGap(ctx, e.cfg.RunID, e.cfg.Symbol, gapFrom, gapTo); writeErr != nil {
		e.logger.Error("failed to write data gap record", zap.Error(writeErr))
	}

	// Attempt to reseed the book from a checkpoint at the gap boundary.
	newCheckpoint, seekErr := e.ingestRepo.QueryNearestCheckpoint(ctx, e.cfg.Symbol, gapTo)
	if seekErr != nil {
		e.logger.Warn("no checkpoint found after gap; continuing with current book state",
			zap.Time("gap_to", gapTo),
			zap.Error(seekErr),
		)
		return false
	}

	e.book.Reset()
	e.book.LoadSnapshot(newCheckpoint)
	e.logger.Info("book reseeded after gap",
		zap.Time("new_snapshot_time", newCheckpoint.CapturedAt),
		zap.Int64("new_last_update_id", newCheckpoint.LastUpdateID),
	)
	return true
}
