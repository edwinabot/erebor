package replay

import (
	"context"
	"fmt"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/publisher"
	btrepository "github.com/edwinabot/erebor/backtest/repository"
	"github.com/edwinabot/erebor/ingest/book"
	ingestrepository "github.com/edwinabot/erebor/ingest/repository"
	"go.uber.org/zap"
)

const progressLogInterval = 1000

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
	runID  string
	symbol string
	from   time.Time
	to     time.Time
	depth  int

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
	runID, symbol string,
	from, to time.Time,
	depth int,
	ingestRepo ingestrepository.Repository,
	btRepo btrepository.Writer,
	l2Pub *publisher.L2Publisher,
	ctrlPub *publisher.ControlPublisher,
	speed *SpeedController,
	logger *zap.Logger,
) *Engine {
	return &Engine{
		runID:      runID,
		symbol:     symbol,
		from:       from,
		to:         to,
		depth:      depth,
		book:       book.New(symbol),
		ingestRepo: ingestRepo,
		btRepo:     btRepo,
		l2Pub:      l2Pub,
		ctrlPub:    ctrlPub,
		speed:      speed,
		logger:     logger.With(zap.String("component", "replay-engine"), zap.String("symbol", symbol)),
	}
}

// Run executes the full replay for the engine's symbol.
// It is safe to call Run concurrently across multiple Engine instances.
// Returns nil on clean completion; returns ctx.Err() on cancellation.
func (e *Engine) Run(ctx context.Context) error {
	start := time.Now()
	e.logger.Info("replay engine starting",
		zap.String("run_id", e.runID),
		zap.Time("from", e.from),
		zap.Time("to", e.to),
		zap.Int("depth", e.depth),
	)

	published, gaps, err := e.replay(ctx)

	elapsed := time.Since(start)
	if err != nil {
		e.logger.Error("replay engine failed",
			zap.String("run_id", e.runID),
			zap.Int("published", published),
			zap.Int("gaps", gaps),
			zap.Duration("elapsed", elapsed),
			zap.Error(err),
		)
		return err
	}

	e.logger.Info("replay engine complete",
		zap.String("run_id", e.runID),
		zap.Int("published", published),
		zap.Int("gaps", gaps),
		zap.Duration("elapsed", elapsed),
	)
	return nil
}

func (e *Engine) replay(ctx context.Context) (published, gaps int, err error) {
	// Step 1: seek nearest checkpoint at or before from.
	checkpoint, err := e.ingestRepo.QueryNearestCheckpoint(ctx, e.symbol, e.from)
	if err != nil {
		return 0, 0, fmt.Errorf("seek checkpoint for %s at %s: %w", e.symbol, e.from, err)
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
	e.logger.Debug("book loaded from snapshot",
		zap.Int64("last_update_id", e.book.LastUpdateID()),
	)

	// Step 3: fetch all diffs in range.
	diffs, err := e.ingestRepo.QueryDiffs(ctx, e.symbol, checkpoint.CapturedAt, e.to)
	if err != nil {
		return 0, 0, fmt.Errorf("query diffs for %s [%s, %s]: %w", e.symbol, checkpoint.CapturedAt, e.to, err)
	}
	e.logger.Info("diffs loaded",
		zap.Int("count", len(diffs)),
		zap.Time("range_from", checkpoint.CapturedAt),
		zap.Time("range_to", e.to),
	)

	if len(diffs) == 0 {
		e.logger.Warn("no diffs found in range; nothing to replay",
			zap.Time("from", e.from),
			zap.Time("to", e.to),
		)
		return 0, 0, nil
	}

	// Step 4: replay loop.
	var prevEventTime time.Time
	for i, diff := range diffs {
		// Gap detection: first_update_id must equal previous final_update_id + 1.
		if i > 0 && diff.FirstUpdateID != diffs[i-1].FinalUpdateID+1 {
			gaps++
			gapFrom := diffs[i-1].EventTime
			gapTo := diff.EventTime
			e.logger.Warn("data gap detected",
				zap.Int64("expected_first_update_id", diffs[i-1].FinalUpdateID+1),
				zap.Int64("actual_first_update_id", diff.FirstUpdateID),
				zap.Time("gap_from", gapFrom),
				zap.Time("gap_to", gapTo),
				zap.Duration("gap_duration", gapTo.Sub(gapFrom)),
			)

			// Notify downstream consumers of the gap.
			if pubErr := e.ctrlPub.Publish(ctx, domain.ControlEvent{
				RunID: e.runID,
				Type:  domain.ControlDataGap,
				Payload: map[string]string{
					"symbol":   e.symbol,
					"gap_from": gapFrom.UTC().Format(time.RFC3339Nano),
					"gap_to":   gapTo.UTC().Format(time.RFC3339Nano),
				},
			}); pubErr != nil {
				e.logger.Error("failed to publish DATA_GAP control event", zap.Error(pubErr))
			}

			// Persist gap record.
			if writeErr := e.btRepo.WriteDataGap(ctx, e.runID, e.symbol, gapFrom, gapTo); writeErr != nil {
				e.logger.Error("failed to write data gap record", zap.Error(writeErr))
			}

			// Attempt to reseed the book from a checkpoint at the gap boundary.
			newCheckpoint, seekErr := e.ingestRepo.QueryNearestCheckpoint(ctx, e.symbol, diff.EventTime)
			if seekErr != nil {
				e.logger.Warn("no checkpoint found after gap; continuing with current book state",
					zap.Time("gap_to", gapTo),
					zap.Error(seekErr),
				)
			} else {
				e.book.Reset()
				e.book.LoadSnapshot(newCheckpoint)
				prevEventTime = time.Time{} // reset pace relative to this checkpoint
				e.logger.Info("book reseeded after gap",
					zap.Time("new_snapshot_time", newCheckpoint.CapturedAt),
					zap.Int64("new_last_update_id", newCheckpoint.LastUpdateID),
				)
			}
		}

		// Apply the diff to the in-memory book.
		if applyErr := e.book.Apply(diff); applyErr != nil {
			e.logger.Error("failed to apply diff; skipping",
				zap.Int64("first_update_id", diff.FirstUpdateID),
				zap.Int64("final_update_id", diff.FinalUpdateID),
				zap.Time("event_time", diff.EventTime),
				zap.Error(applyErr),
			)
			continue
		}

		// Pace the goroutine according to the speed mode.
		if waitErr := e.speed.Wait(ctx, prevEventTime, diff.EventTime); waitErr != nil {
			return published, gaps, fmt.Errorf("speed controller interrupted: %w", waitErr)
		}
		prevEventTime = diff.EventTime

		// Build L2BookUpdateEvent from post-application book state.
		// EventTime comes from the diff — not from book.Snapshot's CapturedAt.
		snap := e.book.Snapshot(e.depth)
		if pubErr := e.l2Pub.Publish(ctx, e.runID, e.symbol, diff.EventTime, snap.LastUpdateID, snap.Bids, snap.Asks); pubErr != nil {
			return published, gaps, fmt.Errorf("publish L2 event for %s at %s: %w", e.symbol, diff.EventTime, pubErr)
		}
		published++

		if published%progressLogInterval == 0 {
			e.logger.Info("replay progress",
				zap.Int("published", published),
				zap.Int("gaps", gaps),
				zap.Time("event_time", diff.EventTime),
				zap.Int("remaining", len(diffs)-i-1),
			)
		}

		// Respect context cancellation between events without waiting for speed.Wait.
		if ctx.Err() != nil {
			return published, gaps, ctx.Err()
		}
	}

	return published, gaps, nil
}
