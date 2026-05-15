package replay

import (
	"context"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"go.uber.org/zap"
)

// SpeedController paces event emission during replay according to the configured mode.
//
//   - AFAP (as-fast-as-possible): no waiting; goroutine publishes at DB read speed.
//   - NX: wall-clock sleep = Δt_event / factor (e.g. factor=10 → 10× faster than real time).
//   - WALL_CLOCK: wall-clock sleep = Δt_event (real-time replay).
type SpeedController struct {
	mode   domain.SpeedMode
	factor float64
	logger *zap.Logger
}

// NewSpeedController creates a SpeedController with the given mode and speed factor.
// factor is only meaningful for NX mode; pass 1 for all other modes.
func NewSpeedController(mode domain.SpeedMode, factor float64, logger *zap.Logger) *SpeedController {
	return &SpeedController{
		mode:   mode,
		factor: factor,
		logger: logger.With(zap.String("component", "speed-controller")),
	}
}

// Wait pauses the caller to honour the configured replay speed.
//
// prevEventTime is the EventTime of the previously emitted event.
// When prevEventTime is zero (first event in a sequence), Wait returns immediately.
// Returns ctx.Err() if the context is cancelled during a sleep.
func (sc *SpeedController) Wait(ctx context.Context, prevEventTime, currEventTime time.Time) error {
	if sc.mode == domain.SpeedAFAP || prevEventTime.IsZero() {
		return nil
	}

	delta := currEventTime.Sub(prevEventTime)
	if delta <= 0 {
		return nil
	}

	var sleep time.Duration
	switch sc.mode {
	case domain.SpeedNX:
		sleep = time.Duration(float64(delta) / sc.factor)
	case domain.SpeedWallClock:
		sleep = delta
	}

	sc.logger.Debug("speed controller sleeping",
		zap.Duration("sleep", sleep),
		zap.Duration("event_delta", delta),
		zap.String("mode", string(sc.mode)),
		zap.Float64("factor", sc.factor),
	)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(sleep):
		return nil
	}
}
