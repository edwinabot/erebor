package replay_test

import (
	"context"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/replay"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func nopLogger() *zap.Logger { return zap.NewNop() }

// ── AFAP mode ────────────────────────────────────────────────────────────────

func TestSpeedController_AFAP_ReturnsImmediately(t *testing.T) {
	sc := replay.NewSpeedController(domain.SpeedAFAP, 1.0, nopLogger())

	t0 := time.Now()
	err := sc.Wait(context.Background(),
		time.Now().Add(-time.Second),
		time.Now(),
	)
	elapsed := time.Since(t0)

	require.NoError(t, err)
	assert.Less(t, elapsed, 10*time.Millisecond, "AFAP must not sleep")
}

func TestSpeedController_AFAP_ZeroPrevTime(t *testing.T) {
	sc := replay.NewSpeedController(domain.SpeedAFAP, 1.0, nopLogger())

	t0 := time.Now()
	err := sc.Wait(context.Background(), time.Time{}, time.Now())
	elapsed := time.Since(t0)

	require.NoError(t, err)
	assert.Less(t, elapsed, 10*time.Millisecond)
}

// ── NX mode ──────────────────────────────────────────────────────────────────

func TestSpeedController_NX_SleesDividedByFactor(t *testing.T) {
	const factor = 10.0
	sc := replay.NewSpeedController(domain.SpeedNX, factor, nopLogger())

	eventDelta := 100 * time.Millisecond
	prev := time.Now()
	curr := prev.Add(eventDelta)

	t0 := time.Now()
	err := sc.Wait(context.Background(), prev, curr)
	elapsed := time.Since(t0)

	require.NoError(t, err)
	expectedSleep := eventDelta / factor // 10ms
	// Allow 5ms margin above and below expected sleep.
	assert.GreaterOrEqual(t, elapsed, expectedSleep-5*time.Millisecond,
		"NX sleep must be at least Δt/factor")
	assert.Less(t, elapsed, expectedSleep+20*time.Millisecond,
		"NX sleep must not significantly exceed Δt/factor")
}

func TestSpeedController_NX_ZeroPrevTimeSkipsSleep(t *testing.T) {
	sc := replay.NewSpeedController(domain.SpeedNX, 2.0, nopLogger())

	t0 := time.Now()
	err := sc.Wait(context.Background(), time.Time{}, time.Now())
	elapsed := time.Since(t0)

	require.NoError(t, err)
	assert.Less(t, elapsed, 10*time.Millisecond, "zero prevTime must skip sleep regardless of mode")
}

func TestSpeedController_NX_NegativeDeltaSkipsSleep(t *testing.T) {
	sc := replay.NewSpeedController(domain.SpeedNX, 2.0, nopLogger())

	prev := time.Now().Add(time.Second) // prev is AFTER curr
	curr := time.Now()

	t0 := time.Now()
	err := sc.Wait(context.Background(), prev, curr)
	elapsed := time.Since(t0)

	require.NoError(t, err)
	assert.Less(t, elapsed, 10*time.Millisecond, "negative delta must skip sleep")
}

// ── WALL_CLOCK mode ──────────────────────────────────────────────────────────

func TestSpeedController_WallClock_SleepsEventDelta(t *testing.T) {
	sc := replay.NewSpeedController(domain.SpeedWallClock, 1.0, nopLogger())

	eventDelta := 50 * time.Millisecond
	prev := time.Now()
	curr := prev.Add(eventDelta)

	t0 := time.Now()
	err := sc.Wait(context.Background(), prev, curr)
	elapsed := time.Since(t0)

	require.NoError(t, err)
	assert.GreaterOrEqual(t, elapsed, eventDelta-5*time.Millisecond)
	assert.Less(t, elapsed, eventDelta+20*time.Millisecond)
}

// ── context cancellation ─────────────────────────────────────────────────────

func TestSpeedController_CancelledContext_ReturnsError(t *testing.T) {
	sc := replay.NewSpeedController(domain.SpeedWallClock, 1.0, nopLogger())

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled

	err := sc.Wait(ctx,
		time.Now().Add(-time.Second),
		time.Now(),
	)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSpeedController_ContextCancelledDuringSleep_ReturnsError(t *testing.T) {
	sc := replay.NewSpeedController(domain.SpeedWallClock, 1.0, nopLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	// Event delta of 500ms — context will expire before sleep completes.
	err := sc.Wait(ctx, time.Now(), time.Now().Add(500*time.Millisecond))
	assert.Error(t, err, "should return error when context expires during sleep")
}
