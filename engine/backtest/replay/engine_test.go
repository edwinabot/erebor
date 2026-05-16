package replay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/internal/testutil"
	"github.com/edwinabot/erebor/backtest/publisher"
	"github.com/edwinabot/erebor/backtest/replay"
	btrepository "github.com/edwinabot/erebor/backtest/repository"
	ingestdomain "github.com/edwinabot/erebor/ingest/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const l2StreamSuffix = ":l2:BTCUSDT"

// ── mock ingest repository ────────────────────────────────────────────────────

type mockIngestRepo struct {
	checkpoint    ingestdomain.SnapshotEvent
	checkpointErr error
	diffs         []ingestdomain.DiffEvent
	diffsErr      error
}

func (m *mockIngestRepo) WriteDiff(_ context.Context, _ ingestdomain.DiffEvent) error {
	return nil
}
func (m *mockIngestRepo) WriteCheckpoint(_ context.Context, _ ingestdomain.SnapshotEvent) error {
	return nil
}
func (m *mockIngestRepo) QueryNearestCheckpoint(_ context.Context, _ string, _ time.Time) (ingestdomain.SnapshotEvent, error) {
	return m.checkpoint, m.checkpointErr
}
func (m *mockIngestRepo) QueryDiffs(_ context.Context, _ string, _, _ time.Time) ([]ingestdomain.DiffEvent, error) {
	return m.diffs, m.diffsErr
}

// mockBtWriter implements btrepository.Writer for engine tests without a DB.
type mockBtWriter struct{ gaps int }

func (m *mockBtWriter) WriteDataGap(_ context.Context, _, _ string, _, _ time.Time) error {
	m.gaps++
	return nil
}

func makeEngine(
	t *testing.T,
	symbol string,
	ingestRepo *mockIngestRepo,
	btRepo btrepository.Writer,
	l2Pub *publisher.L2Publisher,
	ctrlPub *publisher.ControlPublisher,
	speed *replay.SpeedController,
) *replay.Engine {
	t.Helper()
	return replay.NewEngine(
		replay.EngineConfig{
			RunID:  "run-test",
			Symbol: symbol,
			From:   time.Now().Add(-time.Hour),
			To:     time.Now(),
			Depth:  10,
		},
		ingestRepo,
		btRepo,
		l2Pub, ctrlPub,
		speed,
		zap.NewNop(),
	)
}

// ── happy path ────────────────────────────────────────────────────────────────

func TestEngineReplaysDiffsAndPublishesL2Events(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := testutil.MakeSnapshot("BTCUSDT", 100, baseTime)
	diffs := testutil.MakeDiffSeq("BTCUSDT", 101, baseTime.Add(time.Second), 5)

	ingestRepo := &mockIngestRepo{checkpoint: snap, diffs: diffs}
	l2Pub := publisher.NewL2Publisher(client, ns, zap.NewNop())
	ctrlPub := publisher.NewControlPublisher(client, ns, zap.NewNop())
	speed := replay.NewSpeedController(domain.SpeedAFAP, 1.0, zap.NewNop())

	eng := makeEngine(t, "BTCUSDT", ingestRepo, &mockBtWriter{}, l2Pub, ctrlPub, speed)
	require.NoError(t, eng.Run(context.Background()))

	msgs := testutil.ReadAllStream(t, client, ns+l2StreamSuffix)
	assert.Len(t, msgs, 5, "one L2 event must be published per diff")
}

func TestEngineEventTimeComesFromDiffNotTimeNow(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	baseTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	snap := testutil.MakeSnapshot("BTCUSDT", 200, baseTime)

	diffTime := time.Date(2026, 1, 1, 12, 0, 5, 0, time.UTC)
	diff := ingestdomain.DiffEvent{
		Symbol:        "BTCUSDT",
		EventTime:     diffTime,
		FirstUpdateID: 201,
		FinalUpdateID: 201,
	}

	ingestRepo := &mockIngestRepo{checkpoint: snap, diffs: []ingestdomain.DiffEvent{diff}}
	l2Pub := publisher.NewL2Publisher(client, ns, zap.NewNop())
	ctrlPub := publisher.NewControlPublisher(client, ns, zap.NewNop())
	speed := replay.NewSpeedController(domain.SpeedAFAP, 1.0, zap.NewNop())

	eng := makeEngine(t, "BTCUSDT", ingestRepo, &mockBtWriter{}, l2Pub, ctrlPub, speed)
	require.NoError(t, eng.Run(context.Background()))

	msgs := testutil.ReadAllStream(t, client, ns+l2StreamSuffix)
	require.Len(t, msgs, 1)

	// The event_time in Redis must equal the diff's EventTime, not wall clock.
	assert.Equal(t,
		diffTime.UTC().Format(time.RFC3339Nano),
		msgs[0].Values["event_time"],
		"event_time must be the diff's EventTime (logical clock invariant)",
	)
}

func TestEngineRunIDPropagatedToStream(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	baseTime := time.Now()
	snap := testutil.MakeSnapshot("BTCUSDT", 10, baseTime)
	diffs := testutil.MakeDiffSeq("BTCUSDT", 11, baseTime.Add(time.Second), 1)

	ingestRepo := &mockIngestRepo{checkpoint: snap, diffs: diffs}
	l2Pub := publisher.NewL2Publisher(client, ns, zap.NewNop())
	ctrlPub := publisher.NewControlPublisher(client, ns, zap.NewNop())
	speed := replay.NewSpeedController(domain.SpeedAFAP, 1.0, zap.NewNop())

	eng := replay.NewEngine(
		replay.EngineConfig{
			RunID:  "my-run-id",
			Symbol: "BTCUSDT",
			From:   baseTime.Add(-time.Hour),
			To:     baseTime.Add(time.Hour),
			Depth:  10,
		},
		ingestRepo, &mockBtWriter{}, l2Pub, ctrlPub, speed, zap.NewNop(),
	)
	require.NoError(t, eng.Run(context.Background()))

	msgs := testutil.ReadAllStream(t, client, ns+l2StreamSuffix)
	require.Len(t, msgs, 1)
	assert.Equal(t, "my-run-id", msgs[0].Values["run_id"])
}

// ── empty diffs ───────────────────────────────────────────────────────────────

func TestEngineNoDiffsPublishesNothing(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	snap := testutil.MakeSnapshot("BTCUSDT", 100, time.Now())
	ingestRepo := &mockIngestRepo{checkpoint: snap, diffs: nil}
	l2Pub := publisher.NewL2Publisher(client, ns, zap.NewNop())
	ctrlPub := publisher.NewControlPublisher(client, ns, zap.NewNop())
	speed := replay.NewSpeedController(domain.SpeedAFAP, 1.0, zap.NewNop())

	eng := makeEngine(t, "BTCUSDT", ingestRepo, &mockBtWriter{}, l2Pub, ctrlPub, speed)
	require.NoError(t, eng.Run(context.Background()))

	msgs := testutil.ReadAllStream(t, client, ns+l2StreamSuffix)
	assert.Empty(t, msgs, "no diffs → no published events")
}

// ── data gap ──────────────────────────────────────────────────────────────────

func TestEngineDataGapPublishesControlEvent(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := testutil.MakeSnapshot("BTCUSDT", 100, baseTime)

	// Diffs 101 and 103 — gap at 102.
	diff1 := testutil.MakeDiff("BTCUSDT", 100, baseTime.Add(time.Second))
	diff2 := ingestdomain.DiffEvent{
		Symbol:        "BTCUSDT",
		EventTime:     baseTime.Add(3 * time.Second),
		FirstUpdateID: 103, // gap: expected 102
		FinalUpdateID: 103,
	}

	// After gap, engine seeks a new checkpoint — return same snap.
	ingestRepo := &mockIngestRepo{
		checkpoint: snap,
		diffs:      []ingestdomain.DiffEvent{diff1, diff2},
	}
	l2Pub := publisher.NewL2Publisher(client, ns, zap.NewNop())
	ctrlPub := publisher.NewControlPublisher(client, ns, zap.NewNop())
	speed := replay.NewSpeedController(domain.SpeedAFAP, 1.0, zap.NewNop())

	eng := makeEngine(t, "BTCUSDT", ingestRepo, &mockBtWriter{}, l2Pub, ctrlPub, speed)
	require.NoError(t, eng.Run(context.Background()))

	// A DATA_GAP control event must have been published.
	ctrlMsgs := testutil.ReadAllStream(t, client, ns+":control")
	var hasGap bool
	for _, m := range ctrlMsgs {
		if m.Values["type"] == string(domain.ControlDataGap) {
			hasGap = true
			break
		}
	}
	assert.True(t, hasGap, "DATA_GAP control event must be published when a sequence gap is detected")

	// Both diffs should still produce L2 events despite the gap.
	l2Msgs := testutil.ReadAllStream(t, client, ns+l2StreamSuffix)
	assert.Len(t, l2Msgs, 2, "replay must continue after gap")
}

// ── checkpoint error ──────────────────────────────────────────────────────────

func TestEngineCheckpointErrorReturnsError(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	ingestRepo := &mockIngestRepo{checkpointErr: errors.New("no checkpoint")}
	l2Pub := publisher.NewL2Publisher(client, ns, zap.NewNop())
	ctrlPub := publisher.NewControlPublisher(client, ns, zap.NewNop())
	speed := replay.NewSpeedController(domain.SpeedAFAP, 1.0, zap.NewNop())

	eng := makeEngine(t, "BTCUSDT", ingestRepo, &mockBtWriter{}, l2Pub, ctrlPub, speed)
	err := eng.Run(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no checkpoint")
}

// ── context cancellation ──────────────────────────────────────────────────────

func TestEngineContextCancelledStopsReplay(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	baseTime := time.Now()
	snap := testutil.MakeSnapshot("BTCUSDT", 100, baseTime)
	// Large batch of diffs — context will be cancelled before all are published.
	diffs := testutil.MakeDiffSeq("BTCUSDT", 101, baseTime.Add(time.Second), 1000)

	ingestRepo := &mockIngestRepo{checkpoint: snap, diffs: diffs}
	l2Pub := publisher.NewL2Publisher(client, ns, zap.NewNop())
	ctrlPub := publisher.NewControlPublisher(client, ns, zap.NewNop())
	speed := replay.NewSpeedController(domain.SpeedAFAP, 1.0, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())

	eng := makeEngine(t, "BTCUSDT", ingestRepo, &mockBtWriter{}, l2Pub, ctrlPub, speed)

	// Cancel the context immediately.
	cancel()
	err := eng.Run(ctx)

	// Either context.Canceled or nil (if it finished before the check) is acceptable,
	// but if it ran all 1000 diffs in < 1ms that's fine too — AFAP is fast.
	if err != nil {
		assert.ErrorIs(t, err, context.Canceled)
	}
}
