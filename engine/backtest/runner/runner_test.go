package runner_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/internal/testutil"
	"github.com/edwinabot/erebor/backtest/publisher"
	"github.com/edwinabot/erebor/backtest/runner"
	ingestdomain "github.com/edwinabot/erebor/ingest/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const runNamespacePrefix = "erebor:backtest:"

// ── mocks ─────────────────────────────────────────────────────────────────────

type mockRunStore struct {
	createErr      error
	updateErr      error
	writeGapErr    error
	writeMetricErr error

	createCalls  int
	statusCalls  []string
	metricsCalls int
}

func (m *mockRunStore) CreateRun(_ context.Context, _ domain.RunRecord) error {
	m.createCalls++
	return m.createErr
}
func (m *mockRunStore) UpdateRunStatus(_ context.Context, _ string, status domain.RunStatus, _, _ *time.Time, _ string) error {
	m.statusCalls = append(m.statusCalls, string(status))
	return m.updateErr
}
func (m *mockRunStore) WriteDataGap(_ context.Context, _, _ string, _, _ time.Time) error {
	return m.writeGapErr
}
func (m *mockRunStore) WriteTrade(_ context.Context, _ domain.TradeRecord) error       { return nil }
func (m *mockRunStore) WriteEquityPoint(_ context.Context, _ domain.EquityPoint) error { return nil }
func (m *mockRunStore) WriteMetrics(_ context.Context, _ domain.MetricsRecord) error {
	m.metricsCalls++
	return m.writeMetricErr
}
func (m *mockRunStore) QueryTrades(_ context.Context, _ string) ([]domain.TradeRecord, error) {
	return nil, nil
}
func (m *mockRunStore) QueryEquityPoints(_ context.Context, _ string) ([]domain.EquityPoint, error) {
	return nil, nil
}

type mockIngestRepo struct {
	checkpoint    ingestdomain.SnapshotEvent
	checkpointErr error
	diffs         []ingestdomain.DiffEvent
	diffsErr      error
}

func (m *mockIngestRepo) WriteDiff(_ context.Context, _ ingestdomain.DiffEvent) error { return nil }
func (m *mockIngestRepo) WriteCheckpoint(_ context.Context, _ ingestdomain.SnapshotEvent) error {
	return nil
}
func (m *mockIngestRepo) QueryNearestCheckpoint(_ context.Context, _ string, _ time.Time) (ingestdomain.SnapshotEvent, error) {
	return m.checkpoint, m.checkpointErr
}
func (m *mockIngestRepo) QueryDiffs(_ context.Context, _ string, _, _ time.Time) ([]ingestdomain.DiffEvent, error) {
	return m.diffs, m.diffsErr
}

// ── helpers ───────────────────────────────────────────────────────────────────

func makeRunner(
	t *testing.T,
	runID string,
	store *mockRunStore,
	ingest *mockIngestRepo,
	symbols []string,
) *runner.BacktestRunner {
	t.Helper()
	_, client := testutil.NewMiniredis(t)
	ns := runNamespacePrefix + runID
	pubs := runner.Publishers{
		L2:      publisher.NewL2Publisher(client, ns, zap.NewNop()),
		Control: publisher.NewControlPublisher(client, ns, zap.NewNop()),
	}
	return runner.New(
		runner.RunnerConfig{
			RunID:          runID,
			Symbols:        symbols,
			From:           time.Now().Add(-time.Hour),
			To:             time.Now(),
			Depth:          5,
			SpeedMode:      domain.SpeedAFAP,
			SpeedFactor:    1.0,
			StrategyConfig: "{}",
		},
		store, ingest, pubs, client,
		zap.NewNop(),
		runner.WithCollectorBlockDuration(50*time.Millisecond),
	)
}

// ── happy path ────────────────────────────────────────────────────────────────

func TestRunnerHappyPathTransitionsToCompleted(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := testutil.MakeSnapshot("BTCUSDT", 100, baseTime)
	diffs := testutil.MakeDiffSeq("BTCUSDT", 101, baseTime.Add(time.Second), 3)
	ingest := &mockIngestRepo{checkpoint: snap, diffs: diffs}
	store := &mockRunStore{}

	r := makeRunner(t, "run-happy", store, ingest, []string{"BTCUSDT"})
	require.NoError(t, r.Run(context.Background()))

	assert.Equal(t, 1, store.createCalls, "CreateRun must be called once")
	assert.Contains(t, store.statusCalls, string(domain.RunStatusRunning))
	assert.Contains(t, store.statusCalls, string(domain.RunStatusCompleted))
	assert.Equal(t, 1, store.metricsCalls, "WriteMetrics must be called once on COMPLETED")
}

func TestRunnerHappyPathPublishesL2Events(t *testing.T) {
	runID := "run-l2"
	_, client := testutil.NewMiniredis(t)
	ns := runNamespacePrefix + runID

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := testutil.MakeSnapshot("BTCUSDT", 100, baseTime)
	diffs := testutil.MakeDiffSeq("BTCUSDT", 101, baseTime.Add(time.Second), 5)
	ingest := &mockIngestRepo{checkpoint: snap, diffs: diffs}

	pubs := runner.Publishers{
		L2:      publisher.NewL2Publisher(client, ns, zap.NewNop()),
		Control: publisher.NewControlPublisher(client, ns, zap.NewNop()),
	}
	r := runner.New(
		runner.RunnerConfig{
			RunID:          runID,
			Symbols:        []string{"BTCUSDT"},
			From:           baseTime.Add(-time.Hour),
			To:             baseTime.Add(time.Hour),
			Depth:          10,
			SpeedMode:      domain.SpeedAFAP,
			SpeedFactor:    1.0,
			StrategyConfig: "{}",
		},
		&mockRunStore{}, ingest, pubs, client,
		zap.NewNop(),
		runner.WithCollectorBlockDuration(50*time.Millisecond),
	)

	require.NoError(t, r.Run(context.Background()))
	msgs := testutil.ReadAllStream(t, client, ns+":l2:BTCUSDT")
	assert.Len(t, msgs, 5, "one L2 event per diff must be published")
}

func TestRunnerMultipleSymbolsAllComplete(t *testing.T) {
	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := testutil.MakeSnapshot("BTCUSDT", 100, baseTime)
	diffs := testutil.MakeDiffSeq("BTCUSDT", 101, baseTime.Add(time.Second), 2)
	// Both symbols share the same mock repo (same checkpoint/diffs for simplicity).
	ingest := &mockIngestRepo{checkpoint: snap, diffs: diffs}
	store := &mockRunStore{}

	r := makeRunner(t, "run-multi", store, ingest, []string{"BTCUSDT", "ETHUSDT"})
	require.NoError(t, r.Run(context.Background()))

	assert.Contains(t, store.statusCalls, string(domain.RunStatusCompleted))
	assert.Equal(t, 1, store.metricsCalls)
}

// ── FAILED path ───────────────────────────────────────────────────────────────

func TestRunnerEngineErrorTransitionsToFailed(t *testing.T) {
	ingest := &mockIngestRepo{checkpointErr: errors.New("DB down")}
	store := &mockRunStore{}

	r := makeRunner(t, "run-fail", store, ingest, []string{"BTCUSDT"})
	err := r.Run(context.Background())

	require.Error(t, err)
	assert.Contains(t, store.statusCalls, string(domain.RunStatusFailed))
	assert.NotContains(t, store.statusCalls, string(domain.RunStatusCompleted))
	assert.Equal(t, 0, store.metricsCalls, "WriteMetrics must not be called on FAILED")
}

func TestRunnerCreateRunErrorAbortsBeforeRunning(t *testing.T) {
	baseTime := time.Now()
	snap := testutil.MakeSnapshot("BTCUSDT", 100, baseTime)
	ingest := &mockIngestRepo{checkpoint: snap, diffs: nil}
	store := &mockRunStore{createErr: errors.New("db full")}

	r := makeRunner(t, "run-create-fail", store, ingest, []string{"BTCUSDT"})
	err := r.Run(context.Background())

	require.Error(t, err)
	assert.Contains(t, err.Error(), "db full")
	// No status transitions should have occurred
	assert.Empty(t, store.statusCalls)
}

// ── CANCELLED path ────────────────────────────────────────────────────────────

func TestRunnerCancelledContextTransitionsToCancelled(t *testing.T) {
	baseTime := time.Now()
	snap := testutil.MakeSnapshot("BTCUSDT", 100, baseTime)
	diffs := testutil.MakeDiffSeq("BTCUSDT", 101, baseTime.Add(time.Second), 5)
	ingest := &mockIngestRepo{checkpoint: snap, diffs: diffs}
	store := &mockRunStore{}

	r := makeRunner(t, "run-cancel", store, ingest, []string{"BTCUSDT"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel so the engine returns ctx.Canceled on first diff

	err := r.Run(ctx)
	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Contains(t, store.statusCalls, string(domain.RunStatusCancelled))
	assert.NotContains(t, store.statusCalls, string(domain.RunStatusCompleted))
	assert.Equal(t, 0, store.metricsCalls, "WriteMetrics must not be called on CANCELLED")
}

// ── control events ────────────────────────────────────────────────────────────

func TestRunnerPublishesReplayStartAndComplete(t *testing.T) {
	runID := "run-ctrl"
	_, client := testutil.NewMiniredis(t)
	ns := runNamespacePrefix + runID

	baseTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	snap := testutil.MakeSnapshot("BTCUSDT", 100, baseTime)
	diffs := testutil.MakeDiffSeq("BTCUSDT", 101, baseTime.Add(time.Second), 1)
	ingest := &mockIngestRepo{checkpoint: snap, diffs: diffs}

	pubs := runner.Publishers{
		L2:      publisher.NewL2Publisher(client, ns, zap.NewNop()),
		Control: publisher.NewControlPublisher(client, ns, zap.NewNop()),
	}
	r := runner.New(
		runner.RunnerConfig{
			RunID:          runID,
			Symbols:        []string{"BTCUSDT"},
			From:           baseTime.Add(-time.Hour),
			To:             baseTime.Add(time.Hour),
			Depth:          5,
			SpeedMode:      domain.SpeedAFAP,
			SpeedFactor:    1.0,
			StrategyConfig: "{}",
		},
		&mockRunStore{}, ingest, pubs, client,
		zap.NewNop(),
		runner.WithCollectorBlockDuration(50*time.Millisecond),
	)

	require.NoError(t, r.Run(context.Background()))

	ctrlMsgs := testutil.ReadAllStream(t, client, ns+":control")
	var hasStart, hasComplete bool
	for _, m := range ctrlMsgs {
		switch m.Values["type"] {
		case string(domain.ControlReplayStart):
			hasStart = true
		case string(domain.ControlReplayComplete):
			hasComplete = true
		}
	}
	assert.True(t, hasStart, "REPLAY_START must be published")
	assert.True(t, hasComplete, "REPLAY_COMPLETE must be published")
}
