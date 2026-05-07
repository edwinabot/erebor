package dispatch_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/edwinabot/erebor/ingest/dispatch"
	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/edwinabot/erebor/ingest/stream"
	"github.com/edwinabot/erebor/ingest/symbol"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

type recordingHandler struct {
	mu     sync.Mutex
	diffs  []domain.DiffEvent
	state  symbol.SymbolState
	signal chan struct{}
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{
		state:  symbol.Synced,
		signal: make(chan struct{}, 16),
	}
}

func (r *recordingHandler) HandleDiff(ev domain.DiffEvent) {
	r.mu.Lock()
	r.diffs = append(r.diffs, ev)
	r.mu.Unlock()
	select {
	case r.signal <- struct{}{}:
	default:
	}
}

func (r *recordingHandler) State() symbol.SymbolState { return r.state }

func (r *recordingHandler) snapshot() []domain.DiffEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]domain.DiffEvent, len(r.diffs))
	copy(out, r.diffs)
	return out
}

func rawEvent(sym string, first, final int64, ts int64) stream.RawDiffEvent {
	return stream.RawDiffEvent{
		Stream: sym + "@depth",
		Data: stream.RawDiffPayload{
			EventType:     "depthUpdate",
			EventTimeMS:   ts,
			Symbol:        sym,
			FirstUpdateID: first,
			FinalUpdateID: final,
			Bids:          [][]string{{"100.0", "1.5"}, {"99.5", "2.0"}},
			Asks:          [][]string{{"101.0", "0.7"}},
		},
	}
}

func TestDispatcherRoutesEventsByUpperCaseSymbol(t *testing.T) {
	btc := newRecordingHandler()
	eth := newRecordingHandler()

	dp := dispatch.New(map[string]symbol.SymbolHandler{
		"BTCUSDT": btc,
		"ETHUSDT": eth,
	}, zap.NewNop())

	events := make(chan stream.RawDiffEvent, 4)
	events <- rawEvent("BTCUSDT", 1, 2, 1000)
	events <- rawEvent("ethusdt", 3, 4, 2000) // lowercase from wire
	events <- rawEvent("UNKNOWN", 5, 6, 3000) // no handler

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		dp.Run(ctx, events)
		close(done)
	}()

	require.Eventually(t, func() bool {
		return len(btc.snapshot()) >= 1 && len(eth.snapshot()) >= 1
	}, time.Second, 5*time.Millisecond, "both handlers received their event")

	cancel()
	close(events)
	<-done

	btcDiffs := btc.snapshot()
	require.Len(t, btcDiffs, 1)
	require.Equal(t, "BTCUSDT", btcDiffs[0].Symbol)
	require.Equal(t, int64(1), btcDiffs[0].FirstUpdateID)
	require.Equal(t, int64(2), btcDiffs[0].FinalUpdateID)
	require.Equal(t, time.UnixMilli(1000).UTC(), btcDiffs[0].EventTime)
	require.Len(t, btcDiffs[0].Bids, 2)
	require.True(t, btcDiffs[0].Bids[0].Price.String() == "100" || btcDiffs[0].Bids[0].Price.String() == "100.0")
	require.Len(t, btcDiffs[0].Asks, 1)

	ethDiffs := eth.snapshot()
	require.Len(t, ethDiffs, 1)
	require.Equal(t, "ETHUSDT", ethDiffs[0].Symbol)
}

// TestDispatcherSkipsFrameWithMalformedLevels verifies that an event whose
// bid/ask payload fails to parse is logged and skipped — the dispatcher
// continues processing subsequent events.
func TestDispatcherSkipsFrameWithMalformedLevels(t *testing.T) {
	good := newRecordingHandler()
	dp := dispatch.New(map[string]symbol.SymbolHandler{"BTCUSDT": good}, zap.NewNop())

	bad := stream.RawDiffEvent{
		Stream: "btcusdt@depth",
		Data: stream.RawDiffPayload{
			EventType:     "depthUpdate",
			EventTimeMS:   1000,
			Symbol:        "BTCUSDT",
			FirstUpdateID: 1,
			FinalUpdateID: 2,
			Bids:          [][]string{{"100.0"}}, // malformed: only 1 element
			Asks:          [][]string{},
		},
	}
	ok := rawEvent("BTCUSDT", 3, 4, 2000)

	events := make(chan stream.RawDiffEvent, 2)
	events <- bad
	events <- ok

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { dp.Run(ctx, events); close(done) }()

	require.Eventually(t, func() bool { return len(good.snapshot()) == 1 },
		time.Second, 5*time.Millisecond, "good event delivered")
	cancel()
	close(events)
	<-done

	diffs := good.snapshot()
	require.Len(t, diffs, 1, "malformed event dropped, only the good one routed")
	require.Equal(t, int64(4), diffs[0].FinalUpdateID)
}

// TestDispatcherSkipsFrameWithUnparseablePrice covers the parseLevels
// decimal-parse error path.
func TestDispatcherSkipsFrameWithUnparseablePrice(t *testing.T) {
	good := newRecordingHandler()
	dp := dispatch.New(map[string]symbol.SymbolHandler{"BTCUSDT": good}, zap.NewNop())

	bad := stream.RawDiffEvent{
		Stream: "btcusdt@depth",
		Data: stream.RawDiffPayload{
			EventType:     "depthUpdate",
			Symbol:        "BTCUSDT",
			FirstUpdateID: 1,
			FinalUpdateID: 2,
			Bids:          [][]string{{"not-a-number", "1.0"}},
			Asks:          [][]string{},
		},
	}
	events := make(chan stream.RawDiffEvent, 1)
	events <- bad

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { dp.Run(ctx, events); close(done) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	close(events)
	<-done

	require.Empty(t, good.snapshot(), "frame with bad price was dropped")
}

func TestDispatcherExitsWhenChannelClosed(t *testing.T) {
	dp := dispatch.New(map[string]symbol.SymbolHandler{}, zap.NewNop())
	events := make(chan stream.RawDiffEvent)
	close(events)

	done := make(chan struct{})
	go func() {
		dp.Run(context.Background(), events)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Dispatcher.Run did not exit when events channel closed")
	}
}
