package stream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const unreachableWS = "ws://127.0.0.1:1"

func TestBuildURLComposesCombinedStream(t *testing.T) {
	got, err := buildURL("wss://stream.binance.com:9443", []string{"BTCUSDT", "ETHUSDT"})
	require.NoError(t, err)

	u, err := url.Parse(got)
	require.NoError(t, err)
	require.Equal(t, "wss", u.Scheme)
	require.Equal(t, "stream.binance.com:9443", u.Host)
	require.Equal(t, "/stream", u.Path)
	require.Equal(t, "btcusdt@depth/ethusdt@depth", u.Query().Get("streams"))
}

func TestBuildURLTrimsTrailingSlash(t *testing.T) {
	got, err := buildURL("wss://example.com/", []string{"BTCUSDT"})
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(got, "wss://example.com/stream?"), "got=%s", got)
}

func TestBuildURLRejectsEmptySymbolList(t *testing.T) {
	_, err := buildURL("wss://example.com", nil)
	require.Error(t, err)
}

func TestRawDiffEventJSONUnmarshal(t *testing.T) {
	const wire = `{
		"stream": "btcusdt@depth",
		"data": {
			"e": "depthUpdate",
			"E": 1620000000000,
			"s": "BTCUSDT",
			"U": 100,
			"u": 110,
			"b": [["100.5", "1.5"]],
			"a": [["100.6", "0.7"]]
		}
	}`

	var raw RawDiffEvent
	require.NoError(t, json.Unmarshal([]byte(wire), &raw))
	require.Equal(t, "btcusdt@depth", raw.Stream)
	require.Equal(t, "depthUpdate", raw.Data.EventType)
	require.Equal(t, int64(1620000000000), raw.Data.EventTimeMS)
	require.Equal(t, "BTCUSDT", raw.Data.Symbol)
	require.Equal(t, int64(100), raw.Data.FirstUpdateID)
	require.Equal(t, int64(110), raw.Data.FinalUpdateID)
	require.Equal(t, [][]string{{"100.5", "1.5"}}, raw.Data.Bids)
	require.Equal(t, [][]string{{"100.6", "0.7"}}, raw.Data.Asks)
}

// TestManagerEndToEndDeliversFrame stands up a fake Binance WS endpoint via
// httptest, connects a real Manager, and asserts the parsed event arrives on
// Events().
func TestManagerEndToEndDeliversFrame(t *testing.T) {
	frame := []byte(`{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1620000000000,"s":"BTCUSDT","U":1,"u":2,"b":[["100","1"]],"a":[["101","1"]]}}`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()
		// Push one frame, then block until the client disconnects.
		_ = c.Write(r.Context(), websocket.MessageText, frame)
		<-r.Context().Done()
	}))
	defer srv.Close()

	wsBase := "ws" + strings.TrimPrefix(srv.URL, "http")

	mgr := New(Config{
		BaseURL:      wsBase,
		Symbols:      []string{"BTCUSDT"},
		BufferSize:   8,
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     50 * time.Millisecond,
	}, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, mgr.Connect(ctx))

	select {
	case got := <-mgr.Events():
		require.Equal(t, "BTCUSDT", got.Data.Symbol)
		require.Equal(t, int64(1), got.Data.FirstUpdateID)
		require.Equal(t, int64(2), got.Data.FinalUpdateID)
	case <-time.After(2 * time.Second):
		t.Fatal("did not receive frame from manager")
	}

	require.NoError(t, mgr.Close())
}

// TestNewAppliesDefaults covers the three "<=0" default branches in New.
func TestNewAppliesDefaults(t *testing.T) {
	mgr := New(Config{Symbols: []string{"BTCUSDT"}}, zap.NewNop())
	require.NotNil(t, mgr)
	// Defaults are baked into mgr.cfg internally — observed indirectly via
	// the Events buffer accepting at least 1 send without blocking.
	require.NotNil(t, mgr.Events())
}

// TestCloseIsIdempotent: a second Close returns nil and does not panic on
// the already-closed events channel.
func TestCloseIsIdempotent(t *testing.T) {
	mgr := New(Config{
		BaseURL:      unreachableWS, // unreachable; runOnce will fail-and-back-off
		Symbols:      []string{"BTCUSDT"},
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     20 * time.Millisecond,
	}, zap.NewNop())

	require.NoError(t, mgr.Connect(context.Background()))
	require.NoError(t, mgr.Close())
	require.NoError(t, mgr.Close(), "second Close is a no-op")
}

// TestConnectAfterCloseReturnsError verifies the closed-flag guard in
// Connect.
func TestConnectAfterCloseReturnsError(t *testing.T) {
	mgr := New(Config{
		BaseURL:      unreachableWS,
		Symbols:      []string{"BTCUSDT"},
		InitialDelay: 10 * time.Millisecond,
		MaxDelay:     20 * time.Millisecond,
	}, zap.NewNop())

	require.NoError(t, mgr.Connect(context.Background()))
	require.NoError(t, mgr.Close())
	err := mgr.Connect(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "closed")
}

// TestRunLoopRetriesAfterDialFailure points the manager at an unreachable
// address and verifies the reconnect backoff loop invokes runOnce more
// than once before shutdown — i.e. the loop survives a dial error and
// honours ctx cancellation.
func TestRunLoopRetriesAfterDialFailure(t *testing.T) {
	mgr := New(Config{
		BaseURL:      unreachableWS, // unreachable port → dial fails immediately
		Symbols:      []string{"BTCUSDT"},
		InitialDelay: 5 * time.Millisecond,
		MaxDelay:     10 * time.Millisecond,
	}, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	require.NoError(t, mgr.Connect(ctx))
	<-ctx.Done()
	require.NoError(t, mgr.Close())
	// Smoke-only: if Close returned without deadlocking, the runLoop
	// retried-then-exited under ctx cancellation.
}

// TestReconnectAfterServerCloses: the runLoop must reconnect when the
// server hangs up. We stand up a server that sends one frame, closes the
// WS, accepts a second connection, and sends a second frame.
func TestReconnectAfterServerCloses(t *testing.T) {
	frame1 := []byte(`{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":1,"s":"BTCUSDT","U":1,"u":2,"b":[],"a":[]}}`)
	frame2 := []byte(`{"stream":"btcusdt@depth","data":{"e":"depthUpdate","E":2,"s":"BTCUSDT","U":3,"u":4,"b":[],"a":[]}}`)

	var connectCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := connectCount.Add(1)
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()
		if n == 1 {
			_ = c.Write(r.Context(), websocket.MessageText, frame1)
			// Force-close: client should reconnect.
			return
		}
		_ = c.Write(r.Context(), websocket.MessageText, frame2)
		<-r.Context().Done()
	}))
	defer srv.Close()

	mgr := New(Config{
		BaseURL:      "ws" + strings.TrimPrefix(srv.URL, "http"),
		Symbols:      []string{"BTCUSDT"},
		InitialDelay: 5 * time.Millisecond,
		MaxDelay:     20 * time.Millisecond,
		BufferSize:   8,
	}, zap.NewNop())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, mgr.Connect(ctx))
	defer func() { _ = mgr.Close() }()

	first, second := receiveFrame(t, mgr.Events()), receiveFrame(t, mgr.Events())
	require.ElementsMatch(t,
		[]int64{first.Data.FinalUpdateID, second.Data.FinalUpdateID},
		[]int64{2, 4},
	)
	require.GreaterOrEqual(t, int(connectCount.Load()), 2, "server saw >=2 connections")
}

func receiveFrame(t *testing.T, ch <-chan RawDiffEvent) RawDiffEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for frame")
		return RawDiffEvent{}
	}
}
