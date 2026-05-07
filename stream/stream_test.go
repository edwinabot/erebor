package stream

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

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
