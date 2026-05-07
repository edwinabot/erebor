package fetcher_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/edwinabot/erebor/ingest/fetcher"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestFetchSnapshotHappyPath(t *testing.T) {
	const body = `{
		"lastUpdateId": 12345,
		"bids": [["100.50", "1.5"], ["100.40", "2.0"]],
		"asks": [["100.60", "0.7"], ["100.70", "1.2"]]
	}`

	var gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	df := fetcher.New(srv.URL)
	snap, err := df.FetchSnapshot(context.Background(), "btcusdt", 50)
	require.NoError(t, err)

	require.Equal(t, "/api/v3/depth", gotPath)
	require.Contains(t, gotQuery, "symbol=BTCUSDT")
	require.Contains(t, gotQuery, "limit=50")

	require.Equal(t, "BTCUSDT", snap.Symbol)
	require.Equal(t, int64(12345), snap.LastUpdateID)
	require.False(t, snap.CapturedAt.IsZero())

	require.Len(t, snap.Bids, 2)
	require.True(t, snap.Bids[0].Price.Equal(decimal.RequireFromString("100.50")))
	require.True(t, snap.Bids[0].Quantity.Equal(decimal.RequireFromString("1.5")))

	require.Len(t, snap.Asks, 2)
	require.True(t, snap.Asks[1].Price.Equal(decimal.RequireFromString("100.70")))
	require.True(t, snap.Asks[1].Quantity.Equal(decimal.RequireFromString("1.2")))
}

// TestFetchSnapshotReturnsErrorOnNon200 covers the status-code guard.
func TestFetchSnapshotReturnsErrorOnNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"code":-1003,"msg":"too many requests"}`))
	}))
	defer srv.Close()

	df := fetcher.New(srv.URL)
	_, err := df.FetchSnapshot(context.Background(), "BTCUSDT", 50)
	require.Error(t, err)
	require.Contains(t, err.Error(), "503")
}

// TestFetchSnapshotReturnsErrorOnInvalidJSON covers the json.Unmarshal
// error path.
func TestFetchSnapshotReturnsErrorOnInvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not valid json`))
	}))
	defer srv.Close()

	df := fetcher.New(srv.URL)
	_, err := df.FetchSnapshot(context.Background(), "BTCUSDT", 50)
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode depth")
}

// TestFetchSnapshotReturnsErrorOnInvalidPriceLevel covers the parseLevels
// decimal-parse error path.
func TestFetchSnapshotReturnsErrorOnInvalidPriceLevel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"lastUpdateId":1,"bids":[["abc","1"]],"asks":[]}`))
	}))
	defer srv.Close()

	df := fetcher.New(srv.URL)
	_, err := df.FetchSnapshot(context.Background(), "BTCUSDT", 50)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bids")
}

// TestFetchSnapshotReturnsErrorOnMalformedLevel covers parseLevels' length
// guard (level array with fewer than 2 elements).
func TestFetchSnapshotReturnsErrorOnMalformedLevel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"lastUpdateId":1,"bids":[],"asks":[["100"]]}`))
	}))
	defer srv.Close()

	df := fetcher.New(srv.URL)
	_, err := df.FetchSnapshot(context.Background(), "BTCUSDT", 50)
	require.Error(t, err)
	require.Contains(t, err.Error(), "asks")
}

// TestFetchSnapshotPropagatesContextCancellation: cancelling the caller's
// context aborts the in-flight request promptly.
func TestFetchSnapshotPropagatesContextCancellation(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-block
	}))
	defer func() { close(block); srv.Close() }()

	df := fetcher.New(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	_, err := df.FetchSnapshot(ctx, "BTCUSDT", 50)
	require.Error(t, err)
}

func TestFetchSnapshotDefaultsLimitWhenZero(t *testing.T) {
	var gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.RawQuery
		_, _ = w.Write([]byte(`{"lastUpdateId":1,"bids":[],"asks":[]}`))
	}))
	defer srv.Close()

	df := fetcher.New(srv.URL)
	_, err := df.FetchSnapshot(context.Background(), "ethusdt", 0)
	require.NoError(t, err)
	require.Contains(t, gotQuery, "limit=50")
}
