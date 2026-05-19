package l2cache_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/edwinabot/erebor/execution/l2cache"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	testNamespace = "erebor:test"
	testStreamKey = "erebor:test:l2:BTCUSDT"
)

func newClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return mr, c
}

func seedL2(t *testing.T, client *redis.Client, streamKey string, bidPrice, askPrice string) {
	t.Helper()
	bids, _ := json.Marshal([][2]string{{bidPrice, "1.0"}})
	asks, _ := json.Marshal([][2]string{{askPrice, "1.0"}})
	require.NoError(t, client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{
			"run_id":         "",
			"symbol":         "BTCUSDT",
			"event_time":     time.Now().UTC().Format(time.RFC3339Nano),
			"last_update_id": "1",
			"bids":           string(bids),
			"asks":           string(asks),
		},
	}).Err())
}

func TestCacheReturnsBestPrices(t *testing.T) {
	_, client := newClient(t)
	cache := l2cache.New(client, testNamespace, []string{"BTCUSDT"}, zap.NewNop(),
		l2cache.WithBlockDuration(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.Start(ctx)

	seedL2(t, client, testStreamKey, "50000", "50001")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bid, ask, ok := cache.BestPrices("BTCUSDT")
		if ok {
			assert.Equal(t, "50000", bid.String())
			assert.Equal(t, "50001", ask.String())
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cache did not populate within deadline")
}

func TestCacheReturnsNotOkWhenEmpty(t *testing.T) {
	_, client := newClient(t)
	cache := l2cache.New(client, testNamespace, []string{"BTCUSDT"}, zap.NewNop(),
		l2cache.WithBlockDuration(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.Start(ctx)

	_, _, ok := cache.BestPrices("BTCUSDT")
	assert.False(t, ok, "must return false when no L2 data cached yet")
}

func TestCacheUpdatesToLatestPrices(t *testing.T) {
	_, client := newClient(t)
	cache := l2cache.New(client, testNamespace, []string{"BTCUSDT"}, zap.NewNop(),
		l2cache.WithBlockDuration(50*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cache.Start(ctx)

	seedL2(t, client, testStreamKey, "50000", "50001")
	seedL2(t, client, testStreamKey, "51000", "51001")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		bid, _, ok := cache.BestPrices("BTCUSDT")
		if ok && bid.String() == "51000" {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cache did not update to latest price within deadline")
}
