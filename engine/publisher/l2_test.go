package publisher_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/edwinabot/erebor/ingest/publisher"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	testNamespace = "erebor:test"
	testL2Stream  = "erebor:test:l2:BTCUSDT"
)

func newTestClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = c.Close() })
	return mr, c
}

func TestL2PublisherWritesAllFields(t *testing.T) {
	mr, client := newTestClient(t)
	_ = mr
	ns := testNamespace
	pub := publisher.NewL2Publisher(client, ns, zap.NewNop())

	et := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	bids := []domain.PriceLevel{{Price: decimal.RequireFromString("50000"), Quantity: decimal.RequireFromString("1.5")}}
	asks := []domain.PriceLevel{{Price: decimal.RequireFromString("50001"), Quantity: decimal.RequireFromString("1.0")}}

	err := pub.Publish(context.Background(), "", "BTCUSDT", et, 42, bids, asks)
	require.NoError(t, err)

	streamKey := ns + ":l2:BTCUSDT"
	msgs, err := client.XRange(context.Background(), streamKey, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	m := msgs[0].Values
	assert.Equal(t, "", m["run_id"])
	assert.Equal(t, "BTCUSDT", m["symbol"])
	assert.Equal(t, et.Format(time.RFC3339Nano), m["event_time"])
	assert.Equal(t, "42", m["last_update_id"])

	var bidPairs [][2]string
	require.NoError(t, json.Unmarshal([]byte(m["bids"].(string)), &bidPairs))
	assert.Equal(t, "50000", bidPairs[0][0])
	assert.Equal(t, "1.5", bidPairs[0][1])

	var askPairs [][2]string
	require.NoError(t, json.Unmarshal([]byte(m["asks"].(string)), &askPairs))
	assert.Equal(t, "50001", askPairs[0][0])
	assert.Equal(t, "1", askPairs[0][1])
}

func TestL2PublisherSymbolUppercased(t *testing.T) {
	_, client := newTestClient(t)
	pub := publisher.NewL2Publisher(client, testNamespace, zap.NewNop())

	err := pub.Publish(context.Background(), "", "btcusdt", time.Now(), 1, nil, nil)
	require.NoError(t, err)

	msgs, err := client.XRange(context.Background(), testL2Stream, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1, "stream key must use uppercase symbol")
	assert.Equal(t, "BTCUSDT", msgs[0].Values["symbol"])
}

func TestL2PublisherEmptyRunIDForLive(t *testing.T) {
	_, client := newTestClient(t)
	pub := publisher.NewL2Publisher(client, "erebor:live", zap.NewNop())

	err := pub.Publish(context.Background(), "", "BTCUSDT", time.Now(), 1, nil, nil)
	require.NoError(t, err)

	msgs, err := client.XRange(context.Background(), "erebor:live:l2:BTCUSDT", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "", msgs[0].Values["run_id"], "live events must have empty run_id")
}

func TestL2PublisherMaxLenTrimsStream(t *testing.T) {
	_, client := newTestClient(t)
	pub := publisher.NewL2Publisher(client, testNamespace, zap.NewNop(),
		publisher.WithMaxLen(3))

	for i := 0; i < 10; i++ {
		require.NoError(t, pub.Publish(context.Background(), "", "BTCUSDT", time.Now(), int64(i), nil, nil))
	}

	msgs, err := client.XRange(context.Background(), testL2Stream, "-", "+").Result()
	require.NoError(t, err)
	assert.LessOrEqual(t, len(msgs), 3, "MAXLEN=3 should trim stream to at most 3 entries")
}

func TestL2PublisherNoMaxLenByDefault(t *testing.T) {
	_, client := newTestClient(t)
	pub := publisher.NewL2Publisher(client, testNamespace, zap.NewNop())

	for i := 0; i < 5; i++ {
		require.NoError(t, pub.Publish(context.Background(), "", "BTCUSDT", time.Now(), int64(i), nil, nil))
	}

	msgs, err := client.XRange(context.Background(), testL2Stream, "-", "+").Result()
	require.NoError(t, err)
	assert.Len(t, msgs, 5, "without MaxLen, all messages should be present")
}
