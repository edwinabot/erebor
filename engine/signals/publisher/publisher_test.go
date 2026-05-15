package publisher_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/edwinabot/erebor/signals/domain"
	"github.com/edwinabot/erebor/signals/internal/testutil"
	"github.com/edwinabot/erebor/signals/publisher"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	liveNS      = "erebor:live"
	liveSignals = "erebor:live:signals"
	btcMidPrice = "94500.50"
	signalsSfx  = ":signals"
)

var testTime = time.Date(2026, 5, 15, 10, 0, 0, 0, time.UTC)

func d(s string) decimal.Decimal {
	v, _ := decimal.NewFromString(s)
	return v
}

func newPublisher(t *testing.T, namespace string) (*publisher.Publisher, *redis.Client) {
	t.Helper()
	_, client := testutil.NewMiniredis(t)
	return publisher.New(client, namespace), client
}

func readSignals(t *testing.T, client *redis.Client, streamKey string) []redis.XMessage {
	t.Helper()
	msgs, err := client.XRange(context.Background(), streamKey, "-", "+").Result()
	require.NoError(t, err)
	return msgs
}

// ── stream key ────────────────────────────────────────────────────────────────

func TestPublishWritesToLiveSignalsStream(t *testing.T) {
	pub, client := newPublisher(t, liveNS)
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "mid_price", Version: "1", Value: d(btcMidPrice), Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, liveSignals)
	require.Len(t, msgs, 1)
}

func TestPublishWritesToBacktestSignalsStream(t *testing.T) {
	ns := "erebor:backtest:run-xyz"
	pub, client := newPublisher(t, ns)
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "spread_bps", Version: "1", Value: d("1.05"), Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, ns+signalsSfx)
	require.Len(t, msgs, 1)
}

// ── field values ──────────────────────────────────────────────────────────────

func TestPublishFieldValues(t *testing.T) {
	pub, client := newPublisher(t, liveNS)
	sig := domain.SignalEvent{
		RunID:     "run-001",
		Symbol:    "ETHUSDT",
		EventTime: testTime,
		Name:      "book_imbalance",
		Version:   "1",
		Value:     d("0.312"),
		Params:    map[string]string{"depth": "10"},
	}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, liveSignals)
	require.Len(t, msgs, 1)
	fields := msgs[0].Values

	assert.Equal(t, "run-001", fields["run_id"])
	assert.Equal(t, "ETHUSDT", fields["symbol"])
	assert.Equal(t, "book_imbalance", fields["name"])
	assert.Equal(t, "1", fields["version"])
	assert.Equal(t, "0.312", fields["value"])
	assert.Equal(t, testTime.UTC().Format("2006-01-02T15:04:05.999999999Z"), fields["event_time"])
}

func TestPublishEmptyRunIDField(t *testing.T) {
	pub, client := newPublisher(t, liveNS)
	sig := domain.SignalEvent{RunID: "", Symbol: "BTCUSDT", EventTime: testTime, Name: "mid_price", Version: "1", Value: d("0"), Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, liveSignals)
	assert.Equal(t, "", msgs[0].Values["run_id"])
}

// ── decimal precision ─────────────────────────────────────────────────────────

func TestPublishDecimalPrecisionPreserved(t *testing.T) {
	pub, client := newPublisher(t, liveNS)
	// A value that float64 cannot represent exactly.
	precise := d("99.50248756218905")
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "spread_bps", Version: "1", Value: precise, Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, liveSignals)
	got, _ := decimal.NewFromString(msgs[0].Values["value"].(string))
	assert.True(t, precise.Equal(got), "decimal precision must survive Redis round-trip: want %s got %s", precise, got)
}

func TestPublishNegativeImbalanceValue(t *testing.T) {
	pub, client := newPublisher(t, liveNS)
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "book_imbalance", Version: "1", Value: d("-0.75"), Params: map[string]string{"depth": "10"}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, liveSignals)
	got, _ := decimal.NewFromString(msgs[0].Values["value"].(string))
	assert.True(t, d("-0.75").Equal(got))
}

// ── params JSON ───────────────────────────────────────────────────────────────

func TestPublishParamsMarshaled(t *testing.T) {
	pub, client := newPublisher(t, liveNS)
	params := map[string]string{"depth": "10", "version": "1"}
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "book_imbalance", Version: "1", Value: d("0.1"), Params: params}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, liveSignals)
	raw := msgs[0].Values["params"].(string)
	var got map[string]string
	require.NoError(t, json.Unmarshal([]byte(raw), &got))
	assert.Equal(t, params, got)
}

func TestPublishEmptyParamsIsValidJSON(t *testing.T) {
	pub, client := newPublisher(t, liveNS)
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "mid_price", Version: "1", Value: d("94500"), Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, liveSignals)
	raw := msgs[0].Values["params"].(string)
	var got map[string]string
	require.NoError(t, json.Unmarshal([]byte(raw), &got))
	assert.Empty(t, got)
}

// ── multiple publishes ────────────────────────────────────────────────────────

func TestPublishMultipleSignalsAppendToStream(t *testing.T) {
	pub, client := newPublisher(t, liveNS)
	ctx := context.Background()

	signals := []domain.SignalEvent{
		{Symbol: "BTCUSDT", EventTime: testTime, Name: "book_imbalance", Version: "1", Value: d("0.2"), Params: map[string]string{"depth": "10"}},
		{Symbol: "BTCUSDT", EventTime: testTime, Name: "spread_bps", Version: "1", Value: d("1.05"), Params: map[string]string{}},
		{Symbol: "BTCUSDT", EventTime: testTime, Name: "mid_price", Version: "1", Value: d(btcMidPrice), Params: map[string]string{}},
	}

	for _, sig := range signals {
		require.NoError(t, pub.Publish(ctx, sig))
	}

	msgs := readSignals(t, client, liveSignals)
	require.Len(t, msgs, 3)
	assert.Equal(t, "book_imbalance", msgs[0].Values["name"])
	assert.Equal(t, "spread_bps", msgs[1].Values["name"])
	assert.Equal(t, "mid_price", msgs[2].Values["name"])
}

// ── real Redis ────────────────────────────────────────────────────────────────

func TestPublishRealRedis(t *testing.T) {
	client := testutil.RealRedisClient(t)
	ns := testutil.UniqueNamespace(t)
	pub := publisher.New(client, ns)
	ctx := context.Background()

	sig := domain.SignalEvent{
		Symbol:    "BTCUSDT",
		EventTime: testTime,
		Name:      "mid_price",
		Version:   "1",
		Value:     d(btcMidPrice),
		Params:    map[string]string{},
	}

	require.NoError(t, pub.Publish(ctx, sig))

	msgs, err := client.XRange(ctx, ns+signalsSfx, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, btcMidPrice, msgs[0].Values["value"])

	t.Cleanup(func() { client.Del(ctx, ns+signalsSfx) })
}
