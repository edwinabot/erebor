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

func TestPublish_WritesToLiveSignalsStream(t *testing.T) {
	pub, client := newPublisher(t, "erebor:live")
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "mid_price", Version: "1", Value: d("94500.50"), Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, "erebor:live:signals")
	require.Len(t, msgs, 1)
}

func TestPublish_WritesToBacktestSignalsStream(t *testing.T) {
	ns := "erebor:backtest:run-xyz"
	pub, client := newPublisher(t, ns)
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "spread_bps", Version: "1", Value: d("1.05"), Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, ns+":signals")
	require.Len(t, msgs, 1)
}

// ── field values ──────────────────────────────────────────────────────────────

func TestPublish_FieldValues(t *testing.T) {
	pub, client := newPublisher(t, "erebor:live")
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

	msgs := readSignals(t, client, "erebor:live:signals")
	require.Len(t, msgs, 1)
	fields := msgs[0].Values

	assert.Equal(t, "run-001", fields["run_id"])
	assert.Equal(t, "ETHUSDT", fields["symbol"])
	assert.Equal(t, "book_imbalance", fields["name"])
	assert.Equal(t, "1", fields["version"])
	assert.Equal(t, "0.312", fields["value"])
	assert.Equal(t, testTime.UTC().Format("2006-01-02T15:04:05.999999999Z"), fields["event_time"])
}

func TestPublish_EmptyRunIDField(t *testing.T) {
	pub, client := newPublisher(t, "erebor:live")
	sig := domain.SignalEvent{RunID: "", Symbol: "BTCUSDT", EventTime: testTime, Name: "mid_price", Version: "1", Value: d("0"), Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, "erebor:live:signals")
	assert.Equal(t, "", msgs[0].Values["run_id"])
}

// ── decimal precision ─────────────────────────────────────────────────────────

func TestPublish_DecimalPrecisionPreserved(t *testing.T) {
	pub, client := newPublisher(t, "erebor:live")
	// A value that float64 cannot represent exactly.
	precise := d("99.50248756218905")
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "spread_bps", Version: "1", Value: precise, Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, "erebor:live:signals")
	got, _ := decimal.NewFromString(msgs[0].Values["value"].(string))
	assert.True(t, precise.Equal(got), "decimal precision must survive Redis round-trip: want %s got %s", precise, got)
}

func TestPublish_NegativeImbalanceValue(t *testing.T) {
	pub, client := newPublisher(t, "erebor:live")
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "book_imbalance", Version: "1", Value: d("-0.75"), Params: map[string]string{"depth": "10"}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, "erebor:live:signals")
	got, _ := decimal.NewFromString(msgs[0].Values["value"].(string))
	assert.True(t, d("-0.75").Equal(got))
}

// ── params JSON ───────────────────────────────────────────────────────────────

func TestPublish_ParamsMarshaled(t *testing.T) {
	pub, client := newPublisher(t, "erebor:live")
	params := map[string]string{"depth": "10", "version": "1"}
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "book_imbalance", Version: "1", Value: d("0.1"), Params: params}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, "erebor:live:signals")
	raw := msgs[0].Values["params"].(string)
	var got map[string]string
	require.NoError(t, json.Unmarshal([]byte(raw), &got))
	assert.Equal(t, params, got)
}

func TestPublish_EmptyParamsIsValidJSON(t *testing.T) {
	pub, client := newPublisher(t, "erebor:live")
	sig := domain.SignalEvent{Symbol: "BTCUSDT", EventTime: testTime, Name: "mid_price", Version: "1", Value: d("94500"), Params: map[string]string{}}

	require.NoError(t, pub.Publish(context.Background(), sig))

	msgs := readSignals(t, client, "erebor:live:signals")
	raw := msgs[0].Values["params"].(string)
	var got map[string]string
	require.NoError(t, json.Unmarshal([]byte(raw), &got))
	assert.Empty(t, got)
}

// ── multiple publishes ────────────────────────────────────────────────────────

func TestPublish_MultipleSignalsAppendToStream(t *testing.T) {
	pub, client := newPublisher(t, "erebor:live")
	ctx := context.Background()

	signals := []domain.SignalEvent{
		{Symbol: "BTCUSDT", EventTime: testTime, Name: "book_imbalance", Version: "1", Value: d("0.2"), Params: map[string]string{"depth": "10"}},
		{Symbol: "BTCUSDT", EventTime: testTime, Name: "spread_bps", Version: "1", Value: d("1.05"), Params: map[string]string{}},
		{Symbol: "BTCUSDT", EventTime: testTime, Name: "mid_price", Version: "1", Value: d("94500.50"), Params: map[string]string{}},
	}

	for _, sig := range signals {
		require.NoError(t, pub.Publish(ctx, sig))
	}

	msgs := readSignals(t, client, "erebor:live:signals")
	require.Len(t, msgs, 3)
	assert.Equal(t, "book_imbalance", msgs[0].Values["name"])
	assert.Equal(t, "spread_bps", msgs[1].Values["name"])
	assert.Equal(t, "mid_price", msgs[2].Values["name"])
}

// ── real Redis ────────────────────────────────────────────────────────────────

func TestPublish_RealRedis(t *testing.T) {
	client := testutil.RealRedisClient(t)
	ns := testutil.UniqueNamespace(t)
	pub := publisher.New(client, ns)
	ctx := context.Background()

	sig := domain.SignalEvent{
		Symbol:    "BTCUSDT",
		EventTime: testTime,
		Name:      "mid_price",
		Version:   "1",
		Value:     d("94500.50"),
		Params:    map[string]string{},
	}

	require.NoError(t, pub.Publish(ctx, sig))

	msgs, err := client.XRange(ctx, ns+":signals", "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, "94500.50", msgs[0].Values["value"])

	t.Cleanup(func() { client.Del(ctx, ns+":signals") })
}
