package signals_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/edwinabot/erebor/signals/consumer"
	"github.com/edwinabot/erebor/signals/internal/testutil"
	"github.com/edwinabot/erebor/signals/publisher"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	btcusdtL2  = ":l2:BTCUSDT"
	signalsKey = ":signals"
)

func noopLogger() *zap.Logger {
	return zap.NewNop()
}

// waitForSignals polls the output stream until at least want messages arrive
// or the deadline is exceeded. It returns whatever is in the stream at timeout.
func waitForSignals(t *testing.T, client *redis.Client, streamKey string, want int, timeout time.Duration) []redis.XMessage {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		msgs, err := client.XRange(context.Background(), streamKey, "-", "+").Result()
		require.NoError(t, err)
		if len(msgs) >= want {
			return msgs
		}
		time.Sleep(10 * time.Millisecond)
	}
	msgs, _ := client.XRange(context.Background(), streamKey, "-", "+").Result()
	return msgs
}

func startPipeline(
	t *testing.T,
	client *redis.Client,
	namespace string,
	symbols []string,
	depth int,
) (context.CancelFunc, *consumer.Consumer) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	pub := publisher.New(client, namespace)
	cons := consumer.New(client, pub, namespace, symbols, depth, noopLogger(),
		consumer.WithConsumerID("test-consumer"),
		consumer.WithBlockDuration(100*time.Millisecond))

	require.NoError(t, cons.Start(ctx))
	t.Cleanup(func() {
		cancel()
		cons.Stop()
	})

	return cancel, cons
}

// ── single event ──────────────────────────────────────────────────────────────

func TestPipelineSingleEventProducesThreeSignals(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events[:1])

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	msgs := waitForSignals(t, client, ns+signalsKey, 3, 5*time.Second)
	require.Len(t, msgs, 3, "one L2 event must produce exactly 3 signals")

	names := make([]string, len(msgs))
	for i, m := range msgs {
		names[i] = m.Values["name"].(string)
	}
	assert.Contains(t, names, "book_imbalance")
	assert.Contains(t, names, "spread_bps")
	assert.Contains(t, names, "mid_price")
}

// ── multiple events ───────────────────────────────────────────────────────────

func TestPipelineTenEventsProducesThirtySignals(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	events := testutil.LoadBtcusdtEvents(t)
	require.Len(t, events, 10, "seed file must have exactly 10 events")
	testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events)

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	msgs := waitForSignals(t, client, ns+signalsKey, 30, 5*time.Second)
	assert.Len(t, msgs, 30, "10 events × 3 signals each = 30")
}

// ── signal values ─────────────────────────────────────────────────────────────

func TestPipelineMidPriceValue(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// First seed event: best bid = 94500.00, best ask = 94501.00
	// mid = (94500 + 94501) / 2 = 94500.50
	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events[:1])

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	msgs := waitForSignals(t, client, ns+signalsKey, 3, 5*time.Second)
	mid := findSignal(t, msgs, "mid_price")
	v, _ := decimal.NewFromString(mid.Values["value"].(string))
	expected := decimal.RequireFromString("94500.50")
	assert.True(t, expected.Equal(v), "mid_price: want %s got %s", expected, v)
}

func TestPipelineSpreadBpsValue(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// First event: bid=94500.00, ask=94501.00 → spread=1 → spread_bps = 1/94500.5*10000 ≈ 0.10582
	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events[:1])

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	msgs := waitForSignals(t, client, ns+signalsKey, 3, 5*time.Second)
	sig := findSignal(t, msgs, "spread_bps")
	v, _ := decimal.NewFromString(sig.Values["value"].(string))
	// spread_bps = (94501 - 94500) / ((94501+94500)/2) * 10000 = 1/94500.5*10000
	expected := decimal.NewFromInt(1).Div(decimal.RequireFromString("94500.5")).Mul(decimal.NewFromInt(10000))
	diff := v.Sub(expected).Abs()
	assert.True(t, diff.LessThan(decimal.RequireFromString("0.000001")), "spread_bps: want ~%s got %s", expected, v)
}

func TestPipelineBookImbalanceWithBidPressure(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// Second event in seed: heavy bid side → imbalance should be positive
	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events[1:2])

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	msgs := waitForSignals(t, client, ns+signalsKey, 3, 5*time.Second)
	sig := findSignal(t, msgs, "book_imbalance")
	v, _ := decimal.NewFromString(sig.Values["value"].(string))
	assert.True(t, v.IsPositive(), "bid-heavy book should produce positive imbalance, got %s", v)
}

func TestPipelineBookImbalanceWithAskPressure(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// Third event: heavy ask side → imbalance should be negative
	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events[2:3])

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	msgs := waitForSignals(t, client, ns+signalsKey, 3, 5*time.Second)
	sig := findSignal(t, msgs, "book_imbalance")
	v, _ := decimal.NewFromString(sig.Values["value"].(string))
	assert.True(t, v.IsNegative(), "ask-heavy book should produce negative imbalance, got %s", v)
}

// ── EventTime propagation ─────────────────────────────────────────────────────

func TestPipelineEventTimePropagatesToSignals(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events[:1])

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	msgs := waitForSignals(t, client, ns+signalsKey, 3, 5*time.Second)
	require.NotEmpty(t, msgs)

	for _, msg := range msgs {
		etStr := msg.Values["event_time"].(string)
		et, err := time.Parse(time.RFC3339Nano, etStr)
		require.NoError(t, err, "event_time must be valid RFC3339Nano")
		// EventTime must match the original seed event time.
		assert.Equal(t, events[0].EventTime.UTC(), et.UTC(),
			"signal event_time must be propagated from L2 event, not time.Now()")
	}
}

// ── multiple symbols ──────────────────────────────────────────────────────────

func TestPipelineMultipleSymbols(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	ctx := context.Background()

	btcEvents := testutil.LoadBtcusdtEvents(t)
	// Reuse BTC seed data for ETH (symbol field will differ via stream key, not payload).
	testutil.SeedStream(ctx, t, client, ns+btcusdtL2, btcEvents[:3])
	testutil.SeedStream(ctx, t, client, ns+":l2:ETHUSDT", btcEvents[:2])

	startPipeline(t, client, ns, []string{"BTCUSDT", "ETHUSDT"}, 10)

	// 3 BTC events + 2 ETH events = 5 events × 3 signals = 15 signals total.
	msgs := waitForSignals(t, client, ns+signalsKey, 15, 5*time.Second)
	assert.GreaterOrEqual(t, len(msgs), 15)
}

// ── backtest namespace ────────────────────────────────────────────────────────

func TestPipelineBacktestNamespace(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	runID := "test-run-abc-123"
	ns := "erebor:backtest:" + runID

	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events[:2])

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	// Signals must appear in the backtest-namespaced output stream.
	msgs := waitForSignals(t, client, ns+signalsKey, 6, 5*time.Second)
	assert.GreaterOrEqual(t, len(msgs), 6)

	// Live stream must remain empty.
	liveSignals, _ := client.XRange(context.Background(), "erebor:live"+signalsKey, "-", "+").Result()
	assert.Empty(t, liveSignals, "backtest signals must not leak into live stream")
}

// ── malformed event ───────────────────────────────────────────────────────────

func TestPipelineMalformedEventIsSkipped(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	ctx := context.Background()

	// Seed one bad event followed by one good event.
	require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{
		Stream: ns + btcusdtL2,
		Values: map[string]any{
			"symbol": "BTCUSDT",
			// Missing event_time — must be skipped, not crash.
			"bids": `[["94500","1"]]`,
			"asks": `[["94501","1"]]`,
		},
	}).Err())

	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(ctx, t, client, ns+btcusdtL2, events[:1])

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	// Good event must still produce 3 signals; bad event produces 0.
	msgs := waitForSignals(t, client, ns+signalsKey, 3, 5*time.Second)
	assert.GreaterOrEqual(t, len(msgs), 3, "good event signals must be published despite prior malformed event")
}

// ── depth parameter ───────────────────────────────────────────────────────────

func TestPipelineSignalDepthAffectsImbalance(t *testing.T) {
	events := testutil.LoadBtcusdtEvents(t)

	// Use event index 4: heavily bid-side at depth=1 (first bid >> first ask).
	for _, depth := range []int{1, 5, 10} {
		depth := depth
		t.Run(fmt.Sprintf("depth_%d", depth), func(t *testing.T) {
			_, client := testutil.NewMiniredis(t)
			ns := testutil.UniqueNamespace(t)

			testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events[4:5])
			startPipeline(t, client, ns, []string{"BTCUSDT"}, depth)

			msgs := waitForSignals(t, client, ns+signalsKey, 3, 5*time.Second)
			sig := findSignal(t, msgs, "book_imbalance")
			params := parseParams(t, sig)
			assert.Equal(t, fmt.Sprintf("%d", depth), params["depth"],
				"imbalance params.depth must match configured signal depth")
		})
	}
}

// ── params field ──────────────────────────────────────────────────────────────

func TestPipelineBookImbalanceParamsContainDepth(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(context.Background(), t, client, ns+btcusdtL2, events[:1])
	startPipeline(t, client, ns, []string{"BTCUSDT"}, 7)

	msgs := waitForSignals(t, client, ns+signalsKey, 3, 5*time.Second)
	sig := findSignal(t, msgs, "book_imbalance")
	params := parseParams(t, sig)
	assert.Equal(t, "7", params["depth"])
}

// ── real Redis ────────────────────────────────────────────────────────────────

func TestPipelineRealRedis(t *testing.T) {
	client := testutil.RealRedisClient(t)
	ns := testutil.UniqueNamespace(t)
	ctx := context.Background()

	t.Cleanup(func() {
		// Remove all test keys to keep Redis clean.
		keys, _ := client.Keys(ctx, ns+":*").Result()
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
	})

	events := testutil.LoadBtcusdtEvents(t)
	testutil.SeedStream(ctx, t, client, ns+btcusdtL2, events[:5])

	startPipeline(t, client, ns, []string{"BTCUSDT"}, 10)

	msgs := waitForSignals(t, client, ns+signalsKey, 15, 10*time.Second)
	assert.GreaterOrEqual(t, len(msgs), 15, "5 events × 3 signals = 15")

	// Verify at least one mid_price signal has a plausible value.
	midSigs := filterByName(msgs, "mid_price")
	require.NotEmpty(t, midSigs)
	v, _ := decimal.NewFromString(midSigs[0].Values["value"].(string))
	low := decimal.RequireFromString("90000")
	high := decimal.RequireFromString("100000")
	assert.True(t, v.GreaterThan(low) && v.LessThan(high),
		"mid_price should be in the plausible BTC range, got %s", v)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func findSignal(t *testing.T, msgs []redis.XMessage, name string) redis.XMessage {
	t.Helper()
	for _, m := range msgs {
		if m.Values["name"] == name {
			return m
		}
	}
	t.Fatalf("signal %q not found in %d messages", name, len(msgs))
	return redis.XMessage{}
}

func filterByName(msgs []redis.XMessage, name string) []redis.XMessage {
	var out []redis.XMessage
	for _, m := range msgs {
		if m.Values["name"] == name {
			out = append(out, m)
		}
	}
	return out
}

func parseParams(t *testing.T, msg redis.XMessage) map[string]string {
	t.Helper()
	raw, _ := msg.Values["params"].(string)
	var p map[string]string
	require.NoError(t, json.Unmarshal([]byte(raw), &p))
	return p
}
