package execution_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/execution"
	"github.com/edwinabot/erebor/backtest/internal/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	testSymbol  = "BTCUSDT"
	testBidStr  = "50000.00"
	testAskStr  = "50001.00"
	testRunID   = "run-exec-test"
	blockDurStr = "100ms"
)

// seedL2 publishes one synthetic L2BookUpdateEvent to the stream.
// bidQty / askQty control the book imbalance.
func seedL2(t *testing.T, client *redis.Client, streamKey string, et time.Time, bidQty, askQty string) {
	t.Helper()
	bids, _ := json.Marshal([][2]string{{testBidStr, bidQty}})
	asks, _ := json.Marshal([][2]string{{testAskStr, askQty}})
	require.NoError(t, client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: streamKey,
		Values: map[string]any{
			"run_id":         testRunID,
			"symbol":         testSymbol,
			"event_time":     et.UTC().Format(time.RFC3339Nano),
			"last_update_id": "1",
			"bids":           string(bids),
			"asks":           string(asks),
		},
	}).Err())
}

// waitOrders polls the orders stream until at least want messages arrive.
func waitOrders(t *testing.T, client *redis.Client, streamKey string, want int, timeout time.Duration) []redis.XMessage {
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

// startExecutor creates and starts an Executor, returning a cancel func.
func startExecutor(t *testing.T, client *redis.Client, ns string, symbols []string, cfgJSON string) (context.CancelFunc, *execution.Executor) {
	t.Helper()
	cfg, err := execution.ParseStrategyConfig(cfgJSON)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	exec := execution.NewExecutor(client, ns, symbols, cfg, zap.NewNop(),
		execution.WithBlockDuration(100*time.Millisecond))
	exec.Start(ctx)
	t.Cleanup(func() {
		cancel()
		exec.Wait()
	})
	return cancel, exec
}

func ordersKey(ns string) string  { return ns + ":orders" }
func l2Key(ns, sym string) string { return ns + ":l2:" + sym }

// ── BUY on positive imbalance ─────────────────────────────────────────────────

func TestExecutorBuyOnHighPositiveImbalance(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// imbalance = (2.0 - 0.5) / 2.5 = 0.6 > 0.2 threshold → BUY
	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "2.0", "0.5")
	startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5"}`)

	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "Buy", msgs[0].Values["side"])
	assert.Equal(t, "Filled", msgs[0].Values["status"])
	assert.Equal(t, "Market", msgs[0].Values["type"])
}

// ── SELL on negative imbalance ────────────────────────────────────────────────

func TestExecutorSellOnHighNegativeImbalance(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// imbalance = (0.5 - 2.0) / 2.5 = -0.6 < -0.2 → SELL
	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "0.5", "2.0")
	startExecutor(t, client, ns, []string{testSymbol}, `{"sell_threshold":"0.5"}`)

	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "Sell", msgs[0].Values["side"])
	assert.Equal(t, "Filled", msgs[0].Values["status"])
}

// ── No trade when imbalance within threshold ──────────────────────────────────

func TestExecutorNoTradeWhenBalanced(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// imbalance = 0.0 — below 0.2 threshold
	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "1.0", "1.0")
	cancel, _ := startExecutor(t, client, ns, []string{testSymbol}, "{}")

	// Give the executor enough time to process
	time.Sleep(200 * time.Millisecond)
	cancel()

	msgs, err := client.XRange(context.Background(), ordersKey(ns), "-", "+").Result()
	require.NoError(t, err)
	assert.Empty(t, msgs, "balanced book should not fire any order")
}

// ── Toggle: BUY then SELL ─────────────────────────────────────────────────────

func TestExecutorToggleBuyThenSell(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// First event: BUY (bid-heavy). Second event: SELL (ask-heavy).
	seedL2(t, client, l2Key(ns, testSymbol), base, "3.0", "0.5")                  // BUY
	seedL2(t, client, l2Key(ns, testSymbol), base.Add(time.Second), "0.5", "3.0") // SELL

	startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5","sell_threshold":"0.5"}`)

	msgs := waitOrders(t, client, ordersKey(ns), 2, 3*time.Second)
	require.Len(t, msgs, 2)
	assert.Equal(t, "Buy", msgs[0].Values["side"])
	assert.Equal(t, "Sell", msgs[1].Values["side"])
}

// ── Toggle: duplicate BUY is ignored ─────────────────────────────────────────

func TestExecutorToggleIgnoresDuplicateBuy(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Two bid-heavy events in a row → only the first BUY fires
	seedL2(t, client, l2Key(ns, testSymbol), base, "3.0", "0.5")
	seedL2(t, client, l2Key(ns, testSymbol), base.Add(time.Second), "3.0", "0.5")

	cancel, _ := startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5"}`)
	waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	time.Sleep(200 * time.Millisecond)
	cancel()

	msgs, err := client.XRange(context.Background(), ordersKey(ns), "-", "+").Result()
	require.NoError(t, err)
	assert.Len(t, msgs, 1, "second bid-heavy event must not fire a second BUY (already long)")
}

// ── Fill price equals best ask for BUY ───────────────────────────────────────

func TestExecutorFillPriceIsBestAskForBuy(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "3.0", "0.5")
	startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5","slippage_bps":0}`)

	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	require.Len(t, msgs, 1)
	fillPrice, _ := decimal.NewFromString(msgs[0].Values["fill_price"].(string))
	assert.True(t, decimal.RequireFromString(testAskStr).Equal(fillPrice),
		"market BUY fill price must equal best_ask when slippage_bps=0; got %s", fillPrice)
}

// ── Fill price equals best bid for SELL ──────────────────────────────────────

func TestExecutorFillPriceIsBestBidForSell(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "0.5", "3.0")
	startExecutor(t, client, ns, []string{testSymbol}, `{"sell_threshold":"0.5","slippage_bps":0}`)

	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	require.Len(t, msgs, 1)
	fillPrice, _ := decimal.NewFromString(msgs[0].Values["fill_price"].(string))
	assert.True(t, decimal.RequireFromString(testBidStr).Equal(fillPrice),
		"market SELL fill price must equal best_bid when slippage_bps=0; got %s", fillPrice)
}

// ── Slippage added to BUY fill price ─────────────────────────────────────────

func TestExecutorSlippageIncreasesFilledBuyPrice(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// slippage_bps=10 (0.1%) on best_ask=50001 → fill = 50001 * 1.001 = 50051.001
	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "3.0", "0.5")
	startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5","slippage_bps":10}`)

	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	require.Len(t, msgs, 1)
	fillPrice, _ := decimal.NewFromString(msgs[0].Values["fill_price"].(string))
	bestAsk := decimal.RequireFromString(testAskStr)
	expected := bestAsk.Mul(decimal.RequireFromString("1.001"))
	assert.True(t, expected.Equal(fillPrice),
		"slippage buy fill: want %s got %s", expected, fillPrice)
}

// ── Fee = qty × price × taker_fee_bps / 10000 ────────────────────────────────

func TestExecutorFeeComputation(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// qty=0.001, fill_price=50001, taker_fee_bps=10 → fee = 0.001 * 50001 * 10/10000 = 0.050001
	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "3.0", "0.5")
	startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5","trade_qty":"0.001","taker_fee_bps":10,"slippage_bps":0}`)

	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	require.Len(t, msgs, 1)
	fee, _ := decimal.NewFromString(msgs[0].Values["fee"].(string))
	qty := decimal.RequireFromString("0.001")
	price := decimal.RequireFromString(testAskStr)
	expected := qty.Mul(price).Mul(decimal.NewFromInt(10)).Div(decimal.NewFromInt(10000))
	assert.True(t, expected.Equal(fee), "fee: want %s got %s", expected, fee)
}

// ── EventTime propagation ─────────────────────────────────────────────────────

func TestExecutorEventTimePropagation(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	seed := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	seedL2(t, client, l2Key(ns, testSymbol), seed, "3.0", "0.5")
	startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5"}`)

	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	require.Len(t, msgs, 1)
	etStr, _ := msgs[0].Values["event_time"].(string)
	et, err := time.Parse(time.RFC3339Nano, etStr)
	require.NoError(t, err)
	assert.Equal(t, seed.UTC(), et.UTC(), "order event_time must be propagated from L2 event_time")
}

// ── signal_name field ─────────────────────────────────────────────────────────

func TestExecutorSignalNameIsBookImbalance(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "3.0", "0.5")
	startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5"}`)

	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	require.Len(t, msgs, 1)
	assert.Equal(t, "book_imbalance", msgs[0].Values["signal_name"])
}

// ── multiple symbols: independent positions ───────────────────────────────────

func TestExecutorMultipleSymbolsIndependent(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// BTC: bid-heavy (BUY)
	bids, _ := json.Marshal([][2]string{{testBidStr, "3.0"}})
	asks, _ := json.Marshal([][2]string{{testAskStr, "0.5"}})
	require.NoError(t, client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: l2Key(ns, "BTCUSDT"),
		Values: map[string]any{"run_id": testRunID, "symbol": "BTCUSDT",
			"event_time": time.Now().UTC().Format(time.RFC3339Nano), "last_update_id": "1",
			"bids": string(bids), "asks": string(asks)},
	}).Err())

	// ETH: ask-heavy (SELL)
	bids2, _ := json.Marshal([][2]string{{"3000.00", "0.5"}})
	asks2, _ := json.Marshal([][2]string{{"3001.00", "3.0"}})
	require.NoError(t, client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: l2Key(ns, "ETHUSDT"),
		Values: map[string]any{"run_id": testRunID, "symbol": "ETHUSDT",
			"event_time": time.Now().UTC().Format(time.RFC3339Nano), "last_update_id": "1",
			"bids": string(bids2), "asks": string(asks2)},
	}).Err())

	startExecutor(t, client, ns, []string{"BTCUSDT", "ETHUSDT"}, `{"buy_threshold":"0.5","sell_threshold":"0.5"}`)

	msgs := waitOrders(t, client, ordersKey(ns), 2, 5*time.Second)
	require.Len(t, msgs, 2)

	sides := map[string]string{}
	for _, m := range msgs {
		sides[m.Values["symbol"].(string)] = m.Values["side"].(string)
	}
	assert.Equal(t, "Buy", sides["BTCUSDT"])
	assert.Equal(t, "Sell", sides["ETHUSDT"])
}

// ── malformed L2 event is skipped ────────────────────────────────────────────

func TestExecutorMalformedL2EventIsSkipped(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	// Bad event: missing event_time
	require.NoError(t, client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: l2Key(ns, testSymbol),
		Values: map[string]any{
			"run_id": testRunID, "symbol": testSymbol,
			"bids": `[["50000","1"]]`, "asks": `[["50001","1"]]`,
		},
	}).Err())

	// Good event after bad: bid-heavy → BUY
	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "3.0", "0.5")

	startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5"}`)
	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	assert.Len(t, msgs, 1, "good event must still produce an order after a malformed one")
}

// ── order fields are complete ─────────────────────────────────────────────────

func TestExecutorOrderHasRequiredFields(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	seedL2(t, client, l2Key(ns, testSymbol), time.Now(), "3.0", "0.5")
	startExecutor(t, client, ns, []string{testSymbol}, `{"buy_threshold":"0.5"}`)

	msgs := waitOrders(t, client, ordersKey(ns), 1, 3*time.Second)
	require.Len(t, msgs, 1)
	m := msgs[0].Values

	for _, field := range []string{"run_id", "symbol", "event_time", "order_id", "side", "type",
		"quantity", "status", "fill_price", "fill_qty", "fee"} {
		assert.NotEmpty(t, m[field], "field %q must be present and non-empty", field)
	}
	assert.Equal(t, string(domain.OrderStatusFilled), m["status"])
	assert.Equal(t, string(domain.OrderTypeMarket), m["type"])
}
