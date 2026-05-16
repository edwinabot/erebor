package collector_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/collector"
	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/internal/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	signalsSuffix = ":signals"
	ordersSuffix  = ":orders"
	testFillPrice = "50001.00"
	testFee       = "0.050001"
)

// orderSpec bundles the variable parts of a synthetic OrderEvent for seedOrder.
type orderSpec struct {
	side      domain.Side
	status    domain.OrderStatus
	fillPrice string
	qty       string
	fee       string
	eventTime time.Time
}

func nopLogger() *zap.Logger { return zap.NewNop() }

// mockTradeWriter captures WriteTrade and WriteEquityPoint calls for assertions.
type mockTradeWriter struct {
	mu     sync.Mutex
	trades []domain.TradeRecord
	equity []domain.EquityPoint
}

func (m *mockTradeWriter) WriteTrade(_ context.Context, t domain.TradeRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.trades = append(m.trades, t)
	return nil
}

func (m *mockTradeWriter) WriteEquityPoint(_ context.Context, p domain.EquityPoint) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.equity = append(m.equity, p)
	return nil
}

func (m *mockTradeWriter) tradeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.trades)
}

func (m *mockTradeWriter) equityCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.equity)
}

// seedOrder writes one synthetic OrderEvent to the :orders stream.
func seedOrder(t *testing.T, client *redis.Client, ns string, spec orderSpec) {
	t.Helper()
	require.NoError(t, client.XAdd(context.Background(), &redis.XAddArgs{
		Stream: ns + ordersSuffix,
		Values: map[string]any{
			"run_id":      "run-col-test",
			"symbol":      "BTCUSDT",
			"event_time":  spec.eventTime.UTC().Format(time.RFC3339Nano),
			"order_id":    "order-001",
			"side":        string(spec.side),
			"type":        "Market",
			"price":       "0",
			"quantity":    spec.qty,
			"status":      string(spec.status),
			"fill_price":  spec.fillPrice,
			"fill_qty":    spec.qty,
			"fee":         spec.fee,
			"signal_name": "book_imbalance",
		},
	}).Err())
}

// waitTrades polls until the mockTradeWriter has at least n trades or the deadline passes.
func waitTrades(t *testing.T, tw *mockTradeWriter, n int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tw.tradeCount() >= n {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// fastCollector creates a ResultCollector with a short XRead block for fast test teardown.
func fastCollector(client *redis.Client, ns, runID string) *collector.ResultCollector {
	return collector.New(client, ns, runID, nopLogger(), collector.WithBlockDuration(50*time.Millisecond))
}

// seedSignals writes n synthetic signal messages to the signals stream.
func seedSignals(t *testing.T, client *redis.Client, streamKey, symbol string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		require.NoError(t, client.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			Values: map[string]any{
				"run_id":     "run-test",
				"symbol":     symbol,
				"event_time": time.Now().UTC().Format(time.RFC3339Nano),
				"name":       "mid_price",
				"version":    "1",
				"value":      fmt.Sprintf("%d", 50000+i),
				"params":     "{}",
			},
		}).Err())
	}
}

// waitForSignals polls until TotalSignals >= want or deadline passes.
func waitForSignals(t *testing.T, col *collector.ResultCollector, want int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if col.TotalSignals() >= want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ── basic collection ──────────────────────────────────────────────────────────

func TestCollectorCountsSignals(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	seedSignals(t, client, ns+signalsSuffix, "BTCUSDT", 5)

	col := fastCollector(client, ns, "run-test")
	ctx, cancel := context.WithCancel(context.Background())

	col.Start(ctx)
	waitForSignals(t, col, 5, 3*time.Second)
	cancel()
	col.Wait()

	assert.Equal(t, int64(5), col.TotalSignals())
	counts := col.SignalCounts()
	assert.Equal(t, 5, counts["BTCUSDT"])
}

func TestCollectorMultipleSymbols(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	seedSignals(t, client, ns+signalsSuffix, "BTCUSDT", 3)
	seedSignals(t, client, ns+signalsSuffix, "ETHUSDT", 2)

	col := fastCollector(client, ns, "run-multi")
	ctx, cancel := context.WithCancel(context.Background())

	col.Start(ctx)
	waitForSignals(t, col, 5, 3*time.Second)
	cancel()
	col.Wait()

	assert.Equal(t, int64(5), col.TotalSignals())
	counts := col.SignalCounts()
	assert.Equal(t, 3, counts["BTCUSDT"])
	assert.Equal(t, 2, counts["ETHUSDT"])
}

func TestCollectorEmptyStreamStartsAndStopsCleanly(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	col := fastCollector(client, ns, "run-empty")
	ctx, cancel := context.WithCancel(context.Background())

	col.Start(ctx)
	time.Sleep(20 * time.Millisecond)
	cancel()
	col.Wait()

	assert.Equal(t, int64(0), col.TotalSignals())
}

func TestCollectorSignalCountsIsSafeToCallConcurrently(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	seedSignals(t, client, ns+signalsSuffix, "BTCUSDT", 10)

	col := fastCollector(client, ns, "run-concurrent")
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 50 {
			_ = col.SignalCounts()
			time.Sleep(time.Millisecond)
		}
	}()

	waitForSignals(t, col, 10, 3*time.Second)
	cancel()
	col.Wait()
	<-done
}

func TestCollectorStopAndWaitDoesNotBlock(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	col := fastCollector(client, ns, "run-stop")
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)
	cancel()

	done := make(chan struct{})
	go func() {
		col.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Wait() did not return within 3s after context cancellation")
	}
}

// ── messages arrive after Start ───────────────────────────────────────────────

func TestCollectorMessagesArrivedAfterStartAreCollected(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	col := fastCollector(client, ns, "run-late")
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)

	time.Sleep(10 * time.Millisecond)
	seedSignals(t, client, ns+signalsSuffix, "BTCUSDT", 4)

	waitForSignals(t, col, 4, 3*time.Second)
	cancel()
	col.Wait()

	assert.Equal(t, int64(4), col.TotalSignals())
}

// ── namespace isolation ───────────────────────────────────────────────────────

func TestCollectorOnlyReadsOwnNamespace(t *testing.T) {
	_, client := testutil.NewMiniredis(t)

	ns1 := testutil.UniqueNamespace(t)
	ns2 := testutil.UniqueNamespace(t)

	// Seed 3 signals in ns1, 5 signals in ns2.
	seedSignals(t, client, ns1+signalsSuffix, "BTCUSDT", 3)
	seedSignals(t, client, ns2+signalsSuffix, "BTCUSDT", 5)

	col := fastCollector(client, ns1, "run-ns1")
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)

	waitForSignals(t, col, 3, 3*time.Second)
	cancel()
	col.Wait()

	assert.Equal(t, int64(3), col.TotalSignals(),
		"collector must not read signals from other namespaces")
}

// ── orders stream: trade persistence ──────────────────────────────────────────

func collectorWithTradeWriter(client *redis.Client, ns, runID string, tw *mockTradeWriter) *collector.ResultCollector {
	return collector.New(client, ns, runID, nopLogger(),
		collector.WithBlockDuration(50*time.Millisecond),
		collector.WithTradeWriter(tw, decimal.RequireFromString("10000")),
	)
}

func TestCollectorWritesTradeOnFilledOrder(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	tw := &mockTradeWriter{}

	seedOrder(t, client, ns, orderSpec{
		side: domain.SideBuy, status: domain.OrderStatusFilled,
		fillPrice: testFillPrice, qty: "0.001", fee: testFee, eventTime: time.Now(),
	})

	col := collectorWithTradeWriter(client, ns, "run-trade", tw)
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)
	waitTrades(t, tw, 1, 3*time.Second)
	cancel()
	col.Wait()

	assert.Equal(t, 1, tw.tradeCount())
	assert.Equal(t, "BTCUSDT", tw.trades[0].Symbol)
	assert.Equal(t, domain.SideBuy, tw.trades[0].Side)
	assert.True(t, decimal.RequireFromString("50001.00").Equal(tw.trades[0].FillPrice))
	assert.Equal(t, "book_imbalance", tw.trades[0].SignalName)
}

func TestCollectorWritesEquityPointAfterFill(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	tw := &mockTradeWriter{}

	seedOrder(t, client, ns, orderSpec{
		side: domain.SideBuy, status: domain.OrderStatusFilled,
		fillPrice: testFillPrice, qty: "0.001", fee: testFee, eventTime: time.Now(),
	})

	col := collectorWithTradeWriter(client, ns, "run-equity", tw)
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)
	waitTrades(t, tw, 1, 3*time.Second)
	// wait a moment for equity to be recorded
	time.Sleep(50 * time.Millisecond)
	cancel()
	col.Wait()

	assert.Equal(t, 1, tw.equityCount(), "one equity point per fill")
	assert.True(t, tw.equity[0].Equity.IsPositive(), "equity must be positive after first fill")
}

func TestCollectorIgnoresNonFilledOrders(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	tw := &mockTradeWriter{}

	seedOrder(t, client, ns, orderSpec{
		side: domain.SideBuy, status: domain.OrderStatusCancelled,
		fillPrice: testFillPrice, qty: "0.001", fee: "0", eventTime: time.Now(),
	})

	col := collectorWithTradeWriter(client, ns, "run-cancelled", tw)
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()
	col.Wait()

	assert.Equal(t, 0, tw.tradeCount(), "cancelled orders must not produce trades")
}

func TestCollectorEquityDecreasesAfterFeeOnBuy(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	tw := &mockTradeWriter{}
	initialCapital := decimal.RequireFromString("10000")

	// BUY: qty=0.001, price=50001, fee=0.050001
	// cash after: 10000 - 0.001*50001 - 0.050001 = 10000 - 50.001 - 0.050001 = 9949.948999
	seedOrder(t, client, ns, orderSpec{
		side: domain.SideBuy, status: domain.OrderStatusFilled,
		fillPrice: testFillPrice, qty: "0.001", fee: testFee, eventTime: time.Now(),
	})

	col := collector.New(client, ns, "run-equity-dec", nopLogger(),
		collector.WithBlockDuration(50*time.Millisecond),
		collector.WithTradeWriter(tw, initialCapital),
	)
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)
	waitTrades(t, tw, 1, 3*time.Second)
	time.Sleep(50 * time.Millisecond)
	cancel()
	col.Wait()

	require.Equal(t, 1, tw.equityCount())
	// equity = cash + position_value = (10000 - 50.001 - 0.050001) + (0.001 * 50001)
	//        = 9949.948999 + 50.001 = 9999.949999
	// just verify it is less than initial capital (fee was paid)
	assert.True(t, tw.equity[0].Equity.LessThan(initialCapital),
		"equity after BUY with fee must be less than initial capital")
}

func TestCollectorBuyAndSellRestoresEquity(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)
	tw := &mockTradeWriter{}
	initialCapital := decimal.RequireFromString("10000")
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// BUY at 50000, then SELL at 50000 — same price, only fees differ
	seedOrder(t, client, ns, orderSpec{
		side: domain.SideBuy, status: domain.OrderStatusFilled,
		fillPrice: "50000.00", qty: "0.001", fee: "0.05", eventTime: base,
	})
	seedOrder(t, client, ns, orderSpec{
		side: domain.SideSell, status: domain.OrderStatusFilled,
		fillPrice: "50000.00", qty: "0.001", fee: "0.05", eventTime: base.Add(time.Second),
	})

	col := collector.New(client, ns, "run-buy-sell", nopLogger(),
		collector.WithBlockDuration(50*time.Millisecond),
		collector.WithTradeWriter(tw, initialCapital),
	)
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)
	waitTrades(t, tw, 2, 3*time.Second)
	time.Sleep(50 * time.Millisecond)
	cancel()
	col.Wait()

	assert.Equal(t, 2, tw.tradeCount())
	assert.Equal(t, 2, tw.equityCount())
	// After buy+sell at same price, total fees = 0.10; equity = 10000 - 0.10
	expectedFinal := initialCapital.Sub(decimal.RequireFromString("0.10"))
	assert.True(t, expectedFinal.Equal(tw.equity[1].Equity),
		"final equity after buy+sell at same price: want %s got %s", expectedFinal, tw.equity[1].Equity)
}

func TestCollectorNoOrdersLoopWithoutTradeWriter(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	seedOrder(t, client, ns, orderSpec{
		side: domain.SideBuy, status: domain.OrderStatusFilled,
		fillPrice: testFillPrice, qty: "0.001", fee: "0.05", eventTime: time.Now(),
	})

	// Collector without TradeWriter — orders stream is not read
	col := fastCollector(client, ns, "run-no-tw")
	ctx, cancel := context.WithCancel(context.Background())
	col.Start(ctx)
	time.Sleep(200 * time.Millisecond)
	cancel()
	col.Wait()

	// No panic, no crash — orders stream exists but is ignored
	assert.Equal(t, int64(0), col.TotalSignals())
}
