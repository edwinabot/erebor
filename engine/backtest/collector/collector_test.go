package collector_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/collector"
	"github.com/edwinabot/erebor/backtest/internal/testutil"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const signalsSuffix = ":signals"

func nopLogger() *zap.Logger { return zap.NewNop() }

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
