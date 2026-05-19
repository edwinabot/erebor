package consumer_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/edwinabot/erebor/execution/consumer"
	"github.com/edwinabot/erebor/execution/internal/testutil"
	signalsdomain "github.com/edwinabot/erebor/signals/domain"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const signalsSuffix = ":signals"

func TestConsumerCallsHandlerForEachSignal(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	var called int32
	var receivedSig signalsdomain.SignalEvent

	h := func(_ context.Context, _ string, sig signalsdomain.SignalEvent) error {
		receivedSig = sig
		atomic.StoreInt32(&called, 1)
		return nil
	}

	c := consumer.New(client, ns, zap.NewNop(),
		consumer.WithBlockDuration(50*time.Millisecond),
		consumer.WithHandler(h),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, c.Start(ctx))

	sig := testutil.MakeSignal("BTCUSDT", "book_imbalance", "0.5")
	testutil.SeedSignal(t, client, ns+signalsSuffix, sig)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&called) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	assert.Equal(t, int32(1), atomic.LoadInt32(&called))
	assert.Equal(t, "BTCUSDT", receivedSig.Symbol)
	assert.Equal(t, "book_imbalance", receivedSig.Name)
	assert.Equal(t, "0.5", receivedSig.Value.String())
}

func TestConsumerAcksSuccessfulMessages(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	c := consumer.New(client, ns, zap.NewNop(),
		consumer.WithBlockDuration(50*time.Millisecond),
		consumer.WithHandler(func(_ context.Context, _ string, _ signalsdomain.SignalEvent) error {
			return nil
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, c.Start(ctx))

	testutil.SeedSignal(t, client, ns+signalsSuffix, testutil.MakeSignal("BTCUSDT", "book_imbalance", "0.5"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		result, err := client.XPending(ctx, ns+signalsSuffix, "erebor-execution").Result()
		require.NoError(t, err)
		if result.Count == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("message was not ACKed within deadline")
}

func TestConsumerDoesNotAckOnHandlerError(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	var calls int32
	h := func(_ context.Context, _ string, _ signalsdomain.SignalEvent) error {
		atomic.AddInt32(&calls, 1)
		return assert.AnError
	}

	c := consumer.New(client, ns, zap.NewNop(),
		consumer.WithBlockDuration(50*time.Millisecond),
		consumer.WithHandler(h),
	)

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, c.Start(ctx))

	testutil.SeedSignal(t, client, ns+signalsSuffix, testutil.MakeSignal("BTCUSDT", "book_imbalance", "0.5"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&calls) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	c.Stop()

	result, err := client.XPending(context.Background(), ns+signalsSuffix, "erebor-execution").Result()
	require.NoError(t, err)
	assert.Equal(t, int64(1), result.Count, "unacked message must remain in PEL")
}

func TestConsumerSkipsMalformedMessages(t *testing.T) {
	_, client := testutil.NewMiniredis(t)
	ns := testutil.UniqueNamespace(t)

	var validCalls int32
	c := consumer.New(client, ns, zap.NewNop(),
		consumer.WithBlockDuration(50*time.Millisecond),
		consumer.WithHandler(func(_ context.Context, _ string, sig signalsdomain.SignalEvent) error {
			if sig.Symbol != "" {
				atomic.AddInt32(&validCalls, 1)
			}
			return nil
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, c.Start(ctx))

	// Seed a malformed message (missing required fields)
	client.XAdd(ctx, &redis.XAddArgs{
		Stream: ns + signalsSuffix,
		Values: map[string]any{"junk": "data"},
	})

	// Seed a valid message after
	time.Sleep(50 * time.Millisecond)
	testutil.SeedSignal(t, client, ns+signalsSuffix, testutil.MakeSignal("BTCUSDT", "book_imbalance", "0.3"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&validCalls) >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("valid message was not handled after malformed message")
}
