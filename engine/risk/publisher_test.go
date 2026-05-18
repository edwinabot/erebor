package risk_test

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/edwinabot/erebor/risk"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

// TestRedisPublisherWritesCorrectFields verifies that after Publish(), XRANGE returns
// a message with the correct run_id, symbol, event_time (RFC3339Nano), type, detail, equity fields.
func TestRedisPublisherWritesCorrectFields(t *testing.T) {
	_, client := newTestRedis(t)
	namespace := "erebor:test:run-risk-pub"
	pub := risk.NewRedisPublisher(client)

	evTime := time.Date(2026, 3, 15, 10, 30, 0, 123456789, time.UTC)
	evt := risk.Event{
		RunID:     "run-pub-test",
		Symbol:    "BTCUSDT",
		EventTime: evTime,
		Type:      risk.EventPositionLimit,
		Detail:    "abs(1.5) > max 1.0",
		Equity:    decimal.RequireFromString("9500.50"),
	}

	err := pub.Publish(context.Background(), namespace, evt)
	require.NoError(t, err)

	// Read back from stream
	streamKey := namespace + ":risk"
	msgs, err := client.XRange(context.Background(), streamKey, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1, "exactly one message must be written to risk stream")

	v := msgs[0].Values
	assert.Equal(t, "run-pub-test", v["run_id"])
	assert.Equal(t, "BTCUSDT", v["symbol"])
	assert.Equal(t, evTime.UTC().Format(time.RFC3339Nano), v["event_time"])
	assert.Equal(t, string(risk.EventPositionLimit), v["type"])
	assert.Equal(t, "abs(1.5) > max 1.0", v["detail"])
	assert.Equal(t, decimal.RequireFromString("9500.50").String(), v["equity"])
}

// TestRedisPublisherGlobalEventHasEmptySymbol verifies that global events (drawdown/run-loss)
// have an empty symbol field.
func TestRedisPublisherGlobalEventHasEmptySymbol(t *testing.T) {
	_, client := newTestRedis(t)
	namespace := "erebor:test:run-risk-global"
	pub := risk.NewRedisPublisher(client)

	evt := risk.Event{
		RunID:     "run-global-test",
		Symbol:    "", // global event
		EventTime: time.Now(),
		Type:      risk.EventDrawdownHalt,
		Detail:    "equity 9400 < peak 10000 * 0.95 = 9500",
		Equity:    decimal.RequireFromString("9400"),
	}

	err := pub.Publish(context.Background(), namespace, evt)
	require.NoError(t, err)

	streamKey := namespace + ":risk"
	msgs, err := client.XRange(context.Background(), streamKey, "-", "+").Result()
	require.NoError(t, err)
	require.Len(t, msgs, 1)

	assert.Equal(t, "", msgs[0].Values["symbol"], "global event must have empty symbol field")
	assert.Equal(t, string(risk.EventDrawdownHalt), msgs[0].Values["type"])
}

// TestNoopPublisherDoesNotError verifies that NoopPublisher.Publish() always returns nil.
func TestNoopPublisherDoesNotError(t *testing.T) {
	pub := risk.NoopPublisher{}
	evt := risk.Event{
		RunID:     "any",
		Symbol:    "BTCUSDT",
		EventTime: time.Now(),
		Type:      risk.EventRunLossHalt,
		Detail:    "test",
		Equity:    decimal.RequireFromString("1000"),
	}
	err := pub.Publish(context.Background(), "any-namespace", evt)
	assert.NoError(t, err, "NoopPublisher.Publish must always return nil")
}
