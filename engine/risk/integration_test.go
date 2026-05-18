package risk_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/risk"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// redisClientForIntegration returns a miniredis client by default, or a real Redis
// client if INTEGRATION_REDIS_ADDR is set (matching the testutil env-var gating pattern).
func redisClientForIntegration(t *testing.T) *redis.Client {
	t.Helper()
	if addr := os.Getenv("INTEGRATION_REDIS_ADDR"); addr != "" {
		client := redis.NewClient(&redis.Options{
			Addr: addr,
			DB:   1,
		})
		t.Cleanup(func() { _ = client.Close() })
		return client
	}
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestIntegrationPositionLimitEvent verifies that a POSITION_LIMIT event
// is published to {namespace}:risk when CanTrade blocks an oversized order.
func TestIntegrationPositionLimitEvent(t *testing.T) {
	client := redisClientForIntegration(t)
	namespace := "erebor:integration:pos-limit"
	runID := "integration-pos-001"
	evTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	cfg := risk.Config{
		InitialCapital: decimal.RequireFromString("100000"),
		MaxPositionQty: map[string]decimal.Decimal{
			"BTCUSDT": decimal.RequireFromString("0.5"),
		},
	}

	pub := risk.NewRedisPublisher(client)
	checker := risk.NewWithLogger(cfg, pub, zap.NewNop(), namespace, runID)

	// Buy 0.5 — exactly at limit, allowed
	require.NoError(t, checker.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("0.5"), evTime))
	checker.RecordFill("BTCUSDT", domain.SideBuy,
		decimal.RequireFromString("0.5"),
		decimal.RequireFromString("50000"),
		decimal.RequireFromString("2.5"))

	// Try to buy another 0.5 — would exceed limit
	err := checker.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("0.5"), evTime)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "POSITION_LIMIT")

	// Verify POSITION_LIMIT event is in the stream
	streamKey := namespace + ":risk"
	msgs, streamErr := client.XRange(context.Background(), streamKey, "-", "+").Result()
	require.NoError(t, streamErr)
	require.Len(t, msgs, 1, "exactly one risk event (POSITION_LIMIT) must be in stream")

	m := msgs[0].Values
	assert.Equal(t, runID, m["run_id"])
	assert.Equal(t, "BTCUSDT", m["symbol"])
	assert.Equal(t, string(risk.EventPositionLimit), m["type"])
	assert.NotEmpty(t, m["detail"])
	assert.NotEmpty(t, m["event_time"])
	assert.NotEmpty(t, m["equity"])
}

// TestIntegrationDrawdownHaltEvent verifies that a DRAWDOWN_HALT event is published
// and that after the halt, CanTrade always returns an error.
func TestIntegrationDrawdownHaltEvent(t *testing.T) {
	client := redisClientForIntegration(t)
	namespace := "erebor:integration:drawdown"
	runID := "integration-dd-001"
	evTime := time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC)

	// initial_capital=10000, max_drawdown=5% → halt when equity < 9500
	cfg := risk.Config{
		InitialCapital: decimal.RequireFromString("10000"),
		MaxDrawdownPct: decimal.RequireFromString("5"),
	}

	pub := risk.NewRedisPublisher(client)
	checker := risk.NewWithLogger(cfg, pub, zap.NewNop(), namespace, runID)

	// Simulate a buy then sell at a 6% loss: buy 1 BTC at 10000, sell at 9400
	// equity: 10000 - (1*10000) = 0 (cash consumed), then + (1*9400) = 9400
	require.NoError(t, checker.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), evTime))
	checker.RecordFill("BTCUSDT", domain.SideBuy,
		decimal.RequireFromString("1"),
		decimal.RequireFromString("10000"),
		decimal.Zero)
	checker.RecordFill("BTCUSDT", domain.SideSell,
		decimal.RequireFromString("1"),
		decimal.RequireFromString("9400"),
		decimal.Zero)

	// CanTrade must now trigger drawdown halt
	err := checker.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), evTime)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "DRAWDOWN_HALT")
	assert.True(t, checker.Halted(), "Halted() must be true after drawdown halt")

	// Verify DRAWDOWN_HALT event in the stream
	streamKey := namespace + ":risk"
	msgs, streamErr := client.XRange(context.Background(), streamKey, "-", "+").Result()
	require.NoError(t, streamErr)
	require.Len(t, msgs, 1, "exactly one risk event (DRAWDOWN_HALT) must be in stream")

	m := msgs[0].Values
	assert.Equal(t, runID, m["run_id"])
	assert.Equal(t, "", m["symbol"], "drawdown halt is a global event with empty symbol")
	assert.Equal(t, string(risk.EventDrawdownHalt), m["type"])
	assert.NotEmpty(t, m["detail"])

	// After halt, all CanTrade calls must return error
	err = checker.CanTrade("ETHUSDT", domain.SideBuy, decimal.RequireFromString("1"), evTime)
	require.Error(t, err, "halted checker must block all symbols")

	err = checker.CanTrade("BTCUSDT", domain.SideSell, decimal.RequireFromString("1"), evTime)
	require.Error(t, err, "halted checker must block all sides")
}
