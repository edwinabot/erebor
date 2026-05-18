package risk_test

import (
	"context"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/risk"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const testSessionID = "test-session-1"

// ── RedisHaltStore ────────────────────────────────────────────────────────────

func TestRedisHaltStoreSetAndGet(t *testing.T) {
	client := redisClientForIntegration(t)
	store := risk.NewRedisHaltStore(client)
	ctx := context.Background()

	halted, err := store.IsHalted(ctx, testSessionID)
	require.NoError(t, err)
	assert.False(t, halted, "should not be halted initially")

	require.NoError(t, store.SetHalted(ctx, testSessionID))

	halted, err = store.IsHalted(ctx, testSessionID)
	require.NoError(t, err)
	assert.True(t, halted, "should be halted after SetHalted")
}

func TestRedisHaltStoreIsolatedBySessioID(t *testing.T) {
	client := redisClientForIntegration(t)
	store := risk.NewRedisHaltStore(client)
	ctx := context.Background()

	require.NoError(t, store.SetHalted(ctx, "session-A"))

	haltedA, _ := store.IsHalted(ctx, "session-A")
	haltedB, _ := store.IsHalted(ctx, "session-B")
	assert.True(t, haltedA)
	assert.False(t, haltedB, "halt state must be isolated per session")
}

// ── Checker with HaltStore integration ───────────────────────────────────────

func TestCheckerPersistsHaltOnDrawdown(t *testing.T) {
	client := redisClientForIntegration(t)
	store := risk.NewRedisHaltStore(client)
	sessionID := "halt-drawdown-test"
	namespace := "erebor:test:halt"
	evTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	cfg := risk.Config{
		InitialCapital: decimal.RequireFromString("10000"),
		MaxDrawdownPct: decimal.RequireFromString("5"),
	}
	checker := risk.NewWithLogger(cfg, risk.NoopPublisher{}, zap.NewNop(), namespace, sessionID,
		risk.WithHaltStore(store))

	// Simulate a >5% drawdown: buy 1 at 10000, sell at 9400
	require.NoError(t, checker.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), evTime))
	checker.RecordFill("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), decimal.RequireFromString("10000"), decimal.Zero)
	checker.RecordFill("BTCUSDT", domain.SideSell, decimal.RequireFromString("1"), decimal.RequireFromString("9400"), decimal.Zero)

	err := checker.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), evTime)
	require.Error(t, err, "drawdown halt must block CanTrade")

	halted, err := store.IsHalted(context.Background(), sessionID)
	require.NoError(t, err)
	assert.True(t, halted, "halt must be persisted to Redis after drawdown trigger")
}

func TestCheckerLoadsPersistedHaltOnStartup(t *testing.T) {
	client := redisClientForIntegration(t)
	store := risk.NewRedisHaltStore(client)
	sessionID := "halt-reload-test"
	namespace := "erebor:test:reload"
	evTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	// Pre-set halt in Redis (simulating a previous run that halted)
	require.NoError(t, store.SetHalted(context.Background(), sessionID))

	cfg := risk.Config{InitialCapital: decimal.RequireFromString("10000")}
	checker := risk.NewWithLogger(cfg, risk.NoopPublisher{}, zap.NewNop(), namespace, sessionID,
		risk.WithHaltStore(store))

	// Must detect persisted halt on first CanTrade call
	err := checker.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), evTime)
	require.Error(t, err, "checker must detect pre-existing halt from Redis")
}

func TestCheckerWithoutHaltStoreWorks(t *testing.T) {
	cfg := risk.Config{InitialCapital: decimal.RequireFromString("10000")}
	checker := risk.New(cfg, risk.NoopPublisher{})
	evTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	err := checker.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), evTime)
	require.NoError(t, err, "checker without HaltStore must work normally")
}
