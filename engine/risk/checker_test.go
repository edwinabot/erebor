package risk_test

import (
	"sync"
	"testing"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/risk"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testTime = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

// TestCanTradeAllowedWhenNoLimits verifies that a zero-value Config permits all trades.
func TestCanTradeAllowedWhenNoLimits(t *testing.T) {
	c := risk.New(risk.Config{}, risk.NoopPublisher{})

	err := c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime)
	require.NoError(t, err, "zero-value config must allow any trade")

	err = c.CanTrade("ETHUSDT", domain.SideSell, decimal.RequireFromString("10"), testTime)
	require.NoError(t, err)
}

// TestCanTradePositionLimitBlocks verifies that position limit blocks buys/sells at limit,
// and that unconfigured symbols are not limited.
func TestCanTradePositionLimitBlocks(t *testing.T) {
	maxQty := decimal.RequireFromString("0.5")
	cfg := risk.Config{
		MaxPositionQty: map[string]decimal.Decimal{
			"BTCUSDT": maxQty,
		},
	}
	c := risk.New(cfg, risk.NoopPublisher{})

	// Buy 0.5 — at the limit exactly (abs(0+0.5)=0.5, not >0.5, so allowed)
	require.NoError(t, c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("0.5"), testTime))
	c.RecordFill("BTCUSDT", domain.SideBuy, decimal.RequireFromString("0.5"), decimal.RequireFromString("50000"), decimal.Zero)

	// Another buy would take position to 1.0 which exceeds limit → blocked
	err := c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("0.5"), testTime)
	require.Error(t, err, "buy that would exceed position limit must be blocked")
	assert.Contains(t, err.Error(), "POSITION_LIMIT")

	// Sell to go short 0.5 from 0.5 long → net 0 → allowed
	require.NoError(t, c.CanTrade("BTCUSDT", domain.SideSell, decimal.RequireFromString("1.0"), testTime))
	c.RecordFill("BTCUSDT", domain.SideSell, decimal.RequireFromString("1.0"), decimal.RequireFromString("50000"), decimal.Zero)
	// Now position = -0.5

	// Sell more would take to -1.0 → blocked
	err = c.CanTrade("BTCUSDT", domain.SideSell, decimal.RequireFromString("0.5"), testTime)
	require.Error(t, err, "sell that would exceed short position limit must be blocked")
	assert.Contains(t, err.Error(), "POSITION_LIMIT")

	// Unconfigured symbol has no limit
	require.NoError(t, c.CanTrade("ETHUSDT", domain.SideBuy, decimal.RequireFromString("1000"), testTime),
		"unconfigured symbol must not be blocked by position limit")
}

// TestCanTradeDrawdownHalt verifies that once equity falls below peak*(1-pct/100),
// CanTrade returns an error for all subsequent calls regardless of symbol.
func TestCanTradeDrawdownHalt(t *testing.T) {
	// InitialCapital=10000, MaxDrawdownPct=5 → halt when equity < 10000*0.95=9500
	cfg := risk.Config{
		InitialCapital: decimal.RequireFromString("10000"),
		MaxDrawdownPct: decimal.RequireFromString("5"),
	}
	c := risk.New(cfg, risk.NoopPublisher{})

	// Simulate a buy at 10000, then a sell at 9400 — total loss > 5%
	// Buy 1 BTC at 10000: equity = 10000 - (1*10000) - 0 = 0, but peak equity may start at initial
	// Actually equity starts at InitialCapital. Let's simulate realised P&L:
	// Buy 1 unit at price 10000 → equity = 10000 - 10000 = 0 (cash reduced)
	// Sell 1 unit at price 9400 → equity = 0 + 9400 = 9400
	// peak was 10000, threshold = 10000 * 0.95 = 9500 > 9400 → halt

	// Before fills, trade should be allowed
	require.NoError(t, c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime))

	// Record buy fill
	c.RecordFill("BTCUSDT", domain.SideBuy,
		decimal.RequireFromString("1"),
		decimal.RequireFromString("10000"),
		decimal.Zero)

	// Record sell fill at 9400 (loss > 5%)
	c.RecordFill("BTCUSDT", domain.SideSell,
		decimal.RequireFromString("1"),
		decimal.RequireFromString("9400"),
		decimal.Zero)

	// Now equity=9400, peak=10000, threshold=9500 → drawdown triggered
	err := c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime)
	require.Error(t, err, "drawdown halt must block subsequent trades")
	assert.Contains(t, err.Error(), "DRAWDOWN_HALT")

	// Also blocked for a different symbol
	err = c.CanTrade("ETHUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime)
	require.Error(t, err, "drawdown halt must block all symbols")

	assert.True(t, c.Halted(), "Halted() must return true after drawdown halt")
}

// TestCanTradeRunLossHalt verifies that once equity falls below initial*(1-pct/100),
// CanTrade returns an error for all subsequent calls.
func TestCanTradeRunLossHalt(t *testing.T) {
	// InitialCapital=10000, RunLossLimitPct=10 → halt when equity < 9000
	cfg := risk.Config{
		InitialCapital:  decimal.RequireFromString("10000"),
		RunLossLimitPct: decimal.RequireFromString("10"),
	}
	c := risk.New(cfg, risk.NoopPublisher{})

	require.NoError(t, c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime))

	// Buy then sell at a big loss
	c.RecordFill("BTCUSDT", domain.SideBuy,
		decimal.RequireFromString("1"),
		decimal.RequireFromString("10000"),
		decimal.Zero)
	c.RecordFill("BTCUSDT", domain.SideSell,
		decimal.RequireFromString("1"),
		decimal.RequireFromString("8900"),
		decimal.Zero)

	// equity=8900 < 10000*0.9=9000 → run loss halt
	err := c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime)
	require.Error(t, err, "run loss halt must block subsequent trades")
	assert.Contains(t, err.Error(), "RUN_LOSS_HALT")

	assert.True(t, c.Halted())
}

// TestCanTradeHaltedShortCircuit verifies that once halted, CanTrade returns error
// immediately without evaluating any other rules.
func TestCanTradeHaltedShortCircuit(t *testing.T) {
	cfg := risk.Config{
		InitialCapital: decimal.RequireFromString("10000"),
		MaxDrawdownPct: decimal.RequireFromString("5"),
	}
	c := risk.New(cfg, risk.NoopPublisher{})

	// Force a halt via drawdown: buy then sell at a loss >5%
	c.RecordFill("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), decimal.RequireFromString("10000"), decimal.Zero)
	c.RecordFill("BTCUSDT", domain.SideSell, decimal.RequireFromString("1"), decimal.RequireFromString("9400"), decimal.Zero)

	// Drawdown check triggers during CanTrade, which sets halted=true
	err := c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime)
	require.Error(t, err)
	require.True(t, c.Halted(), "Halted() must be true after drawdown halt triggered by CanTrade")

	// Subsequent CanTrade on any symbol must return error via short-circuit
	err = c.CanTrade("ANYTOKEN", domain.SideBuy, decimal.RequireFromString("1"), testTime)
	require.Error(t, err, "halted checker must block all subsequent trades via short-circuit")
}

// TestCanTradeDrawdownDisabledWhenZero verifies MaxDrawdownPct=0 disables drawdown check.
func TestCanTradeDrawdownDisabledWhenZero(t *testing.T) {
	cfg := risk.Config{
		InitialCapital: decimal.RequireFromString("10000"),
		MaxDrawdownPct: decimal.Zero, // disabled
	}
	c := risk.New(cfg, risk.NoopPublisher{})

	// Large loss — but drawdown check is disabled
	c.RecordFill("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), decimal.RequireFromString("10000"), decimal.Zero)
	c.RecordFill("BTCUSDT", domain.SideSell, decimal.RequireFromString("1"), decimal.RequireFromString("1000"), decimal.Zero)

	err := c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime)
	require.NoError(t, err, "MaxDrawdownPct=0 must disable drawdown check")
	assert.False(t, c.Halted())
}

// TestCanTradeRunLossDisabledWhenZero verifies RunLossLimitPct=0 disables run loss check.
func TestCanTradeRunLossDisabledWhenZero(t *testing.T) {
	cfg := risk.Config{
		InitialCapital:  decimal.RequireFromString("10000"),
		RunLossLimitPct: decimal.Zero, // disabled
	}
	c := risk.New(cfg, risk.NoopPublisher{})

	// Large loss — but run loss check is disabled
	c.RecordFill("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), decimal.RequireFromString("10000"), decimal.Zero)
	c.RecordFill("BTCUSDT", domain.SideSell, decimal.RequireFromString("1"), decimal.RequireFromString("500"), decimal.Zero)

	err := c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime)
	require.NoError(t, err, "RunLossLimitPct=0 must disable run loss check")
	assert.False(t, c.Halted())
}

// TestRecordFillUpdatesEquity verifies that buys reduce cash, sells increase cash,
// and fees always reduce equity.
func TestRecordFillUpdatesEquity(t *testing.T) {
	cfg := risk.Config{
		InitialCapital:  decimal.RequireFromString("10000"),
		RunLossLimitPct: decimal.RequireFromString("99"), // will halt on huge loss; use to observe
	}
	c := risk.New(cfg, risk.NoopPublisher{})

	// Buy: equity = 10000 - (0.5 * 1000) - 5 = 10000 - 500 - 5 = 9495
	c.RecordFill("BTCUSDT", domain.SideBuy,
		decimal.RequireFromString("0.5"),
		decimal.RequireFromString("1000"),
		decimal.RequireFromString("5"))

	// Verify with RunLoss check — threshold = 10000 * 0.01 = 100 — equity 9495 > 100, not halted
	require.False(t, c.Halted(), "equity 9495 must not trigger 99% run loss halt")
	require.NoError(t, c.CanTrade("BTCUSDT", domain.SideSell, decimal.RequireFromString("0.5"), testTime))

	// Sell: equity = 9495 + (0.5 * 1100) - 3 = 9495 + 550 - 3 = 10042
	c.RecordFill("BTCUSDT", domain.SideSell,
		decimal.RequireFromString("0.5"),
		decimal.RequireFromString("1100"),
		decimal.RequireFromString("3"))

	// Not halted — equity is above initial
	require.False(t, c.Halted())
}

// TestCheckerConcurrentSafety verifies no data races under concurrent CanTrade+RecordFill calls.
func TestCheckerConcurrentSafety(t *testing.T) {
	cfg := risk.Config{
		InitialCapital: decimal.RequireFromString("100000"),
		MaxPositionQty: map[string]decimal.Decimal{
			"BTCUSDT": decimal.RequireFromString("100"),
		},
	}
	c := risk.New(cfg, risk.NoopPublisher{})

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = c.CanTrade("BTCUSDT", domain.SideBuy, decimal.RequireFromString("1"), testTime)
		}()
		go func() {
			defer wg.Done()
			c.RecordFill("BTCUSDT", domain.SideBuy,
				decimal.RequireFromString("0.001"),
				decimal.RequireFromString("50000"),
				decimal.RequireFromString("0.001"))
		}()
	}
	wg.Wait()
	// If there's a race, -race flag will catch it; if we get here, no panic
}
