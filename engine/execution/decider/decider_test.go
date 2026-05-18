package decider_test

import (
	"testing"
	"time"

	backtestdomain "github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/execution"
	"github.com/edwinabot/erebor/execution/decider"
	signalsdomain "github.com/edwinabot/erebor/signals/domain"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cfg(buy, sell string) execution.StrategyConfig {
	c, err := execution.ParseStrategyConfig(`{"buy_threshold":"` + buy + `","sell_threshold":"` + sell + `"}`)
	if err != nil {
		panic(err)
	}
	return c
}

func sig(name, value string) signalsdomain.SignalEvent {
	return signalsdomain.SignalEvent{
		Symbol:    "BTCUSDT",
		Name:      name,
		Value:     decimal.RequireFromString(value),
		EventTime: time.Now(),
	}
}

func TestDeciderBuyOnHighPositiveImbalance(t *testing.T) {
	d := decider.New(cfg("0.2", "0.2"))
	side, ok := d.Decide("BTCUSDT", sig("book_imbalance", "0.5"), decider.Flat)
	require.True(t, ok)
	assert.Equal(t, backtestdomain.SideBuy, side)
}

func TestDeciderSellOnHighNegativeImbalance(t *testing.T) {
	d := decider.New(cfg("0.2", "0.2"))
	side, ok := d.Decide("BTCUSDT", sig("book_imbalance", "-0.5"), decider.Flat)
	require.True(t, ok)
	assert.Equal(t, backtestdomain.SideSell, side)
}

func TestDeciderNoTradeWhenBalanced(t *testing.T) {
	d := decider.New(cfg("0.2", "0.2"))
	_, ok := d.Decide("BTCUSDT", sig("book_imbalance", "0.1"), decider.Flat)
	assert.False(t, ok)
}

func TestDeciderNoBuyWhenAlreadyLong(t *testing.T) {
	d := decider.New(cfg("0.2", "0.2"))
	_, ok := d.Decide("BTCUSDT", sig("book_imbalance", "0.8"), decider.Long)
	assert.False(t, ok, "must not BUY when already long")
}

func TestDeciderNoSellWhenAlreadyShort(t *testing.T) {
	d := decider.New(cfg("0.2", "0.2"))
	_, ok := d.Decide("BTCUSDT", sig("book_imbalance", "-0.8"), decider.Short)
	assert.False(t, ok, "must not SELL when already short")
}

func TestDeciderBuyClosesShort(t *testing.T) {
	d := decider.New(cfg("0.2", "0.2"))
	side, ok := d.Decide("BTCUSDT", sig("book_imbalance", "0.8"), decider.Short)
	require.True(t, ok)
	assert.Equal(t, backtestdomain.SideBuy, side)
}

func TestDeciderSellClosesLong(t *testing.T) {
	d := decider.New(cfg("0.2", "0.2"))
	side, ok := d.Decide("BTCUSDT", sig("book_imbalance", "-0.8"), decider.Long)
	require.True(t, ok)
	assert.Equal(t, backtestdomain.SideSell, side)
}

func TestDeciderIgnoresUnknownSignalName(t *testing.T) {
	d := decider.New(cfg("0.2", "0.2"))
	_, ok := d.Decide("BTCUSDT", sig("spread_bps", "100"), decider.Flat)
	assert.False(t, ok, "non-book_imbalance signals must be ignored")
}
