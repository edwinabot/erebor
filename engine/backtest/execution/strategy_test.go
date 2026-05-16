package execution_test

import (
	"testing"

	"github.com/edwinabot/erebor/backtest/execution"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseStrategyConfigDefaults(t *testing.T) {
	cfg, err := execution.ParseStrategyConfig("{}")
	require.NoError(t, err)
	assert.Equal(t, 10, cfg.MakerFeeBps)
	assert.Equal(t, 10, cfg.TakerFeeBps)
	assert.Equal(t, 0, cfg.SlippageBps)
	assert.True(t, decimal.RequireFromString("0.001").Equal(cfg.TradeQty))
	assert.True(t, decimal.RequireFromString("0.2").Equal(cfg.BuyThreshold))
	assert.True(t, decimal.RequireFromString("0.2").Equal(cfg.SellThreshold))
	assert.True(t, decimal.RequireFromString("10000").Equal(cfg.InitialCapital))
}

func TestParseStrategyConfigEmptyString(t *testing.T) {
	cfg, err := execution.ParseStrategyConfig("")
	require.NoError(t, err)
	assert.Equal(t, 10, cfg.TakerFeeBps)
}

func TestParseStrategyConfigPartialOverride(t *testing.T) {
	cfg, err := execution.ParseStrategyConfig(`{"taker_fee_bps":20,"trade_qty":"0.005"}`)
	require.NoError(t, err)
	assert.Equal(t, 20, cfg.TakerFeeBps)
	assert.Equal(t, 10, cfg.MakerFeeBps)
	assert.True(t, decimal.RequireFromString("0.005").Equal(cfg.TradeQty))
	assert.True(t, decimal.RequireFromString("10000").Equal(cfg.InitialCapital))
}

func TestParseStrategyConfigFullOverride(t *testing.T) {
	raw := `{"maker_fee_bps":5,"taker_fee_bps":8,"slippage_bps":3,"trade_qty":"0.01","buy_threshold":"0.3","sell_threshold":"0.3","initial_capital":"50000"}`
	cfg, err := execution.ParseStrategyConfig(raw)
	require.NoError(t, err)
	assert.Equal(t, 5, cfg.MakerFeeBps)
	assert.Equal(t, 8, cfg.TakerFeeBps)
	assert.Equal(t, 3, cfg.SlippageBps)
	assert.True(t, decimal.RequireFromString("0.01").Equal(cfg.TradeQty))
	assert.True(t, decimal.RequireFromString("0.3").Equal(cfg.BuyThreshold))
	assert.True(t, decimal.RequireFromString("50000").Equal(cfg.InitialCapital))
}

func TestParseStrategyConfigInvalidJSON(t *testing.T) {
	_, err := execution.ParseStrategyConfig("not json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse strategy_config")
}
