package execution

import (
	"encoding/json"
	"fmt"

	"github.com/shopspring/decimal"
)

// StrategyConfig holds execution parameters parsed from strategy_config JSON.
// Fields absent from the JSON retain their default values.
type StrategyConfig struct {
	MakerFeeBps    int             `json:"maker_fee_bps"`
	TakerFeeBps    int             `json:"taker_fee_bps"`
	SlippageBps    int             `json:"slippage_bps"`
	TradeQty       decimal.Decimal `json:"trade_qty"`
	BuyThreshold   decimal.Decimal `json:"buy_threshold"`  // buy when book_imbalance > this
	SellThreshold  decimal.Decimal `json:"sell_threshold"` // sell when book_imbalance < -this
	InitialCapital decimal.Decimal `json:"initial_capital"`
}

var strategyDefaults = StrategyConfig{
	MakerFeeBps:    10,
	TakerFeeBps:    10,
	SlippageBps:    0,
	TradeQty:       decimal.RequireFromString("0.001"),
	BuyThreshold:   decimal.RequireFromString("0.2"),
	SellThreshold:  decimal.RequireFromString("0.2"),
	InitialCapital: decimal.RequireFromString("10000"),
}

// ParseStrategyConfig unmarshals raw JSON over the built-in defaults.
// An empty or "{}" string returns all defaults unchanged.
func ParseStrategyConfig(raw string) (StrategyConfig, error) {
	cfg := strategyDefaults
	if raw == "" || raw == "{}" {
		return cfg, nil
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return cfg, fmt.Errorf("parse strategy_config: %w", err)
	}
	return cfg, nil
}
