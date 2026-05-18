// Package decider maps a SignalEvent and current position state to a trade decision.
package decider

import (
	backtestdomain "github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/backtest/execution"
	signalsdomain "github.com/edwinabot/erebor/signals/domain"
)

// Position represents the net directional state of a symbol.
type Position int8

const (
	Flat  Position = 0
	Long  Position = 1
	Short Position = -1
)

// Decider maps (signal, position) → (Side, shouldTrade).
// It is stateless; callers supply current position.
type Decider struct {
	cfg execution.StrategyConfig
}

// New creates a Decider with the given strategy config.
func New(cfg execution.StrategyConfig) *Decider {
	return &Decider{cfg: cfg}
}

// Decide returns the trade side and true if the signal warrants a trade,
// or ("", false) if no trade should be placed.
// Currently only the "book_imbalance" signal drives decisions.
func (d *Decider) Decide(symbol string, sig signalsdomain.SignalEvent, pos Position) (backtestdomain.Side, bool) {
	if sig.Name != "book_imbalance" {
		return "", false
	}
	wantBuy := sig.Value.GreaterThan(d.cfg.BuyThreshold)
	wantSell := sig.Value.LessThan(d.cfg.SellThreshold.Neg())

	switch {
	case wantBuy && (pos == Flat || pos == Short):
		return backtestdomain.SideBuy, true
	case wantSell && (pos == Flat || pos == Long):
		return backtestdomain.SideSell, true
	default:
		return "", false
	}
}
