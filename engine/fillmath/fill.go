// Package fillmath provides pure fill price and fee arithmetic for paper trading simulation.
// The pessimistic market fill model assumes taker execution on every order.
//
// BUY:  fill = bestAsk × (1 + slippageBps/10000)  — pays more than quoted
// SELL: fill = bestBid × (1 − slippageBps/10000)  — receives less than quoted
//
// This formula is the canonical spec definition shared by all erebor execution
// components. Any change here must be reflected in the paper-trading spec.
package fillmath

import "github.com/shopspring/decimal"

var ten4 = decimal.NewFromInt(10000)

// ComputeFillPrice returns the pessimistic market fill price.
// isBuy=true → BUY (crosses ask side), isBuy=false → SELL (crosses bid side).
// bestPrice is the best ask for a BUY or best bid for a SELL.
func ComputeFillPrice(isBuy bool, bestPrice decimal.Decimal, slippageBps int) decimal.Decimal {
	slip := decimal.NewFromInt(int64(slippageBps)).Div(ten4)
	if isBuy {
		return bestPrice.Add(bestPrice.Mul(slip))
	}
	return bestPrice.Sub(bestPrice.Mul(slip))
}

// ComputeFee returns the taker fee for a fill.
// fee = qty × fillPrice × feeBps / 10000
func ComputeFee(qty, fillPrice decimal.Decimal, feeBps int) decimal.Decimal {
	return qty.Mul(fillPrice).Mul(decimal.NewFromInt(int64(feeBps))).Div(ten4)
}
