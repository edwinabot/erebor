package fillmath_test

import (
	"testing"

	fillmath "github.com/edwinabot/erebor/fillmath"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
)

func TestComputeFillPriceBuyNoSlippage(t *testing.T) {
	bestAsk := decimal.RequireFromString("50001")
	got := fillmath.ComputeFillPrice(true, bestAsk, 0)
	assert.True(t, bestAsk.Equal(got), "BUY with 0 slippage: want %s got %s", bestAsk, got)
}

func TestComputeFillPriceSellNoSlippage(t *testing.T) {
	bestBid := decimal.RequireFromString("50000")
	got := fillmath.ComputeFillPrice(false, bestBid, 0)
	assert.True(t, bestBid.Equal(got), "SELL with 0 slippage: want %s got %s", bestBid, got)
}

func TestComputeFillPriceBuySlippage(t *testing.T) {
	// bestAsk=50001, slippage=10bps (0.1%) → 50001 * 1.001 = 50051.001
	bestAsk := decimal.RequireFromString("50001")
	got := fillmath.ComputeFillPrice(true, bestAsk, 10)
	want := decimal.RequireFromString("50001").Mul(decimal.RequireFromString("1.001"))
	assert.True(t, want.Equal(got), "BUY with 10bps slippage: want %s got %s", want, got)
}

func TestComputeFillPriceSellSlippage(t *testing.T) {
	// bestBid=50000, slippage=10bps → 50000 * (1 - 0.001) = 49950
	bestBid := decimal.RequireFromString("50000")
	got := fillmath.ComputeFillPrice(false, bestBid, 10)
	want := decimal.RequireFromString("50000").Mul(decimal.RequireFromString("0.999"))
	assert.True(t, want.Equal(got), "SELL with 10bps slippage: want %s got %s", want, got)
}

func TestComputeFillPricePessimisticBuy(t *testing.T) {
	// BUY increases the fill price (pays more)
	bestAsk := decimal.RequireFromString("100")
	noSlippage := fillmath.ComputeFillPrice(true, bestAsk, 0)
	withSlippage := fillmath.ComputeFillPrice(true, bestAsk, 5)
	assert.True(t, withSlippage.GreaterThan(noSlippage), "BUY fill should be higher with slippage")
}

func TestComputeFillPricePessimisticSell(t *testing.T) {
	// SELL decreases the fill price (receives less)
	bestBid := decimal.RequireFromString("100")
	noSlippage := fillmath.ComputeFillPrice(false, bestBid, 0)
	withSlippage := fillmath.ComputeFillPrice(false, bestBid, 5)
	assert.True(t, withSlippage.LessThan(noSlippage), "SELL fill should be lower with slippage")
}

func TestComputeFee(t *testing.T) {
	// qty=0.001, price=50001, feeBps=10 → fee = 0.001 * 50001 * 10 / 10000 = 0.050001
	qty := decimal.RequireFromString("0.001")
	price := decimal.RequireFromString("50001")
	got := fillmath.ComputeFee(qty, price, 10)
	want := decimal.RequireFromString("0.050001")
	assert.True(t, want.Equal(got), "fee: want %s got %s", want, got)
}

func TestComputeFeeZeroRate(t *testing.T) {
	qty := decimal.RequireFromString("1")
	price := decimal.RequireFromString("50000")
	got := fillmath.ComputeFee(qty, price, 0)
	assert.True(t, decimal.Zero.Equal(got), "zero fee rate must return zero")
}
