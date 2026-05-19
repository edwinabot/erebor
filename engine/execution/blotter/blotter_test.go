package blotter_test

import (
	"context"
	"testing"
	"time"

	backtestdomain "github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/execution/blotter"
	"github.com/edwinabot/erebor/execution/decider"
	"github.com/edwinabot/erebor/execution/repository"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

const (
	testSessID     = "sess-1"
	postOpenEquity = "9999.95"
)

// memStore is an in-memory FillStore for tests.
type memStore struct {
	fills     []repository.TradeRecord
	positions map[string]repository.PositionRecord
	equities  []repository.EquityRecord
}

func newMemStore() *memStore {
	return &memStore{positions: make(map[string]repository.PositionRecord)}
}

func (m *memStore) RecordFill(_ context.Context, trade repository.TradeRecord, pos repository.PositionRecord, eq repository.EquityRecord) error {
	m.fills = append(m.fills, trade)
	m.positions[pos.Symbol] = pos
	m.equities = append(m.equities, eq)
	return nil
}

func fill(symbol string, side backtestdomain.Side, price, qty, fee string) blotter.FillRequest {
	return blotter.FillRequest{
		TradeID:        "t-" + string(side) + price,
		Symbol:         symbol,
		Side:           side,
		FillPrice:      decimal.RequireFromString(price),
		FillQty:        decimal.RequireFromString(qty),
		Fee:            decimal.RequireFromString(fee),
		EventTime:      time.Now().UTC(),
		SignalName:     "book_imbalance",
		SignalStreamID: "1-0",
	}
}

func TestBlotterOpenLongDeductsOnlyFee(t *testing.T) {
	store := newMemStore()
	b := blotter.New(testSessID, decimal.NewFromInt(10000), store, zap.NewNop())

	err := b.RecordFill(context.Background(), fill("BTCUSDT", backtestdomain.SideBuy, "50000", "0.001", "0.05"))
	require.NoError(t, err)

	pos, qty := b.Position("BTCUSDT")
	assert.Equal(t, decider.Long, pos)
	assert.Equal(t, "0.001", qty.String())
	// equity = 10000 - 0.05 (fee only; no realised pnl on opening)
	assert.Equal(t, postOpenEquity, b.Equity().String())
}

func TestBlotterSellClosesLongAndComputesPnL(t *testing.T) {
	store := newMemStore()
	b := blotter.New(testSessID, decimal.NewFromInt(10000), store, zap.NewNop())

	require.NoError(t, b.RecordFill(context.Background(), fill("BTCUSDT", backtestdomain.SideBuy, "50000", "0.001", "0.05")))
	require.NoError(t, b.RecordFill(context.Background(), fill("BTCUSDT", backtestdomain.SideSell, "50100", "0.001", "0.05")))

	pos, qty := b.Position("BTCUSDT")
	assert.Equal(t, decider.Flat, pos)
	assert.True(t, qty.IsZero())
	// realised pnl = (50100 - 50000) × 0.001 = 0.1; fees = 0.05 + 0.05 = 0.1
	// equity = 10000 + 0.1 - 0.05 (open fee) - 0.05 (close fee) = 10000
	assert.Equal(t, "10000", b.Equity().String())

	require.Len(t, store.fills, 2)
	assert.Equal(t, "0", store.fills[0].RealisedPnL.String())
	assert.Equal(t, "0.1", store.fills[1].RealisedPnL.String())
}

func TestBlotterOpenShortDeductsOnlyFee(t *testing.T) {
	store := newMemStore()
	b := blotter.New(testSessID, decimal.NewFromInt(10000), store, zap.NewNop())

	require.NoError(t, b.RecordFill(context.Background(), fill("BTCUSDT", backtestdomain.SideSell, "50000", "0.001", "0.05")))

	pos, qty := b.Position("BTCUSDT")
	assert.Equal(t, decider.Short, pos)
	assert.True(t, qty.IsNegative())
	assert.Equal(t, postOpenEquity, b.Equity().String())
}

func TestBlotterBuyClosesShortAndComputesPnL(t *testing.T) {
	store := newMemStore()
	b := blotter.New(testSessID, decimal.NewFromInt(10000), store, zap.NewNop())

	require.NoError(t, b.RecordFill(context.Background(), fill("BTCUSDT", backtestdomain.SideSell, "50000", "0.001", "0.05")))
	require.NoError(t, b.RecordFill(context.Background(), fill("BTCUSDT", backtestdomain.SideBuy, "49900", "0.001", "0.05")))

	pos, _ := b.Position("BTCUSDT")
	assert.Equal(t, decider.Flat, pos)
	// realised pnl = (50000 - 49900) × 0.001 = 0.1; fees = 0.05 + 0.05
	assert.Equal(t, "10000", b.Equity().String())
}

func TestBlotterPositionPersisted(t *testing.T) {
	store := newMemStore()
	b := blotter.New(testSessID, decimal.NewFromInt(10000), store, zap.NewNop())

	require.NoError(t, b.RecordFill(context.Background(), fill("BTCUSDT", backtestdomain.SideBuy, "50000", "0.001", "0.05")))

	posRec := store.positions["BTCUSDT"]
	assert.Equal(t, "0.001", posRec.NetQty.String())
	assert.Equal(t, "50000", posRec.AvgEntry.String())
}

func TestBlotterEquityPersisted(t *testing.T) {
	store := newMemStore()
	b := blotter.New(testSessID, decimal.NewFromInt(10000), store, zap.NewNop())

	require.NoError(t, b.RecordFill(context.Background(), fill("BTCUSDT", backtestdomain.SideBuy, "50000", "0.001", "0.05")))

	require.Len(t, store.equities, 1)
	assert.Equal(t, b.Equity().String(), store.equities[0].Equity.String())
}

func TestBlotterSeedPositions(t *testing.T) {
	store := newMemStore()
	b := blotter.New(testSessID, decimal.RequireFromString(postOpenEquity), store, zap.NewNop())

	b.SeedPositions([]repository.PositionRecord{
		{SessionID: testSessID, Symbol: "BTCUSDT", NetQty: decimal.RequireFromString("0.001"), AvgEntry: decimal.RequireFromString("50000")},
	})

	pos, qty := b.Position("BTCUSDT")
	assert.Equal(t, decider.Long, pos)
	assert.Equal(t, "0.001", qty.String())
}

func TestBlotterFlatWhenNoData(t *testing.T) {
	store := newMemStore()
	b := blotter.New(testSessID, decimal.NewFromInt(10000), store, zap.NewNop())

	pos, qty := b.Position("BTCUSDT")
	assert.Equal(t, decider.Flat, pos)
	assert.True(t, qty.IsZero())
}
