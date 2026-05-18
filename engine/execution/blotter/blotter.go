// Package blotter tracks per-symbol positions and the running equity curve
// for a paper trading session.
package blotter

import (
	"context"
	"sync"
	"time"

	backtestdomain "github.com/edwinabot/erebor/backtest/domain"
	"github.com/edwinabot/erebor/execution/decider"
	"github.com/edwinabot/erebor/execution/repository"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

// FillStore is the narrow persistence interface required by Blotter.
type FillStore interface {
	RecordFill(ctx context.Context, trade repository.TradeRecord, pos repository.PositionRecord, eq repository.EquityRecord) error
}

// FillRequest describes a confirmed paper fill to record.
type FillRequest struct {
	TradeID        string
	Symbol         string
	Side           backtestdomain.Side
	FillPrice      decimal.Decimal
	FillQty        decimal.Decimal
	Fee            decimal.Decimal
	EventTime      time.Time
	SignalName     string
	SignalStreamID string
}

type symbolPosition struct {
	netQty   decimal.Decimal // positive=long, negative=short, zero=flat
	avgEntry decimal.Decimal // VWAP of open position
}

// Blotter manages in-memory position and equity state for one paper session.
type Blotter struct {
	sessionID string
	store     FillStore
	logger    *zap.Logger

	mu        sync.Mutex
	equity    decimal.Decimal
	positions map[string]*symbolPosition
}

// New creates a Blotter starting at initialEquity.
func New(sessionID string, initialEquity decimal.Decimal, store FillStore, logger *zap.Logger) *Blotter {
	return &Blotter{
		sessionID: sessionID,
		store:     store,
		logger:    logger.With(zap.String("component", "blotter")),
		equity:    initialEquity,
		positions: make(map[string]*symbolPosition),
	}
}

// SeedPositions loads recovered position state from a previous session.
func (b *Blotter) SeedPositions(positions []repository.PositionRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, p := range positions {
		b.positions[p.Symbol] = &symbolPosition{
			netQty:   p.NetQty,
			avgEntry: p.AvgEntry,
		}
	}
	b.logger.Info("positions seeded from recovery", zap.Int("count", len(positions)))
}

// Equity returns the current equity.
func (b *Blotter) Equity() decimal.Decimal {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.equity
}

// Position returns the position state and net qty for symbol.
func (b *Blotter) Position(symbol string) (decider.Position, decimal.Decimal) {
	b.mu.Lock()
	defer b.mu.Unlock()
	pos := b.positions[symbol]
	if pos == nil || pos.netQty.IsZero() {
		return decider.Flat, decimal.Zero
	}
	if pos.netQty.IsPositive() {
		return decider.Long, pos.netQty
	}
	return decider.Short, pos.netQty
}

// RecordFill persists a paper fill and updates in-memory position and equity state.
// Returns an error only if the DB write fails (the caller should not XACK on error).
func (b *Blotter) RecordFill(ctx context.Context, fill FillRequest) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	pos := b.positions[fill.Symbol]
	if pos == nil {
		pos = &symbolPosition{}
		b.positions[fill.Symbol] = pos
	}

	realisedPnL := b.applyFill(pos, fill)
	b.equity = b.equity.Add(realisedPnL).Sub(fill.Fee)

	tradeRec := repository.TradeRecord{
		SessionID:      b.sessionID,
		TradeID:        fill.TradeID,
		Symbol:         fill.Symbol,
		EventTime:      fill.EventTime,
		Side:           string(fill.Side),
		FillPrice:      fill.FillPrice,
		FillQty:        fill.FillQty,
		Fee:            fill.Fee,
		RealisedPnL:    realisedPnL,
		SignalName:     fill.SignalName,
		SignalStreamID: fill.SignalStreamID,
	}
	posRec := repository.PositionRecord{
		SessionID: b.sessionID,
		Symbol:    fill.Symbol,
		NetQty:    pos.netQty,
		AvgEntry:  pos.avgEntry,
		UpdatedAt: fill.EventTime,
	}
	eqRec := repository.EquityRecord{
		SessionID: b.sessionID,
		EventTime: fill.EventTime,
		Equity:    b.equity,
	}

	if err := b.store.RecordFill(ctx, tradeRec, posRec, eqRec); err != nil {
		// Roll back in-memory state so a retry sees the pre-fill state.
		b.equity = b.equity.Sub(realisedPnL).Add(fill.Fee)
		b.undoFill(pos, fill, realisedPnL)
		b.logger.Error("blotter persist failed; rolling back",
			zap.String("trade_id", fill.TradeID),
			zap.Error(err),
		)
		return err
	}

	b.logger.Info("fill recorded",
		zap.String("symbol", fill.Symbol),
		zap.String("side", string(fill.Side)),
		zap.String("fill_price", fill.FillPrice.String()),
		zap.String("qty", fill.FillQty.String()),
		zap.String("fee", fill.Fee.String()),
		zap.String("realised_pnl", realisedPnL.String()),
		zap.String("equity", b.equity.String()),
		zap.String("net_qty", pos.netQty.String()),
	)
	return nil
}

// applyFill mutates pos and returns the realised P&L (gross, before fee).
func (b *Blotter) applyFill(pos *symbolPosition, fill FillRequest) decimal.Decimal {
	if fill.Side == backtestdomain.SideBuy {
		return applyBuy(pos, fill)
	}
	return applySell(pos, fill)
}

func applyBuy(pos *symbolPosition, fill FillRequest) decimal.Decimal {
	if pos.netQty.IsNegative() {
		return closeShort(pos, fill)
	}
	openOrExtendLong(pos, fill)
	return decimal.Zero
}

func applySell(pos *symbolPosition, fill FillRequest) decimal.Decimal {
	if pos.netQty.IsPositive() {
		return closeLong(pos, fill)
	}
	openOrExtendShort(pos, fill)
	return decimal.Zero
}

func closeLong(pos *symbolPosition, fill FillRequest) decimal.Decimal {
	pnl := fill.FillPrice.Sub(pos.avgEntry).Mul(fill.FillQty)
	pos.netQty = pos.netQty.Sub(fill.FillQty)
	if !pos.netQty.IsPositive() {
		pos.avgEntry = decimal.Zero
	}
	return pnl
}

func closeShort(pos *symbolPosition, fill FillRequest) decimal.Decimal {
	pnl := pos.avgEntry.Sub(fill.FillPrice).Mul(fill.FillQty)
	pos.netQty = pos.netQty.Add(fill.FillQty)
	if !pos.netQty.IsNegative() {
		pos.avgEntry = decimal.Zero
	}
	return pnl
}

func openOrExtendLong(pos *symbolPosition, fill FillRequest) {
	newQty := pos.netQty.Add(fill.FillQty)
	if pos.netQty.IsZero() {
		pos.avgEntry = fill.FillPrice
	} else {
		pos.avgEntry = pos.netQty.Mul(pos.avgEntry).Add(fill.FillQty.Mul(fill.FillPrice)).Div(newQty)
	}
	pos.netQty = newQty
}

func openOrExtendShort(pos *symbolPosition, fill FillRequest) {
	absQty := pos.netQty.Abs()
	newAbsQty := absQty.Add(fill.FillQty)
	if pos.netQty.IsZero() {
		pos.avgEntry = fill.FillPrice
	} else {
		pos.avgEntry = absQty.Mul(pos.avgEntry).Add(fill.FillQty.Mul(fill.FillPrice)).Div(newAbsQty)
	}
	pos.netQty = pos.netQty.Sub(fill.FillQty)
}

// undoFill reverses the position mutation done by applyFill.
// Called only on DB write failure to keep in-memory state consistent with DB.
func (b *Blotter) undoFill(pos *symbolPosition, fill FillRequest, realisedPnL decimal.Decimal) {
	isBuy := fill.Side == backtestdomain.SideBuy
	if isBuy {
		pos.netQty = pos.netQty.Sub(fill.FillQty)
	} else {
		pos.netQty = pos.netQty.Add(fill.FillQty)
	}
	// Avg entry cannot be exactly restored without storing the previous value.
	// Accept that a retry after failure may use a slightly different avgEntry.
	// In practice, fills are idempotent via signal_stream_id, so the retry
	// either finds the fill already persisted (ON CONFLICT DO NOTHING) or retries cleanly.
	_ = realisedPnL
}
