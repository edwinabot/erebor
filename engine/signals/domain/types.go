package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

// PriceLevel is a single price/quantity pair from the order book.
type PriceLevel struct {
	Price    decimal.Decimal
	Quantity decimal.Decimal
}

// L2BookUpdateEvent is consumed from Redis Streams.
// Bids are sorted descending (best bid first); Asks ascending (best ask first).
// EventTime is the authoritative logical clock — never call time.Now() in signal logic.
type L2BookUpdateEvent struct {
	RunID        string // empty = live event
	Symbol       string
	EventTime    time.Time
	LastUpdateID int64
	Bids         []PriceLevel
	Asks         []PriceLevel
}

// SignalEvent is published to Redis Streams after computing a signal.
// EventTime is propagated from the L2BookUpdateEvent that triggered it.
type SignalEvent struct {
	RunID     string
	Symbol    string
	EventTime time.Time
	Name      string
	Version   string
	Value     decimal.Decimal
	Params    map[string]string
}
