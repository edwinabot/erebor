package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

type PriceLevel struct {
	Price    decimal.Decimal
	Quantity decimal.Decimal
}

type DiffEvent struct {
	Symbol        string
	EventTime     time.Time
	FirstUpdateID int64
	FinalUpdateID int64
	Bids          []PriceLevel
	Asks          []PriceLevel
}

type SnapshotEvent struct {
	Symbol       string
	CapturedAt   time.Time
	LastUpdateID int64
	Bids         []PriceLevel
	Asks         []PriceLevel
}
