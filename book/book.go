package book

import (
	"sort"
	"sync"
	"time"

	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/shopspring/decimal"
)

type OrderBook interface {
	Apply(diff domain.DiffEvent) error
	Snapshot(depth int) domain.SnapshotEvent
	LastUpdateID() int64
	Reset()
}

type Book struct {
	mu           sync.Mutex
	symbol       string
	bids         map[string]decimal.Decimal
	asks         map[string]decimal.Decimal
	lastUpdateID int64
}

func New(symbol string) *Book {
	return &Book{
		symbol: symbol,
		bids:   make(map[string]decimal.Decimal),
		asks:   make(map[string]decimal.Decimal),
	}
}

func (b *Book) Apply(diff domain.DiffEvent) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, lvl := range diff.Bids {
		applyLevel(b.bids, lvl)
	}
	for _, lvl := range diff.Asks {
		applyLevel(b.asks, lvl)
	}
	b.lastUpdateID = diff.FinalUpdateID
	return nil
}

func applyLevel(side map[string]decimal.Decimal, lvl domain.PriceLevel) {
	key := lvl.Price.String()
	if lvl.Quantity.IsZero() {
		delete(side, key)
		return
	}
	side[key] = lvl.Quantity
}

func (b *Book) Snapshot(depth int) domain.SnapshotEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	bids := sortedLevels(b.bids, true, depth)
	asks := sortedLevels(b.asks, false, depth)

	return domain.SnapshotEvent{
		Symbol:       b.symbol,
		CapturedAt:   time.Now().UTC(),
		LastUpdateID: b.lastUpdateID,
		Bids:         bids,
		Asks:         asks,
	}
}

func sortedLevels(side map[string]decimal.Decimal, descending bool, depth int) []domain.PriceLevel {
	levels := make([]domain.PriceLevel, 0, len(side))
	for priceStr, qty := range side {
		price, err := decimal.NewFromString(priceStr)
		if err != nil {
			continue
		}
		levels = append(levels, domain.PriceLevel{Price: price, Quantity: qty})
	}
	sort.Slice(levels, func(i, j int) bool {
		if descending {
			return levels[i].Price.GreaterThan(levels[j].Price)
		}
		return levels[i].Price.LessThan(levels[j].Price)
	})
	if depth > 0 && len(levels) > depth {
		levels = levels[:depth]
	}
	return levels
}

func (b *Book) LastUpdateID() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastUpdateID
}

func (b *Book) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bids = make(map[string]decimal.Decimal)
	b.asks = make(map[string]decimal.Decimal)
	b.lastUpdateID = 0
}

func (b *Book) LoadSnapshot(snap domain.SnapshotEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bids = make(map[string]decimal.Decimal)
	b.asks = make(map[string]decimal.Decimal)
	for _, lvl := range snap.Bids {
		if !lvl.Quantity.IsZero() {
			b.bids[lvl.Price.String()] = lvl.Quantity
		}
	}
	for _, lvl := range snap.Asks {
		if !lvl.Quantity.IsZero() {
			b.asks[lvl.Price.String()] = lvl.Quantity
		}
	}
	b.lastUpdateID = snap.LastUpdateID
}
