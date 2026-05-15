package compute

import (
	"fmt"

	"github.com/edwinabot/erebor/signals/domain"
	"github.com/shopspring/decimal"
)

var (
	two    = decimal.NewFromInt(2)
	ten000 = decimal.NewFromInt(10000)
)

// All computes every registered signal for the event and returns the results.
// depth is the number of price levels used for book_imbalance.
func All(event domain.L2BookUpdateEvent, depth int) []domain.SignalEvent {
	return []domain.SignalEvent{
		bookImbalance(event, depth),
		spreadBps(event),
		midPrice(event),
	}
}

// bookImbalance computes (bid_qty - ask_qty) / (bid_qty + ask_qty) over the top
// depth levels. Returns zero when total quantity is zero.
func bookImbalance(event domain.L2BookUpdateEvent, depth int) domain.SignalEvent {
	bidQty := sumQty(event.Bids, depth)
	askQty := sumQty(event.Asks, depth)
	total := bidQty.Add(askQty)

	var value decimal.Decimal
	if !total.IsZero() {
		value = bidQty.Sub(askQty).Div(total)
	}

	return domain.SignalEvent{
		RunID:     event.RunID,
		Symbol:    event.Symbol,
		EventTime: event.EventTime,
		Name:      "book_imbalance",
		Version:   "1",
		Value:     value,
		Params:    map[string]string{"depth": fmt.Sprintf("%d", depth)},
	}
}

// spreadBps computes (best_ask - best_bid) / mid_price * 10000.
// Returns zero when the book is empty or mid-price is zero.
func spreadBps(event domain.L2BookUpdateEvent) domain.SignalEvent {
	var value decimal.Decimal
	if len(event.Bids) > 0 && len(event.Asks) > 0 {
		bestBid := event.Bids[0].Price
		bestAsk := event.Asks[0].Price
		mid := bestBid.Add(bestAsk).Div(two)
		if !mid.IsZero() {
			value = bestAsk.Sub(bestBid).Div(mid).Mul(ten000)
		}
	}

	return domain.SignalEvent{
		RunID:     event.RunID,
		Symbol:    event.Symbol,
		EventTime: event.EventTime,
		Name:      "spread_bps",
		Version:   "1",
		Value:     value,
		Params:    map[string]string{},
	}
}

// midPrice computes (best_bid + best_ask) / 2.
// Returns zero when the book is empty.
func midPrice(event domain.L2BookUpdateEvent) domain.SignalEvent {
	var value decimal.Decimal
	if len(event.Bids) > 0 && len(event.Asks) > 0 {
		value = event.Bids[0].Price.Add(event.Asks[0].Price).Div(two)
	}

	return domain.SignalEvent{
		RunID:     event.RunID,
		Symbol:    event.Symbol,
		EventTime: event.EventTime,
		Name:      "mid_price",
		Version:   "1",
		Value:     value,
		Params:    map[string]string{},
	}
}

func sumQty(levels []domain.PriceLevel, depth int) decimal.Decimal {
	total := decimal.Zero
	for i, lvl := range levels {
		if depth > 0 && i >= depth {
			break
		}
		total = total.Add(lvl.Quantity)
	}
	return total
}
