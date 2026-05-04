package dispatch

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/edwinabot/erebor/ingest/domain"
	"github.com/edwinabot/erebor/ingest/stream"
	"github.com/edwinabot/erebor/ingest/symbol"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

type Dispatcher struct {
	logger   *zap.Logger
	handlers map[string]symbol.SymbolHandler
}

func New(handlers map[string]symbol.SymbolHandler, logger *zap.Logger) *Dispatcher {
	registered := make(map[string]symbol.SymbolHandler, len(handlers))
	for sym, h := range handlers {
		registered[strings.ToUpper(sym)] = h
	}
	return &Dispatcher{
		logger:   logger.With(zap.String("component", "dispatch")),
		handlers: registered,
	}
}

func (d *Dispatcher) Run(ctx context.Context, events <-chan stream.RawDiffEvent) {
	var wg sync.WaitGroup
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case ev, ok := <-events:
			if !ok {
				wg.Wait()
				return
			}
			diff, err := convertRaw(ev)
			if err != nil {
				d.logger.Warn("failed to convert raw diff",
					zap.String("symbol", ev.Data.Symbol),
					zap.Error(err),
				)
				continue
			}
			handler, ok := d.handlers[diff.Symbol]
			if !ok {
				d.logger.Debug("no handler for symbol",
					zap.String("symbol", diff.Symbol),
				)
				continue
			}
			handler.HandleDiff(diff)
		}
	}
}

func convertRaw(ev stream.RawDiffEvent) (domain.DiffEvent, error) {
	bids, err := parseLevels(ev.Data.Bids)
	if err != nil {
		return domain.DiffEvent{}, fmt.Errorf("bids: %w", err)
	}
	asks, err := parseLevels(ev.Data.Asks)
	if err != nil {
		return domain.DiffEvent{}, fmt.Errorf("asks: %w", err)
	}
	return domain.DiffEvent{
		Symbol:        strings.ToUpper(ev.Data.Symbol),
		EventTime:     time.UnixMilli(ev.Data.EventTimeMS).UTC(),
		FirstUpdateID: ev.Data.FirstUpdateID,
		FinalUpdateID: ev.Data.FinalUpdateID,
		Bids:          bids,
		Asks:          asks,
	}, nil
}

func parseLevels(in [][]string) ([]domain.PriceLevel, error) {
	out := make([]domain.PriceLevel, 0, len(in))
	for i, pair := range in {
		if len(pair) < 2 {
			return nil, fmt.Errorf("level %d malformed", i)
		}
		price, err := decimal.NewFromString(pair[0])
		if err != nil {
			return nil, fmt.Errorf("level %d price: %w", i, err)
		}
		qty, err := decimal.NewFromString(pair[1])
		if err != nil {
			return nil, fmt.Errorf("level %d qty: %w", i, err)
		}
		out = append(out, domain.PriceLevel{Price: price, Quantity: qty})
	}
	return out, nil
}
