package execution

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/edwinabot/erebor/backtest/domain"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"go.uber.org/zap"
)

const (
	defaultExecBlockDur = 5 * time.Second
	execBatchSize       = 20
)

// Option configures an Executor.
type Option func(*Executor)

// WithBlockDuration overrides the XRead block timeout. Default 5s; tests use shorter.
func WithBlockDuration(d time.Duration) Option {
	return func(e *Executor) { e.blockDur = d }
}

// Executor reads L2BookUpdateEvents, computes book_imbalance inline, and
// publishes filled OrderEvents for a paper trading simulation. One goroutine
// per symbol; positions are tracked independently per symbol.
type Executor struct {
	client    *redis.Client
	namespace string
	symbols   []string
	cfg       StrategyConfig
	blockDur  time.Duration
	logger    *zap.Logger

	ordersKey string
	wg        sync.WaitGroup
}

// NewExecutor creates an Executor that publishes orders to {namespace}:orders.
func NewExecutor(
	client *redis.Client,
	namespace string,
	symbols []string,
	cfg StrategyConfig,
	logger *zap.Logger,
	opts ...Option,
) *Executor {
	e := &Executor{
		client:    client,
		namespace: namespace,
		symbols:   symbols,
		cfg:       cfg,
		blockDur:  defaultExecBlockDur,
		logger:    logger.With(zap.String("component", "executor")),
		ordersKey: namespace + ":orders",
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// Start launches one goroutine per symbol. Each goroutine reads its :l2:{SYMBOL}
// stream, evaluates the book_imbalance strategy, and publishes OrderEvents.
func (e *Executor) Start(ctx context.Context) {
	e.logger.Info("executor starting",
		zap.Strings("symbols", e.symbols),
		zap.String("orders_stream", e.ordersKey),
	)
	for _, sym := range e.symbols {
		sym := sym
		e.wg.Add(1)
		go e.symbolLoop(ctx, sym)
	}
}

// Wait blocks until all symbol goroutines have exited and drained their streams.
func (e *Executor) Wait() {
	e.wg.Wait()
}

// positionState tracks the net position for one symbol.
// Positive = long, negative = short, zero = flat.
type positionState struct {
	qty decimal.Decimal
}

func (p *positionState) isFlat() bool  { return p.qty.IsZero() }
func (p *positionState) isLong() bool  { return p.qty.IsPositive() }
func (p *positionState) isShort() bool { return p.qty.IsNegative() }

func (e *Executor) symbolLoop(ctx context.Context, symbol string) {
	defer e.wg.Done()

	streamKey := e.namespace + ":l2:" + strings.ToUpper(symbol)
	pos := &positionState{}

	e.logger.Info("symbol executor started",
		zap.String("symbol", symbol),
		zap.String("stream", streamKey),
	)

	lastID := e.runL2Reader(ctx, streamKey, symbol, pos)
	e.drainL2(streamKey, symbol, pos, lastID)

	e.logger.Info("symbol executor drained and stopped", zap.String("symbol", symbol))
}

// runL2Reader blocks on the L2 stream until ctx is cancelled, then returns the last consumed ID.
func (e *Executor) runL2Reader(ctx context.Context, streamKey, symbol string, pos *positionState) string {
	lastID := "0-0"
	for {
		results, err := e.client.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamKey, lastID},
			Count:   execBatchSize,
			Block:   e.blockDur,
		}).Result()
		if err != nil {
			if errors.Is(err, redis.Nil) {
				if ctx.Err() != nil {
					return lastID
				}
				continue
			}
			if ctx.Err() != nil {
				return lastID
			}
			e.logger.Error("xread error on L2 stream",
				zap.String("stream", streamKey),
				zap.String("last_id", lastID),
				zap.Error(err),
			)
			continue
		}
		lastID = e.processL2Batch(results, symbol, pos, lastID)
	}
}

func (e *Executor) processL2Batch(results []redis.XStream, symbol string, pos *positionState, lastID string) string {
	for _, stream := range results {
		for _, msg := range stream.Messages {
			e.handleL2(context.Background(), symbol, pos, msg)
			lastID = msg.ID
		}
	}
	return lastID
}

// drainL2 reads any remaining L2 events non-blocking via XRANGE after ctx cancellation.
// XRANGE is always non-blocking; Block:0 in XRead means "wait forever" in the Redis protocol.
func (e *Executor) drainL2(streamKey, symbol string, pos *positionState, lastID string) {
	drainStart := "(" + lastID
	if lastID == "0-0" {
		drainStart = "-"
	}
	for {
		msgs, err := e.client.XRangeN(context.Background(), streamKey, drainStart, "+", execBatchSize).Result()
		if err != nil || len(msgs) == 0 {
			break
		}
		for _, msg := range msgs {
			e.handleL2(context.Background(), symbol, pos, msg)
			lastID = msg.ID
		}
		drainStart = "(" + lastID
	}
}

func (e *Executor) handleL2(ctx context.Context, symbol string, pos *positionState, msg redis.XMessage) {
	ev, err := decodeL2Event(msg.Values)
	if err != nil {
		e.logger.Error("decode L2 event failed",
			zap.String("symbol", symbol),
			zap.String("msg_id", msg.ID),
			zap.Error(err),
		)
		return
	}

	imbalance := bookImbalance(ev.bids, ev.asks, 10)

	e.logger.Debug("L2 event processed",
		zap.String("symbol", symbol),
		zap.Time("event_time", ev.EventTime),
		zap.String("imbalance", imbalance.String()),
	)

	side, shouldTrade := e.tradeDecision(imbalance, pos)
	if !shouldTrade {
		return
	}

	fillPrice, err := e.computeFillPrice(side, ev.bids, ev.asks)
	if err != nil {
		e.logger.Warn("cannot compute fill price; skipping",
			zap.String("symbol", symbol),
			zap.String("side", string(side)),
			zap.Error(err),
		)
		return
	}

	qty := e.cfg.TradeQty
	fee := computeFee(qty, fillPrice, e.cfg.TakerFeeBps)
	orderID := newOrderID()

	order := domain.OrderEvent{
		RunID:      ev.runID,
		Symbol:     symbol,
		EventTime:  ev.EventTime,
		OrderID:    orderID,
		Side:       side,
		Type:       domain.OrderTypeMarket,
		Price:      decimal.Zero,
		Quantity:   qty,
		Status:     domain.OrderStatusFilled,
		FillPrice:  fillPrice,
		FillQty:    qty,
		Fee:        fee,
		SignalName: "book_imbalance",
	}

	// Update position before publishing so back-to-back events are handled correctly.
	if side == domain.SideBuy {
		pos.qty = pos.qty.Add(qty)
	} else {
		pos.qty = pos.qty.Sub(qty)
	}

	e.logger.Info("order filled",
		zap.String("symbol", symbol),
		zap.String("side", string(side)),
		zap.String("fill_price", fillPrice.String()),
		zap.String("qty", qty.String()),
		zap.String("fee", fee.String()),
		zap.String("position", pos.qty.String()),
		zap.Time("event_time", ev.EventTime),
	)

	if pubErr := e.publishOrder(ctx, order); pubErr != nil {
		e.logger.Error("publish order failed",
			zap.String("order_id", orderID),
			zap.String("symbol", symbol),
			zap.Error(pubErr),
		)
	}
}

// tradeDecision returns the side and whether to trade based on the book_imbalance
// and current position (toggle logic: max one position per symbol at a time).
func (e *Executor) tradeDecision(imbalance decimal.Decimal, pos *positionState) (domain.Side, bool) {
	wantBuy := imbalance.GreaterThan(e.cfg.BuyThreshold)
	wantSell := imbalance.LessThan(e.cfg.SellThreshold.Neg())

	switch {
	case wantBuy && (pos.isFlat() || pos.isShort()):
		return domain.SideBuy, true
	case wantSell && (pos.isFlat() || pos.isLong()):
		return domain.SideSell, true
	default:
		return "", false
	}
}

func (e *Executor) computeFillPrice(side domain.Side, bids, asks []priceLevel) (decimal.Decimal, error) {
	slip := decimal.NewFromInt(int64(e.cfg.SlippageBps)).Div(decimal.NewFromInt(10000))
	if side == domain.SideBuy {
		if len(asks) == 0 {
			return decimal.Zero, fmt.Errorf("empty asks: cannot compute BUY fill price")
		}
		bestAsk := asks[0].Price
		return bestAsk.Add(bestAsk.Mul(slip)), nil
	}
	if len(bids) == 0 {
		return decimal.Zero, fmt.Errorf("empty bids: cannot compute SELL fill price")
	}
	bestBid := bids[0].Price
	return bestBid.Sub(bestBid.Mul(slip)), nil
}

func (e *Executor) publishOrder(ctx context.Context, order domain.OrderEvent) error {
	if err := e.client.XAdd(ctx, &redis.XAddArgs{
		Stream: e.ordersKey,
		Values: map[string]any{
			"run_id":      order.RunID,
			"symbol":      order.Symbol,
			"event_time":  order.EventTime.UTC().Format(time.RFC3339Nano),
			"order_id":    order.OrderID,
			"side":        string(order.Side),
			"type":        string(order.Type),
			"price":       order.Price.String(),
			"quantity":    order.Quantity.String(),
			"status":      string(order.Status),
			"fill_price":  order.FillPrice.String(),
			"fill_qty":    order.FillQty.String(),
			"fee":         order.Fee.String(),
			"signal_name": order.SignalName,
		},
	}).Err(); err != nil {
		return fmt.Errorf("xadd %s: %w", e.ordersKey, err)
	}
	return nil
}

// ── L2 event decoding ─────────────────────────────────────────────────────────

type l2EventData struct {
	runID     string
	EventTime time.Time
	bids      []priceLevel
	asks      []priceLevel
}

type priceLevel struct {
	Price    decimal.Decimal
	Quantity decimal.Decimal
}

func decodeL2Event(values map[string]any) (l2EventData, error) {
	var ev l2EventData
	ev.runID, _ = values["run_id"].(string)

	tsStr, _ := values["event_time"].(string)
	if tsStr == "" {
		return ev, fmt.Errorf("missing event_time")
	}
	var err error
	ev.EventTime, err = time.Parse(time.RFC3339Nano, tsStr)
	if err != nil {
		return ev, fmt.Errorf("parse event_time %q: %w", tsStr, err)
	}

	bidsRaw, _ := values["bids"].(string)
	ev.bids, err = decodePriceLevels(bidsRaw)
	if err != nil {
		return ev, fmt.Errorf("decode bids: %w", err)
	}

	asksRaw, _ := values["asks"].(string)
	ev.asks, err = decodePriceLevels(asksRaw)
	if err != nil {
		return ev, fmt.Errorf("decode asks: %w", err)
	}

	return ev, nil
}

func decodePriceLevels(raw string) ([]priceLevel, error) {
	if raw == "" {
		return nil, nil
	}
	var pairs [][2]string
	if err := json.Unmarshal([]byte(raw), &pairs); err != nil {
		return nil, err
	}
	levels := make([]priceLevel, 0, len(pairs))
	for _, pair := range pairs {
		price, err := decimal.NewFromString(pair[0])
		if err != nil {
			return nil, fmt.Errorf("parse price %q: %w", pair[0], err)
		}
		qty, err := decimal.NewFromString(pair[1])
		if err != nil {
			return nil, fmt.Errorf("parse qty %q: %w", pair[1], err)
		}
		levels = append(levels, priceLevel{Price: price, Quantity: qty})
	}
	return levels, nil
}

// ── Signal computation (inline, no external module dependency) ────────────────

func bookImbalance(bids, asks []priceLevel, depth int) decimal.Decimal {
	bidQty := sumQty(bids, depth)
	askQty := sumQty(asks, depth)
	total := bidQty.Add(askQty)
	if total.IsZero() {
		return decimal.Zero
	}
	return bidQty.Sub(askQty).Div(total)
}

func sumQty(levels []priceLevel, depth int) decimal.Decimal {
	total := decimal.Zero
	for i, lvl := range levels {
		if depth > 0 && i >= depth {
			break
		}
		total = total.Add(lvl.Quantity)
	}
	return total
}

func computeFee(qty, price decimal.Decimal, feeBps int) decimal.Decimal {
	return qty.Mul(price).Mul(decimal.NewFromInt(int64(feeBps))).Div(decimal.NewFromInt(10000))
}

func newOrderID() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.New().String()
	}
	return id.String()
}
