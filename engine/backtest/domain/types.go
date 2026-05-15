package domain

import (
	"time"

	"github.com/shopspring/decimal"
)

// SpeedMode controls how the replay engine paces event emission.
type SpeedMode string

const (
	SpeedAFAP      SpeedMode = "AFAP"
	SpeedNX        SpeedMode = "NX"
	SpeedWallClock SpeedMode = "WALL_CLOCK"
)

// ControlEventType enumerates events sent on the control stream.
type ControlEventType string

const (
	ControlReplayStart    ControlEventType = "REPLAY_START"
	ControlReplayComplete ControlEventType = "REPLAY_COMPLETE"
	ControlDataGap        ControlEventType = "DATA_GAP"
	ControlCancelled      ControlEventType = "CANCELLED"
)

// ControlEvent is published by erebor-backtest to the run's control stream.
// All downstream consumers (erebor-signals, erebor-execution) subscribe to this
// stream to coordinate lifecycle.
type ControlEvent struct {
	RunID   string
	Type    ControlEventType
	Payload map[string]string
}

// RunStatus represents the lifecycle state of a backtest run.
type RunStatus string

const (
	RunStatusPending   RunStatus = "PENDING"
	RunStatusRunning   RunStatus = "RUNNING"
	RunStatusCompleted RunStatus = "COMPLETED"
	RunStatusFailed    RunStatus = "FAILED"
	RunStatusCancelled RunStatus = "CANCELLED"
)

// RunRecord holds the parameters and metadata for a single backtest run.
type RunRecord struct {
	RunID          string
	Symbols        []string
	FromTime       time.Time
	ToTime         time.Time
	SpeedMode      SpeedMode
	SpeedFactor    *float64 // nil for AFAP and WALL_CLOCK
	StrategyConfig string   // raw JSON; stored in strategy_config JSONB column
	Status         RunStatus
}

// Side, OrderType, OrderStatus, and OrderEvent are stubs that define the
// stream contract for when erebor-execution ships. Not yet consumed by
// this binary.

type Side string
type OrderType string
type OrderStatus string

const (
	SideBuy  Side = "Buy"
	SideSell Side = "Sell"

	OrderTypeLimit  OrderType = "Limit"
	OrderTypeMarket OrderType = "Market"

	OrderStatusPending         OrderStatus = "Pending"
	OrderStatusOpen            OrderStatus = "Open"
	OrderStatusPartiallyFilled OrderStatus = "PartiallyFilled"
	OrderStatusFilled          OrderStatus = "Filled"
	OrderStatusCancelled       OrderStatus = "Cancelled"
)

// OrderEvent is published by erebor-execution to the run's orders stream.
// EventTime is propagated from the L2BookUpdateEvent that triggered the order.
type OrderEvent struct {
	RunID     string
	Symbol    string
	EventTime time.Time
	OrderID   string
	Side      Side
	Type      OrderType
	Price     decimal.Decimal
	Quantity  decimal.Decimal
	Status    OrderStatus
	FillPrice decimal.Decimal
	FillQty   decimal.Decimal
	Fee       decimal.Decimal
}
