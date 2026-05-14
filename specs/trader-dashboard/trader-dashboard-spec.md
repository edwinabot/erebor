# Trader Dashboard — Specification

**Status:** Draft  
**Date:** 2026-05  
**Component:** Dashboard — Trading UI  
**Depends on:** ADR-003 (Next.js + TradingView `lightweight-charts`)

---

## 1. Overview

The trader dashboard is a real-time web UI for monitoring L2 order book data. It is read-only in this iteration — no order entry. The primary data source is the erebor-ingestion service, which persists Binance order book diffs and periodic snapshots to TimescaleDB.

---

## 2. Visualizations

### 2.1 Order Book Ladder (DOM — Depth of Market)

The central panel. Displays all active bid and ask price levels with their quantities, centered around the spread. Updates at the ingestion cadence (~100 ms). Click-to-trade is out of scope for this iteration.

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OrderBookSnapshot",
  "type": "object",
  "required": ["symbol", "timestamp", "last_update_id", "bids", "asks"],
  "properties": {
    "symbol":         { "type": "string", "example": "BTCUSDT" },
    "timestamp":      { "type": "string", "format": "date-time" },
    "last_update_id": { "type": "integer" },
    "bids": {
      "type": "array",
      "description": "Sorted descending by price (best bid first)",
      "items": {
        "type": "object",
        "required": ["price", "quantity"],
        "properties": {
          "price":    { "type": "string", "description": "Decimal string" },
          "quantity": { "type": "string", "description": "Decimal string" }
        }
      }
    },
    "asks": {
      "type": "array",
      "description": "Sorted ascending by price (best ask first)",
      "items": {
        "type": "object",
        "required": ["price", "quantity"],
        "properties": {
          "price":    { "type": "string" },
          "quantity": { "type": "string" }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "symbol": "BTCUSDT",
  "timestamp": "2026-05-13T10:00:00.123Z",
  "last_update_id": 4872910234,
  "bids": [
    { "price": "94500.00", "quantity": "0.532" },
    { "price": "94499.50", "quantity": "1.234" },
    { "price": "94498.00", "quantity": "3.100" }
  ],
  "asks": [
    { "price": "94501.00", "quantity": "0.800" },
    { "price": "94502.50", "quantity": "2.100" },
    { "price": "94504.00", "quantity": "0.450" }
  ]
}
```

---

### 2.2 Market Depth Chart

A step/mountain chart showing cumulative quantity available on each side across price levels. Bids grow left, asks grow right from the spread. Gives an immediate sense of where large walls are sitting.

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "MarketDepth",
  "type": "object",
  "required": ["symbol", "timestamp", "bids", "asks"],
  "properties": {
    "symbol":    { "type": "string" },
    "timestamp": { "type": "string", "format": "date-time" },
    "bids": {
      "type": "array",
      "description": "Sorted descending by price; quantity is cumulative from best bid outward",
      "items": {
        "type": "object",
        "required": ["price", "cumulative_quantity"],
        "properties": {
          "price":               { "type": "string" },
          "cumulative_quantity": { "type": "string" }
        }
      }
    },
    "asks": {
      "type": "array",
      "description": "Sorted ascending by price; quantity is cumulative from best ask outward",
      "items": {
        "type": "object",
        "required": ["price", "cumulative_quantity"],
        "properties": {
          "price":               { "type": "string" },
          "cumulative_quantity": { "type": "string" }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "symbol": "BTCUSDT",
  "timestamp": "2026-05-13T10:00:00.123Z",
  "bids": [
    { "price": "94500.00", "cumulative_quantity": "0.532" },
    { "price": "94499.50", "cumulative_quantity": "1.766" },
    { "price": "94498.00", "cumulative_quantity": "4.866" }
  ],
  "asks": [
    { "price": "94501.00", "cumulative_quantity": "0.800" },
    { "price": "94502.50", "cumulative_quantity": "2.900" },
    { "price": "94504.00", "cumulative_quantity": "3.350" }
  ]
}
```

---

### 2.3 Candlestick Chart (OHLCV)

Standard candlestick price history with volume bars underneath. Supports multiple timeframes (1m, 5m, 15m, 1h, 4h, 1d). This is the primary price history view.

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "CandleSeries",
  "type": "object",
  "required": ["symbol", "interval", "candles"],
  "properties": {
    "symbol":   { "type": "string" },
    "interval": {
      "type": "string",
      "enum": ["1m", "5m", "15m", "1h", "4h", "1d"]
    },
    "candles": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["open_time", "open", "high", "low", "close", "volume", "close_time", "trade_count"],
        "properties": {
          "open_time":   { "type": "string", "format": "date-time" },
          "open":        { "type": "string" },
          "high":        { "type": "string" },
          "low":         { "type": "string" },
          "close":       { "type": "string" },
          "volume":      { "type": "string", "description": "Base asset volume traded in interval" },
          "close_time":  { "type": "string", "format": "date-time" },
          "trade_count": { "type": "integer" }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "symbol": "BTCUSDT",
  "interval": "1m",
  "candles": [
    {
      "open_time":   "2026-05-13T10:00:00.000Z",
      "open":        "94480.00",
      "high":        "94610.00",
      "low":         "94390.00",
      "close":       "94550.00",
      "volume":      "12.345",
      "close_time":  "2026-05-13T10:00:59.999Z",
      "trade_count": 248
    }
  ]
}
```

---

### 2.4 Time & Sales (Tape)

A live-scrolling feed of every executed trade: price, quantity, and aggressor side. Traders read this to judge momentum and detect large prints.

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Trade",
  "type": "object",
  "required": ["trade_id", "symbol", "timestamp", "price", "quantity", "aggressor_side"],
  "properties": {
    "trade_id":       { "type": "integer" },
    "symbol":         { "type": "string" },
    "timestamp":      { "type": "string", "format": "date-time" },
    "price":          { "type": "string" },
    "quantity":       { "type": "string" },
    "aggressor_side": { "type": "string", "enum": ["buy", "sell"] }
  }
}
```

**Example:**

```json
{
  "trade_id":       987654321,
  "symbol":         "BTCUSDT",
  "timestamp":      "2026-05-13T10:00:01.234Z",
  "price":          "94501.00",
  "quantity":       "0.042",
  "aggressor_side": "buy"
}
```

---

### 2.5 Volume Profile

A horizontal histogram showing total traded volume at each price level over a selected time window. Identifies high-activity price zones (support/resistance). Typically overlaid on the right side of the candlestick chart.

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "VolumeProfile",
  "type": "object",
  "required": ["symbol", "period_start", "period_end", "levels"],
  "properties": {
    "symbol":       { "type": "string" },
    "period_start": { "type": "string", "format": "date-time" },
    "period_end":   { "type": "string", "format": "date-time" },
    "levels": {
      "type": "array",
      "description": "One entry per price level, sorted ascending by price",
      "items": {
        "type": "object",
        "required": ["price", "volume", "buy_volume", "sell_volume"],
        "properties": {
          "price":       { "type": "string" },
          "volume":      { "type": "string", "description": "Total traded volume at this price" },
          "buy_volume":  { "type": "string" },
          "sell_volume": { "type": "string" }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "symbol":       "BTCUSDT",
  "period_start": "2026-05-13T00:00:00Z",
  "period_end":   "2026-05-13T10:00:00Z",
  "levels": [
    { "price": "94500.00", "volume": "45.234", "buy_volume": "23.000", "sell_volume": "22.234" },
    { "price": "94510.00", "volume": "18.900", "buy_volume": "10.500", "sell_volume": "8.400" }
  ]
}
```

---

### 2.6 Order Book Heatmap

A 2D heatmap with price on the Y axis and time on the X axis. Cell intensity encodes the resting quantity at that price level at that moment. Shows how liquidity accumulates, moves, and is pulled — key for detecting spoofing and iceberg behaviour.

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OrderBookHeatmapSeries",
  "type": "object",
  "required": ["symbol", "frames"],
  "properties": {
    "symbol": { "type": "string" },
    "frames": {
      "type": "array",
      "description": "Ordered chronologically; each frame is a point-in-time order book slice",
      "items": {
        "type": "object",
        "required": ["timestamp", "bids", "asks"],
        "properties": {
          "timestamp": { "type": "string", "format": "date-time" },
          "bids": {
            "type": "array",
            "items": {
              "type": "object",
              "required": ["price", "quantity"],
              "properties": {
                "price":    { "type": "string" },
                "quantity": { "type": "string" }
              }
            }
          },
          "asks": {
            "type": "array",
            "items": {
              "type": "object",
              "required": ["price", "quantity"],
              "properties": {
                "price":    { "type": "string" },
                "quantity": { "type": "string" }
              }
            }
          }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "symbol": "BTCUSDT",
  "frames": [
    {
      "timestamp": "2026-05-13T10:00:00.000Z",
      "bids": [
        { "price": "94500.00", "quantity": "0.532" },
        { "price": "94499.50", "quantity": "1.234" }
      ],
      "asks": [
        { "price": "94501.00", "quantity": "0.800" },
        { "price": "94502.50", "quantity": "2.100" }
      ]
    },
    {
      "timestamp": "2026-05-13T10:00:00.100Z",
      "bids": [
        { "price": "94500.00", "quantity": "0.100" },
        { "price": "94499.50", "quantity": "1.234" }
      ],
      "asks": [
        { "price": "94501.00", "quantity": "1.200" },
        { "price": "94502.50", "quantity": "2.100" }
      ]
    }
  ]
}
```

---

### 2.7 Footprint / Delta Chart

A candlestick variant where each candle shows buy vs. sell volume at every price level within the bar, and the net delta (buys minus sells). Identifies where aggressive buying or selling occurred. Requires actual trade execution data with aggressor side.

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "FootprintSeries",
  "type": "object",
  "required": ["symbol", "interval", "candles"],
  "properties": {
    "symbol":   { "type": "string" },
    "interval": { "type": "string", "enum": ["1m", "5m", "15m", "1h"] },
    "candles": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["open_time", "close_time", "levels", "total_delta", "cumulative_delta"],
        "properties": {
          "open_time":         { "type": "string", "format": "date-time" },
          "close_time":        { "type": "string", "format": "date-time" },
          "total_delta":       { "type": "string", "description": "Net buy minus sell volume for this candle" },
          "cumulative_delta":  { "type": "string", "description": "Running delta from session open" },
          "levels": {
            "type": "array",
            "description": "One entry per price traded within the candle",
            "items": {
              "type": "object",
              "required": ["price", "buy_volume", "sell_volume", "delta"],
              "properties": {
                "price":       { "type": "string" },
                "buy_volume":  { "type": "string" },
                "sell_volume": { "type": "string" },
                "delta":       { "type": "string" }
              }
            }
          }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "symbol":   "BTCUSDT",
  "interval": "1m",
  "candles": [
    {
      "open_time":        "2026-05-13T10:00:00.000Z",
      "close_time":       "2026-05-13T10:00:59.999Z",
      "total_delta":      "5.200",
      "cumulative_delta": "12.400",
      "levels": [
        { "price": "94500.00", "buy_volume": "2.500", "sell_volume": "1.200", "delta": "1.300" },
        { "price": "94501.00", "buy_volume": "3.100", "sell_volume": "0.700", "delta": "2.400" }
      ]
    }
  ]
}
```

---

### 2.8 Spread & Mid-price Chart

A time-series line chart showing best bid, best ask, mid-price, and spread (in price and basis points) over time. Used to assess market tightness and detect unusual spread widening.

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "SpreadSeries",
  "type": "object",
  "required": ["symbol", "samples"],
  "properties": {
    "symbol": { "type": "string" },
    "samples": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["timestamp", "best_bid", "best_ask", "mid_price", "spread", "spread_bps"],
        "properties": {
          "timestamp":   { "type": "string", "format": "date-time" },
          "best_bid":    { "type": "string" },
          "best_ask":    { "type": "string" },
          "mid_price":   { "type": "string" },
          "spread":      { "type": "string", "description": "Absolute spread (ask - bid)" },
          "spread_bps":  { "type": "string", "description": "Spread in basis points: (spread / mid_price) * 10000" }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "symbol": "BTCUSDT",
  "samples": [
    {
      "timestamp":  "2026-05-13T10:00:00.000Z",
      "best_bid":   "94500.00",
      "best_ask":   "94501.00",
      "mid_price":  "94500.50",
      "spread":     "1.00",
      "spread_bps": "1.06"
    },
    {
      "timestamp":  "2026-05-13T10:00:00.100Z",
      "best_bid":   "94499.50",
      "best_ask":   "94501.50",
      "mid_price":  "94500.50",
      "spread":     "2.00",
      "spread_bps": "2.12"
    }
  ]
}
```

---

### 2.9 Order Book Imbalance

A time-series indicator showing the ratio of bid quantity to ask quantity at a configurable depth. A positive imbalance suggests buying pressure; negative suggests selling pressure. Useful as a leading signal.

Formula: `imbalance = (bid_qty - ask_qty) / (bid_qty + ask_qty)` → range [-1, +1]

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "ImbalanceSeries",
  "type": "object",
  "required": ["symbol", "depth_levels", "samples"],
  "properties": {
    "symbol":       { "type": "string" },
    "depth_levels": { "type": "integer", "description": "Number of price levels summed on each side" },
    "samples": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["timestamp", "bid_qty", "ask_qty", "imbalance"],
        "properties": {
          "timestamp":  { "type": "string", "format": "date-time" },
          "bid_qty":    { "type": "string", "description": "Total resting quantity on bid side within depth" },
          "ask_qty":    { "type": "string", "description": "Total resting quantity on ask side within depth" },
          "imbalance":  { "type": "string", "description": "Value in [-1, 1]; positive = bid-heavy" }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "symbol":       "BTCUSDT",
  "depth_levels": 10,
  "samples": [
    {
      "timestamp": "2026-05-13T10:00:00.000Z",
      "bid_qty":   "45.234",
      "ask_qty":   "32.100",
      "imbalance": "0.171"
    },
    {
      "timestamp": "2026-05-13T10:00:00.100Z",
      "bid_qty":   "28.500",
      "ask_qty":   "51.200",
      "imbalance": "-0.286"
    }
  ]
}
```

---

### 2.10 Positions & P&L Panel

Displays current open positions with entry price, market price, unrealized P&L, and realized P&L. Read-only in this iteration.

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "PositionSnapshot",
  "type": "object",
  "required": ["as_of", "positions", "total_unrealized_pnl", "total_realized_pnl"],
  "properties": {
    "as_of": { "type": "string", "format": "date-time" },
    "total_unrealized_pnl": { "type": "string" },
    "total_realized_pnl":   { "type": "string" },
    "positions": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["symbol", "side", "quantity", "avg_entry_price", "current_price", "unrealized_pnl", "realized_pnl"],
        "properties": {
          "symbol":            { "type": "string" },
          "side":              { "type": "string", "enum": ["long", "short"] },
          "quantity":          { "type": "string" },
          "avg_entry_price":   { "type": "string" },
          "current_price":     { "type": "string" },
          "unrealized_pnl":    { "type": "string" },
          "realized_pnl":      { "type": "string" }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "as_of": "2026-05-13T10:00:00.000Z",
  "total_unrealized_pnl": "750.00",
  "total_realized_pnl":   "250.00",
  "positions": [
    {
      "symbol":          "BTCUSDT",
      "side":            "long",
      "quantity":        "0.500",
      "avg_entry_price": "93000.00",
      "current_price":   "94500.00",
      "unrealized_pnl":  "750.00",
      "realized_pnl":    "250.00"
    }
  ]
}
```

---

### 2.11 Open Orders Panel

Displays all pending orders with their status. Includes cancel action (out of scope to implement in this iteration — display only).

**Data schema:**

```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "OpenOrders",
  "type": "object",
  "required": ["as_of", "orders"],
  "properties": {
    "as_of": { "type": "string", "format": "date-time" },
    "orders": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["order_id", "symbol", "side", "type", "price", "quantity", "filled_quantity", "status", "created_at"],
        "properties": {
          "order_id":        { "type": "string" },
          "symbol":          { "type": "string" },
          "side":            { "type": "string", "enum": ["buy", "sell"] },
          "type":            { "type": "string", "enum": ["limit", "market", "stop_limit"] },
          "price":           { "type": "string", "description": "null for market orders" },
          "quantity":        { "type": "string" },
          "filled_quantity": { "type": "string" },
          "status":          { "type": "string", "enum": ["open", "partially_filled", "cancelled"] },
          "created_at":      { "type": "string", "format": "date-time" }
        }
      }
    }
  }
}
```

**Example:**

```json
{
  "as_of": "2026-05-13T10:00:00.000Z",
  "orders": [
    {
      "order_id":        "ord_8a3f91c",
      "symbol":          "BTCUSDT",
      "side":            "buy",
      "type":            "limit",
      "price":           "94000.00",
      "quantity":        "0.100",
      "filled_quantity": "0.000",
      "status":          "open",
      "created_at":      "2026-05-13T09:55:00.000Z"
    }
  ]
}
```

---

## 3. Feasibility Against Current Ingestion Data

The erebor-ingestion service currently collects two data types from the Binance `@depth` WebSocket stream:

| Table | Contents |
|---|---|
| `order_book_diffs` | event_time, symbol, first_update_id, final_update_id, bids JSONB `[[price, qty]]`, asks JSONB |
| `order_book_snapshots` | snapshot_time, symbol, last_update_id, depth, bids JSONB, asks JSONB |

**What is not collected:** actual trade executions (Binance `@trade` stream), OHLCV candles (`@kline`), position data, or order data. The ingestion service subscribes only to `@depth` streams.

---

### 3.1 Implementable Now

| # | Visualization | Source | Notes |
|---|---|---|---|
| 2.1 | Order Book Ladder | `order_book_snapshots` | Direct — snapshot has price/qty on both sides |
| 2.2 | Market Depth Chart | `order_book_snapshots` | Compute cumulative sums in the query or API layer |
| 2.6 | Order Book Heatmap | `order_book_diffs` | Replay diffs from a time window into frames; group by snapshot interval (e.g. 100 ms) |
| 2.8 | Spread & Mid-price Chart | `order_book_snapshots` | Extract `bids[0]` and `asks[0]` from each snapshot, compute spread and mid-price |
| 2.9 | Order Book Imbalance | `order_book_snapshots` | Sum resting qty on each side to configured depth, apply imbalance formula |

All five rely solely on `order_book_snapshots` and/or `order_book_diffs`. No new ingestion is required.

---

### 3.2 Requires Additional Ingestion

| # | Visualization | Missing data | What to add |
|---|---|---|---|
| 2.3 | Candlestick Chart | Trade executions / OHLCV | Subscribe to Binance `@kline` or `@trade` stream; store OHLCV aggregates or raw trades |
| 2.4 | Time & Sales | Individual trade executions with aggressor side | Subscribe to Binance `@trade` stream; store `trade_id`, `price`, `qty`, `is_buyer_maker`, `trade_time` |
| 2.5 | Volume Profile | Traded volume at each price level | Derived from `@trade` data once collected |
| 2.7 | Footprint / Delta Chart | Buy vs sell volume per price level per candle | Derived from `@trade` data with `is_buyer_maker` flag |
| 2.10 | Positions & P&L | Position tracking | Requires an order management / execution layer (out of scope for ingestion service) |
| 2.11 | Open Orders | Order state | Same — requires order management layer |

---

### 3.3 Summary

Five of eleven visualizations are implementable today using only the existing TimescaleDB tables. The other four market-data visualizations (2.3, 2.4, 2.5, 2.7) are blocked solely on adding a `@trade` stream subscription to erebor-ingestion — a contained change that adds a new table without modifying existing ones. The two operational panels (2.10, 2.11) depend on a future order management component and are deferred.

**Recommended build order:**

1. Implement the five feasible visualizations (2.1, 2.2, 2.6, 2.8, 2.9) — no new ingestion work.
2. Add `@trade` stream to erebor-ingestion + `trades` hypertable.
3. Implement 2.3, 2.4, 2.5, 2.7 once trade data is flowing.
4. Defer 2.10 and 2.11 until an order management layer exists.
