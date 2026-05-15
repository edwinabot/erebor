export interface SnapshotRow {
  snapshot_time: Date;
  symbol: string;
  last_update_id: string;
  bids: [string, string][];
  asks: [string, string][];
}

export interface OrderBookResponse {
  symbol: string;
  timestamp: string;
  last_update_id: number;
  bids: Array<{ price: string; quantity: string }>;
  asks: Array<{ price: string; quantity: string }>;
}

export interface MarketDepthResponse {
  symbol: string;
  timestamp: string;
  bids: Array<{ price: string; cumulative_quantity: string }>;
  asks: Array<{ price: string; cumulative_quantity: string }>;
}

export interface SpreadData {
  symbol: string;
  samples: Array<{
    timestamp: string;
    best_bid: string;
    best_ask: string;
    mid_price: string;
    spread: string;
    spread_bps: string;
  }>;
}

export interface ImbalanceData {
  symbol: string;
  depth_levels: number;
  samples: Array<{
    timestamp: string;
    bid_qty: string;
    ask_qty: string;
    imbalance: string;
  }>;
}

export function toOrderBookResponse(row: SnapshotRow): OrderBookResponse {
  return {
    symbol: row.symbol,
    timestamp: row.snapshot_time.toISOString(),
    last_update_id: parseInt(row.last_update_id, 10),
    bids: row.bids.map(([price, quantity]) => ({ price, quantity })),
    asks: row.asks.map(([price, quantity]) => ({ price, quantity })),
  };
}

export function toMarketDepthResponse(row: SnapshotRow): MarketDepthResponse {
  let cumulativeBid = 0;
  const bids = row.bids.map(([price, qty]) => {
    cumulativeBid += parseFloat(qty);
    return { price, cumulative_quantity: cumulativeBid.toFixed(3) };
  });

  let cumulativeAsk = 0;
  const asks = row.asks.map(([price, qty]) => {
    cumulativeAsk += parseFloat(qty);
    return { price, cumulative_quantity: cumulativeAsk.toFixed(3) };
  });

  return {
    symbol: row.symbol,
    timestamp: row.snapshot_time.toISOString(),
    bids,
    asks,
  };
}

export function toSpreadData(rows: SnapshotRow[]): SpreadData {
  if (rows.length === 0) {
    return { symbol: "", samples: [] };
  }

  const samples = rows
    .filter((row) => row.bids.length > 0 && row.asks.length > 0)
    .map((row) => {
      const bestBid = parseFloat(row.bids[0][0]);
      const bestAsk = parseFloat(row.asks[0][0]);
      const midPrice = (bestBid + bestAsk) / 2;
      const spread = bestAsk - bestBid;
      const spreadBps = (spread / midPrice) * 10000;

      return {
        timestamp: row.snapshot_time.toISOString(),
        best_bid: bestBid.toFixed(2),
        best_ask: bestAsk.toFixed(2),
        mid_price: midPrice.toFixed(2),
        spread: spread.toFixed(2),
        spread_bps: spreadBps.toFixed(2),
      };
    });

  return { symbol: rows[0].symbol, samples };
}

export function toImbalanceData(
  rows: SnapshotRow[],
  depthLevels: number
): ImbalanceData {
  if (rows.length === 0) {
    return { symbol: "", depth_levels: depthLevels, samples: [] };
  }

  const samples = rows
    .filter((row) => row.bids.length > 0 && row.asks.length > 0)
    .map((row) => {
      const bidQty = row.bids
        .slice(0, depthLevels)
        .reduce((sum, [, qty]) => sum + parseFloat(qty), 0);
      const askQty = row.asks
        .slice(0, depthLevels)
        .reduce((sum, [, qty]) => sum + parseFloat(qty), 0);
      const total = bidQty + askQty;
      const imbalance = total === 0 ? 0 : (bidQty - askQty) / total;

      return {
        timestamp: row.snapshot_time.toISOString(),
        bid_qty: bidQty.toFixed(2),
        ask_qty: askQty.toFixed(2),
        imbalance: Math.max(-1, Math.min(1, imbalance)).toFixed(4),
      };
    });

  return { symbol: rows[0].symbol, depth_levels: depthLevels, samples };
}
