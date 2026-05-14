import { useEffect, useState } from "react";

export interface OrderBookHeatmapData {
  symbol: string;
  frames: Array<{
    timestamp: string;
    bids: Array<{ price: string; quantity: string }>;
    asks: Array<{ price: string; quantity: string }>;
  }>;
}

const BASE_PRICE = 94500;
// Fixed price tick: 1.00 per level. Keeps levels stable across frames.
const TICK = 1.0;
const LEVELS = 20;
const FRAME_COUNT = 60;

function makeFrame(timestamp: Date): OrderBookHeatmapData["frames"][number] {
  const bids = Array.from({ length: LEVELS }, (_, i) => ({
    price: (BASE_PRICE - (i + 1) * TICK).toFixed(2),
    quantity: (Math.random() * 2 + 0.05).toFixed(3),
  }));
  const asks = Array.from({ length: LEVELS }, (_, i) => ({
    price: (BASE_PRICE + (i + 1) * TICK).toFixed(2),
    quantity: (Math.random() * 2 + 0.05).toFixed(3),
  }));
  return { timestamp: timestamp.toISOString(), bids, asks };
}

function initialData(symbol: string): OrderBookHeatmapData {
  const now = Date.now();
  return {
    symbol,
    frames: Array.from({ length: FRAME_COUNT }, (_, i) =>
      makeFrame(new Date(now - (FRAME_COUNT - i) * 100)),
    ),
  };
}

export function useOrderBookHeatmapData(symbol: string) {
  const [data, setData] = useState<OrderBookHeatmapData | null>(null);

  useEffect(() => {
    setData(initialData(symbol));

    const interval = setInterval(() => {
      const newFrame = makeFrame(new Date());
      setData((prev) => {
        if (!prev) return initialData(symbol);
        return {
          symbol,
          frames: [...prev.frames.slice(1), newFrame],
        };
      });
    }, 100);

    return () => clearInterval(interval);
  }, [symbol]);

  return data;
}
