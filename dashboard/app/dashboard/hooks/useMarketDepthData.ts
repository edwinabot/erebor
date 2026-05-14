import { useEffect, useState } from "react";

export interface MarketDepthData {
  symbol: string;
  timestamp: string;
  bids: Array<{ price: string; cumulative_quantity: string }>;
  asks: Array<{ price: string; cumulative_quantity: string }>;
}

function generateMockMarketDepth(symbol: string): MarketDepthData {
  const basePrice = 94500;
  const spread = 0.5 + Math.random() * 2;
  const now = new Date().toISOString();

  let cumulativeBidQty = 0;
  const bids: Array<{ price: string; cumulative_quantity: string }> = [];
  for (let i = 0; i < 20; i++) {
    const price = (basePrice - (i + 1) * spread).toFixed(2);
    cumulativeBidQty += Math.random() * 0.5 + 0.1;
    bids.push({
      price,
      cumulative_quantity: cumulativeBidQty.toFixed(3),
    });
  }

  let cumulativeAskQty = 0;
  const asks: Array<{ price: string; cumulative_quantity: string }> = [];
  for (let i = 0; i < 20; i++) {
    const price = (basePrice + (i + 1) * spread).toFixed(2);
    cumulativeAskQty += Math.random() * 0.5 + 0.1;
    asks.push({
      price,
      cumulative_quantity: cumulativeAskQty.toFixed(3),
    });
  }

  return {
    symbol,
    timestamp: now,
    bids,
    asks,
  };
}

export function useMarketDepthData(symbol: string) {
  const [data, setData] = useState<MarketDepthData | null>(null);

  useEffect(() => {
    setData(generateMockMarketDepth(symbol));

    const interval = setInterval(() => {
      setData(generateMockMarketDepth(symbol));
    }, 500);

    return () => clearInterval(interval);
  }, [symbol]);

  return data;
}
