import { useEffect, useState } from "react";

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

function generateMockSpreadData(symbol: string): SpreadData {
  const now = new Date();
  const samples = [];

  // Generate 60 historical samples (1 per second)
  for (let i = 0; i < 60; i++) {
    const sampleTime = new Date(now.getTime() - (60 - i) * 1000);
    const basePrice = 94500 + (Math.random() - 0.5) * 50;
    const spread = 0.5 + Math.random() * 3;

    const bestBid = (basePrice - spread / 2).toFixed(2);
    const bestAsk = (basePrice + spread / 2).toFixed(2);
    const midPrice = basePrice.toFixed(2);

    const spreadBps = (
      ((parseFloat(bestAsk) - parseFloat(bestBid)) / basePrice) *
      10000
    ).toFixed(2);

    samples.push({
      timestamp: sampleTime.toISOString(),
      best_bid: bestBid,
      best_ask: bestAsk,
      mid_price: midPrice,
      spread: spread.toFixed(2),
      spread_bps: spreadBps,
    });
  }

  return {
    symbol,
    samples,
  };
}

export function useSpreadData(symbol: string) {
  const [data, setData] = useState<SpreadData | null>(null);

  useEffect(() => {
    setData(generateMockSpreadData(symbol));

    const interval = setInterval(() => {
      setData((prevData) => {
        if (!prevData) return generateMockSpreadData(symbol);

        const now = new Date();
        const basePrice = 94500 + (Math.random() - 0.5) * 50;
        const spread = 0.5 + Math.random() * 3;

        const bestBid = (basePrice - spread / 2).toFixed(2);
        const bestAsk = (basePrice + spread / 2).toFixed(2);
        const midPrice = basePrice.toFixed(2);

        const spreadBps = (
          ((parseFloat(bestAsk) - parseFloat(bestBid)) / basePrice) *
          10000
        ).toFixed(2);

        const newSample = {
          timestamp: now.toISOString(),
          best_bid: bestBid,
          best_ask: bestAsk,
          mid_price: midPrice,
          spread: spread.toFixed(2),
          spread_bps: spreadBps,
        };

        return {
          ...prevData,
          samples: [...prevData.samples.slice(1), newSample],
        };
      });
    }, 1000);

    return () => clearInterval(interval);
  }, [symbol]);

  return data;
}
