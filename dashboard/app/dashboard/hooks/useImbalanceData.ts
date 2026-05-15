import { useEffect, useState } from "react";

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

function calculateImbalance(bidQty: number, askQty: number): number {
  if (bidQty + askQty === 0) return 0;
  return (bidQty - askQty) / (bidQty + askQty);
}

function generateMockImbalanceData(
  symbol: string,
  depthLevels: number = 10
): ImbalanceData {
  const now = new Date();
  const samples = [];

  // Generate 60 historical samples
  for (let i = 0; i < 60; i++) {
    const sampleTime = new Date(now.getTime() - (60 - i) * 1000);

    // Simulate imbalance trending slightly with random walk
    const bidQty = Math.max(10, 30 + Math.random() * 30);
    const askQty = Math.max(10, 30 + Math.random() * 30);
    const imbalance = calculateImbalance(bidQty, askQty);

    samples.push({
      timestamp: sampleTime.toISOString(),
      bid_qty: bidQty.toFixed(2),
      ask_qty: askQty.toFixed(2),
      imbalance: Math.max(-1, Math.min(1, imbalance)).toFixed(4),
    });
  }

  return {
    symbol,
    depth_levels: depthLevels,
    samples,
  };
}

export function useImbalanceData(symbol: string, depthLevels: number = 10) {
  const [data, setData] = useState<ImbalanceData | null>(null);

  useEffect(() => {
    setData(generateMockImbalanceData(symbol, depthLevels));

    const interval = setInterval(() => {
      setData((prevData) => {
        if (!prevData) return generateMockImbalanceData(symbol, depthLevels);

        const now = new Date();
        const bidQty = Math.max(10, 30 + Math.random() * 30);
        const askQty = Math.max(10, 30 + Math.random() * 30);
        const imbalance = calculateImbalance(bidQty, askQty);

        const newSample = {
          timestamp: now.toISOString(),
          bid_qty: bidQty.toFixed(2),
          ask_qty: askQty.toFixed(2),
          imbalance: Math.max(-1, Math.min(1, imbalance)).toFixed(4),
        };

        return {
          ...prevData,
          samples: [...prevData.samples.slice(1), newSample],
        };
      });
    }, 500);

    return () => clearInterval(interval);
  }, [symbol, depthLevels]);

  return data;
}
