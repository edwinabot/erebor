import { useEffect, useState } from "react";

export interface OrderBookData {
  symbol: string;
  timestamp: string;
  last_update_id: number;
  bids: Array<{ price: string; quantity: string }>;
  asks: Array<{ price: string; quantity: string }>;
}

// Mock data generator
function generateMockOrderBook(symbol: string): OrderBookData {
  const basePrice = 94500;
  const now = new Date().toISOString();
  const spread = 0.5 + Math.random() * 2;

  // Generate bids
  const bids: Array<{ price: string; quantity: string }> = [];
  for (let i = 0; i < 20; i++) {
    const price = (basePrice - (i + 1) * spread).toFixed(2);
    const quantity = (Math.random() * 2 + 0.1).toFixed(3);
    bids.push({ price, quantity });
  }

  // Generate asks
  const asks: Array<{ price: string; quantity: string }> = [];
  for (let i = 0; i < 20; i++) {
    const price = (basePrice + (i + 1) * spread).toFixed(2);
    const quantity = (Math.random() * 2 + 0.1).toFixed(3);
    asks.push({ price, quantity });
  }

  return {
    symbol,
    timestamp: now,
    last_update_id: Math.floor(Math.random() * 10000000000),
    bids,
    asks,
  };
}

export function useOrderBookData(symbol: string) {
  const [data, setData] = useState<OrderBookData | null>(null);

  useEffect(() => {
    // Initialize with mock data
    setData(generateMockOrderBook(symbol));

    // Simulate updates every 100ms
    const interval = setInterval(() => {
      setData(generateMockOrderBook(symbol));
    }, 100);

    return () => clearInterval(interval);
  }, [symbol]);

  return data;
}
