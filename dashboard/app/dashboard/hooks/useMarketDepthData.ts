import { useEffect, useState } from "react";

export interface MarketDepthData {
  symbol: string;
  timestamp: string;
  bids: Array<{ price: string; cumulative_quantity: string }>;
  asks: Array<{ price: string; cumulative_quantity: string }>;
}

export function useMarketDepthData(symbol: string) {
  const [data, setData] = useState<MarketDepthData | null>(null);

  useEffect(() => {
    let cancelled = false;

    const fetchData = async () => {
      try {
        const res = await fetch(`/api/market-depth?symbol=${encodeURIComponent(symbol)}`);
        const json = await res.json();
        if (!cancelled && res.ok) setData(json);
      } catch {
        // transient network error; next poll will retry
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 500);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [symbol]);

  return data;
}
