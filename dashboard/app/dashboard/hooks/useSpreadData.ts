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

export function useSpreadData(symbol: string) {
  const [data, setData] = useState<SpreadData | null>(null);

  useEffect(() => {
    let cancelled = false;

    const fetchData = async () => {
      try {
        const res = await fetch(
          `/api/spread?symbol=${encodeURIComponent(symbol)}&limit=60`
        );
        const json = await res.json();
        if (!cancelled && res.ok) setData(json);
      } catch {
        // transient network error; next poll will retry
      }
    };

    fetchData();
    const interval = setInterval(fetchData, 1000);
    return () => {
      cancelled = true;
      clearInterval(interval);
    };
  }, [symbol]);

  return data;
}
