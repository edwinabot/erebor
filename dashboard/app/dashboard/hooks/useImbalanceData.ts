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

export function useImbalanceData(symbol: string, depthLevels: number = 10) {
  const [data, setData] = useState<ImbalanceData | null>(null);

  useEffect(() => {
    let cancelled = false;

    const fetchData = async () => {
      try {
        const res = await fetch(
          `/api/imbalance?symbol=${encodeURIComponent(symbol)}&depth=${depthLevels}&limit=60`
        );
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
  }, [symbol, depthLevels]);

  return data;
}
