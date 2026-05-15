import { useEffect, useState } from "react";

export interface OrderBookData {
  symbol: string;
  timestamp: string;
  last_update_id: number;
  bids: Array<{ price: string; quantity: string }>;
  asks: Array<{ price: string; quantity: string }>;
}

export function useOrderBookData(symbol: string) {
  const [data, setData] = useState<OrderBookData | null>(null);

  useEffect(() => {
    let cancelled = false;
    let inFlight = false;
    let timerId: ReturnType<typeof setTimeout>;

    const fetchData = async () => {
      if (inFlight) return;
      inFlight = true;
      try {
        const res = await fetch(`/api/orderbook?symbol=${encodeURIComponent(symbol)}`);
        const json = await res.json();
        if (!cancelled && res.ok) setData(json);
      } catch {
        // transient network error; next poll will retry
      } finally {
        inFlight = false;
        if (!cancelled) timerId = setTimeout(fetchData, 100);
      }
    };

    fetchData();
    return () => {
      cancelled = true;
      clearTimeout(timerId);
    };
  }, [symbol]);

  return data;
}
