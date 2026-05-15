"use client";

import { useOrderBookData } from "../hooks/useOrderBookData";
import styles from "./OrderBookLadder.module.css";

interface LadderLevel {
  price: string;
  quantity: string;
  cumQty: number;
}

interface OrderBookLadderProps {
  symbol: string;
}

function addCumulativeQty(levels: Array<{ price: string; quantity: string }>): LadderLevel[] {
  let cum = 0;
  return levels.map((lvl) => {
    cum += parseFloat(lvl.quantity);
    return { ...lvl, cumQty: cum };
  });
}

export default function OrderBookLadder({ symbol }: OrderBookLadderProps) {
  const data = useOrderBookData(symbol);

  if (!data) {
    return (
      <div className={styles.container}>
        <div className={styles.loading}>LOADING ORDER BOOK...</div>
      </div>
    );
  }

  const displayAsks = addCumulativeQty(data.asks).slice(0, 14).reverse();
  const displayBids = addCumulativeQty(data.bids).slice(0, 14);

  const maxQty = Math.max(
    ...displayAsks.map((l) => parseFloat(l.quantity)),
    ...displayBids.map((l) => parseFloat(l.quantity)),
    1
  );

  const spread =
    data.asks.length > 0 && data.bids.length > 0
      ? (parseFloat(data.asks[0].price) - parseFloat(data.bids[0].price)).toFixed(2)
      : "—";

  const midPrice =
    data.asks.length > 0 && data.bids.length > 0
      ? ((parseFloat(data.asks[0].price) + parseFloat(data.bids[0].price)) / 2).toFixed(2)
      : "—";

  return (
    <div className={styles.container}>
      <div className={styles.spreadInfo}>
        <div className={styles.spreadItem}>
          <span className={styles.label}>BID</span>
          <span className={`${styles.value} ${styles.buy}`}>{data.bids[0]?.price ?? "—"}</span>
        </div>
        <div className={styles.spreadItem}>
          <span className={styles.label}>SPREAD</span>
          <span className={styles.value}>{spread}</span>
        </div>
        <div className={styles.spreadItem}>
          <span className={styles.label}>ASK</span>
          <span className={`${styles.value} ${styles.sell}`}>{data.asks[0]?.price ?? "—"}</span>
        </div>
        <div className={styles.spreadItem}>
          <span className={styles.label}>MID</span>
          <span className={styles.value}>{midPrice}</span>
        </div>
      </div>

      <div className={styles.ladderContainer}>
        {/* Asks */}
        <div className={styles.asksSection}>
          <div className={styles.header}>
            <span className={styles.priceCol}>PRICE</span>
            <span className={styles.qtyCol}>QTY</span>
            <span className={styles.totalCol}>CUM QTY</span>
          </div>
          <div className={styles.levels}>
            {displayAsks.map((ask) => (
              <LevelRow key={ask.price} level={ask} side="ask" maxQty={maxQty} />
            ))}
          </div>
        </div>

        <div className={styles.divider} />

        {/* Bids */}
        <div className={styles.bidsSection}>
          <div className={styles.header}>
            <span className={styles.priceCol}>PRICE</span>
            <span className={styles.qtyCol}>QTY</span>
            <span className={styles.totalCol}>CUM QTY</span>
          </div>
          <div className={styles.levels}>
            {displayBids.map((bid) => (
              <LevelRow key={bid.price} level={bid} side="bid" maxQty={maxQty} />
            ))}
          </div>
        </div>
      </div>

      <div className={styles.footer}>
        <span>Last Update: {new Date(data.timestamp).toLocaleTimeString()}</span>
        <span>Update ID: {data.last_update_id}</span>
      </div>
    </div>
  );
}

function LevelRow({
  level,
  side,
  maxQty,
}: {
  level: LadderLevel;
  side: "bid" | "ask";
  maxQty: number;
}) {
  const qty = parseFloat(level.quantity);
  const barWidth = (qty / maxQty) * 100;

  return (
    <div className={`${styles.levelRow} ${styles[side]}`}>
      <span className={`${styles.price} ${side === "bid" ? styles.buy : styles.sell}`}>
        {level.price}
      </span>
      <span className={styles.qty}>{qty.toFixed(3)}</span>
      <span className={styles.total}>{level.cumQty.toFixed(3)}</span>
      <div className={styles.bar} style={{ width: `${barWidth}%` }} data-side={side} />
    </div>
  );
}
