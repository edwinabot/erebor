"use client";

import { useOrderBookData } from "../hooks/useOrderBookData";

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
      <div
        className="flex h-full w-full items-center justify-center overflow-hidden"
        style={{ backgroundColor: "var(--bg-primary)" }}
      >
        <span
          className="text-sm tracking-widest animate-pulse"
          style={{ color: "var(--text-secondary)" }}
        >
          LOADING ORDER BOOK...
        </span>
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
    <div
      className="flex flex-col h-full w-full overflow-hidden gap-0"
      style={{ backgroundColor: "var(--bg-primary)", fontFamily: '"IBM Plex Mono", monospace' }}
    >
      {/* Spread Info */}
      <div
        className="grid grid-cols-4 gap-2 px-3 py-2 shrink-0"
        style={{
          background: "linear-gradient(135deg, var(--bg-secondary) 0%, var(--bg-tertiary) 100%)",
          borderBottom: "1px solid var(--border-color)",
        }}
      >
        <SpreadItem label="BID" value={data.bids[0]?.price ?? "—"} valueColor="var(--color-buy)" />
        <SpreadItem label="SPREAD" value={spread} />
        <SpreadItem label="ASK" value={data.asks[0]?.price ?? "—"} valueColor="var(--color-sell)" />
        <SpreadItem label="MID" value={midPrice} />
      </div>

      {/* Ladder */}
      <div className="flex flex-col flex-1 overflow-hidden">
        {/* Asks */}
        <div className="flex flex-col overflow-hidden flex-1">
          <LadderHeader />
          <div className="flex-1 overflow-y-auto flex flex-col">
            {displayAsks.map((ask) => (
              <LevelRow key={ask.price} level={ask} side="ask" maxQty={maxQty} />
            ))}
          </div>
        </div>

        {/* Divider */}
        <div
          className="h-0.5 my-2 mx-0 shrink-0"
          style={{
            background: "linear-gradient(90deg, transparent, var(--color-neutral), transparent)",
            boxShadow: "0 0 12px rgba(0, 191, 255, 0.4)",
          }}
        />

        {/* Bids */}
        <div className="flex flex-col overflow-hidden flex-1">
          <LadderHeader />
          <div className="flex-1 overflow-y-auto flex flex-col">
            {displayBids.map((bid) => (
              <LevelRow key={bid.price} level={bid} side="bid" maxQty={maxQty} />
            ))}
          </div>
        </div>
      </div>

      {/* Footer */}
      <div
        className="flex justify-between px-3 py-1.5 text-[10px] uppercase tracking-wider shrink-0"
        style={{
          borderTop: "1px solid var(--border-color)",
          backgroundColor: "var(--bg-tertiary)",
          color: "var(--text-secondary)",
        }}
      >
        <span>{new Date(data.timestamp).toLocaleTimeString()}</span>
        <span>#{data.last_update_id}</span>
      </div>
    </div>
  );
}

function SpreadItem({
  label,
  value,
  valueColor,
}: {
  label: string;
  value: string;
  valueColor?: string;
}) {
  return (
    <div className="flex flex-col gap-0.5">
      <span
        className="text-[9px] uppercase tracking-wider font-semibold"
        style={{ color: "var(--text-secondary)" }}
      >
        {label}
      </span>
      <span
        className="text-xs font-semibold tracking-tight"
        style={{ color: valueColor ?? "var(--text-primary)" }}
      >
        {value}
      </span>
    </div>
  );
}

function LadderHeader() {
  return (
    <div
      className="grid grid-cols-3 gap-2 px-3 py-1.5 text-[9px] font-bold uppercase tracking-wider shrink-0"
      style={{
        backgroundColor: "var(--bg-tertiary)",
        borderBottom: "1px solid var(--border-color)",
        color: "var(--text-secondary)",
      }}
    >
      <span className="text-right">PRICE</span>
      <span className="text-right">QTY</span>
      <span className="text-right">CUM QTY</span>
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
  const isBid = side === "bid";

  return (
    <div
      className="grid grid-cols-3 gap-2 px-3 py-1.5 text-[11px] relative border-b items-center"
      style={{
        backgroundColor: isBid ? "rgba(0, 217, 102, 0.03)" : "rgba(255, 23, 68, 0.03)",
        borderColor: "var(--border-color)",
      }}
    >
      <span
        className="text-right font-semibold z-10 relative"
        style={{ color: isBid ? "var(--color-buy)" : "var(--color-sell)" }}
      >
        {level.price}
      </span>
      <span className="text-right z-10 relative" style={{ color: "var(--text-secondary)" }}>
        {qty.toFixed(3)}
      </span>
      <span className="text-right z-10 relative" style={{ color: "var(--text-secondary)" }}>
        {level.cumQty.toFixed(3)}
      </span>
      {/* Volume bar */}
      <div
        className="absolute right-0 top-0 h-full opacity-[0.18]"
        style={{
          width: `${barWidth}%`,
          backgroundColor: isBid ? "var(--color-buy)" : "var(--color-sell)",
          borderRadius: isBid ? "0 4px 4px 0" : "4px 0 0 4px",
        }}
        data-side={side}
      />
    </div>
  );
}
