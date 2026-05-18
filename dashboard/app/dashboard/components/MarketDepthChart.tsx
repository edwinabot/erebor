"use client";

import { useEffect, useRef } from "react";
import { COLORS } from "../lib/colors";
import { useMarketDepthData } from "../hooks/useMarketDepthData";

interface MarketDepthChartProps {
  symbol: string;
}

export default function MarketDepthChart({ symbol }: MarketDepthChartProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const data = useMarketDepthData(symbol);

  useEffect(() => {
    if (!data || !canvasRef.current) return;

    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;

    const width = canvas.width;
    const height = canvas.height;
    const pad = { top: 20, right: 20, bottom: 30, left: 55 };
    const gw = width - pad.left - pad.right;
    const gh = height - pad.top - pad.bottom;

    ctx.fillStyle = COLORS.bgPrimary;
    ctx.fillRect(0, 0, width, height);

    const bids = data.bids ?? [];
    const asks = data.asks ?? [];
    if (bids.length === 0 || asks.length === 0) return;

    // Price range: worst bid (lowest) to worst ask (highest)
    const worstBidPrice = parseFloat(bids[bids.length - 1].price);
    const bestBidPrice = parseFloat(bids[0].price);
    const bestAskPrice = parseFloat(asks[0].price);
    const worstAskPrice = parseFloat(asks[asks.length - 1].price);
    const totalPriceRange = worstAskPrice - worstBidPrice;

    const maxCumQty = Math.max(
      parseFloat(bids[bids.length - 1].cumulative_quantity),
      parseFloat(asks[asks.length - 1].cumulative_quantity)
    );

    // Map a price to canvas x
    const px = (price: number) => pad.left + ((price - worstBidPrice) / totalPriceRange) * gw;

    // Map a cumulative qty to canvas y (qty grows upward)
    const py = (cumQty: number) => pad.top + gh - (cumQty / maxCumQty) * gh;

    // Grid lines
    ctx.strokeStyle = COLORS.border;
    ctx.lineWidth = 0.5;
    for (let i = 1; i <= 4; i++) {
      const y = pad.top + (gh / 5) * i;
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(width - pad.right, y);
      ctx.stroke();
    }

    // Draw bid staircase (left of center, growing left)
    // Bids: sorted descending by price, cumulative qty increases outward
    ctx.fillStyle = COLORS.buyFill;
    ctx.strokeStyle = COLORS.buy;
    ctx.lineWidth = 1.5;
    ctx.beginPath();
    const bidBase = py(0);
    ctx.moveTo(px(bestBidPrice), bidBase);

    for (let i = 0; i < bids.length; i++) {
      const price = parseFloat(bids[i].price);
      const cumQty = parseFloat(bids[i].cumulative_quantity);
      const x = px(price);
      const y = py(cumQty);

      // Step: go horizontal first (at current cumQty level), then vertical
      if (i === 0) {
        ctx.lineTo(x, y);
      } else {
        const prevX = px(parseFloat(bids[i - 1].price));
        ctx.lineTo(prevX, y);
        ctx.lineTo(x, y);
      }
    }
    ctx.lineTo(px(worstBidPrice), bidBase);
    ctx.closePath();
    ctx.fill();
    ctx.stroke();

    // Draw ask staircase (right of center, growing right)
    ctx.fillStyle = COLORS.sellFill;
    ctx.strokeStyle = COLORS.sell;
    ctx.beginPath();
    ctx.moveTo(px(bestAskPrice), py(0));

    for (let i = 0; i < asks.length; i++) {
      const price = parseFloat(asks[i].price);
      const cumQty = parseFloat(asks[i].cumulative_quantity);
      const x = px(price);
      const y = py(cumQty);

      if (i === 0) {
        ctx.lineTo(x, y);
      } else {
        const prevX = px(parseFloat(asks[i - 1].price));
        ctx.lineTo(prevX, y);
        ctx.lineTo(x, y);
      }
    }
    ctx.lineTo(px(worstAskPrice), py(0));
    ctx.closePath();
    ctx.fill();
    ctx.stroke();

    // Spread center line
    const centerX = (px(bestBidPrice) + px(bestAskPrice)) / 2;
    ctx.strokeStyle = COLORS.neutral;
    ctx.lineWidth = 1;
    ctx.setLineDash([3, 3]);
    ctx.beginPath();
    ctx.moveTo(centerX, pad.top);
    ctx.lineTo(centerX, pad.top + gh);
    ctx.stroke();
    ctx.setLineDash([]);

    // Axes
    ctx.strokeStyle = COLORS.textSecondary;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(pad.left, pad.top);
    ctx.lineTo(pad.left, pad.top + gh);
    ctx.lineTo(pad.left + gw, pad.top + gh);
    ctx.stroke();

    // Y-axis labels
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '10px "IBM Plex Mono"';
    ctx.textAlign = "right";
    for (let i = 0; i <= 4; i++) {
      const qty = (maxCumQty / 4) * i;
      const y = py(qty) + 3;
      ctx.fillText(qty.toFixed(1), pad.left - 6, y);
    }

    // X-axis price labels
    ctx.textAlign = "center";
    ctx.fillStyle = COLORS.buy;
    ctx.fillText(bestBidPrice.toFixed(2), px(bestBidPrice), pad.top + gh + 18);
    ctx.fillStyle = COLORS.sell;
    ctx.fillText(bestAskPrice.toFixed(2), px(bestAskPrice), pad.top + gh + 18);
  }, [data]);

  return (
    <div
      className="flex flex-col h-full w-full overflow-hidden"
      style={{ backgroundColor: "var(--bg-primary)" }}
    >
      {/* Stats bar */}
      <div
        className="flex items-center justify-between px-3 py-1.5 shrink-0 gap-4"
        style={{
          borderBottom: "1px solid var(--border-color)",
          backgroundColor: "var(--bg-tertiary)",
          fontFamily: '"IBM Plex Mono", monospace',
        }}
      >
        <span className="text-[11px] tracking-wider" style={{ color: "var(--text-secondary)" }}>
          {data ? new Date(data.timestamp).toLocaleTimeString() : "—"}
        </span>
        <div className="flex items-center gap-4">
          <LegendItem color="var(--color-buy)" label="BID SIDE" />
          <LegendItem color="var(--color-sell)" label="ASK SIDE" />
        </div>
      </div>

      {/* Canvas */}
      <canvas
        ref={canvasRef}
        className="flex-1 w-full block"
        style={{ backgroundColor: "var(--bg-secondary)" }}
      />
    </div>
  );
}

function LegendItem({ color, label }: { color: string; label: string }) {
  return (
    <div
      className="flex items-center gap-1.5 text-[9px] tracking-wider uppercase font-semibold"
      style={{ color: "var(--text-secondary)" }}
    >
      <span
        className="inline-block w-2.5 h-2.5 rounded-sm opacity-70"
        style={{ backgroundColor: color }}
      />
      {label}
    </div>
  );
}
