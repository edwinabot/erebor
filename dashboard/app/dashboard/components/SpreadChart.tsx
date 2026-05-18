"use client";

import { useEffect, useRef } from "react";
import { COLORS } from "../lib/colors";
import { useSpreadData } from "../hooks/useSpreadData";

interface SpreadChartProps {
  symbol: string;
}

export default function SpreadChart({ symbol }: SpreadChartProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const data = useSpreadData(symbol);

  useEffect(() => {
    if (!data || !canvasRef.current) return;

    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;

    const width = canvas.width;
    const height = canvas.height;
    const pad = { top: 20, right: 20, bottom: 30, left: 65 };
    const gw = width - pad.left - pad.right;
    const gh = height - pad.top - pad.bottom;

    ctx.fillStyle = COLORS.bgPrimary;
    ctx.fillRect(0, 0, width, height);

    const samples = data.samples;
    if (samples.length === 0) return;

    const bids = samples.map((s) => parseFloat(s.best_bid));
    const asks = samples.map((s) => parseFloat(s.best_ask));
    const mids = samples.map((s) => parseFloat(s.mid_price));

    const allPrices = [...bids, ...asks, ...mids];
    const minPrice = Math.min(...allPrices);
    const maxPrice = Math.max(...allPrices);
    const priceRange = maxPrice - minPrice || 1;
    const paddedMin = minPrice - priceRange * 0.05;
    const paddedMax = maxPrice + priceRange * 0.05;
    const paddedRange = paddedMax - paddedMin;

    const gx = (idx: number) => pad.left + (idx / Math.max(samples.length - 1, 1)) * gw;
    const gy = (price: number) => pad.top + gh - ((price - paddedMin) / paddedRange) * gh;

    // Grid
    ctx.strokeStyle = COLORS.border;
    ctx.lineWidth = 0.5;
    for (let i = 1; i <= 4; i++) {
      const y = pad.top + (gh / 5) * i;
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(width - pad.right, y);
      ctx.stroke();
    }

    // Spread filled area (between bid and ask)
    ctx.fillStyle = COLORS.accentFill;
    ctx.beginPath();
    bids.forEach((bid, i) => {
      if (i === 0) ctx.moveTo(gx(i), gy(bid));
      else ctx.lineTo(gx(i), gy(bid));
    });
    for (let i = samples.length - 1; i >= 0; i--) {
      ctx.lineTo(gx(i), gy(asks[i]));
    }
    ctx.closePath();
    ctx.fill();

    // Mid-price line
    ctx.strokeStyle = COLORS.textPrimary;
    ctx.lineWidth = 2;
    ctx.beginPath();
    mids.forEach((mid, i) => {
      if (i === 0) ctx.moveTo(gx(i), gy(mid));
      else ctx.lineTo(gx(i), gy(mid));
    });
    ctx.stroke();

    // Bid line (dashed)
    ctx.strokeStyle = COLORS.buy;
    ctx.lineWidth = 1.5;
    ctx.setLineDash([4, 4]);
    ctx.beginPath();
    bids.forEach((bid, i) => {
      if (i === 0) ctx.moveTo(gx(i), gy(bid));
      else ctx.lineTo(gx(i), gy(bid));
    });
    ctx.stroke();

    // Ask line (dashed)
    ctx.strokeStyle = COLORS.sell;
    ctx.beginPath();
    asks.forEach((ask, i) => {
      if (i === 0) ctx.moveTo(gx(i), gy(ask));
      else ctx.lineTo(gx(i), gy(ask));
    });
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
      const price = paddedMin + (paddedRange / 4) * i;
      const y = gy(price) + 3;
      ctx.fillText(price.toFixed(2), pad.left - 6, y);
    }

    // X-axis time labels
    ctx.textAlign = "center";
    const step = Math.max(1, Math.ceil(samples.length / 6));
    for (let i = 0; i < samples.length; i += step) {
      ctx.fillText(
        new Date(samples[i].timestamp).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        }),
        gx(i),
        pad.top + gh + 18
      );
    }
  }, [data]);

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
          LOADING SPREAD DATA...
        </span>
      </div>
    );
  }

  const latest = data.samples[data.samples.length - 1];

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
        <div className="flex gap-4">
          <Stat label="SPREAD" value={`${latest.spread} (${latest.spread_bps} bps)`} />
          <Stat label="MID" value={latest.mid_price} />
        </div>
        <div className="flex items-center gap-3">
          <LegendItem color="var(--text-primary)" label="MID-PRICE" />
          <LegendItem color="var(--color-buy)" label="BID" />
          <LegendItem color="var(--color-sell)" label="ASK" />
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

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5">
      <span
        className="text-[9px] tracking-wider uppercase font-semibold"
        style={{ color: "var(--text-secondary)" }}
      >
        {label}
      </span>
      <span className="text-xs font-semibold" style={{ color: "var(--color-accent)" }}>
        {value}
      </span>
    </div>
  );
}

function LegendItem({ color, label }: { color: string; label: string }) {
  return (
    <div
      className="flex items-center gap-1.5 text-[9px] tracking-wider uppercase font-semibold"
      style={{ color: "var(--text-secondary)" }}
    >
      <span className="inline-block w-2 h-2 rounded-full" style={{ backgroundColor: color }} />
      {label}
    </div>
  );
}
