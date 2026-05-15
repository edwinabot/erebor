"use client";

import { useEffect, useRef } from "react";
import { COLORS } from "../lib/colors";
import { useOrderBookHeatmapData } from "../hooks/useOrderBookHeatmapData";
import styles from "./OrderBookHeatmap.module.css";

interface OrderBookHeatmapProps {
  symbol: string;
}

export default function OrderBookHeatmap({ symbol }: OrderBookHeatmapProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const data = useOrderBookHeatmapData(symbol);

  useEffect(() => {
    if (!data || !canvasRef.current) return;

    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;

    const width = canvas.width;
    const height = canvas.height;
    const pad = { top: 10, right: 20, bottom: 30, left: 65 };
    const gw = width - pad.left - pad.right;
    const gh = height - pad.top - pad.bottom;

    ctx.fillStyle = COLORS.bgPrimary;
    ctx.fillRect(0, 0, width, height);

    const frames = data.frames;
    if (frames.length === 0) return;

    // Collect all unique price levels as numbers, sorted descending
    const priceSet = new Set<number>();
    frames.forEach((frame) => {
      frame.bids.forEach((b) => priceSet.add(parseFloat(b.price)));
      frame.asks.forEach((a) => priceSet.add(parseFloat(a.price)));
    });
    const sortedPrices = [...priceSet].sort((a, b) => b - a);

    if (sortedPrices.length === 0) return;

    const cellW = gw / frames.length;
    const cellH = gh / sortedPrices.length;

    // Find global max qty for normalisation
    let maxQty = 1;
    frames.forEach((frame) => {
      [...frame.bids, ...frame.asks].forEach((lvl) => {
        const q = parseFloat(lvl.quantity);
        if (q > maxQty) maxQty = q;
      });
    });

    frames.forEach((frame, fi) => {
      // Build a map keyed by the numeric price value (avoids string precision mismatch)
      const priceMap = new Map<number, number>();
      frame.bids.forEach((b) => priceMap.set(parseFloat(b.price), parseFloat(b.quantity)));
      frame.asks.forEach((a) => priceMap.set(parseFloat(a.price), parseFloat(a.quantity)));

      sortedPrices.forEach((price, pi) => {
        const qty = priceMap.get(price) ?? 0;
        const t = qty / maxQty; // 0..1

        let r: number, g: number, b: number, a: number;
        if (t < 0.25) {
          [r, g, b, a] = [0, 191, 255, t * 0.8]; // blue (sparse)
        } else if (t < 0.5) {
          [r, g, b, a] = [0, 217, 102, t * 0.8]; // green
        } else if (t < 0.75) {
          [r, g, b, a] = [255, 215, 0, t * 0.8]; // yellow
        } else {
          [r, g, b, a] = [255, 23, 68, t * 0.9]; // red (dense)
        }

        const x = pad.left + fi * cellW;
        const y = pad.top + pi * cellH;

        ctx.fillStyle = `rgba(${r},${g},${b},${a})`;
        ctx.fillRect(x, y, cellW, cellH);
      });
    });

    // Axis lines
    ctx.strokeStyle = COLORS.textSecondary;
    ctx.lineWidth = 1;
    ctx.beginPath();
    ctx.moveTo(pad.left, pad.top);
    ctx.lineTo(pad.left, pad.top + gh);
    ctx.lineTo(pad.left + gw, pad.top + gh);
    ctx.stroke();

    // Y-axis price labels
    ctx.fillStyle = COLORS.textSecondary;
    ctx.font = '9px "IBM Plex Mono"';
    ctx.textAlign = "right";
    const labelStep = Math.max(1, Math.ceil(sortedPrices.length / 8));
    sortedPrices.forEach((price, pi) => {
      if (pi % labelStep !== 0) return;
      const y = pad.top + pi * cellH + cellH / 2 + 3;
      ctx.fillText(price.toFixed(2), pad.left - 4, y);
    });

    // X-axis time labels
    ctx.textAlign = "center";
    const tStep = Math.max(1, Math.ceil(frames.length / 6));
    for (let i = 0; i < frames.length; i += tStep) {
      const x = pad.left + i * cellW + cellW / 2;
      ctx.fillText(
        new Date(frames[i].timestamp).toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        }),
        x,
        pad.top + gh + 18
      );
    }
  }, [data]);

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h3>ORDER BOOK HEATMAP</h3>
        <span className={styles.timestamp}>
          {data && data.frames.length > 0
            ? new Date(data.frames[data.frames.length - 1].timestamp).toLocaleTimeString()
            : "—"}
        </span>
      </div>
      <canvas ref={canvasRef} className={styles.canvas} />
      <div className={styles.legend}>
        <span className={styles.label}>Low Activity</span>
        <div className={styles.gradientBar} />
        <span className={styles.label}>High Activity</span>
      </div>
    </div>
  );
}
