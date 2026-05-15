"use client";

import { useEffect, useRef } from "react";
import { COLORS } from "../lib/colors";
import { useSpreadData } from "../hooks/useSpreadData";
import styles from "./SpreadChart.module.css";

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

    const gx = (idx: number) =>
      pad.left + (idx / Math.max(samples.length - 1, 1)) * gw;
    const gy = (price: number) =>
      pad.top + gh - ((price - paddedMin) / paddedRange) * gh;

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
        pad.top + gh + 18,
      );
    }
  }, [data]);

  if (!data) {
    return (
      <div className={styles.container}>
        <div className={styles.loading}>LOADING SPREAD DATA...</div>
      </div>
    );
  }

  const latest = data.samples[data.samples.length - 1];

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h3>SPREAD & MID-PRICE</h3>
        <div className={styles.stats}>
          <div className={styles.stat}>
            <span className={styles.label}>SPREAD</span>
            <span className={styles.value}>
              {latest.spread} ({latest.spread_bps} bps)
            </span>
          </div>
          <div className={styles.stat}>
            <span className={styles.label}>MID</span>
            <span className={styles.value}>{latest.mid_price}</span>
          </div>
        </div>
      </div>
      <canvas ref={canvasRef} className={styles.canvas} />
      <div className={styles.legend}>
        <div className={styles.legendItem}>
          <div className={styles.dot + " " + styles.mid} />
          <span>MID-PRICE</span>
        </div>
        <div className={styles.legendItem}>
          <div className={styles.dot + " " + styles.bid} />
          <span>BID</span>
        </div>
        <div className={styles.legendItem}>
          <div className={styles.dot + " " + styles.ask} />
          <span>ASK</span>
        </div>
      </div>
    </div>
  );
}
