"use client";

import { useEffect, useRef } from "react";
import { COLORS } from "../lib/colors";
import { useImbalanceData } from "../hooks/useImbalanceData";
import styles from "./ImbalanceChart.module.css";

interface ImbalanceChartProps {
  symbol: string;
}

export default function ImbalanceChart({ symbol }: ImbalanceChartProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const data = useImbalanceData(symbol);

  useEffect(() => {
    if (!data || !canvasRef.current) return;

    const canvas = canvasRef.current;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    canvas.width = canvas.offsetWidth;
    canvas.height = canvas.offsetHeight;

    const width = canvas.width;
    const height = canvas.height;
    const pad = { top: 20, right: 20, bottom: 30, left: 70 };
    const gw = width - pad.left - pad.right;
    const gh = height - pad.top - pad.bottom;

    ctx.fillStyle = COLORS.bgPrimary;
    ctx.fillRect(0, 0, width, height);

    const samples = data.samples;
    if (samples.length === 0) return;

    const centerY = pad.top + gh / 2;

    // Maps index → x, imbalance [-1,+1] → y
    const gx = (i: number) => pad.left + (i / Math.max(samples.length - 1, 1)) * gw;
    const gy = (imbalance: number) => centerY - imbalance * (gh / 2);

    // Grid lines
    ctx.strokeStyle = COLORS.border;
    ctx.lineWidth = 0.5;
    for (let i = 1; i <= 4; i++) {
      const y = pad.top + (gh / 5) * i;
      ctx.beginPath();
      ctx.moveTo(pad.left, y);
      ctx.lineTo(pad.left + gw, y);
      ctx.stroke();
    }

    // Zero line
    ctx.strokeStyle = COLORS.border;
    ctx.lineWidth = 1;
    ctx.setLineDash([3, 3]);
    ctx.beginPath();
    ctx.moveTo(pad.left, centerY);
    ctx.lineTo(pad.left + gw, centerY);
    ctx.stroke();
    ctx.setLineDash([]);

    // Bar fill beneath the line
    samples.forEach((sample, i) => {
      const imbalance = parseFloat(sample.imbalance);
      const x = gx(i);
      const y = gy(imbalance);
      const alpha = Math.abs(imbalance) * 0.4;

      ctx.fillStyle =
        imbalance >= 0 ? `rgba(0, 217, 102, ${alpha})` : `rgba(255, 23, 68, ${alpha})`;
      ctx.fillRect(x, Math.min(y, centerY), 2, Math.abs(centerY - y));
    });

    // Imbalance line
    ctx.strokeStyle = COLORS.neutral;
    ctx.lineWidth = 2;
    ctx.beginPath();
    samples.forEach((sample, i) => {
      const y = gy(parseFloat(sample.imbalance));
      if (i === 0) ctx.moveTo(gx(i), y);
      else ctx.lineTo(gx(i), y);
    });
    ctx.stroke();

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
    [
      { v: 1, label: "+1.0 (bid)" },
      { v: 0.5, label: "+0.5" },
      { v: 0, label: "0" },
      { v: -0.5, label: "-0.5" },
      { v: -1, label: "-1.0 (ask)" },
    ].forEach(({ v, label }) => {
      ctx.fillText(label, pad.left - 6, gy(v) + 3);
    });

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
      <div className={styles.container}>
        <div className={styles.loading}>LOADING IMBALANCE DATA...</div>
      </div>
    );
  }

  const latest = data.samples[data.samples.length - 1];
  const imbalance = parseFloat(latest.imbalance);

  return (
    <div className={styles.container}>
      <div className={styles.header}>
        <h3>ORDER BOOK IMBALANCE (DEPTH: {data.depth_levels})</h3>
        <div className={styles.stats}>
          <div className={styles.stat}>
            <span className={styles.label}>BID QTY</span>
            <span className={styles.value + " " + styles.buy}>{latest.bid_qty}</span>
          </div>
          <div className={styles.stat}>
            <span className={styles.label}>ASK QTY</span>
            <span className={styles.value + " " + styles.sell}>{latest.ask_qty}</span>
          </div>
          <div className={styles.stat}>
            <span className={styles.label}>IMBALANCE</span>
            <span className={`${styles.value} ${imbalance >= 0 ? styles.buy : styles.sell}`}>
              {(imbalance * 100).toFixed(1)}%
            </span>
          </div>
        </div>
      </div>
      <canvas ref={canvasRef} className={styles.canvas} />
      <div className={styles.legend}>
        <span className={styles.legendLabel}>Positive = Bid Heavy | Negative = Ask Heavy</span>
      </div>
    </div>
  );
}
