"use client";

import { useState, useEffect } from "react";
import OrderBookLadder from "./components/OrderBookLadder";
import MarketDepthChart from "./components/MarketDepthChart";
import SpreadChart from "./components/SpreadChart";
import ImbalanceChart from "./components/ImbalanceChart";

export default function DashboardPage() {
  const [symbol, setSymbol] = useState("BTCUSDT");
  const [clock, setClock] = useState("");

  useEffect(() => {
    const update = () => setClock(new Date().toLocaleTimeString());
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <div
      className="flex flex-col h-screen overflow-hidden"
      style={{
        backgroundColor: "var(--bg-primary)",
        color: "var(--text-primary)",
        border: "1px solid var(--border-color)",
      }}
    >
      {/* ── Header ──────────────────────────────────────────────────────── */}
      <header
        className="flex items-center justify-between px-6 py-3 z-10 shrink-0"
        style={{
          borderBottom: "1px solid var(--border-color)",
          background: "linear-gradient(180deg, var(--bg-secondary) 0%, var(--bg-primary) 100%)",
          boxShadow: "var(--shadow-md)",
        }}
      >
        <div className="flex flex-col gap-0.5">
          <h1
            className="text-2xl font-bold tracking-widest"
            style={{ textShadow: "0 0 20px rgba(0, 217, 102, 0.3)" }}
          >
            EREBOR
          </h1>
          <span
            className="text-[10px] tracking-[0.15em] uppercase"
            style={{ fontFamily: '"IBM Plex Mono", monospace', color: "var(--text-secondary)" }}
          >
            TRADING TERMINAL
          </span>
        </div>

        <div className="flex items-center gap-3">
          <input
            type="text"
            className="w-[120px] px-3 py-2 rounded text-[13px] font-semibold uppercase tracking-wider outline-none transition-all"
            style={{
              backgroundColor: "var(--bg-tertiary)",
              border: "1px solid var(--border-color)",
              color: "var(--color-neutral)",
              fontFamily: '"IBM Plex Mono", monospace',
            }}
            value={symbol}
            onChange={(e) => setSymbol(e.target.value.toUpperCase())}
            placeholder="Symbol"
            maxLength={10}
            onFocus={(e) => {
              e.currentTarget.style.borderColor = "var(--color-neutral)";
              e.currentTarget.style.boxShadow = "0 0 10px rgba(0, 191, 255, 0.3)";
            }}
            onBlur={(e) => {
              e.currentTarget.style.borderColor = "var(--border-color)";
              e.currentTarget.style.boxShadow = "none";
            }}
          />
          <LiveIndicator />
          <span
            className="text-xs tracking-wider tabular-nums"
            style={{ color: "var(--text-secondary)", fontFamily: '"IBM Plex Mono", monospace' }}
          >
            {clock}
          </span>
        </div>
      </header>

      {/* ── Main content: two rows ───────────────────────────────────────── */}
      <main className="flex flex-col flex-1 overflow-hidden" style={{ minHeight: 0 }}>
        {/* Top row — Order Book Ladder + Market Depth side by side */}
        <div
          className="flex flex-1 overflow-hidden"
          style={{ borderBottom: "1px solid var(--border-color)" }}
        >
          {/* Ladder */}
          <div
            className="w-[280px] shrink-0 flex flex-col overflow-hidden"
            style={{ borderRight: "1px solid var(--border-color)" }}
          >
            <PanelLabel label="ORDER BOOK LADDER" />
            <div className="flex-1 overflow-hidden h-full w-full">
              <OrderBookLadder symbol={symbol} />
            </div>
          </div>

          {/* Market Depth fills remaining top-row width */}
          <div className="flex-1 flex flex-col overflow-hidden" style={{ minWidth: 0 }}>
            <PanelLabel label="MARKET DEPTH" />
            <div className="flex-1 overflow-hidden h-full w-full">
              <MarketDepthChart symbol={symbol} />
            </div>
          </div>
        </div>

        {/* Bottom row — Spread + Imbalance */}
        <div className="flex flex-1 overflow-hidden">
          <div
            className="flex-1 flex flex-col overflow-hidden"
            style={{ borderRight: "1px solid var(--border-color)" }}
          >
            <PanelLabel label="SPREAD & MID-PRICE" />
            <div className="flex-1 overflow-hidden h-full w-full">
              <SpreadChart symbol={symbol} />
            </div>
          </div>

          <div className="flex-1 flex flex-col overflow-hidden">
            <PanelLabel label="ORDER BOOK IMBALANCE" />
            <div className="flex-1 overflow-hidden h-full w-full">
              <ImbalanceChart symbol={symbol} />
            </div>
          </div>
        </div>
      </main>

      {/* ── Footer ──────────────────────────────────────────────────────── */}
      <footer
        className="flex items-center justify-between px-6 py-2 shrink-0 text-[10px] uppercase tracking-wider"
        style={{
          borderTop: "1px solid var(--border-color)",
          backgroundColor: "var(--bg-secondary)",
          color: "var(--text-secondary)",
          fontFamily: '"IBM Plex Mono", monospace',
        }}
      >
        <div className="flex items-center gap-3">
          <span>SYMBOL: {symbol}</span>
          <Dot />
          <span>
            UPDATE:{" "}
            <span
              className="font-semibold"
              style={{ color: "var(--color-buy)", animation: "textPulse 1s ease-in-out infinite" }}
            >
              100ms
            </span>
          </span>
        </div>
        <div className="flex items-center gap-3">
          <span>DATA SOURCE: EREBOR-INGESTION</span>
          <Dot />
          <span>{clock}</span>
        </div>
      </footer>
    </div>
  );
}

function PanelLabel({ label }: { label: string }) {
  return (
    <div
      className="px-3 py-1.5 text-[10px] font-bold tracking-widest uppercase shrink-0"
      style={{
        borderBottom: "1px solid var(--border-color)",
        backgroundColor: "var(--bg-secondary)",
        color: "var(--text-secondary)",
        fontFamily: '"IBM Plex Mono", monospace',
      }}
    >
      {label}
    </div>
  );
}

function LiveIndicator() {
  return (
    <span
      className="inline-flex items-center gap-1.5 px-3 py-1.5 rounded text-[11px] font-bold tracking-widest uppercase"
      style={{
        backgroundColor: "rgba(0, 217, 102, 0.15)",
        border: "1px solid var(--color-buy)",
        color: "var(--color-buy)",
      }}
    >
      <span
        className="inline-block w-1.5 h-1.5 rounded-full"
        style={{
          backgroundColor: "var(--color-buy)",
          animation: "pulse 1.5s ease-in-out infinite",
        }}
      />
      LIVE
    </span>
  );
}

function Dot() {
  return <span style={{ color: "var(--border-color)" }}>•</span>;
}
