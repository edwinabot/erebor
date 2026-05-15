"use client";

import { useState } from "react";
import OrderBookLadder from "./components/OrderBookLadder";
import MarketDepthChart from "./components/MarketDepthChart";
import OrderBookHeatmap from "./components/OrderBookHeatmap";
import SpreadChart from "./components/SpreadChart";
import ImbalanceChart from "./components/ImbalanceChart";
import styles from "./Dashboard.module.css";

type ViewType =
  | "ladder"
  | "depth"
  | "heatmap"
  | "spread"
  | "imbalance";

export default function DashboardPage() {
  const [activeView, setActiveView] = useState<ViewType>("ladder");
  const [symbol, setSymbol] = useState("BTCUSDT");

  const tabs: { id: ViewType; label: string }[] = [
    { id: "ladder", label: "Order Book" },
    { id: "depth", label: "Market Depth" },
    { id: "heatmap", label: "Heatmap" },
    { id: "spread", label: "Spread" },
    { id: "imbalance", label: "Imbalance" },
  ];

  return (
    <div className={styles.dashboard}>
      {/* Header */}
      <header className={styles.header}>
        <div className={styles.logo}>
          <h1>EREBOR</h1>
          <span className={styles.subtitle}>TRADING TERMINAL</span>
        </div>

        <div className={styles.controls}>
          <input
            type="text"
            className={styles.symbolInput}
            value={symbol}
            onChange={(e) => setSymbol(e.target.value.toUpperCase())}
            placeholder="Symbol"
            maxLength={10}
          />
          <span className={styles.statusIndicator}>LIVE</span>
        </div>
      </header>

      {/* Tab Navigation */}
      <nav className={styles.tabs}>
        {tabs.map((tab) => (
          <button
            key={tab.id}
            className={`${styles.tab} ${
              activeView === tab.id ? styles.active : ""
            }`}
            onClick={() => setActiveView(tab.id)}
            aria-selected={activeView === tab.id}
          >
            {tab.label}
          </button>
        ))}
      </nav>

      {/* Main Content Area */}
      <main className={styles.mainContent}>
        {activeView === "ladder" && <OrderBookLadder symbol={symbol} />}
        {activeView === "depth" && <MarketDepthChart symbol={symbol} />}
        {activeView === "heatmap" && <OrderBookHeatmap symbol={symbol} />}
        {activeView === "spread" && <SpreadChart symbol={symbol} />}
        {activeView === "imbalance" && <ImbalanceChart symbol={symbol} />}
      </main>

      {/* Footer Status Bar */}
      <footer className={styles.footer}>
        <div className={styles.statusLeft}>
          <span>SYMBOL: {symbol}</span>
          <span className={styles.separator}>•</span>
          <span>
            UPDATE: <span className={styles.pulse}>100ms</span>
          </span>
        </div>
        <div className={styles.statusRight}>
          <span>DATA SOURCE: EREBOR-INGESTION</span>
          <span className={styles.separator}>•</span>
          <span>{new Date().toLocaleTimeString()}</span>
        </div>
      </footer>
    </div>
  );
}
