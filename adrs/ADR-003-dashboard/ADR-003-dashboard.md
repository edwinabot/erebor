# ADR-003: Dashboard

**Status:** Draft
**Date:** 2026-05
**Component:** Dashboard
**Author:** Erebor Architecture Session

---

## Context

The dashboard serves two distinct audiences: traders, who need market depth, signals, and top-of-book data; and operators, who need ingestion health, system metrics, and alerting.

---

## Decision 1: Dashboard — Next.js for Trading UI, Grafana for Ops

### Options Considered

**Next.js only.** Single application covering both trading visualizations and operational metrics. Full control. More development time — operational panels (ingestion lag, error rates, system metrics) that Grafana provides for free must be built from scratch.

**Grafana only.** Excellent for time-series operational metrics with native TimescaleDB support. Cannot produce a market depth chart without writing a custom plugin in React and TypeScript — at which point the advantage over Next.js disappears for the trading UI.

**Next.js for trading UI + Grafana for ops.** Split by concern. Each tool does what it is designed for. No overlap, no gaps.

**Angular.** Not considered — the dashboard is React and TypeScript.

### Decision

**Next.js (React, TypeScript) for the trading dashboard. Grafana for operational observability. Both self-hosted in Docker.**

Charting library: **TradingView `lightweight-charts`** — purpose-built for financial visualizations, handles sub-second update rates, covers all required chart types natively.

### Rationale

The target visualizations — market depth, top-of-book price levels, spread over time, volume per second — require a market depth chart as the centrepiece. Grafana has no built-in depth chart panel; implementing one requires a Grafana plugin written in React and TypeScript, which eliminates Grafana's setup advantage for the trading UI entirely.

TradingView's `lightweight-charts` handles the full set natively and is designed for 10+ updates/second — consistent with the 100ms order book cadence. Next.js API routes stream live data to the browser via WebSocket or SSE.

Grafana covers the operational side with zero custom development: ingestion throughput, DB write latency, WebSocket reconnect counts, bootstrap durations. It connects to TimescaleDB directly as a PostgreSQL data source. Adding a new operational panel is a configuration task, not a coding task.

**Next.js deployment:** `output: 'standalone'` in `next.config.js` produces a self-contained Node.js server. Runs as a Docker service. No Vercel dependency — Vercel remains available for personal convenience but is not the canonical path.

**Database access:** Next.js API routes connect to TimescaleDB via the Compose service name (`timescaledb:5432`). Grafana connects to the same instance via its PostgreSQL data source plugin. DSNs are injected via environment variables in both cases.

---

## Consequences

- Next.js API routes are the only programmatic interface to TimescaleDB for trading data.
- Grafana connects directly as a read-only PostgreSQL data source.
- The two tools are independent and do not share session state or authentication.
