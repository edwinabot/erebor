# ADR-002: Infrastructure and Deployment Platform

**Status:** Draft
**Date:** 2026-05
**Component:** Platform
**Author:** Erebor Architecture Session

---

## Context

Erebor consists of three runtime concerns that need a home:

1. **erebor-ingest** — a long-lived Go process maintaining a persistent WebSocket connection to Binance and writing continuously to TimescaleDB.
2. **TimescaleDB** — a PostgreSQL-backed time-series store holding order book diffs and checkpoints.
3. **erebor-dashboard** — a web application providing visibility into ingestion health and market data, backed by an API that queries TimescaleDB.

Two goals shape these decisions: getting the system working, and learning Kubernetes. The deployment strategy is structured to serve both without the second blocking the first.

---

## Decision 1: Deployment Target — Local Machine

### Options Considered

**Cloud (AWS EC2).** Pay-per-use compute with managed networking. Operational overhead is low. Monthly cost constrains iteration — every restart, schema wipe, or experiment has a background cost. Kubernetes on AWS (EKS) costs $73/month for the control plane alone before any nodes run.

**Local machine (mini PC, ~$200–250 one-time).** No recurring compute cost. Full control. k3s (lightweight Kubernetes) runs comfortably on a 16 GB machine alongside TimescaleDB and the ingest service. Remote access via Tailscale (free tier). Remaining year-one budget preserved for cloud experiments or a future migration.

### Decision

**Local machine.**

### Rationale

Cloud compute imposes a background cost on every iteration during prototyping. A local machine eliminates that pressure entirely and provides more RAM for less money. The one-time hardware cost leaves meaningful budget headroom for cloud experimentation once the system is stable.

The local setup is not a dead end. When Kubernetes is introduced (see Decision 3), the manifests developed locally transfer directly to EKS or a cloud-hosted k3s node. Existing CDK and IAM experience make that migration straightforward.

Remote access via Tailscale requires no port forwarding, no VPN server, and no cloud infrastructure. The machine is reachable from anywhere on the Tailscale network.

---

## Decision 2: Container Orchestration — Docker Compose

### Options Considered

**Docker Compose.** Declarative multi-container configuration. Single command to bring the full stack up. Mirrors the existing local development environment exactly. No learning curve overhead during active feature development.

**k3s (single-node Kubernetes).** Real Kubernetes API and tooling. StatefulSets, PersistentVolumeClaims, Ingress, Helm. Meaningful operational and learning investment before the first service runs.

### Decision

**Docker Compose.** k3s is the explicit next deployment target once the system is working end-to-end (see Deferred Decisions).

### Rationale

Docker Compose keeps iteration fast while the core system — ingestion correctness, TimescaleDB schema, dashboard API — is being built out. The orchestration layer is not the learning objective at this stage; the system is.

k3s is deferred, not abandoned. When the system reaches a stable baseline (ingest reliable, dashboard functional, schema settled), the migration from Compose to k3s is a contained exercise with a working system as the reference point. Learning Kubernetes against a system whose behaviour is already understood is more productive than co-developing both simultaneously.

---

## Decision 3: Dashboard and API — Next.js on Vercel

### Options Considered

**Next.js on Vercel (free tier).** Full-stack React framework with file-based API routes. Vercel's free tier covers personal-scale traffic. No server management. API routes connect to TimescaleDB via a secured connection string over Tailscale or a public Elastic IP when deployed to cloud.

**Next.js self-hosted on the local machine.** Same framework. Adds nginx, process management, and SSL to the operational scope of the ingest host. No material benefit at this stage.

**Separate REST API + SPA frontend.** Two deployments, two runtimes, a CORS policy. Justified when the API needs to serve multiple independent clients; not justified here.

**Angular.** Not considered.

### Decision

**Next.js on Vercel (free tier).**

### Rationale

Vercel eliminates all frontend infrastructure management. The dashboard has no availability coupling to the ingest machine — a Compose restart or a machine reboot does not take down the frontend. API routes provide a sufficient backend layer for the dashboard's query needs against TimescaleDB.

**Database access from Vercel:** API routes connect to TimescaleDB using the machine's Tailscale IP (during local operation) or a stable public address (when migrated to cloud). The DSN is stored as a Vercel environment variable and never committed to source.

---

## Service Topology

```
Local Machine (mini PC, 16 GB RAM)
┌─────────────────────────────────────────────────────┐
│  docker-compose.yml                                 │
│                                                     │
│  ┌──────────────────────┐  ┌─────────────────────┐  │
│  │  erebor-ingest (Go)  │  │  TimescaleDB        │  │
│  │                      │─▶│  PostgreSQL +       │  │
│  │                      │  │  Timescale ext.     │  │
│  └──────────────────────┘  └──────────┬──────────┘  │
│                                        │ named volume│
│  Tailscale — reachable as              │             │
│  machine.tailnet.ts.net                │             │
└────────────────────────────────────────┘
                    ▲
    sslmode=require │ (Tailscale IP or future public IP)
                    │
Vercel (free tier)
┌───────────────────┴─────────────────────────────────┐
│  erebor-dashboard (Next.js)                         │
│  ├── /app  — React frontend                         │
│  └── /api  — API routes querying TimescaleDB        │
└─────────────────────────────────────────────────────┘
```

---

## Security Posture

- TimescaleDB DSN (including password) is sourced from environment variables in all runtimes. Never in source code, logs, or committed configuration files.
- Vercel stores the DSN as an encrypted environment variable scoped to production deployments.
- SSH access to the local machine is key-based only.
- When the system is migrated to a cloud host, PostgreSQL inbound access is restricted by security group to known source IPs only. No unrestricted port 5432.
- EBS encryption at rest is enabled if/when an AWS volume is provisioned.

---

## Deferred Decisions

| Concern | Deferral Rationale |
|---|---|
| k3s migration | Deferred until ingest, DB, and dashboard are stable end-to-end; manifests will be developed against a known-working system |
| Cloud migration (AWS) | Deferred until local setup is stable; existing CDK/IAM experience makes this a contained exercise when ready |
| EKS | EKS control plane costs $73/month; only warranted after k3s experience and budget allows |
| RDS for TimescaleDB | Managed backups are valuable but not justified until cloud migration; local backups via volume snapshots in the interim |
| Authentication on the dashboard | Dashboard is private by default (Tailscale or Vercel password protection); formal auth deferred |
| CI/CD for infrastructure | Single machine, single operator; not warranted yet |

---

## Consequences

- The ingest machine is the single point of failure for both ingestion and TimescaleDB. This is accepted at this stage. The bootstrap protocol in ADR-001 handles recovery automatically on restart.
- The API layer is Next.js API routes within the dashboard application. If the API needs to evolve into an independently versioned service, it will be extracted at that point.
- All future service additions must account for this topology: any service reading from TimescaleDB connects via the Tailscale IP and consumes the DSN from environment variables.
- When k3s is introduced, the docker-compose.yml serves as the specification for the equivalent Kubernetes manifests. It should be kept accurate.
