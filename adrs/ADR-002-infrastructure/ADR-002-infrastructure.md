# ADR-002: Cloud Infrastructure and Service Topology

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

The project is at an early stage. Correctness and operational simplicity outweigh scalability at this point. Infrastructure decisions should prefer managed services and low operational overhead, while remaining evolvable as the project matures.

---

## Decision 1: Cloud Provider — AWS

### Options Considered

**AWS.** Dominant market position, richest service catalog, strong free tier and low-cost instance types. Broad ecosystem of tooling and documentation.

**GCP.** Strong on managed data services and container workloads. Broadly comparable cost. Less personal familiarity with the platform.

**Azure.** Enterprise-oriented. Cost profile less favorable for small workloads.

**Self-hosted / VPS (Hetzner, DigitalOcean).** Lower raw cost per compute unit. No managed services. Operational burden falls entirely on the operator.

### Decision

**AWS.**

### Rationale

AWS provides the right combination of low-cost compute options, mature managed services, and operational tooling for a project at this scale. Lightsail, EC2 reserved instances, and serverless primitives (Lambda, API Gateway) cover every required runtime without over-provisioning. The ecosystem of monitoring, alerting, and security tooling reduces operational overhead relative to a self-hosted VPS.

Self-hosted VPS options offer lower raw compute cost but shift operational complexity (OS patching, network hardening, backup orchestration) entirely to the operator. That trade-off is unfavorable at this stage.

---

## Decision 2: Compute Model for erebor-ingest and TimescaleDB — Co-located on a Single EC2 Instance

### Options Considered

**Option A — Separate EC2 instances for ingest service and database.**
Clean separation of concerns. Independent scaling and restart. Higher cost: two instances, two EBS volumes.

**Option B — Co-located on a single EC2 instance.**
`erebor-ingest` and TimescaleDB run on the same host. Single monthly compute cost. Network latency between service and database is negligible (loopback). Single point of failure — a host failure takes down both.

**Option C — RDS for PostgreSQL (managed TimescaleDB via Timescale Cloud or self-managed extension).**
Managed backups, automated failover, point-in-time recovery. Significantly higher monthly cost relative to a self-managed instance. TimescaleDB extension must be explicitly enabled; RDS supports it.

**Option D — Containers on ECS Fargate.**
No EC2 management. Pay per task CPU/memory second. Well-suited for bursty workloads. TimescaleDB as a sidecar is operationally awkward; a stateful database should not run as a Fargate task. Would still require RDS or a dedicated host for the database.

### Decision

**Option B — Co-located on a single EC2 instance (t3.small, reserved or on-demand).**
TimescaleDB runs as a managed Docker container (docker-compose, mirroring the local development setup). `erebor-ingest` runs as a systemd service or Docker container on the same host.

### Rationale

At current data volumes — up to 10 symbols at 100ms cadence — a single t3.small is not the bottleneck. The ingest service is I/O-bound (WebSocket read + DB write), not CPU-bound. TimescaleDB's working set for recent diffs fits comfortably in the 2 GB RAM of a t3.small.

Co-location eliminates inter-service network hops for the hot write path (WriteDiff, WriteCheckpoint), which matters for sequence continuity. It also reduces the monthly cost to a single line item.

The SPOF is acceptable at this stage. A host failure terminates ingestion temporarily; the bootstrap protocol in ADR-001 handles reconnection and re-synchronisation from the point of failure automatically. Historical data persisted before the failure is not lost (EBS survives instance termination).

RDS is deferred to a later maturity stage when the operational benefits (automated failover, managed backups) justify the cost delta.

**EBS configuration:** A single gp3 volume attached to the instance. Snapshots scheduled via AWS Data Lifecycle Manager provide point-in-time backup at negligible additional cost.

---

## Decision 3: Dashboard and API — Next.js on Vercel

### Options Considered

**Option A — Next.js on Vercel.**
Full-stack React framework with file-based API routes. Vercel's free tier covers personal-scale traffic (100 GB bandwidth, serverless function execution). No server management. Built-in preview deployments on branch push. API routes connect directly to TimescaleDB via a secured connection string.

**Option B — Next.js self-hosted on the EC2 instance.**
Same framework, same code. Eliminates Vercel dependency. Adds process management (PM2 or systemd) and SSL termination (nginx + certbot) to the operational scope of the ingest host. Introduces resource contention with TimescaleDB on the same host.

**Option C — Separate REST API (Go, FastAPI, etc.) + SPA frontend.**
Explicit separation of API and frontend. More operational surface area: two deployments, two runtimes to manage, a CORS policy to maintain. Justified when the API needs to serve multiple clients independently; not justified here.

**Option D — Angular frontend.**
Not considered.

### Decision

**Option A — Next.js on Vercel.**

### Rationale

Next.js's API routes provide a sufficient backend layer for the dashboard: querying TimescaleDB for ingestion health metrics, recent order book snapshots, and symbol-level statistics. The API does not need to be independently deployable or versioned at this stage — co-locating it with the frontend in the same Next.js project is the correct scope.

Vercel eliminates all frontend infrastructure management. SSL, CDN, and preview deployments are provided out of the box. The free tier is adequate for a personal dashboard with low concurrent traffic.

Self-hosting Next.js on the ingest EC2 instance would be operationally simpler in one sense (one host) but couples frontend availability to the ingest host's operational state. A mis-configured nginx or a Node.js process leak should not put the ingest service at risk.

**Database access from Vercel:** API routes connect to TimescaleDB using the EC2 instance's public IP (or an Elastic IP) over a TLS-secured PostgreSQL connection (`sslmode=require`). The EC2 security group restricts inbound PostgreSQL access to Vercel's published IP ranges. The DSN is stored as a Vercel environment variable and never committed to source.

---

## Decision 4: Networking — VPC with Public Subnet, Security Group Ingress Control

### Decision

The EC2 instance runs in the default VPC, public subnet. No NAT gateway, no private subnet. Inbound access is restricted via security group rules:

| Port | Protocol | Source | Purpose |
|---|---|---|---|
| 22 | TCP | Operator IP only | SSH |
| 5432 | TCP | Vercel IP ranges | TimescaleDB (API routes) |
| 443 | TCP | 0.0.0.0/0 | HTTPS (if any future public endpoint) |

Outbound is unrestricted (required for Binance WebSocket and REST calls).

### Rationale

A private subnet with a NAT gateway adds ~$32/month for the NAT gateway alone. At this scale, a well-configured security group on a public subnet provides equivalent security for the actual attack surface (which is: PostgreSQL port, SSH port). All sensitive ports are locked to specific source IPs or CIDR ranges.

An Elastic IP is assigned to the instance to provide a stable address for the security group rule and DNS. Cost is negligible when the instance is running.

---

## Service Topology

```
┌─────────────────────────────────────────────────────────────────┐
│  AWS EC2 (t3.small)  ·  Elastic IP                             │
│                                                                  │
│  ┌─────────────────────────┐   ┌──────────────────────────┐    │
│  │  erebor-ingest (Go)     │   │  TimescaleDB             │    │
│  │  systemd / Docker       │──▶│  PostgreSQL + Timescale  │    │
│  │                         │   │  Docker (port 5432)      │    │
│  └─────────────────────────┘   └────────────┬─────────────┘    │
│                                              │ EBS gp3 volume   │
│  Security Group:                             │                   │
│  5432 ← Vercel IPs only                      │                   │
│  22   ← Operator IP only                     │                   │
└─────────────────────────────────────────────────────────────────┘
                                              ▲
                              sslmode=require │
                                              │
┌─────────────────────────────────────────────┴───────────────────┐
│  Vercel (free tier)                                              │
│                                                                  │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  erebor-dashboard (Next.js)                               │  │
│  │  ├── /app  — React frontend (order book, health views)   │  │
│  │  └── /api  — API routes querying TimescaleDB             │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Security Posture

- TimescaleDB DSN (including password) is sourced from environment variables in all runtimes. It must never appear in source code, logs, or configuration files committed to the repository.
- The EC2 instance must have no inbound rule permitting unrestricted PostgreSQL access. Vercel IP range allowlisting is the only permitted source for port 5432.
- SSH access is key-based only. Password authentication is disabled.
- Vercel stores the DSN as an encrypted environment variable. It is scoped to production deployments only; preview deployments use a separate read-only credential or no database access.
- EBS volume encryption is enabled at rest.

---

## Deferred Decisions

| Concern | Deferral Rationale |
|---|---|
| Read replica for TimescaleDB | Not needed at current query volume; API routes run infrequently |
| VPC private subnets | NAT gateway cost not justified at this scale |
| RDS managed PostgreSQL | Adds significant monthly cost; manual backup via EBS snapshots is sufficient now |
| Multi-region | Not warranted until ingestion uptime SLA becomes a hard requirement |
| Authentication on the dashboard | Dashboard initially private (Vercel password protection or IP-restricted); formal auth deferred |
| CI/CD pipeline for infrastructure | Manual EC2 provisioning acceptable for a single instance; IaC (Terraform, CDK) deferred |

---

## Consequences

- The ingest EC2 instance is the single point of failure for both ingestion and TimescaleDB. A host failure interrupts ingestion; the bootstrap protocol in ADR-001 handles recovery automatically on restart. Historical data is protected by EBS persistence.
- The API layer is Next.js API routes within the dashboard application. If the API needs to evolve into an independently versioned service (e.g., to serve additional clients), it will be extracted into a standalone deployment at that point.
- Vercel's free tier imposes function execution limits. If the dashboard generates heavy query traffic (e.g., long-running analytical queries from API routes), costs will increase or queries will need to be moved closer to the data.
- All future ADRs for erebor services should account for this topology: any new service that reads from TimescaleDB must be granted access via the security group and must consume the DSN from environment variables.
