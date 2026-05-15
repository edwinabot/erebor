# ADR-002: Infrastructure and Deployment Platform

**Status:** Draft
**Date:** 2026-05
**Component:** Platform
**Author:** Erebor Architecture Session

---

## Context

Erebor is a multi-service system being built incrementally. The services, in rough dependency order:

| Module | Runtime | Responsibility |
|---|---|---|
| **erebor-ingest** | Go | Continuous Binance L2 order book ingestion into TimescaleDB |
| **erebor-signals** | Go | Consumes L2 events from Redis Streams, computes market signals |
| **erebor-risk** | Go | Risk management — enforces position and exposure limits before execution |
| **erebor-execution** | Go | Order placement on Binance (live) or simulated fills (paper), gated by risk |
| **erebor-backtest** | Go | Replays historical L2 data through the signal/execution pipeline |
| **erebor-dashboard** | Next.js · React · TypeScript | Trading UI — market depth, top-of-book, spread, signals |
| **Grafana** | — | Ops observability — ingestion health, system metrics, alerting |
| **TimescaleDB** | — | Time-series store for order book diffs, checkpoints, signals, and backtest results |
| **Redis** | — | Event bus — Redis Streams for L2 events, signals, orders, and backtest control |

Two goals shape these decisions: getting the system working, and learning Kubernetes. The deployment strategy is structured to serve both without the second blocking the first.

---

## Decision 1: Deployment Target — Local Machine

### Options Considered

**Cloud (AWS EC2).** Pay-per-use compute with managed networking. Monthly cost constrains iteration — every restart, schema wipe, or experiment has a background cost. Kubernetes on AWS (EKS) costs $73/month for the control plane alone before any nodes run.

**Local machine (mini PC, ~$200–250 one-time).** No recurring compute cost. Full control. k3s runs comfortably on 16 GB alongside all Erebor services. Remote access via Tailscale (free tier). Year-one budget preserved for cloud experiments or a future migration.

### Decision

**Local machine.**

### Rationale

Cloud compute imposes a background cost on every iteration during prototyping. A local machine eliminates that pressure and provides more RAM for less money. The one-time hardware cost leaves meaningful budget headroom for cloud experimentation once the system is stable.

The local setup is not a dead end. When Kubernetes is introduced (see Decision 3), the manifests developed locally transfer directly to EKS or a cloud-hosted k3s node. Existing CDK and IAM experience make that migration straightforward when the time comes.

Remote access via Tailscale requires no port forwarding, no VPN server, and no cloud infrastructure.

---

## Decision 2: Container Orchestration — Docker Compose

### Options Considered

**Docker Compose.** Declarative multi-container configuration. Single command to bring the full stack up. Scales naturally to additional services — each new module is an entry in `docker-compose.yml`. No learning curve overhead during active feature development.

**k3s (single-node Kubernetes).** Real Kubernetes API and tooling. StatefulSets, PersistentVolumeClaims, Ingress, Helm. Meaningful operational and learning investment before the first service runs.

### Decision

**Docker Compose.** k3s is the explicit next deployment target once the system is working end-to-end (see Deferred Decisions).

### Rationale

Docker Compose keeps iteration fast while the core system is being built. The `docker-compose.yml` expands naturally as new modules are introduced — adding `erebor-signals`, `erebor-risk`, and `erebor-execution` is additive, not structural change.

k3s is deferred, not abandoned. When the system reaches a stable baseline, the migration from Compose to k3s is a contained exercise with a working system as the reference point. The `docker-compose.yml` serves as the specification for the equivalent Kubernetes manifests; it should be kept accurate as services are added.

---

## Decision 3: Inter-Service Messaging — Redis Streams

### Options Considered

**TimescaleDB as shared store.** Services poll the database for new data. Simple, no new infrastructure. Polling latency is incompatible with sub-second signal propagation; it also couples the write and read paths of every consumer to the same table, making backtest isolation impossible without schema gymnastics.

**NATS JetStream.** Lightweight, high-throughput, persistent messaging. Strong Kubernetes story. Adds a non-trivial broker to operate. Consumer groups require careful ack management. Overkill for a single-machine deployment at current volume.

**Redis Streams.** Persistent, ordered, consumer-group-based messaging built into Redis. `XADD` / `XREADGROUP` provide exactly-once delivery semantics per consumer group. Redis is already a common dependency in trading systems and is straightforward to operate. Minimal footprint: a single `redis:7-alpine` container.

### Decision

**Redis Streams.**

### Rationale

Redis Streams provides the persistent, ordered event bus required by the backtest/replay engine without the operational overhead of a dedicated broker. The backtest engine's stream namespace isolation (`erebor:backtest:{run_id}:*` vs `erebor:live:*`) is trivially enforced by key prefix — no broker-level topic partitioning is needed.

The critical architectural invariant it enables: `erebor-signals` reads from a Redis stream and is completely unaware of whether the source is live ingestion or a historical replay. The stream key it reads is the only runtime difference between live and backtest modes.

### Stream Namespace Convention

| Namespace pattern | Used by |
|---|---|
| `erebor:live:l2:{symbol}` | Live L2 events from `erebor-ingest` |
| `erebor:live:signals` | Live signals from `erebor-signals` |
| `erebor:backtest:{run_id}:l2:{symbol}` | Replayed L2 events from `erebor-backtest` |
| `erebor:backtest:{run_id}:signals` | Backtest signals |
| `erebor:backtest:{run_id}:orders` | Simulated fills |
| `erebor:backtest:{run_id}:control` | Run lifecycle events (start, complete, gap, cancel) |

Backtest streams expire 24 hours after run completion. Persisted results (trades, equity curve, metrics) are written to TimescaleDB before expiry.

---

## Decision 4: Dashboard

Covered in [ADR-003: Dashboard](../ADR-003-dashboard/ADR-003-dashboard.md).

---

## Security Posture

- All credentials (TimescaleDB DSN, Binance API key/secret) are sourced from environment variables. Never in source code, logs, or committed configuration files.
- The Binance API key used by `erebor-execution` must be scoped to order placement only — no withdrawal permissions.
- `erebor-execution` is the only service that holds Binance trading credentials. Signal and risk services do not have exchange API access.
- When migrated to a cloud host, PostgreSQL inbound access is restricted by security group to known source IPs only. No unrestricted port 5432.

---

## Deferred Decisions

| Concern | Deferral Rationale |
|---|---|
| k3s migration | Deferred until all services are stable end-to-end; manifests will be derived from the Compose file |
| Cloud migration (AWS) | Deferred until local setup is stable; existing CDK/IAM experience makes this a contained exercise |
| EKS | Control plane costs $73/month; only warranted after k3s experience and budget allows |
| RDS for TimescaleDB | Not justified until cloud migration; local persistence via named Docker volume with periodic S3 backups |
| Redis persistence mode | AOF (`appendonly yes`) is used by default; switching to RDB-only or disabling persistence for the live stream bus is a tuning decision once volume is understood |
| Authentication (dashboard + Grafana) | Both are private by default (Tailscale network); Grafana ships with built-in auth; formal SSO deferred |

---

## Consequences

- The `docker-compose.yml` is the authoritative definition of the full service topology. It must be kept current as modules are added. It is also the source specification for future k3s manifests.
- New modules that read from TimescaleDB connect via the Compose service name (`timescaledb:5432`) and consume the DSN from `TIMESCALE_DSN`.
- New modules that read or write Redis Streams connect via `REDIS_ADDR` (`redis:6379` inside Compose) and `REDIS_PASSWORD`. They must never call `time.Now()` in signal or execution logic — all time is sourced from `event.EventTime`.
- The backtest/replay engine design is specified in `specs/backtest-replay/backtest-replay-spec.md`. The Redis stream namespace convention there is the authoritative reference.
- The ingest machine is the single point of failure for all services. This is accepted at this stage. The bootstrap protocol in ADR-001 handles ingest recovery automatically on restart.
