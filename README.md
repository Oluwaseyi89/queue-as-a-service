# queue-as-a-service

[![CI](https://github.com/Oluwaseyi89/queue-as-a-service/actions/workflows/ci.yml/badge.svg)](https://github.com/Oluwaseyi89/queue-as-a-service/actions/workflows/ci.yml)

Queue-as-a-Service (QaaS) — an MCP-native, agent-first message queue built in Rust. Durable, resumable agent workflows, token/cost-aware admission control, semantic dedup, and LLM-assisted DLQ triage, on top of HA Raft clustering, FIFO/priority queues, and consumer groups. API-key/JWT auth with per-tenant isolation and quotas, Prometheus metrics and OpenTelemetry tracing, a real-time operator dashboard, and structured audit logging round out the production picture. Built for the AI agent era, not just microservices.

## Workspace Layout

This is a Cargo workspace, not a single crate — each crate has one job so
later phases (persistence, clustering, MCP integration) extend a clean
boundary instead of a monolith:

| Crate | Kind | Responsibility |
|---|---|---|
| [`crates/qaas-types`](crates/qaas-types) | lib | Shared wire types: message envelope, identifiers, error types. Depended on by everything else; depends on nothing else in the workspace. |
| [`crates/qaas-core`](crates/qaas-core) | lib | The queue engine itself — FIFO/priority queues, WAL persistence, delivery semantics, clustering. No networking. |
| [`crates/qaas-server`](crates/qaas-server) | bin | The broker process: wires `qaas-core` up to a network-facing API (gRPC/HTTP/MCP). |
| [`crates/qaas-client`](crates/qaas-client) | lib | Rust client SDK for producers/consumers talking to a running broker. |

### Building

```bash
cargo build --workspace
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

`cargo deny check` additionally enforces the license/advisory policy in
[`deny.toml`](deny.toml) — install it locally with `cargo install
cargo-deny`. All four checks above, plus `cargo-deny`, run in
[CI](.github/workflows/ci.yml) on every push and pull request against
`main`.

### Local Dev Stack

A [`justfile`](justfile) wraps the [Docker Compose](https://docs.docker.com/compose/)
dev stack ([`docker/docker-compose.yml`](docker/docker-compose.yml)) —
one `qaas-server` node, Prometheus, and Grafana — so bringing it up is one
command instead of remembering flags and file paths. Install
[`just`](https://github.com/casey/just) (`cargo install just`, or via your
package manager), then from the repo root:

```bash
just up             # build + start the stack in the foreground
just up-detached    # same, but detached
just logs           # tail logs from every service
just down           # stop the stack (keeps Prometheus/Grafana data)
just clean          # stop the stack and delete its volumes too
just check          # run the same checks CI runs
just --list         # see every recipe
```

Once it's up:

| Service | Address | Notes |
|---|---|---|
| Grafana | http://localhost:3000 | Anonymous viewer access, no login — dev stack only, never do this in the production/HA stack. Prometheus is pre-provisioned as its data source; no dashboard JSON is provisioned yet (see below). |
| Prometheus | http://localhost:9090 | Scrapes itself and the `qaas-server` target below. |
| `qaas-server` MCP (HTTP) | http://localhost:8080/mcp | The streamable-HTTP MCP transport — requires an `Authorization: Bearer` API key or JWT (`feature/api-auth`). Mint a key via the stdio-only `create_api_key` tool first: `docker compose -f docker/docker-compose.yml attach queue-node` (after `just up-detached`) attaches to the container's own stdin, which *is* the stdio MCP transport — not `docker compose exec`, which starts an unrelated new process instead. |
| `qaas-server` operator dashboard | http://localhost:9091 | Real-time queue depth, DLQ, token/cost spend, and a tenant leaderboard — unauthenticated by design, the same "operator-only, not a tenant-facing surface" reasoning as the metrics port below. |
| `qaas-server` metrics | *(container-internal only)* | `QAAS_METRICS_ADDR` binds `0.0.0.0:9090` inside the `queue-node` container so the `prometheus` service can scrape it by Docker DNS name, but isn't published to the host — port 9090 is already `prometheus`'s own. Curl it from inside the container, or via Prometheus's own UI/API, not directly from the host. |

Both API-key/JWT auth and Prometheus metrics landed after this dev stack
was first scaffolded (`feature/api-auth`, `feature/tracing-metrics`) —
the table above reflects what's actually running now, not the original
placeholders. Grafana's dashboard-provisioning directory
(`docker/grafana/provisioning/dashboards`) is still genuinely empty:
`feature/monitoring-dashboard` built the queue-depth/DLQ/token-spend/
top-agents dashboard as qaas-server's own standalone page (the operator
dashboard row above) rather than a Grafana panel, so there's nothing to
provision there yet unless someone wants a second, Grafana-native view
over the same `/metrics` data.
