# queue-as-a-service

[![CI](https://github.com/Oluwaseyi89/queue-as-a-service/actions/workflows/ci.yml/badge.svg)](https://github.com/Oluwaseyi89/queue-as-a-service/actions/workflows/ci.yml)

Queue-as-a-Service (QaaS) — an MCP-native, agent-first message queue built in Rust. Durable, resumable agent workflows, token/cost-aware admission control, semantic dedup, and LLM-assisted DLQ triage, on top of HA Raft clustering, FIFO/priority queues, and consumer groups. Built for the AI agent era, not just microservices.

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
| Grafana | http://localhost:3000 | Anonymous viewer access, no login — dev stack only, never do this in the production/HA stack. Prometheus is pre-provisioned as its data source. |
| Prometheus | http://localhost:9090 | Scrapes itself and a `qaas-server` target. |
| `qaas-server` | *(no port yet)* | Runs and logs, but doesn't bind a socket until a network API lands — nothing to curl yet. |

The `qaas-server` Prometheus target will show as `down` — that's expected,
not a bug: the `/metrics` endpoint doesn't exist until
`feature/tracing-metrics` (Phase 6) lands. The scrape config and Grafana
datasource are wired up now so that branch only has to add the endpoint,
not build the observability plumbing from scratch.
