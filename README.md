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
