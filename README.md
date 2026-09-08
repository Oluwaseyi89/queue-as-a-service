# queue-as-a-service
Queue-as-a-Service (QaaS) – High-performance, distributed message queue with persistence, dead-letter handling, and at-least-once delivery. Built in Rust for memory safety and speed. Features FIFO/priority queues, consumer groups, retry policies, monitoring dashboard, and HA Docker cluster for microservices needing reliable async communication.

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
[`deny.toml`](deny.toml) once you `cargo install cargo-deny` locally — CI
wiring for all of the above lands in `feature/cicd-pipeline`.
