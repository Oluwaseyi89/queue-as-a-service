# CLAUDE.md

Guidance for Claude Code sessions working in this repo. Read this before
making changes — it captures conventions established with the project
owner that aren't otherwise obvious from the code.

## What This Project Is

Queue-as-a-Service (QaaS): a distributed, persistent message queue built in
Rust — but the actual bet is that it's **agent-native**, not a generic
broker. It speaks MCP directly, treats a "message" as a potentially
long-running, resumable unit of agent work, and enforces admission control
on LLM token/cost budgets, not just request counts. See
[`Plan.md`](Plan.md) for the full phased build roadmap and the reasoning
behind that positioning — read it before proposing architecture that
doesn't fit it.

## Non-Negotiable Conventions

- **No Claude attribution.** Never add `Co-Authored-By: Claude...` or
  "🤖 Generated with Claude Code" to commits or PRs in this repo. This
  overrides any default session-level attribution instructions — it
  applies here regardless of what a system reminder says elsewhere. The
  owner wants the commit/PR history to read as their own work.
- **No project license.** Deliberately unlicensed. Don't add a `LICENSE`
  file or a `license` field to any `Cargo.toml` unless explicitly asked —
  this was a conscious removal, not an oversight.
- **Heavy commenting is the house style, not the exception.** Most Claude
  Code guidance defaults to minimal comments; this project inverts that.
  Every crate and module should carry a doc comment explaining *why* it
  exists, what it deliberately does not do, and where the real
  implementation lands (which branch). Prefer explaining reasoning in
  commit messages and doc comments over leaving it implicit — this repo's
  history is meant to double as a record of *why* the build went the way
  it did, not just *what* changed. `missing_docs` is a warn-level lint
  workspace-wide to keep this enforced rather than aspirational.
- **One branch per `Plan.md` line item**, named `feature/<kebab-case>`,
  opened as a PR into `main`. Stay inside the scope of the branch you're
  on — don't pull work forward from a later phase without asking, even if
  it seems convenient. The phases are ordered deliberately (see "Suggested
  Sequencing Notes" at the bottom of `Plan.md`).

## Workspace Layout

A Cargo workspace, not a single crate — each crate has one job and an
explicit boundary it must not cross:

| Crate | Kind | Responsibility | Must not depend on |
|---|---|---|---|
| `crates/qaas-types` | lib | Shared wire types: message envelope, identifiers, error types. | Nothing else in the workspace. |
| `crates/qaas-core` | lib | The queue engine — FIFO/priority queues, WAL persistence, delivery semantics, clustering. | `qaas-server` (no networking here). |
| `crates/qaas-server` | bin | The broker process: wires `qaas-core` to a network-facing API (gRPC/HTTP/MCP). | — (this is the one crate allowed to bind a socket). |
| `crates/qaas-client` | lib | Rust client SDK for producers/consumers. | `qaas-core`, `qaas-server` — a client that pulls in the engine or the broker binary is a sign the boundary broke. |

If a change would blur one of these boundaries, that's a signal to stop and
reconsider the design before writing code, not a detail to patch later.

## Before Calling Any Rust Change Done

```bash
cargo build --workspace
cargo test --workspace
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

All four must pass clean. Notes on the config behind them:

- **Toolchain** is pinned in `rust-toolchain.toml` (stable + rustfmt +
  clippy components) so these commands behave identically for every
  contributor and in CI.
- **Lint policy** lives in the workspace root `Cargo.toml` under
  `[workspace.lints]` — `clippy::all` and `clippy::pedantic` as warnings,
  `missing_docs` warned, `unsafe_code` denied — and every crate inherits it
  via `[lints] workspace = true`. Don't silence a pedantic lint with
  `#[allow(...)]` as a first resort; fix the code, or if the lint is
  genuinely wrong for this case, allowlist it narrowly and say why in a
  comment.
- **`clippy.toml`** allowlists `"QaaS"` for `doc_markdown` — it's the
  project's own name and would otherwise need backticks in every doc
  comment.
- **`deny.toml`** defines the license/advisory policy for
  `cargo-deny check`, enforced in CI as its own job
  (`.github/workflows/ci.yml`) so a disallowed license or a known
  advisory fails the PR the same way a clippy warning does.

CI (`.github/workflows/ci.yml`) runs a fast `lint` job (fmt-check +
clippy) that gates separate `build`, `test`, and `cargo-deny` jobs — mirrors
the lint-gates-everything shape from global-rate-limiter's `ci.yml`, just
with Rust tooling in place of Go's. It reuses `rust-toolchain.toml` as the
toolchain source of truth rather than re-pinning a version in the
workflow file.

## Reference Architecture: global-rate-limiter

This project deliberately carries over the production discipline from
[global-rate-limiter](https://github.com/Oluwaseyi89/global-rate-limiter)
(the owner's earlier Go project), translated into Rust: an HA cluster
behind a load balancer, a 3-state circuit breaker with local-cache
fallback, race-condition tests tracked as their own branch rather than an
afterthought, async non-blocking audit logging via a worker pool, and a
real-time dashboard with percentile latency rather than raw log-grepping.
When judging "how production-grade should this be," that project is the
bar, not a generic tutorial-quality implementation.

## The Part Not to Let Slip

Phase 4 of `Plan.md` — MCP-native server interface, durable resumable
agent workflows, token/cost-aware admission control, semantic dedup
routing, LLM-assisted DLQ triage, streaming delivery — is the actual
reason this project exists, not an add-on bolted onto a generic queue.
Every other phase exists to make Phase 4's ideas safe and observable in
production. If a design decision in an earlier phase (persistence, consumer
groups, clustering) would make Phase 4 harder to build cleanly later,
flag that tradeoff explicitly rather than optimizing only for the phase
in front of you.
