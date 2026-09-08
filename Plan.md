# Queue-as-a-Service (QaaS) — Build Plan

## Vision

A distributed, persistent message queue built in Rust — but not a generic
"Kafka-lite." The bet is that by 2026+, the primary *producers and consumers*
of task queues are AI agents, not just microservices. QaaS is designed
agent-native from day one: it speaks MCP directly, treats a "message" as a
potentially long-running, resumable unit of agent work rather than a fire-
and-forget blob, and enforces admission control on token/cost budgets, not
just request counts. Everything else — FIFO/priority queues, consumer
groups, retry policies, DLQ, HA clustering — is table stakes needed to
support that bet in production.

## What Makes This Innovative for the Agent Era

- **MCP-native, not MCP-adapted** — agents enqueue/claim/ack work as a tool
  call against the queue's own MCP server, no bridge process required.
- **Durable agent workflows** — a task can pause for a human, a tool result,
  or a follow-up LLM call and resume exactly where it left off after a
  consumer crash, without a separate workflow engine bolted on.
- **Token/cost-aware backpressure** — admission control throttles on live
  LLM provider budgets, not just req/sec, directly extending the lineage of
  [global-rate-limiter](https://github.com/Oluwaseyi89/global-rate-limiter)
  from request quotas into token economics.
- **Semantic dedup & routing** — embedding similarity collapses redundant
  agent tasks and routes work to the right specialized consumer.
- **Self-healing DLQ** — an LLM triage step classifies dead-lettered work
  (transient vs. permanent, retryable vs. needs-human) instead of leaving it
  for a human to manually inspect.

## Patterns Carried Over from global-rate-limiter

That project's production discipline is the bar for this one, ported to Rust:
- HA cluster behind a load balancer, 3-state circuit breaker + local fallback
  when a dependency is degraded.
- Race-condition tests as a first-class, separately tracked branch — not an
  afterthought.
- Async, non-blocking audit logging via a worker pool so the hot path never
  waits on I/O.
- A real-time analytics dashboard with percentile latency and top-consumer
  breakdowns, not just raw logs.
- Docker Compose HA stack + scripted scale/failover testing as the standard
  way to prove availability claims locally before they reach production.
- One feature per branch, named `feature/<kebab-case-description>`, merged
  via PR — this Plan.md is organized the same way so each row below maps
  1:1 to a branch.

## Branch Naming Convention

`feature/<kebab-case-description>`, branched from `main`, one coherent
capability per branch. Phases below are the suggested build order — later
phases depend on earlier ones (e.g., HA clustering needs the WAL; the DLQ
triage agent needs the DLQ).

---

## Phase 0 — Foundation & Tooling

- **`feature/cargo-workspace-scaffolding`** — Set up the Cargo workspace
  with crates for the core engine, server, client SDK, and shared types,
  plus rustfmt/clippy/cargo-deny config. Establishes clean module
  boundaries so every later phase builds on a workspace, not a monolith.
- **`feature/cicd-pipeline`** — Add GitHub Actions for build, test, clippy,
  fmt-check, and cargo-deny on every PR. Mirrors the CI discipline from
  global-rate-limiter so broken code never lands on `main`.
- **`feature/dev-environment-docker`** — Add a docker-compose dev stack
  (queue node + Prometheus + Grafana) and a justfile/Makefile for
  one-command local bring-up. Removes manual setup friction for
  contributors and for CI.

## Phase 1 — Core Queue Engine

- **`feature/in-memory-queue-core`** — Implement the core FIFO and priority
  queue data structures with async-safe enqueue/dequeue APIs on tokio. This
  is the foundational engine every later feature builds on top of.
- **`feature/wal-persistence`** — Add a write-ahead log (sled or a custom
  append-only log) so queue state survives restarts and crashes. Durability
  has to exist before any HA or replication work begins.
- **`feature/message-schema-versioning`** — Define the wire format/message
  envelope (headers, metadata, idempotency keys, trace IDs) with version
  negotiation. Locks in a stable contract that clients, SDKs, and the
  dashboard can all depend on without breaking each other.

## Phase 2 — Delivery Semantics & Reliability

- **`feature/consumer-groups`** — Implement consumer group semantics
  (partition/lease assignment, visibility timeout, ack/nack) for
  at-least-once delivery. Lets multiple competing consumers share work
  without double-processing.
- **`feature/retry-and-backoff-policies`** — Add configurable per-queue
  retry policies with exponential backoff and jitter. Prevents thundering-
  herd retries against downstream services and LLM providers.
- **`feature/dead-letter-queue`** — Route exhausted-retry messages to a DLQ
  with full failure metadata attached. Gives operators, and later an LLM
  triage agent, a place to inspect and reprocess failures instead of
  silently dropping them.
- **`feature/idempotent-delivery`** — Add idempotency-key deduplication on
  both the producer and consumer side. Critical for agent workloads, where a
  retried LLM tool-call must never be billed or executed twice.

## Phase 3 — Distribution & High Availability

- **`feature/raft-replication`** — Integrate a Raft implementation (e.g.
  openraft) for leader election and log replication across nodes. Forms the
  backbone of a self-healing HA cluster instead of a single point of
  failure.
- **`feature/cluster-membership-discovery`** — Add gossip- or config-based
  node discovery with dynamic membership changes. Lets the cluster scale
  nodes up/down without manual reconfiguration, echoing the "scale
  instances" workflow from the rate limiter.
- **`feature/circuit-breaker-fallback`** — Port the 3-state circuit breaker
  + local-cache hybrid fallback pattern so producers degrade gracefully
  during partial outages. Keeps the service available even when a replica
  or downstream dependency is unhealthy.

## Phase 4 — AI-Agent-Native Innovation (the differentiating phase)

- **`feature/mcp-server-interface`** — Expose the queue as a native MCP
  server so any AI agent can enqueue, claim, and ack work as a first-class
  tool call with no bridge process. This is the core 2026+ bet: the queue
  speaks the protocol agents already use to talk to tools.
- **`feature/durable-agent-workflows`** — Add checkpointed, resumable task
  state so a multi-step or multi-hour agent workflow (chained LLM calls,
  human-in-the-loop pauses) survives consumer crashes and resumes exactly
  where it left off. Brings durable-execution semantics to agent
  orchestration without bolting on a separate workflow engine.
- **`feature/token-cost-aware-admission`** — Add admission control that
  throttles enqueue/dequeue based on live LLM provider token budgets and
  cost ceilings, not just request counts. Extends the rate-limiter lineage
  into token economics, stopping agents from blowing through API budgets.
- **`feature/semantic-dedup-routing`** — Use embedding similarity to detect
  and collapse near-duplicate agent tasks before they're enqueued, and to
  route tasks to the right specialized consumer. Cuts redundant LLM spend
  when multiple agents independently queue overlapping work.
- **`feature/llm-assisted-dlq-triage`** — Have a small triage agent classify
  DLQ failures (transient vs. permanent, retryable vs. needs-human) and
  auto-apply the matching policy. Turns the dead-letter queue from a
  graveyard into a self-healing feedback loop.
- **`feature/streaming-delivery`** — Support SSE/WebSocket delivery of
  partial results for long-running streaming agent responses, not just
  final payloads. Matches how agents actually consume LLM output —
  incrementally, not as one blocking response.

## Phase 5 — Security & Multi-Tenancy

- **`feature/api-auth`** — Add API-key and JWT-based authentication with
  per-tenant scoping. Establishes the trust boundary needed before exposing
  the service to multiple teams or external agents.
- **`feature/multi-tenant-quotas`** — Add per-tenant queue limits, rate
  quotas, and resource isolation. Prevents one noisy tenant, or a runaway
  agent loop, from starving the rest of the cluster.

## Phase 6 — Observability & Dashboard

- **`feature/tracing-metrics`** — Instrument the service with OpenTelemetry
  tracing and Prometheus metrics (queue depth, latency percentiles,
  consumer lag). Gives operators the same P95/P99 visibility the rate
  limiter had, now broken down per-queue and per-agent.
- **`feature/monitoring-dashboard`** — Build a real-time web dashboard
  (queue depth, DLQ trends, token spend, top agents) analogous to the rate
  limiter's analytics API. Makes agent workload behavior visible without
  grepping logs.
- **`feature/structured-audit-logging`** — Add async, non-blocking audit
  logging with a worker pool, mirroring the rate limiter's logger design.
  Keeps a durable record of every enqueue/ack/nack for compliance and
  debugging without hurting hot-path latency.

## Phase 7 — Testing & Performance

- **`feature/race-condition-tests`** — Add `cargo test` plus loom/miri-based
  concurrency tests targeting the queue core and consumer-group locking
  paths. Catches data races before production, the same discipline the
  rate limiter's dedicated race-condition-tests branch enforced.
- **`feature/load-performance-benchmarks`** — Add k6 (or Rust-native goose)
  load tests and criterion micro-benchmarks to establish throughput/latency
  baselines. Prevents silent performance regressions as features are added.
- **`feature/chaos-failover-tests`** — Add chaos tests that kill nodes,
  partition the network, and drop dependency connections to validate the
  circuit breaker and Raft failover paths. Proves HA claims under real
  failure conditions instead of just in theory.

## Phase 8 — Deployment & Production

- **`feature/docker-ha-cluster`** — Add multi-stage Dockerfiles and a
  docker-compose HA stack (N nodes + load balancer), mirroring the rate
  limiter's production setup. Gives a one-command way to run a realistic HA
  cluster locally or in CI.
- **`feature/kubernetes-helm-charts`** — Add Helm charts/k8s manifests with
  liveness/readiness probes, HPA, and PodDisruptionBudgets. Makes the
  service deployable on real production clusters, not just docker-compose.
- **`feature/production-hardening`** — Add graceful shutdown, connection
  draining, resource limits, and security hardening (TLS, secrets
  management). Closes the gap between "works in dev" and "safe to run in
  production."

## Phase 9 — SDKs, Docs & Launch

- **`feature/client-sdks`** — Build Rust, Python, and TypeScript client
  SDKs plus the MCP client config, so agents and services in any language
  can integrate quickly. Lowers the adoption barrier the same way a clean
  API contract did for the rate limiter.
- **`feature/docs-and-examples`** — Write architecture docs, API reference,
  and runnable example agents (LangGraph/AutoGen-style) demonstrating the
  MCP integration. Turns the innovative pieces into something adopters can
  copy and run immediately.
- **`feature/v1-production-launch`** — Final integration pass, versioned
  release, changelog, and production-readiness review before tagging v1.0.
  The culmination branch that merges everything into a deployable,
  documented, production-grade release.

---

## Suggested Sequencing Notes

1. Phases 0–2 are non-negotiable groundwork — nothing else is testable
   without a durable, single-node queue with real delivery semantics.
2. Phase 3 (HA) and Phase 4 (agent-native) can run in parallel once Phase 2
   lands, since they touch different layers (replication vs. protocol
   surface) — but merge Phase 3 first if only one can go at a time, since
   Phase 4's durable workflows want to survive node failure, not just
   process crashes.
3. Phase 4 is the differentiator and the reason this project exists — don't
   let it slip to "if we have time." Everything in Phases 5–9 exists to make
   Phase 4's ideas safe and observable in production, not to replace them.
