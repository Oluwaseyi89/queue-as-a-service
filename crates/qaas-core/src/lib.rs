//! The QaaS queue engine.
//!
//! This crate owns everything that makes QaaS a queue rather than a
//! network service: the FIFO/priority queue data structures, the
//! write-ahead log that gives them durability, consumer-group delivery
//! semantics, and (later) Raft-based replication for clustering.
//! Deliberately, none of that is network-aware — `qaas-server` is the only
//! crate that binds a socket or speaks a wire protocol. Keeping that
//! boundary means the engine can be exercised directly in unit and
//! property tests, and reused unmodified if we ever want a second
//! transport (e.g. an embedded, in-process mode) alongside the broker.
//!
//! [`queue::FifoQueue`] and [`queue::PriorityQueue`] (`feature/in-memory-queue-core`)
//! are async-safe, in-memory queue data structures with no durability and
//! no delivery semantics. [`wal::Wal`] and the [`queue::PersistentFifoQueue`]
//! / [`queue::PersistentPriorityQueue`] wrappers built on it
//! (`feature/wal-persistence`) add durability: enqueues and dequeues
//! survive a crash, but "dequeue" is still a single, irreversible step —
//! there's no way to get a message back if whoever dequeued it crashes
//! before finishing with it. [`consumer_group::ConsumerGroup`]
//! (`feature/consumer-groups`) replaces that with lease-based delivery
//! (claim/ack/nack, visibility timeouts) for real at-least-once
//! semantics under multiple competing consumers. [`retry::RetryPolicy`]
//! (`feature/retry-and-backoff-policies`) governs how long a nacked or
//! expired message waits before it's claimable again — exponential
//! backoff with jitter, so a struggling downstream dependency doesn't
//! get hammered by every consumer retrying in lockstep — and, via
//! `max_attempts`, how many times it gets to fail before giving up.
//! [`dead_letter::DeadLetterQueue`] (`feature/dead-letter-queue`) is
//! where a message that's given up on lands: a durable, inspectable
//! destination instead of being silently dropped. `ConsumerGroup` owns
//! one alongside its live queue and routes exhausted messages there
//! automatically — see [`ConsumerGroup::dead_letters`],
//! [`ConsumerGroup::reprocess_dead_letter`], and
//! [`ConsumerGroup::purge_dead_letter`].
//!
//! This is also the first thing in this crate to depend on
//! [`qaas_types`]: `ConsumerGroup` reuses `qaas_types::MessageId` for
//! message identity rather than inventing a second ID type, since the
//! whole reason `MessageId` was built as `UUIDv7` (time-ordered) applies
//! just as much here as in [`Envelope`](qaas_types::Envelope). Dead
//! letters similarly reuse `qaas_types::Timestamp`.
//!
//! [`raft`] (`feature/raft-replication`) is a separate concern from
//! everything above: leader election and log replication *across
//! nodes*, using [`openraft`], the backbone Phase 3 builds a
//! self-healing HA cluster on instead of a single point of failure. It
//! is not yet wired to `ConsumerGroup` — see the module's own docs for
//! why that's a deliberate scope cut, and for two placeholders
//! (an in-memory log store, an in-process network transport) it's
//! honest about not being production-ready yet.
//! [`raft::membership`] (`feature/cluster-membership-discovery`) adds
//! config-based discovery on top: a running cluster's node set can grow
//! or shrink by editing what a
//! [`MembershipSource`](raft::MembershipSource) reports, with no
//! process restart and no operator hand-driving `openraft`'s membership
//! API directly.
//!
//! [`circuit_breaker`] (`feature/circuit-breaker-fallback`) is a
//! different kind of resilience than any of the above: not "how does a
//! message survive a crash" but "how does a caller stay available when
//! whatever it depends on doesn't." [`circuit_breaker::CircuitBreaker`]
//! and [`circuit_breaker::HybridGuard`] port global-rate-limiter's 3-state
//! breaker and local-cache fallback pattern (see `CLAUDE.md`'s reference
//! architecture section) as a generic primitive, proven against a real
//! dependency this crate already has — a [`raft`] cluster's leader — in
//! `raft`'s own `circuit_breaker_tests`.
//!
//! `feature/durable-agent-workflows` opens Phase 4 in this crate with
//! [`ConsumerGroup::checkpoint`]: a multi-step agent task (chained LLM
//! calls, a human-in-the-loop pause) can durably save its progress
//! against the message it's currently working on, and see that progress
//! again on a later [`Claim`] — whether that claim follows a crash, an
//! expired lease, or a deliberate `nack` used to release the message for
//! a pause — instead of restarting the whole task from nothing. This
//! branch also fixed a real, pre-existing durability bug it found while
//! implementing checkpoint replay: a dead-lettered message used to
//! resurrect as pending after a restart, because dead-lettering never
//! recorded anything in `ConsumerGroup`'s own WAL — see
//! `consumer_group::WalRecord::DeadLettered`'s docs.
//!
//! [`admission`] (`feature/token-cost-aware-admission`) is this crate's
//! second port of a global-rate-limiter mechanism, after
//! [`circuit_breaker`]: [`admission::AdmissionController`] is the same
//! sliding-window algorithm as `SlidingWindowLimiter`'s actual Go source,
//! denominated in LLM tokens and dollars instead of request counts.
//! Deliberately not wired into `ConsumerGroup` for the same reason
//! `circuit_breaker` isn't — a domain-agnostic queue engine has no
//! business knowing what a token costs. `qaas-server`'s MCP layer is
//! where it's actually consulted.
//!
//! [`semantic`] (`feature/semantic-dedup-routing`) has no rate-limiter
//! ancestor — it's genuinely new here: [`semantic::Embedding`] validates
//! a vector a *caller* computed (this crate has no embedding model of
//! its own, and never will — see the module's own docs for why), and
//! [`semantic::EmbeddingIndex`] finds the closest match to a query
//! embedding by cosine similarity, generic over what's being searched.
//! `qaas-server` uses one instance keyed by queue name for routing an
//! untargeted `enqueue` to the best-matching queue, and one per queue
//! keyed by `MessageId` for collapsing near-duplicate tasks before they
//! ever reach `ConsumerGroup` — same domain-agnostic-core split as
//! `circuit_breaker` and `admission` again.
//!
//! `feature/llm-assisted-dlq-triage` lands back in [`dead_letter`], the
//! module that's been carrying a note pointing at this branch since
//! `feature/dead-letter-queue` first wrote it: [`DeadLetterQueue::annotate`]
//! lets a verdict attach to a dead letter without resolving it — the
//! same "durable, doesn't end this entry's life" shape
//! `ConsumerGroup::checkpoint` already has for a live message's lease —
//! and [`DeadLetter::checkpoint`] finally carries a workflow's
//! last-saved progress onto the dead letter itself, which
//! `feature/durable-agent-workflows` deliberately deferred to here. Both
//! exist for the same reason: a triage verdict is only as good as what
//! it has to go on.
//!
//! `feature/streaming-delivery` is Phase 4's actual closer, and its
//! whole addition is one method pair on [`ConsumerGroup`], not a new
//! module: [`ConsumerGroup::publish_partial_result`] and
//! [`ConsumerGroup::partial_results_since`]. See that method's own docs
//! for why it's ephemeral rather than durable — the one place this
//! phase's running "durable, resumable everything" theme deliberately
//! breaks, and the doc comment explains why that's the right call rather
//! than an inconsistency.

pub mod admission;
pub mod circuit_breaker;
pub mod consumer_group;
pub mod dead_letter;
pub mod queue;
pub mod raft;
pub mod retry;
pub mod semantic;
pub mod wal;

pub use admission::{AdmissionConfig, AdmissionController, AdmissionDecision, UsageSnapshot};
pub use circuit_breaker::{
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError, CircuitState, FallbackCache,
    HybridGuard, HybridOutcome,
};
pub use consumer_group::{Claim, ConsumerGroup, LeaseToken, PartialResultsPoll, StreamChunk};
pub use dead_letter::{DeadLetter, DeadLetterQueue, TriageClassification, TriageVerdict};
pub use queue::{FifoQueue, PersistentFifoQueue, PersistentPriorityQueue, Priority, PriorityQueue};
pub use retry::RetryPolicy;
pub use semantic::{Embedding, EmbeddingIndex, InvalidEmbedding};
pub use wal::Wal;
