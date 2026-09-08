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

pub mod consumer_group;
pub mod dead_letter;
pub mod queue;
pub mod retry;
pub mod wal;

pub use consumer_group::{Claim, ConsumerGroup, LeaseToken};
pub use dead_letter::{DeadLetter, DeadLetterQueue};
pub use queue::{FifoQueue, PersistentFifoQueue, PersistentPriorityQueue, Priority, PriorityQueue};
pub use retry::RetryPolicy;
pub use wal::Wal;
