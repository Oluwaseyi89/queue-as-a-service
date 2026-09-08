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
//! survive a crash. Still missing, deliberately: any idea what a
//! "message envelope" looks like (`feature/message-schema-versioning`),
//! and real delivery semantics — ack/nack, redelivery, visibility
//! timeout (`feature/consumer-groups`). Both queue types stay generic
//! over an arbitrary payload type throughout, so none of this needs to
//! wait on the wire schema to be useful and testable on its own.

pub mod queue;
pub mod wal;

pub use queue::{FifoQueue, PersistentFifoQueue, PersistentPriorityQueue, Priority, PriorityQueue};
pub use wal::Wal;
