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
//! [`queue::FifoQueue`] and [`queue::PriorityQueue`] are the first thing
//! implemented here (`feature/in-memory-queue-core`): async-safe,
//! in-memory queue data structures with no durability, no delivery
//! semantics (no ack/nack, no redelivery), and no idea what a "message
//! envelope" looks like — those are `feature/wal-persistence`,
//! `feature/consumer-groups`, and `feature/message-schema-versioning`
//! respectively. Both queue types are generic over an arbitrary payload
//! type so they don't need to wait on the wire schema to be useful and
//! testable on their own.

pub mod queue;

pub use queue::{FifoQueue, Priority, PriorityQueue};
