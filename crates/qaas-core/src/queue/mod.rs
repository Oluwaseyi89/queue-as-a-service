//! Queue data structures: in-memory, and WAL-backed durable wrappers
//! around them.
//!
//! [`FifoQueue`] and [`PriorityQueue`] are deliberately separate types
//! rather than two implementations of a shared `Queue` trait: their
//! `enqueue` signatures already differ (priority queues need a
//! [`Priority`] argument, FIFO queues don't), and nothing in this crate
//! yet needs to be generic over "some kind of queue." A trait can be
//! extracted later — from `feature/consumer-groups` onward, say — if and
//! when something genuinely needs to treat both the same way.
//!
//! [`PersistentFifoQueue`] and [`PersistentPriorityQueue`] follow the
//! same reasoning: wrappers around the in-memory types, not trait impls
//! of them, since their `enqueue`/`dequeue` methods return `io::Result`
//! (a WAL write can fail) where the in-memory versions never fail at
//! all — a real, honest API difference, not one worth papering over with
//! a shared abstraction.

mod fifo;
mod persistent_fifo;
mod persistent_priority;
mod priority;

pub use fifo::FifoQueue;
pub use persistent_fifo::PersistentFifoQueue;
pub use persistent_priority::PersistentPriorityQueue;
pub use priority::{Priority, PriorityQueue};
