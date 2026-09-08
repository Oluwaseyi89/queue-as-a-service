//! In-memory queue data structures.
//!
//! [`FifoQueue`] and [`PriorityQueue`] are deliberately separate types
//! rather than two implementations of a shared `Queue` trait: their
//! `enqueue` signatures already differ (priority queues need a
//! [`Priority`] argument, FIFO queues don't), and nothing in this crate
//! yet needs to be generic over "some kind of queue." A trait can be
//! extracted later — from `feature/consumer-groups` onward, say — if and
//! when something genuinely needs to treat both the same way.

mod fifo;
mod priority;

pub use fifo::FifoQueue;
pub use priority::{Priority, PriorityQueue};
