//! Raft consensus: leader election and log replication across nodes.
//!
//! This is the consensus engine itself — [`openraft`] wired up with this
//! crate's chosen log store, state machine, and network — not yet the
//! thing that makes `ConsumerGroup` itself replicated. That's a
//! deliberate scope cut, not an oversight: routing every enqueue/ack/nack
//! through a Raft log is a substantial rework of how `ConsumerGroup`
//! persists state, and rushing it into the same branch that's also
//! standing up consensus for the first time would risk getting both
//! wrong at once. What lands here is a solid, independently correct,
//! independently tested foundation — [`queue::PersistentFifoQueue`
//! wrapping `FifoQueue`](crate::queue) is the precedent for this same
//! shape: build the primitive, integrate it later.
//!
//! Two things in here are placeholders for real infrastructure that
//! doesn't exist yet in this project, and are documented as such rather
//! than presented as finished:
//!
//! - [`log_store`] is **in-memory only**. `openraft`'s log storage
//!   contract (arbitrary-index truncation, purge-up-to-index, random
//!   access reads) doesn't fit this crate's [`Wal`](crate::wal::Wal) —
//!   `Wal` is a strictly-append, replay-from-start primitive, and
//!   force-fitting Raft's requirements onto it would mean either
//!   weakening `Wal`'s simple guarantees or building a second log format
//!   that just happens to also be called a WAL. A **durable** Raft log
//!   is real, separate work for whenever this actually gets deployed
//!   multi-process — a real deployment restarting *every* node at once
//!   would lose committed log entries with this implementation, which is
//!   a genuine safety gap, not a cosmetic one.
//! - [`network`] dispatches Raft RPCs **in-process**, between `Raft`
//!   instances living in the same Rust process, not over a real
//!   network. `qaas-server` doesn't have a network listener yet (that's
//!   Phase 4/8 work) — there is nothing today for a real
//!   [`RaftNetwork`](openraft::RaftNetwork) implementation to connect
//!   to. What's tested here — leader election, log replication, a
//!   crashed leader triggering re-election — is the real consensus
//!   algorithm behaving correctly; only the transport connecting nodes
//!   is a stand-in.
//!
//! [`state_machine`] is a small key-value store (`Set`/apply, read the
//! current value), the same shape `openraft`'s own reference examples
//! use — deliberately not `ConsumerGroup`'s actual operations, for the
//! same reason the log store and network are placeholders: proving the
//! consensus plumbing works with a simple, obviously-correct state
//! machine first, before mapping this crate's real domain onto it.

use std::io::Cursor;

use openraft::declare_raft_types;

pub mod log_store;
pub mod network;
pub mod state_machine;

#[cfg(test)]
mod tests;

pub use log_store::LogStore;
pub use network::{InProcessNetwork, InProcessNetworkHub};
pub use state_machine::{Request, Response, StateMachineStore};

/// This cluster's node identifier type. A plain integer is enough for
/// now — a real deployment would use it as a key into a configuration
/// table mapping id to network address, which is exactly what
/// [`openraft::BasicNode`] (this config's `Node` type) exists to carry.
pub type NodeId = u64;

declare_raft_types!(
    /// The concrete type parameterization of every `openraft` generic
    /// used in this crate. See the module docs for what's real here
    /// (the consensus algorithm) versus a placeholder (the log store's
    /// durability, the network's transport).
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = NodeId,
        Node = openraft::BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

/// A running Raft node for this crate's [`TypeConfig`]: the consensus
/// engine, parameterized with this module's log store, state machine,
/// and network implementations.
pub type Raft = openraft::Raft<TypeConfig>;
