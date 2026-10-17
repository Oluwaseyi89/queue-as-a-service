//! Exhaustive interleaving check for [`FifoQueue`](qaas_core::FifoQueue)'s
//! bounded push/pop critical section.
//!
//! This does **not** drive the real `FifoQueue` type. `loom` works by
//! swapping `std::sync`/`std::thread` for its own instrumented versions
//! under a cfg flag and exhaustively scheduling every legal
//! interleaving of them — but it has no concept of `async`/`.await` at
//! all, and `FifoQueue` is built on `tokio::sync::Mutex`/`Notify`, whose
//! locking depends on a real async executor polling futures, not on
//! `std::thread`. There is no async-aware equivalent loom ships to swap
//! those for. (This isn't a limitation specific to this crate — it's why
//! `tokio` itself keeps its own loom tests scoped to its internal
//! synchronization primitives rather than arbitrary async business logic
//! built on top of them.)
//!
//! What *is* loom-testable, faithfully, is the actual critical section
//! `FifoQueue::try_enqueue`/`try_dequeue` run while holding their lock —
//! lock, check capacity, `push_back`/`pop_front`, release — because that
//! section is itself fully synchronous: this crate's own rule (see
//! `consumer_group.rs`'s module docs) is to never hold a state lock
//! across an `.await`, and `FifoQueue` already follows it. `BoundedCore`
//! below is that exact sequence, reimplemented against `loom::sync`
//! instead of `tokio::sync` so loom can drive it — a deliberate, narrow
//! model of the locking logic, not a trimmed-down copy of the public
//! type (its fields are private to `fifo.rs` regardless, so nothing
//! outside that module could reuse them even if the async/sync mismatch
//! didn't already rule it out).
//!
//! Given how simple this critical section already is (one lock, no
//! nested locking, no cross-structure invariant), loom isn't expected to
//! turn up anything a handful of `cargo test` runs with real contention
//! wouldn't eventually turn up too — the value here is the *exhaustive*
//! guarantee (every legal interleaving, not just the ones that happened
//! to get scheduled), not bug-hunting in code that's already this
//! small.
//!
//! Gated behind `--cfg qaas_loom`, deliberately *not* plain `--cfg
//! loom` — `RUSTFLAGS` applies to every crate `rustc` compiles in that
//! invocation, tokio included, and tokio's own source checks bare
//! `cfg(loom)` in several places (`runtime/driver.rs`, `runtime/park.rs`,
//! `runtime/builder.rs`, among others) to swap in *its own* loom-
//! instrumented internals for *its own* test suite. Setting plain
//! `--cfg loom` here doesn't just gate this file — it also flips real,
//! production tokio (which the rest of this crate unconditionally
//! depends on, `tokio::fs` included) into a reduced mode never meant to
//! be combined with actually using tokio, and the build breaks in ways
//! that have nothing to do with this file at all. Confirmed by hitting
//! exactly that break first, not by reading tokio's source up front —
//! `qaas_loom` is a name tokio never checks for, so this crate's own
//! gate can't collide with it.
//!
//! Compiled only with that flag set — building under the normal
//! `stable` toolchain with no flag (what `cargo test --workspace` and CI
//! both do) sees an empty test binary here, not a missing dependency
//! error, since `loom` itself is still a normal dev-dependency either
//! way. Run it deliberately:
//!
//! ```text
//! RUSTFLAGS="--cfg qaas_loom" cargo test --release --test loom_fifo
//! ```
//!
//! `--release` matters more here than almost anywhere else in this
//! workspace: loom's exhaustive search is slow enough in debug builds
//! that this can go from seconds to minutes for the same two tiny models
//! below.

#![cfg(qaas_loom)]

use std::collections::VecDeque;
use std::sync::Arc;

use loom::sync::Mutex;

/// A fixed-capacity slot, matching [`FifoQueue::try_enqueue`]/
/// `try_dequeue`'s critical section exactly: lock, check capacity,
/// `push_back`/`pop_front`, release.
struct BoundedCore<T> {
    items: Mutex<VecDeque<T>>,
    capacity: usize,
}

impl<T> BoundedCore<T> {
    fn new(capacity: usize) -> Self {
        Self { items: Mutex::new(VecDeque::new()), capacity }
    }

    fn try_push(&self, item: T) -> Result<(), T> {
        let mut guard = self.items.lock().unwrap();
        if guard.len() < self.capacity {
            guard.push_back(item);
            Ok(())
        } else {
            Err(item)
        }
    }

    fn try_pop(&self) -> Option<T> {
        self.items.lock().unwrap().pop_front()
    }
}

/// Two producers racing to push into a slot sized to exactly fit both of
/// them, each pushing once. No retry loop on either side — loom's own
/// docs call this out directly: a thread that spins waiting for another
/// thread's progress blows up the branch count loom has to explore,
/// since "try again" is itself a schedulable step multiplied across
/// every interleaving. `FifoQueue::enqueue`'s real check-then-wait-then-
/// retry loop only ever retries because *capacity* was the blocking
/// condition; sizing this model's capacity to the producer count removes
/// that condition entirely without weakening what's actually being
/// checked — the interesting question was always whether the *lock*
/// serializes concurrent `push_back` calls correctly (no lost or
/// duplicated item), not whether a full queue correctly rejects a push,
/// which is a trivial, non-concurrent branch of `try_push` either way.
/// Draining happens after both producers join, for the same reason: by
/// then there's nothing left to race, so no retry is needed there
/// either.
#[test]
fn two_producers_racing_the_same_lock_never_lose_or_duplicate_an_item() {
    loom::model(|| {
        let core = Arc::new(BoundedCore::new(2));

        let producers: Vec<_> = (0..2)
            .map(|id| {
                let core = Arc::clone(&core);
                loom::thread::spawn(move || {
                    core.try_push(id).expect("capacity matches producer count; this must not fail")
                })
            })
            .collect();

        for producer in producers {
            producer.join().unwrap();
        }

        let mut received = Vec::new();
        while let Some(item) = core.try_pop() {
            received.push(item);
        }

        received.sort_unstable();
        assert_eq!(received, vec![0, 1], "both producers' items must appear, exactly once each");
    });
}
