//! Exhaustive interleaving check for
//! [`ConsumerGroup::claim`](qaas_core::ConsumerGroup::claim)'s
//! lease-granting critical section.
//!
//! See `loom_fifo.rs`'s own module docs for why this models the critical
//! section rather than driving the real, `tokio::sync::Mutex`-based
//! `ConsumerGroup` type directly — the same reasoning applies here
//! unchanged. The one thing specific to `claim` worth modeling at all:
//! it allocates a [`LeaseToken`](qaas_core) via
//! `next_lease_token.fetch_add` *while already holding* the state lock
//! (see `claim`'s own source — the `fetch_add` sits between the lock being
//! taken and the resulting lease being inserted), which is exactly the
//! kind of "is this atomic's ordering actually doing anything, or is the
//! mutex already providing all the ordering that matters" question loom
//! is suited to settle by exhaustive search rather than by inspection.
//!
//! `ClaimCore` below mirrors that shape with one lock guarding both the
//! pending queue and the leased map together (matching `ConsumerGroup`'s
//! single `state: Mutex<State<T>>`, not two separate locks — modeling it
//! as two locks would test a *different*, more concurrent structure than
//! the one that actually exists, which would be dishonest about what
//! this file is verifying).
//!
//! Gated behind `--cfg qaas_loom`, not plain `--cfg loom` — see
//! `loom_fifo.rs`'s own module docs for why: tokio's own source reacts
//! to the bare name and a global `RUSTFLAGS` applies it everywhere,
//! tokio included, breaking the real tokio this crate otherwise depends
//! on. Run deliberately, the same way as `loom_fifo.rs`:
//!
//! ```text
//! RUSTFLAGS="--cfg qaas_loom" cargo test --release --test loom_consumer_group
//! ```

#![cfg(qaas_loom)]

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use loom::sync::Mutex;
use loom::sync::atomic::{AtomicU64, Ordering};

struct ModelState {
    pending: VecDeque<u64>,
    /// id -> token, standing in for `ConsumerGroup::State::leased`'s
    /// `HashMap<MessageId, Leased<T>>` — only the token matters for what
    /// this model checks, so nothing else `Leased` carries is worth
    /// reproducing here.
    leased: HashMap<u64, u64>,
}

struct ClaimCore {
    state: Mutex<ModelState>,
    next_token: AtomicU64,
}

impl ClaimCore {
    fn new(pending_ids: Vec<u64>) -> Self {
        Self {
            state: Mutex::new(ModelState {
                pending: pending_ids.into_iter().collect(),
                leased: HashMap::new(),
            }),
            next_token: AtomicU64::new(0),
        }
    }

    /// Mirrors `ConsumerGroup::claim`'s own sequence exactly: lock
    /// `state`, pop the next pending id, mint a token via `fetch_add`
    /// *before* dropping the lock, insert the new lease, return both.
    /// `Ordering::Relaxed` here matches the real code's own choice —
    /// correct precisely because the mutex already orders every access,
    /// which is the claim this test exists to check under every
    /// interleaving, not just the ones a few manual runs happen to hit.
    fn try_claim(&self) -> Option<(u64, u64)> {
        let mut state = self.state.lock().unwrap();
        let id = state.pending.pop_front()?;
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        state.leased.insert(id, token);
        Some((id, token))
    }
}

/// Two concurrent claimers against two pending ids: every id must be
/// claimed by exactly one of them, and the two tokens minted must never
/// collide — both would be real, serious bugs (a duplicate delivery, or
/// two leases a caller can't tell apart) if the mutex around `state`
/// ever let two claims interleave rather than strictly serialize.
#[test]
fn concurrent_claims_never_double_assign_an_id_or_a_token() {
    loom::model(|| {
        let core = Arc::new(ClaimCore::new(vec![100, 200]));

        let claimers: Vec<_> = (0..2)
            .map(|_| {
                let core = Arc::clone(&core);
                loom::thread::spawn(move || core.try_claim())
            })
            .collect();

        let results: Vec<(u64, u64)> = claimers
            .into_iter()
            .map(|claimer| {
                claimer.join().unwrap().expect("exactly as many ids as claimers were seeded")
            })
            .collect();

        let mut ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![100, 200], "each pending id must be claimed exactly once");

        assert_ne!(
            results[0].1, results[1].1,
            "two concurrent claims must never mint the same lease token"
        );
    });
}
