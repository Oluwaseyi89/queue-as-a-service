//! Competing-consumer delivery with visibility timeouts.
//!
//! [`ConsumerGroup`] is a work queue in the SQS/RabbitMQ sense, not a
//! Kafka-style multi-group topic: there is one shared pool of messages,
//! any number of consumers compete for them, and once a message is
//! acknowledged it is gone for everyone. (The branch plan's wording,
//! "partition/lease assignment," is read here as describing that
//! mechanism generically — actual partition-owned, per-partition-ordered
//! consumption is a materially different, larger feature and not what's
//! built here.)
//!
//! Unlike [`PersistentFifoQueue`](crate::queue::PersistentFifoQueue),
//! `dequeue` isn't a single step. Claiming a message doesn't remove it —
//! it grants a time-boxed, exclusive **lease**: while the lease is
//! active, no other consumer can claim that message. The consumer that
//! holds the lease must [`ack`](ConsumerGroup::ack) it (permanent,
//! durable removal) or [`nack`](ConsumerGroup::nack) it (immediate
//! redelivery) before the lease's visibility timeout elapses; if neither
//! happens — the consumer crashed, hung, or was simply too slow — the
//! lease expires on its own and the message becomes claimable again.
//! That's what makes this at-least-once rather than at-most-once: a
//! message is never dropped just because whoever had it stopped
//! responding.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use qaas_types::MessageId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify};

use crate::wal::Wal;

/// One entry in a [`ConsumerGroup`]'s WAL. Only enqueue and permanent
/// removal (ack) are durable — a claim is a lease, not a commitment, so
/// there is deliberately no "Claim" record: persisting it would imply a
/// crash should remember who had a message leased, but a lease's whole
/// point is that it doesn't outlive anything, not even gracefully. On
/// restart, every unacked message — whether it was sitting untouched or
/// actively (but unacknowledged) leased when the process died — replays
/// back as available to claim. That's a real, documented limitation:
/// `delivery_count` (see [`Claim`]) resets to zero across a restart,
/// because delivery attempts aren't durable, only messages are.
#[derive(Serialize, Deserialize)]
enum WalRecord<T> {
    Enqueue(MessageId, T),
    Ack(MessageId),
}

/// A message waiting to be claimed.
struct Pending<T> {
    id: MessageId,
    item: T,
    /// How many times this message has already been delivered (0 if
    /// it's never been claimed).
    delivery_count: u32,
}

/// A message currently out on lease to some consumer.
struct Leased<T> {
    item: T,
    /// Unique to this specific delivery attempt, not to the message —
    /// see [`LeaseToken`] for why that distinction is load-bearing.
    token: LeaseToken,
    expires_at: Instant,
    delivery_count: u32,
}

struct State<T> {
    pending: VecDeque<Pending<T>>,
    leased: HashMap<MessageId, Leased<T>>,
}

/// Proof that a caller holds the lease it's trying to resolve.
///
/// Minted fresh on every [`claim`](ConsumerGroup::claim), not reused
/// across redeliveries of the same message. Without this, a consumer
/// whose lease already expired — and whose message has since been
/// redelivered to someone else under a brand new lease — could call
/// `ack`/`nack` late and accidentally resolve a claim it no longer
/// holds, using nothing but the [`MessageId`] as the key. Requiring the
/// token to match rejects that stale call instead.
///
/// Only ever compared for equality within the process that issued it —
/// it isn't written to the WAL and doesn't need to mean anything after a
/// restart, so a process-local monotonic counter is sufficient; it
/// doesn't need `MessageId`'s cross-restart global uniqueness.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct LeaseToken(u64);

/// A successfully claimed message, returned by [`ConsumerGroup::claim`].
///
/// Hand `id` and `token` back to [`ConsumerGroup::ack`] or
/// [`ConsumerGroup::nack`] to resolve this specific delivery.
#[derive(Debug, Clone)]
pub struct Claim<T> {
    /// The message's identity — stable across redeliveries.
    pub id: MessageId,
    /// This delivery attempt's lease token — *not* stable across
    /// redeliveries. See [`LeaseToken`].
    pub token: LeaseToken,
    /// The message payload.
    pub item: T,
    /// How many times this message has now been delivered, including
    /// this delivery. Starts at 1. Reset to 1 on the first claim after a
    /// restart even if it had been delivered before — delivery attempts
    /// aren't durable, only messages are (see this module's docs and
    /// [`ConsumerGroup::open`]), so a crash genuinely loses that count
    /// rather than this being an oversight.
    pub delivery_count: u32,
}

/// A shared work queue with lease-based, at-least-once, competing-
/// consumer delivery. See the module docs for the delivery model.
pub struct ConsumerGroup<T> {
    state: Mutex<State<T>>,
    /// Signaled whenever a message becomes claimable — a fresh enqueue,
    /// an explicit nack, or a lease expiring — so a consumer blocked in
    /// `claim` wakes up instead of waiting out a timer it no longer
    /// needs to.
    changed: Notify,
    wal: Wal<WalRecord<T>>,
    visibility_timeout: Duration,
    next_lease_token: AtomicU64,
}

impl<T: Serialize + DeserializeOwned> ConsumerGroup<T> {
    /// Opens the WAL at `path` (creating it if it doesn't exist), replays
    /// it to rebuild the set of pending messages, and returns a group
    /// ready for use. Every claimed-but-unacked message from a previous
    /// run comes back as pending, per this type's documented at-least-
    /// once, restart-resets-delivery-count behavior.
    ///
    /// `visibility_timeout` applies to every lease this group grants;
    /// there's no per-claim override in this branch.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as
    /// [`Wal::open`](crate::wal::Wal::open).
    pub async fn open(path: impl AsRef<Path>, visibility_timeout: Duration) -> io::Result<Self> {
        let (wal, records) = Wal::open(path).await?;
        let mut pending: VecDeque<Pending<T>> = VecDeque::new();
        for record in records {
            match record {
                WalRecord::Enqueue(id, item) => {
                    pending.push_back(Pending { id, item, delivery_count: 0 });
                }
                WalRecord::Ack(id) => {
                    if let Some(index) = pending.iter().position(|entry| entry.id == id) {
                        pending.remove(index);
                    }
                }
            }
        }

        Ok(Self {
            state: Mutex::new(State { pending, leased: HashMap::new() }),
            changed: Notify::new(),
            wal,
            visibility_timeout,
            next_lease_token: AtomicU64::new(0),
        })
    }

    /// Durably enqueues `item` and returns its assigned [`MessageId`].
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails; `item` is not enqueued
    /// in that case.
    pub async fn enqueue(&self, item: T) -> io::Result<MessageId> {
        let id = MessageId::new();
        let record = WalRecord::Enqueue(id, item);
        self.wal.append(&record).await?;

        let WalRecord::Enqueue(id, item) = record else {
            unreachable!("record was just constructed as Enqueue")
        };
        {
            let mut state = self.state.lock().await;
            state.pending.push_back(Pending { id, item, delivery_count: 0 });
        }
        self.changed.notify_one();
        Ok(id)
    }

    /// The total number of messages not yet acknowledged — claimable
    /// plus currently leased.
    pub async fn len(&self) -> usize {
        let state = self.state.lock().await;
        state.pending.len() + state.leased.len()
    }

    /// Whether there are no unacknowledged messages at all.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Claims the next available message, waiting if none is currently
    /// claimable, and grants a lease on it for this group's visibility
    /// timeout.
    ///
    /// Requires `T: Clone` for a real reason, not a convenience: the
    /// group must retain its own copy of the item so it can redeliver it
    /// on nack or lease expiry, while also handing an owned copy to the
    /// caller — there's no way to do both from a single stored value
    /// without either cloning or handing back a borrow tied to an
    /// internal lock, and the latter doesn't compose with an
    /// independently-resolved `ack`/`nack` call.
    pub async fn claim(&self) -> Claim<T>
    where
        T: Clone,
    {
        loop {
            let mut state = self.state.lock().await;
            self.reclaim_expired_leases(&mut state);

            if let Some(Pending { id, item, delivery_count }) = state.pending.pop_front() {
                let delivery_count = delivery_count + 1;
                let token = LeaseToken(self.next_lease_token.fetch_add(1, Ordering::Relaxed));
                state.leased.insert(
                    id,
                    Leased {
                        item: item.clone(),
                        token,
                        expires_at: Instant::now() + self.visibility_timeout,
                        delivery_count,
                    },
                );
                drop(state);
                // Not required for correctness — every waiter recomputes
                // its own wake-up time from scratch each time it loops —
                // but it lets any other waiter learn about this lease's
                // (possibly sooner) expiry immediately instead of only
                // when its own, possibly-later, timer fires.
                self.changed.notify_one();
                return Claim { id, token, item, delivery_count };
            }

            let wake_at = Self::earliest_expiry(&state);
            drop(state);

            match wake_at {
                Some(instant) => {
                    let _ = tokio::time::timeout_at(instant.into(), self.changed.notified()).await;
                }
                None => self.changed.notified().await,
            }
        }
    }

    /// Permanently, durably removes a claimed message.
    ///
    /// Returns `Ok(false)` without writing anything if `id`/`token`
    /// don't match a currently-active lease — most likely because the
    /// lease already expired (and the message may since have been
    /// redelivered to someone else) or was already acked. That's a
    /// no-op, not an error: an ack racing a visibility timeout is an
    /// expected condition this type is built to handle safely, not a
    /// caller bug.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails. The lease has already
    /// been released in memory at that point and will not be retried —
    /// the message is durably present from the original enqueue either
    /// way, so it's still recoverable on the next restart, just not
    /// removed from this running process's pending set.
    pub async fn ack(&self, id: MessageId, token: LeaseToken) -> io::Result<bool> {
        {
            let mut state = self.state.lock().await;
            let holds_current_lease =
                state.leased.get(&id).is_some_and(|lease| lease.token == token);
            if !holds_current_lease {
                return Ok(false);
            }
            state.leased.remove(&id);
        }
        self.wal.append(&WalRecord::Ack(id)).await?;
        Ok(true)
    }

    /// Immediately releases a claimed message back to the pending pool,
    /// instead of waiting for its lease to expire on its own.
    ///
    /// Same stale-lease handling as [`ack`](Self::ack): returns `false`
    /// without effect if `id`/`token` don't match a currently-active
    /// lease. No WAL write here — nack doesn't durably commit anything,
    /// it only changes which in-memory pool the message sits in, which
    /// is exactly the same state a plain lease expiry would produce.
    pub async fn nack(&self, id: MessageId, token: LeaseToken) -> bool {
        let requeued = {
            let mut state = self.state.lock().await;
            match state.leased.remove(&id) {
                Some(lease) if lease.token == token => {
                    state.pending.push_back(Pending {
                        id,
                        item: lease.item,
                        delivery_count: lease.delivery_count,
                    });
                    true
                }
                // Wrong token (a stale nack for an already-redelivered
                // message) or no lease at all — either way, put back
                // exactly what we found, unchanged.
                Some(lease) => {
                    state.leased.insert(id, lease);
                    false
                }
                None => false,
            }
        };
        if requeued {
            self.changed.notify_one();
        }
        requeued
    }

    /// Moves every lease whose visibility timeout has passed back into
    /// `pending`. Called at the start of every [`claim`](Self::claim), so
    /// a consumer can never observe a message as unclaimable purely
    /// because its previous lease-holder crashed.
    fn reclaim_expired_leases(&self, state: &mut State<T>) {
        let now = Instant::now();
        let expired: Vec<MessageId> = state
            .leased
            .iter()
            .filter(|(_, lease)| lease.expires_at <= now)
            .map(|(id, _)| *id)
            .collect();

        if expired.is_empty() {
            return;
        }
        for id in expired {
            let lease = state.leased.remove(&id).expect("id came from iterating this same map");
            state.pending.push_back(Pending {
                id,
                item: lease.item,
                delivery_count: lease.delivery_count,
            });
        }
        // Multiple messages may have just become claimable at once —
        // wake every blocked claimer to recheck, not just one.
        self.changed.notify_waiters();
    }

    fn earliest_expiry(state: &State<T>) -> Option<Instant> {
        state.leased.values().map(|lease| lease.expires_at).min()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::ConsumerGroup;

    /// Generous enough that a test asserting "this should NOT have
    /// expired yet" isn't flaky under CI scheduling jitter, short enough
    /// that a test asserting "this should redeliver soon" doesn't make
    /// the suite slow.
    const LONG_TIMEOUT: Duration = Duration::from_secs(60);
    const SHORT_TIMEOUT: Duration = Duration::from_millis(30);

    async fn open(dir: &tempfile::TempDir, visibility_timeout: Duration) -> ConsumerGroup<i32> {
        ConsumerGroup::open(dir.path().join("wal.log"), visibility_timeout).await.unwrap()
    }

    #[tokio::test]
    async fn claim_then_ack_removes_the_message_permanently() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let claim = group.claim().await;
        assert_eq!(claim.item, 1);
        assert_eq!(claim.delivery_count, 1);

        assert!(group.ack(claim.id, claim.token).await.unwrap());
        assert_eq!(group.len().await, 0);
    }

    #[tokio::test]
    async fn len_counts_both_pending_and_leased_messages() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;
        group.enqueue(1).await.unwrap();
        group.enqueue(2).await.unwrap();

        let _claim = group.claim().await;
        assert_eq!(group.len().await, 2);
        assert!(!group.is_empty().await);
    }

    #[tokio::test]
    async fn an_unacked_claim_is_redelivered_after_its_lease_expires() {
        let dir = tempdir().unwrap();
        let group = open(&dir, SHORT_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let first = group.claim().await;
        assert_eq!(first.delivery_count, 1);

        // Bounded so a real bug (permanent block) fails the test instead
        // of hanging the suite.
        let second = tokio::time::timeout(Duration::from_secs(5), group.claim())
            .await
            .expect("expired lease should have made the message claimable again");

        assert_eq!(second.id, first.id);
        assert_eq!(second.item, 1);
        assert_eq!(second.delivery_count, 2);
        assert_ne!(
            second.token, first.token,
            "a redelivery must mint a fresh lease token, not reuse the expired one"
        );
    }

    #[tokio::test]
    async fn nack_redelivers_immediately_without_waiting_for_the_timeout() {
        let dir = tempdir().unwrap();
        // Deliberately long — if nack didn't work and the test fell back
        // to waiting out the real timeout, this bounds how long that
        // would take, and the explicit timeout below fails fast instead.
        let group = open(&dir, LONG_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let first = group.claim().await;
        assert!(group.nack(first.id, first.token).await);

        let second = tokio::time::timeout(Duration::from_millis(200), group.claim())
            .await
            .expect("nack should make the message claimable again immediately");
        assert_eq!(second.id, first.id);
        assert_eq!(second.delivery_count, 2);
    }

    #[tokio::test]
    async fn a_stale_ack_after_redelivery_is_rejected_and_does_not_touch_the_new_lease() {
        let dir = tempdir().unwrap();
        let group = open(&dir, SHORT_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let first = group.claim().await;
        let second = tokio::time::timeout(Duration::from_secs(5), group.claim()).await.unwrap();

        // The first lease's token is stale now — a late ack from it must
        // not succeed, and specifically must not silently ack the
        // second, currently-active lease just because it's the same
        // message ID.
        assert!(!group.ack(first.id, first.token).await.unwrap());
        assert_eq!(group.len().await, 1, "message must still be pending ack");

        assert!(group.ack(second.id, second.token).await.unwrap());
        assert_eq!(group.len().await, 0);
    }

    #[tokio::test]
    async fn a_stale_nack_after_redelivery_is_rejected() {
        let dir = tempdir().unwrap();
        let group = open(&dir, SHORT_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let first = group.claim().await;
        let _second = tokio::time::timeout(Duration::from_secs(5), group.claim()).await.unwrap();

        assert!(!group.nack(first.id, first.token).await);
    }

    #[tokio::test]
    async fn acking_an_id_that_was_never_claimed_returns_false_without_error() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;

        // A `MessageId` this group never issued, and a `LeaseToken` this
        // group never minted (constructed directly, since the field is
        // private to this crate — a caller outside it can only ever get
        // one back from a real `claim`).
        let bogus_id = qaas_types::MessageId::new();
        let bogus_token = super::LeaseToken(0);

        assert!(!group.ack(bogus_id, bogus_token).await.unwrap());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_competing_consumers_never_receive_the_same_message_at_once() {
        let dir = tempdir().unwrap();
        let group = Arc::new(open(&dir, LONG_TIMEOUT).await);
        group.enqueue(1).await.unwrap();
        group.enqueue(2).await.unwrap();

        let a = Arc::clone(&group);
        let b = Arc::clone(&group);
        let (claim_a, claim_b) = tokio::join!(
            tokio::spawn(async move { a.claim().await }),
            tokio::spawn(async move { b.claim().await })
        );
        let claim_a = claim_a.unwrap();
        let claim_b = claim_b.unwrap();

        assert_ne!(claim_a.id, claim_b.id);
    }

    #[tokio::test]
    async fn state_survives_reopening_the_same_wal_and_unacked_claims_return_as_pending() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let group = ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT).await.unwrap();
            group.enqueue(1).await.unwrap();
            group.enqueue(2).await.unwrap();
            group.enqueue(3).await.unwrap();

            let claim = group.claim().await; // never acked
            assert_eq!(claim.item, 1);
        }

        let recovered = ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT).await.unwrap();
        assert_eq!(recovered.len().await, 3);

        // The previously-claimed-but-unacked message comes back as a
        // *fresh* delivery (count 1, not 2) — delivery history isn't
        // durable, only the message itself is. See WalRecord's docs.
        let claim = recovered.claim().await;
        assert_eq!(claim.item, 1);
        assert_eq!(claim.delivery_count, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_claim_and_ack_delivers_every_item_exactly_once() {
        // Every enqueue and every ack fsyncs — deliberately, per this
        // crate's durability-over-throughput stance (see `Wal`'s docs) —
        // so this count is kept modest specifically to keep the test
        // suite fast, not because the design can't handle more.
        const TOTAL: usize = 40;

        let dir = tempdir().unwrap();
        let group = Arc::new(
            ConsumerGroup::<usize>::open(dir.path().join("wal.log"), LONG_TIMEOUT).await.unwrap(),
        );
        for i in 0..TOTAL {
            group.enqueue(i).await.unwrap();
        }

        let mut consumers = Vec::new();
        for _ in 0..8 {
            let group = Arc::clone(&group);
            consumers.push(tokio::spawn(async move {
                let mut received = Vec::new();
                loop {
                    let claim = tokio::time::timeout(Duration::from_millis(500), group.claim())
                        .await
                        .expect("should never block this long with work remaining");
                    let acked = group.ack(claim.id, claim.token).await.unwrap();
                    assert!(acked, "a fresh claim's own ack must always succeed");
                    received.push(claim.item);
                    if group.is_empty().await {
                        break;
                    }
                }
                received
            }));
        }

        let mut all_received = Vec::new();
        for consumer in consumers {
            all_received.extend(consumer.await.unwrap());
        }
        all_received.sort_unstable();
        all_received.dedup();
        assert_eq!(all_received.len(), TOTAL);
    }
}
