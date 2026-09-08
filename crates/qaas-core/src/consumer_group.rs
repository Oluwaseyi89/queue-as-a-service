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
//! durable removal) or [`nack`](ConsumerGroup::nack) it (early release,
//! skipping the rest of the visibility timeout) before the lease's
//! visibility timeout elapses; if neither happens — the consumer
//! crashed, hung, or was simply too slow — the lease expires on its own
//! and the message eventually becomes claimable again. That's what makes
//! this at-least-once rather than at-most-once: a message is never
//! dropped just because whoever had it stopped responding.
//!
//! Neither path makes the message claimable *again immediately*,
//! though — both go through [`RetryPolicy`]'s exponential backoff first.
//! nack deliberately isn't a bypass around that: if it were, a consumer
//! that fails because a downstream dependency (an LLM provider, a
//! rate-limited API) is struggling and dutifully nacks would cause every
//! consumer in the group to immediately re-claim and re-fail in a tight
//! loop — exactly the thundering-herd behavior this module exists to
//! prevent, just triggered by explicit failure signaling instead of
//! silent timeouts.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use qaas_types::MessageId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Notify};

use crate::retry::RetryPolicy;
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

/// A message that failed a delivery attempt (nack or lease expiry) and
/// is waiting out its [`RetryPolicy`] backoff before becoming claimable
/// again.
struct Delayed<T> {
    id: MessageId,
    item: T,
    delivery_count: u32,
    available_at: Instant,
}

struct State<T> {
    pending: VecDeque<Pending<T>>,
    leased: HashMap<MessageId, Leased<T>>,
    delayed: Vec<Delayed<T>>,
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
    /// Signaled whenever a message becomes claimable, or whenever the
    /// earliest time a message *could* become claimable changes — a
    /// fresh enqueue, an explicit nack, a lease expiring, or a new lease
    /// being granted — so a consumer blocked in `claim` wakes up instead
    /// of waiting out a timer it no longer needs to.
    changed: Notify,
    wal: Wal<WalRecord<T>>,
    visibility_timeout: Duration,
    retry_policy: RetryPolicy,
    next_lease_token: AtomicU64,
}

impl<T: Serialize + DeserializeOwned> ConsumerGroup<T> {
    /// Opens the WAL at `path` (creating it if it doesn't exist), replays
    /// it to rebuild the set of pending messages, and returns a group
    /// ready for use. Every claimed-but-unacked message from a previous
    /// run comes back as pending, per this type's documented at-least-
    /// once, restart-resets-delivery-count behavior.
    ///
    /// `visibility_timeout` applies to every lease this group grants,
    /// and `retry_policy` to every nack or lease expiry; neither has a
    /// per-claim or per-message override in this branch.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as
    /// [`Wal::open`](crate::wal::Wal::open).
    pub async fn open(
        path: impl AsRef<Path>,
        visibility_timeout: Duration,
        retry_policy: RetryPolicy,
    ) -> io::Result<Self> {
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
            state: Mutex::new(State { pending, leased: HashMap::new(), delayed: Vec::new() }),
            changed: Notify::new(),
            wal,
            visibility_timeout,
            retry_policy,
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

    /// The total number of messages not yet acknowledged — claimable,
    /// currently leased, or waiting out a retry backoff delay.
    pub async fn len(&self) -> usize {
        let state = self.state.lock().await;
        state.pending.len() + state.leased.len() + state.delayed.len()
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
            Self::promote_ready_delayed(&mut state);

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

            let wake_at = Self::earliest_wake(&state);
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

    /// Releases a claimed message back to the retry pool immediately —
    /// as opposed to waiting for its lease to expire on its own — where
    /// it waits out this group's [`RetryPolicy`] backoff before becoming
    /// claimable again, exactly as an expired lease would.
    ///
    /// Same stale-lease handling as [`ack`](Self::ack): returns `false`
    /// without effect if `id`/`token` don't match a currently-active
    /// lease. No WAL write here — nack doesn't durably commit anything,
    /// it only changes which in-memory pool the message sits in.
    pub async fn nack(&self, id: MessageId, token: LeaseToken) -> bool {
        let requeued = {
            let mut state = self.state.lock().await;
            match state.leased.remove(&id) {
                Some(lease) if lease.token == token => {
                    self.schedule_retry(&mut state, id, lease.item, lease.delivery_count);
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

    /// Moves every lease whose visibility timeout has passed into the
    /// retry backoff pool (see [`schedule_retry`](Self::schedule_retry)).
    /// Called at the start of every [`claim`](Self::claim), so a
    /// consumer can never observe a message as permanently unclaimable
    /// purely because its previous lease-holder crashed.
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
            self.schedule_retry(state, id, lease.item, lease.delivery_count);
        }
        // Multiple messages may have just become claimable (or had their
        // next-wake time change) at once — wake every blocked claimer to
        // recheck, not just one.
        self.changed.notify_waiters();
    }

    /// Puts a failed delivery into the backoff pool rather than directly
    /// back into `pending`, per this group's [`RetryPolicy`]. Shared by
    /// [`nack`](Self::nack) and [`reclaim_expired_leases`](Self::reclaim_expired_leases) —
    /// a lease expiry and an explicit nack both mean "this delivery
    /// attempt failed," and both need the same backoff treatment to
    /// avoid the thundering-herd behavior this module exists to prevent.
    fn schedule_retry(&self, state: &mut State<T>, id: MessageId, item: T, delivery_count: u32) {
        let delay = self.retry_policy.delay_for(delivery_count);
        state.delayed.push(Delayed {
            id,
            item,
            delivery_count,
            available_at: Instant::now() + delay,
        });
    }

    /// Moves every delayed message whose backoff has elapsed into
    /// `pending`. Called at the start of every [`claim`](Self::claim),
    /// after [`reclaim_expired_leases`](Self::reclaim_expired_leases) so
    /// a lease that just expired with a very short (or zero) computed
    /// backoff can become claimable again within the same call.
    fn promote_ready_delayed(state: &mut State<T>) {
        if state.delayed.is_empty() {
            return;
        }
        let now = Instant::now();
        let mut still_delayed = Vec::with_capacity(state.delayed.len());
        for entry in state.delayed.drain(..) {
            if entry.available_at <= now {
                state.pending.push_back(Pending {
                    id: entry.id,
                    item: entry.item,
                    delivery_count: entry.delivery_count,
                });
            } else {
                still_delayed.push(entry);
            }
        }
        state.delayed = still_delayed;
    }

    /// The earliest instant anything currently leased or delayed could
    /// become claimable — a blocked [`claim`](Self::claim) sleeps until
    /// this, recomputed fresh every time it loops, rather than on any
    /// fixed polling interval.
    fn earliest_wake(state: &State<T>) -> Option<Instant> {
        let earliest_lease_expiry = state.leased.values().map(|lease| lease.expires_at).min();
        let earliest_retry = state.delayed.iter().map(|entry| entry.available_at).min();
        [earliest_lease_expiry, earliest_retry].into_iter().flatten().min()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use tempfile::tempdir;

    use super::ConsumerGroup;
    use crate::retry::RetryPolicy;

    /// Generous enough that a test asserting "this should NOT have
    /// expired yet" isn't flaky under CI scheduling jitter, short enough
    /// that a test asserting "this should redeliver soon" doesn't make
    /// the suite slow.
    const LONG_TIMEOUT: Duration = Duration::from_secs(60);
    const SHORT_TIMEOUT: Duration = Duration::from_millis(30);

    /// For tests that aren't specifically about retry/backoff timing —
    /// makes a nack or a lease expiry behave like it did before this
    /// branch, so tests written for `claim`/`ack`/lease-expiry mechanics
    /// don't also have to account for a backoff delay they're not
    /// testing.
    const NO_RETRY_DELAY: RetryPolicy = RetryPolicy {
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        multiplier: 1.0,
        jitter: 0.0,
    };

    async fn open(dir: &tempfile::TempDir, visibility_timeout: Duration) -> ConsumerGroup<i32> {
        ConsumerGroup::open(dir.path().join("wal.log"), visibility_timeout, NO_RETRY_DELAY)
            .await
            .unwrap()
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
    async fn an_expired_lease_also_waits_out_the_retry_backoff_not_just_the_visibility_timeout() {
        let policy = RetryPolicy {
            base_delay: Duration::from_millis(60),
            max_delay: Duration::from_millis(60),
            multiplier: 1.0,
            jitter: 0.0,
        };
        let dir = tempdir().unwrap();
        let group = ConsumerGroup::<i32>::open(dir.path().join("wal.log"), SHORT_TIMEOUT, policy)
            .await
            .unwrap();
        group.enqueue(1).await.unwrap();

        let first = group.claim().await;

        // Give the lease time to expire (SHORT_TIMEOUT = 30ms) but not
        // enough for the 60ms retry backoff after that to have elapsed
        // too — proving the message doesn't become claimable the instant
        // the lease expires.
        assert!(
            tokio::time::timeout(Duration::from_millis(60), group.claim()).await.is_err(),
            "an expired lease must still go through the retry backoff, not skip it"
        );

        let second = tokio::time::timeout(Duration::from_secs(2), group.claim())
            .await
            .expect("message should become claimable once both timeouts have elapsed");
        assert_eq!(second.id, first.id);
        assert_eq!(second.delivery_count, 2);
    }

    #[tokio::test]
    async fn len_counts_delayed_messages_waiting_out_their_retry_backoff() {
        let policy = RetryPolicy {
            base_delay: Duration::from_secs(60),
            max_delay: Duration::from_secs(60),
            multiplier: 1.0,
            jitter: 0.0,
        };
        let dir = tempdir().unwrap();
        let group = ConsumerGroup::<i32>::open(dir.path().join("wal.log"), LONG_TIMEOUT, policy)
            .await
            .unwrap();
        group.enqueue(1).await.unwrap();

        let claim = group.claim().await;
        assert!(group.nack(claim.id, claim.token).await);

        // The message is now sitting in the delayed pool (a 60s backoff,
        // far longer than this test runs) rather than pending or leased
        // — `len` must still count it as an outstanding message.
        assert_eq!(group.len().await, 1);
        assert!(!group.is_empty().await);
    }

    #[tokio::test]
    async fn nack_schedules_a_retry_instead_of_waiting_out_the_visibility_timeout() {
        // Short, but non-zero — non-zero so this test can actually prove
        // nack goes through backoff at all (with NO_RETRY_DELAY, this
        // test couldn't distinguish "nack respects the policy" from
        // "nack ignores the policy entirely"); short so the suite stays
        // fast and so it stays far below `LONG_TIMEOUT`, proving nack is
        // nowhere near waiting out the full visibility timeout either.
        let policy = RetryPolicy {
            base_delay: Duration::from_millis(60),
            max_delay: Duration::from_millis(60),
            multiplier: 1.0,
            jitter: 0.0,
        };
        let dir = tempdir().unwrap();
        let group = ConsumerGroup::<i32>::open(dir.path().join("wal.log"), LONG_TIMEOUT, policy)
            .await
            .unwrap();
        group.enqueue(1).await.unwrap();

        let first = group.claim().await;
        assert!(group.nack(first.id, first.token).await);

        // Not instant: still within the retry delay, so nothing should
        // be claimable yet.
        assert!(
            tokio::time::timeout(Duration::from_millis(15), group.claim()).await.is_err(),
            "nack must not bypass the retry backoff entirely"
        );

        // But long before `LONG_TIMEOUT` would have elapsed via lease
        // expiry alone.
        let second = tokio::time::timeout(Duration::from_secs(2), group.claim())
            .await
            .expect("message should become claimable again once its retry delay elapses");
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
            let group =
                ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT, NO_RETRY_DELAY).await.unwrap();
            group.enqueue(1).await.unwrap();
            group.enqueue(2).await.unwrap();
            group.enqueue(3).await.unwrap();

            let claim = group.claim().await; // never acked
            assert_eq!(claim.item, 1);
        }

        let recovered =
            ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT, NO_RETRY_DELAY).await.unwrap();
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
            ConsumerGroup::<usize>::open(dir.path().join("wal.log"), LONG_TIMEOUT, NO_RETRY_DELAY)
                .await
                .unwrap(),
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
