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
//!
//! Backoff isn't forever, though: once `retry_policy` considers a
//! message's `delivery_count` exhausted, a nack or expiry routes it to
//! this group's [`DeadLetterQueue`] instead of scheduling yet another
//! retry — durably, with the failure reason attached, so an operator (or
//! `feature/llm-assisted-dlq-triage`, later) has somewhere to look
//! instead of the message just disappearing.
//!
//! Idempotency (`feature/idempotent-delivery`) works on two sides of
//! this type, for two different reasons. On the producer side,
//! [`enqueue_with_key`](ConsumerGroup::enqueue_with_key) deduplicates
//! against a caller-supplied [`IdempotencyKey`]: a retried enqueue call
//! (a producer that timed out waiting for a response and resent the
//! same logical request) returns the *original* message's id instead of
//! creating a second message, for as long as that original message is
//! still somewhere in the system — pending, leased, delayed, or sitting
//! in the DLQ. On the consumer side, every [`Claim`] carries an
//! `idempotency_key` that's *always* present, whether or not the
//! producer supplied one, falling back to one derived from the
//! message's own id: a consumer that forwards this key as the
//! idempotency key on its own downstream call (to an LLM provider's API,
//! say) gets that provider's own idempotency handling for free, so a
//! redelivered message that's reprocessed doesn't get executed — or
//! billed — twice, even though this queue's own delivery guarantee is
//! only ever at-least-once.
//!
//! Checkpointing (`feature/durable-agent-workflows`) is a different
//! problem from any of the above: not "does the message survive a
//! crash" (it always has, since `feature/wal-persistence`) but "does a
//! multi-step *task* survive one without redoing steps it already
//! finished." [`checkpoint`](ConsumerGroup::checkpoint) durably saves a
//! caller-shaped progress snapshot against the currently-leased message,
//! without touching the lease itself; the next
//! [`claim`](ConsumerGroup::claim) of that message — after a crash, an
//! expired lease, or an explicit `nack` used deliberately to release the
//! message for a human-in-the-loop pause that might last hours — surfaces
//! it as [`Claim::checkpoint`]. Reusing `nack` for pausing rather than
//! adding a dedicated "pause" primitive is deliberate, not an oversight:
//! it means a long-running workflow's pauses count toward
//! `retry_policy`'s `max_attempts` the same way failures do, which is a
//! real, accepted tradeoff (an operator running workflows with many
//! human-in-the-loop steps should configure a generous or unlimited
//! `max_attempts` for that queue) in exchange for not bolting on a
//! second delivery-lifecycle verb next to `ack`/`nack` — exactly the
//! "without a separate workflow engine" framing this branch's own
//! `Plan.md` entry asks for.

use std::collections::{HashMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use qaas_types::{IdempotencyKey, MessageId, Timestamp};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, Notify};

use crate::dead_letter::{DeadLetter, DeadLetterQueue};
use crate::retry::RetryPolicy;
use crate::wal::Wal;

/// One entry in a [`ConsumerGroup`]'s WAL. A claim is a lease, not a
/// commitment, so there is deliberately no "Claim" record: persisting it
/// would imply a crash should remember who had a message leased, but a
/// lease's whole point is that it doesn't outlive anything, not even
/// gracefully. On restart, every message that's still *live* — whether
/// it was sitting untouched, actively (but unacknowledged) leased, or
/// waiting out a retry backoff when the process died — replays back as
/// available to claim. That's a real, documented limitation:
/// `delivery_count` (see [`Claim`]) resets to zero across a restart,
/// because delivery attempts aren't durable, only messages are.
///
/// "Live" is the operative word: [`Ack`](Self::Ack) and
/// [`DeadLettered`](Self::DeadLettered) both durably end a message's
/// life in *this* WAL (permanently, or by handing it off to the DLQ's
/// own WAL respectively) — without one of those, the message's original
/// [`Enqueue`](Self::Enqueue) record would still be sitting un-acked
/// here forever, which is exactly what used to make a dead-lettered
/// message resurrect as pending after a restart despite also sitting in
/// the DLQ, before `DeadLettered` existed. See
/// [`resolve_failed_delivery`](ConsumerGroup::resolve_failed_delivery)
/// for why a `DeadLettered` record — unlike `Ack` — must never cause a
/// replaying restart to release the message's idempotency key.
#[derive(Serialize, Deserialize)]
enum WalRecord<T> {
    /// The `Option<IdempotencyKey>` is exactly what the caller passed to
    /// `enqueue`/`enqueue_with_key` — `None` for a plain `enqueue`, never
    /// a synthesized fallback. [`Claim::idempotency_key`] fills that
    /// fallback in later, on demand, rather than it being stored here;
    /// storing a derived value durably when it can always be recomputed
    /// identically from the id would just be redundant.
    Enqueue(MessageId, Option<IdempotencyKey>, T),
    Ack(MessageId),
    /// Recorded when a failed delivery exhausts its
    /// [`RetryPolicy`](crate::RetryPolicy) and the message moves to the
    /// DLQ instead of being scheduled for another attempt — see this
    /// enum's own docs for why this needs to be its own variant rather
    /// than reusing `Ack`.
    DeadLettered(MessageId),
    /// Durably records `feature/durable-agent-workflows`' checkpoint
    /// state for a message that's still live (pending, leased, or
    /// delayed) — the resumable progress a multi-step agent workflow
    /// saves via [`checkpoint`](ConsumerGroup::checkpoint) so a later
    /// claim, whether after a crash or a deliberate pause, can pick up
    /// where the last one left off instead of starting the whole task
    /// over. Only the latest checkpoint per message matters; an older
    /// one is simply superseded, never merged with the new one.
    Checkpoint(MessageId, Value),
}

/// A message waiting to be claimed.
struct Pending<T> {
    id: MessageId,
    item: T,
    /// How many times this message has already been delivered (0 if
    /// it's never been claimed).
    delivery_count: u32,
    idempotency_key: Option<IdempotencyKey>,
    /// The latest state saved via [`ConsumerGroup::checkpoint`] for this
    /// message, if any — carried forward from wherever it was last set
    /// (a prior lease, a prior delayed retry) so a claim sees it.
    checkpoint: Option<Value>,
}

/// A message currently out on lease to some consumer.
struct Leased<T> {
    item: T,
    /// Unique to this specific delivery attempt, not to the message —
    /// see [`LeaseToken`] for why that distinction is load-bearing.
    token: LeaseToken,
    expires_at: Instant,
    delivery_count: u32,
    idempotency_key: Option<IdempotencyKey>,
    /// See [`Pending::checkpoint`]. Updatable in place, mid-lease, via
    /// [`ConsumerGroup::checkpoint`] — unlike every other field here,
    /// this one can change without the lease itself changing.
    checkpoint: Option<Value>,
}

/// A message that failed a delivery attempt (nack or lease expiry) and
/// is waiting out its [`RetryPolicy`] backoff before becoming claimable
/// again.
struct Delayed<T> {
    id: MessageId,
    item: T,
    delivery_count: u32,
    available_at: Instant,
    idempotency_key: Option<IdempotencyKey>,
    /// See [`Pending::checkpoint`].
    checkpoint: Option<Value>,
}

struct State<T> {
    pending: VecDeque<Pending<T>>,
    leased: HashMap<MessageId, Leased<T>>,
    delayed: Vec<Delayed<T>>,
    /// Producer-supplied idempotency keys for every message currently
    /// somewhere in this group (pending, leased, delayed, *or* in the
    /// DLQ — dead-lettering doesn't clear an entry here, only
    /// [`ack`](ConsumerGroup::ack) and
    /// [`purge_dead_letter`](ConsumerGroup::purge_dead_letter) do, since
    /// only those mean the message is truly gone rather than just not
    /// currently live). Only ever populated for keys a caller actually
    /// supplied — an auto-derived fallback key is, by construction,
    /// already unique, so dedup has nothing to check it against.
    dedup: HashMap<IdempotencyKey, MessageId>,
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

impl LeaseToken {
    /// The token's underlying value, for a caller that needs to carry it
    /// across a boundary this type itself doesn't understand — a network
    /// wire format, a database column — and hand back an equivalent
    /// [`LeaseToken`] later via [`from_u64`](Self::from_u64).
    ///
    /// Deliberately not `Serialize`/`Deserialize`: those traits would
    /// make "a bare integer" part of this type's API contract implicitly,
    /// forever, the moment any crate outside `qaas-core` derives through
    /// it. An explicit method pair keeps that choice visible at the one
    /// call site (`qaas-server`'s MCP tool layer, as of
    /// `feature/mcp-server-interface`) that actually needs it, rather
    /// than baking it into the type for every future caller whether they
    /// need it or not.
    #[must_use]
    pub fn as_u64(self) -> u64 {
        self.0
    }

    /// Reconstructs a token from a value previously obtained from
    /// [`as_u64`](Self::as_u64). Does not — cannot — verify the value
    /// actually corresponds to a lease this group ever granted; that
    /// check happens the same place it always has, inside
    /// [`ConsumerGroup::ack`]/[`nack`](ConsumerGroup::nack) comparing
    /// against the currently-active lease. A forged or stale value simply
    /// fails to match there, exactly as a token from an expired lease
    /// already does.
    #[must_use]
    pub fn from_u64(value: u64) -> Self {
        Self(value)
    }
}

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
    /// A stable idempotency key for this message: the producer-supplied
    /// one if `enqueue_with_key` was used, otherwise one derived from
    /// `id`. Always present, and always the same across every
    /// redelivery of this message — pass it as the idempotency key on a
    /// downstream call to make that call safe against this queue's
    /// at-least-once redelivery. See this module's docs.
    pub idempotency_key: IdempotencyKey,
    /// The message payload.
    pub item: T,
    /// How many times this message has now been delivered, including
    /// this delivery. Starts at 1. Reset to 1 on the first claim after a
    /// restart even if it had been delivered before — delivery attempts
    /// aren't durable, only messages are (see this module's docs and
    /// [`ConsumerGroup::open`]), so a crash genuinely loses that count
    /// rather than this being an oversight.
    pub delivery_count: u32,
    /// The latest state a previous holder of this message saved via
    /// [`ConsumerGroup::checkpoint`], if any — `None` for a message's
    /// first-ever claim, or one that was never checkpointed. Unlike
    /// `delivery_count`, this *does* survive a restart (it's recorded in
    /// the WAL) and *does* survive an explicit [`nack`](ConsumerGroup::nack)
    /// — it's how a multi-step agent workflow resumes a task instead of
    /// starting over, whether the previous attempt ended in a crash or a
    /// deliberate pause. See the module docs for the worked scenario.
    pub checkpoint: Option<Value>,
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
    dlq: DeadLetterQueue<T>,
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
    /// This group's dead-letter queue lives alongside `path`: if `path`
    /// is `"orders.wal"`, the DLQ's own WAL is `"orders.wal.dlq"` in the
    /// same directory. Deriving it rather than taking a second `path`
    /// argument keeps this constructor's signature from growing every
    /// time this group gains another durable side-structure, and there's
    /// no use case yet for a DLQ shared across multiple groups that
    /// would need it passed in independently.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as
    /// [`Wal::open`](crate::wal::Wal::open), for either WAL.
    pub async fn open(
        path: impl AsRef<Path>,
        visibility_timeout: Duration,
        retry_policy: RetryPolicy,
    ) -> io::Result<Self> {
        let path = path.as_ref();
        let (wal, records) = Wal::open(path).await?;
        let mut pending: VecDeque<Pending<T>> = VecDeque::new();
        let mut dedup: HashMap<IdempotencyKey, MessageId> = HashMap::new();
        // Accumulated separately from `pending` rather than looked up and
        // written in place on every `Checkpoint` record: a checkpoint can
        // arrive, later in the same replay, for a message that's since
        // been acked or dead-lettered — simplest to just keep the latest
        // per id throughout and apply it once, at the end, only to
        // whatever's actually still in `pending` by then.
        let mut checkpoints: HashMap<MessageId, Value> = HashMap::new();
        for record in records {
            match record {
                WalRecord::Enqueue(id, idempotency_key, item) => {
                    if let Some(key) = &idempotency_key {
                        dedup.insert(key.clone(), id);
                    }
                    pending.push_back(Pending {
                        id,
                        item,
                        delivery_count: 0,
                        idempotency_key,
                        checkpoint: None,
                    });
                }
                WalRecord::Ack(id) => {
                    if let Some(index) = pending.iter().position(|entry| entry.id == id)
                        && let Some(acked) = pending.remove(index)
                        && let Some(key) = &acked.idempotency_key
                    {
                        dedup.remove(key);
                    }
                }
                WalRecord::DeadLettered(id) => {
                    // Unlike `Ack`, deliberately does *not* touch
                    // `dedup` — dead-lettering never releases a
                    // message's idempotency key (see `State::dedup`'s
                    // docs), and the loop below re-seeds `dedup` from
                    // the DLQ's own keys regardless, so removing it here
                    // would just be undone a few lines later anyway.
                    if let Some(index) = pending.iter().position(|entry| entry.id == id) {
                        pending.remove(index);
                    }
                }
                WalRecord::Checkpoint(id, state) => {
                    checkpoints.insert(id, state);
                }
            }
        }
        for entry in &mut pending {
            entry.checkpoint = checkpoints.remove(&entry.id);
        }
        let dlq = DeadLetterQueue::open(Self::dlq_path(path)).await?;
        // Dead-lettering doesn't release a message's idempotency key —
        // only ack and purge do (see `State::dedup`'s docs) — so a
        // restart has to re-seed dedup with the DLQ's own keys too, not
        // just pending's.
        for (id, key) in dlq.ids_and_keys().await {
            if let Some(key) = key {
                dedup.insert(key, id);
            }
        }

        Ok(Self {
            state: Mutex::new(State {
                pending,
                leased: HashMap::new(),
                delayed: Vec::new(),
                dedup,
            }),
            changed: Notify::new(),
            wal,
            dlq,
            visibility_timeout,
            retry_policy,
            next_lease_token: AtomicU64::new(0),
        })
    }

    /// See [`open`](Self::open)'s docs for why this is derived rather
    /// than a separate constructor argument.
    fn dlq_path(path: &Path) -> PathBuf {
        let mut file_name = path.file_name().unwrap_or_default().to_os_string();
        file_name.push(".dlq");
        path.with_file_name(file_name)
    }

    /// Durably enqueues `item`, with no idempotency key, and returns its
    /// assigned [`MessageId`]. Every call creates a new message — for
    /// producer-side deduplication, use
    /// [`enqueue_with_key`](Self::enqueue_with_key) instead.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails; `item` is not enqueued
    /// in that case.
    pub async fn enqueue(&self, item: T) -> io::Result<MessageId> {
        self.enqueue_with_id_and_key(MessageId::new(), item, None).await
    }

    /// Durably enqueues `item` under `idempotency_key`.
    ///
    /// If a message is already anywhere in this group (pending, leased,
    /// delayed, or dead-lettered) under the same key, this is a no-op:
    /// no new message is created, no WAL write happens, and the
    /// *original* message's id is returned — exactly what a retried
    /// enqueue call from a producer that timed out waiting for a
    /// response needs, so its retry doesn't create a second, duplicate
    /// message.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails (only possible when this
    /// key hasn't been seen before); `item` is not enqueued in that
    /// case, and the key is released so a later, successful enqueue with
    /// it isn't wrongly deduplicated against a write that never actually
    /// happened.
    pub async fn enqueue_with_key(
        &self,
        item: T,
        idempotency_key: IdempotencyKey,
    ) -> io::Result<MessageId> {
        self.enqueue_with_id_and_key(MessageId::new(), item, Some(idempotency_key)).await
    }

    /// Used by [`enqueue`](Self::enqueue) /
    /// [`enqueue_with_key`](Self::enqueue_with_key) for a fresh id and
    /// key. **Not** used by
    /// [`reprocess_dead_letter`](Self::reprocess_dead_letter), even
    /// though it also needs to write an `Enqueue` record for a
    /// (non-fresh) id and key — see
    /// [`write_enqueue_record`](Self::write_enqueue_record)'s docs for
    /// why sharing this dedup-checking path with reprocessing would be
    /// actively wrong, not just redundant.
    async fn enqueue_with_id_and_key(
        &self,
        id: MessageId,
        item: T,
        idempotency_key: Option<IdempotencyKey>,
    ) -> io::Result<MessageId> {
        if let Some(key) = &idempotency_key {
            let mut state = self.state.lock().await;
            if let Some(existing_id) = state.dedup.get(key) {
                return Ok(*existing_id);
            }
            // Reserve the key for `id` now, before releasing the lock —
            // otherwise two concurrent calls with the same key could
            // both see it as unclaimed and both go on to durably write
            // a duplicate message, which is exactly what this method
            // exists to prevent.
            state.dedup.insert(key.clone(), id);
        }
        self.write_enqueue_record(id, item, idempotency_key).await
    }

    /// Durably writes an `Enqueue` record for `id`/`item`/`idempotency_key`
    /// and adds the message to `pending`. No dedup check — callers that
    /// need one ([`enqueue_with_id_and_key`](Self::enqueue_with_id_and_key))
    /// do it themselves before calling this.
    ///
    /// [`reprocess_dead_letter`](Self::reprocess_dead_letter) calls this
    /// directly rather than going through the dedup-checking path,
    /// deliberately: dead-lettering never releases a message's
    /// idempotency key (see `State::dedup`'s docs), so by the time
    /// reprocessing runs, `dedup` already maps this exact key to this
    /// exact id. Checking again here wouldn't detect a genuine
    /// duplicate — it would just find that same pre-existing
    /// registration and wrongly treat the reprocess itself as the
    /// duplicate, short-circuiting before the message ever made it back
    /// into `pending`. (This is not a hypothetical: it's a real bug this
    /// branch shipped once and caught by actually running the tests —
    /// see the commit history.)
    async fn write_enqueue_record(
        &self,
        id: MessageId,
        item: T,
        idempotency_key: Option<IdempotencyKey>,
    ) -> io::Result<MessageId> {
        let record = WalRecord::Enqueue(id, idempotency_key.clone(), item);
        if let Err(error) = self.wal.append(&record).await {
            if let Some(key) = &idempotency_key {
                self.state.lock().await.dedup.remove(key);
            }
            return Err(error);
        }

        let WalRecord::Enqueue(id, idempotency_key, item) = record else {
            unreachable!("record was just constructed as Enqueue")
        };
        {
            let mut state = self.state.lock().await;
            state.pending.push_back(Pending {
                id,
                item,
                delivery_count: 0,
                idempotency_key,
                checkpoint: None,
            });
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
    ///
    /// # A note on wrapping this in an external timeout
    ///
    /// Doing so (`tokio::time::timeout(d, group.claim())`) is a
    /// reasonable way to poll with a bounded wait, but be generous with
    /// `d`: this call may need to reclaim an expired lease as a side
    /// effect of finding you something to claim, and if that reclaim
    /// turns out to mean dead-lettering a different, unrelated message
    /// (see the module docs), that's a real, fsync-backed WAL write.
    /// Cancelling `claim` — by dropping a `timeout` future once it
    /// elapses — mid-write does not roll that write back; it can lose
    /// the message being dead-lettered. Prefer a timeout comfortably
    /// longer than this group's WAL write latency (milliseconds to tens
    /// of milliseconds, typically) over an aggressive one.
    pub async fn claim(&self) -> Claim<T>
    where
        T: Clone,
    {
        loop {
            // Resolving an expired lease can now mean dead-lettering it
            // (durable DLQ I/O), so this can't run as a plain sync
            // helper under the state lock any more — it does its own
            // locking internally instead, in short critical sections,
            // never holding the lock across the `.await` inside
            // `resolve_failed_delivery`.
            self.reclaim_expired_leases().await;

            let mut state = self.state.lock().await;
            Self::promote_ready_delayed(&mut state);

            if let Some(Pending { id, item, delivery_count, idempotency_key, checkpoint }) =
                state.pending.pop_front()
            {
                let delivery_count = delivery_count + 1;
                let token = LeaseToken(self.next_lease_token.fetch_add(1, Ordering::Relaxed));
                state.leased.insert(
                    id,
                    Leased {
                        item: item.clone(),
                        token,
                        expires_at: Instant::now() + self.visibility_timeout,
                        delivery_count,
                        idempotency_key: idempotency_key.clone(),
                        checkpoint: checkpoint.clone(),
                    },
                );
                drop(state);
                // Not required for correctness — every waiter recomputes
                // its own wake-up time from scratch each time it loops —
                // but it lets any other waiter learn about this lease's
                // (possibly sooner) expiry immediately instead of only
                // when its own, possibly-later, timer fires.
                self.changed.notify_one();
                let idempotency_key = Self::effective_idempotency_key(id, idempotency_key);
                return Claim { id, token, idempotency_key, item, delivery_count, checkpoint };
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
            match state.leased.remove(&id) {
                Some(lease) if lease.token == token => {
                    // The message is truly gone now — release its
                    // idempotency key (if any) so a *future* enqueue
                    // reusing it is treated as a new message, not
                    // deduplicated against this completed one.
                    if let Some(key) = &lease.idempotency_key {
                        state.dedup.remove(key);
                    }
                }
                Some(lease) => {
                    state.leased.insert(id, lease);
                    return Ok(false);
                }
                None => return Ok(false),
            }
        }
        self.wal.append(&WalRecord::Ack(id)).await?;
        Ok(true)
    }

    /// Releases a claimed message back to the retry pool immediately —
    /// as opposed to waiting for its lease to expire on its own — with
    /// `reason` recorded if this delivery is the one that ends up
    /// exhausting the retry policy, in which case it's dead-lettered
    /// instead of scheduled again (see this module's docs). Otherwise it
    /// waits out this group's [`RetryPolicy`] backoff before becoming
    /// claimable again, exactly as an expired lease would.
    ///
    /// Same stale-lease handling as [`ack`](Self::ack): returns
    /// `Ok(false)` without effect if `id`/`token` don't match a
    /// currently-active lease.
    ///
    /// # Errors
    ///
    /// Returns an error if this delivery turns out to be the one that
    /// exhausts the retry policy and the resulting dead-letter WAL write
    /// fails. A nack that doesn't exhaust the policy never does I/O and
    /// can't fail this way — scheduling a retry is purely an in-memory
    /// operation.
    pub async fn nack(
        &self,
        id: MessageId,
        token: LeaseToken,
        reason: Option<String>,
    ) -> io::Result<bool> {
        let failed_delivery = {
            let mut state = self.state.lock().await;
            match state.leased.remove(&id) {
                Some(lease) if lease.token == token => Some((
                    lease.item,
                    lease.delivery_count,
                    lease.idempotency_key,
                    lease.checkpoint,
                )),
                // Wrong token (a stale nack for an already-redelivered
                // message) or no lease at all — either way, put back
                // exactly what we found, unchanged.
                Some(lease) => {
                    state.leased.insert(id, lease);
                    None
                }
                None => None,
            }
        };

        let Some((item, delivery_count, idempotency_key, checkpoint)) = failed_delivery else {
            return Ok(false);
        };

        self.resolve_failed_delivery(id, item, delivery_count, idempotency_key, checkpoint, reason)
            .await?;
        self.changed.notify_one();
        Ok(true)
    }

    /// Durably records `state` as this message's latest progress,
    /// without otherwise touching the lease `id`/`token` hold — the
    /// lease keeps running exactly as it was, still due to expire at the
    /// same instant, still resolved by the caller's own eventual
    /// `ack`/`nack`. This is `feature/durable-agent-workflows`' core
    /// primitive: a multi-step task — chained LLM calls, a
    /// human-in-the-loop wait — calls this after each step so that if
    /// the current delivery is lost (the consumer crashes, or the
    /// caller simply nacks to release the message for a pause that
    /// might last hours) a later [`claim`](Self::claim) sees `state` in
    /// [`Claim::checkpoint`] and can resume the task instead of running
    /// every already-completed step over again.
    ///
    /// Only the *latest* `state` per message is kept — a second
    /// checkpoint call supersedes the first, it doesn't append to it.
    /// Shaping `state` as, say, `{"completed_steps": [...], ...}` so it
    /// reads as a full snapshot rather than a delta is the caller's job;
    /// this method has no opinion on what `state` contains.
    ///
    /// Same stale-lease handling as [`ack`](Self::ack)/[`nack`](Self::nack):
    /// returns `Ok(false)` without effect if `id`/`token` don't match a
    /// currently-active lease — most likely because it already expired,
    /// in which case whoever now holds the redelivered message (if
    /// anyone yet) is the one who gets to decide what happens next, not
    /// a caller that's already lost the lease race.
    ///
    /// There is one narrow, accepted race here, the same shape as the
    /// ordering tradeoff documented on `resolve_failed_delivery`: the
    /// lease is checked once before the (async, fsync-backed) WAL write
    /// and once after, and if it expired in between — this call's own
    /// lease-holder having taken just long enough to lose the race —
    /// `state` has already been durably written by the time the second
    /// check finds a stale lease. This method still returns `Ok(false)`
    /// in that case (the in-memory lease genuinely wasn't updated), but
    /// `state` remains in the WAL and will be replayed as the message's
    /// checkpoint after a future restart regardless. Narrowing that
    /// window further would mean holding the state lock across the WAL
    /// write, which this crate's own rule (never hold the state lock
    /// across an `.await`) rules out.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails; `state` is not recorded
    /// in that case, and the lease's in-memory checkpoint is left
    /// exactly as it was before this call — a failed checkpoint write
    /// must not silently look like it succeeded to a caller checking
    /// `Claim::checkpoint` after a later crash and replay.
    pub async fn checkpoint(
        &self,
        id: MessageId,
        token: LeaseToken,
        state: Value,
    ) -> io::Result<bool> {
        {
            let locked = self.state.lock().await;
            match locked.leased.get(&id) {
                Some(lease) if lease.token == token => {}
                _ => return Ok(false),
            }
        }

        self.wal.append(&WalRecord::Checkpoint(id, state.clone())).await?;

        let mut locked = self.state.lock().await;
        match locked.leased.get_mut(&id) {
            Some(lease) if lease.token == token => {
                lease.checkpoint = Some(state);
                Ok(true)
            }
            // See this method's docs on the narrow race this covers:
            // `state` is durably recorded either way, but the in-memory
            // lease is no longer this caller's to update.
            _ => Ok(false),
        }
    }

    /// Moves every lease whose visibility timeout has passed to
    /// [`resolve_failed_delivery`](Self::resolve_failed_delivery), with a
    /// synthetic failure reason since there was no consumer around to
    /// give one. Called at the start of every [`claim`](Self::claim), so
    /// a consumer can never observe a message as permanently unclaimable
    /// purely because its previous lease-holder crashed.
    ///
    /// There's no background sweeper — this only ever runs as a side
    /// effect of some caller calling `claim`. An expired lease whose
    /// retry policy is now exhausted sits un-dead-lettered, and
    /// [`dead_letters`](Self::dead_letters) won't show it, until
    /// something calls `claim` again, even if that call has nothing else
    /// to do and blocks afterward. In practice this is rarely
    /// observable — a group with no consumers calling `claim` has no one
    /// waiting on the outcome either — but it means "when exactly does a
    /// message get dead-lettered" isn't purely a function of time
    /// elapsed.
    ///
    /// Any error dead-lettering an expired lease is logged rather than
    /// propagated — `claim` has no way to surface an error about some
    /// *other*, unrelated message than the one it's trying to return,
    /// and a caller blocked in `claim` shouldn't fail because a
    /// different message's DLQ write happened to fail.
    async fn reclaim_expired_leases(&self) {
        // `(MessageId, Leased<T>)` rather than unpacking `Leased` into a
        // same-shaped tuple here — it already has exactly the fields
        // this needs (`clippy::type_complexity` agrees: a 5-element
        // tuple crossed its threshold the moment `checkpoint` joined the
        // other fields already being threaded through).
        let expired: Vec<(MessageId, Leased<T>)> = {
            let mut state = self.state.lock().await;
            let now = Instant::now();
            let expired_ids: Vec<MessageId> = state
                .leased
                .iter()
                .filter(|(_, lease)| lease.expires_at <= now)
                .map(|(id, _)| *id)
                .collect();
            expired_ids
                .into_iter()
                .map(|id| {
                    let lease =
                        state.leased.remove(&id).expect("id came from iterating this same map");
                    (id, lease)
                })
                .collect()
        };

        if expired.is_empty() {
            return;
        }
        for (id, lease) in expired {
            let reason = Some("visibility timeout expired".to_string());
            if let Err(error) = self
                .resolve_failed_delivery(
                    id,
                    lease.item,
                    lease.delivery_count,
                    lease.idempotency_key,
                    lease.checkpoint,
                    reason,
                )
                .await
            {
                tracing::error!(
                    %error,
                    message_id = %id,
                    "failed to dead-letter an expired lease; message may be lost"
                );
            }
        }
        // Multiple messages may have just become claimable (or had their
        // next-wake time change) at once — wake every blocked claimer to
        // recheck, not just one.
        self.changed.notify_waiters();
    }

    /// The common resolution for any failed delivery attempt (nack or
    /// expired lease): dead-letters `item` if `retry_policy` now
    /// considers `delivery_count` exhausted, otherwise schedules it for
    /// another attempt after the policy's backoff delay.
    ///
    /// Takes no lock itself before deciding which path to take — the
    /// dead-letter path does DLQ I/O, and this crate's rule is to never
    /// hold the state lock across an `.await`. If that DLQ write fails,
    /// `item` is lost rather than retried — the same "a failed durable
    /// write loses whatever it was writing" behavior every other
    /// WAL-backed operation in this crate already has (`enqueue`,
    /// `ack`), not a special case invented here.
    ///
    /// `checkpoint` is dropped, not carried into the [`DeadLetter`], on
    /// the exhausted path — a deliberate, documented scope cut, not an
    /// oversight: giving dead letters their own view into a workflow's
    /// last-saved progress is squarely `feature/llm-assisted-dlq-triage`'s
    /// job (a later branch whose entire point is making dead letters
    /// diagnosable), not this one's. On the retry path, `checkpoint`
    /// *is* carried forward into the [`Delayed`] entry — a message that
    /// hasn't exhausted its retries yet is still the *same* in-progress
    /// workflow, not a new one.
    ///
    /// The dead-letter path does two durable writes, not one: the DLQ's
    /// own record, then a [`WalRecord::DeadLettered`] in *this* group's
    /// main WAL so the message doesn't resurrect as pending on the next
    /// restart despite also sitting in the DLQ (see [`WalRecord`]'s
    /// docs). Deliberately in that order, not the reverse: if the
    /// process crashes between the two, the worst case is the old bug
    /// this fixes — the message resurrects as pending *and* stays in the
    /// DLQ, a duplicate an idempotency key makes safe to reprocess.
    /// Writing the main-WAL record first and crashing before the DLQ
    /// write would instead permanently lose the message — it would be
    /// gone from `pending` with nothing durable ever having recorded
    /// where it went. A duplicate is recoverable; silent loss isn't.
    ///
    /// # Errors
    ///
    /// Returns an error along the dead-letter path if either the DLQ's
    /// write or this group's own `DeadLettered` write fails. Scheduling
    /// a retry is in-memory only and never fails.
    async fn resolve_failed_delivery(
        &self,
        id: MessageId,
        item: T,
        delivery_count: u32,
        idempotency_key: Option<IdempotencyKey>,
        checkpoint: Option<Value>,
        reason: Option<String>,
    ) -> io::Result<()> {
        if self.retry_policy.is_exhausted(delivery_count) {
            self.dlq
                .record(DeadLetter {
                    id,
                    idempotency_key,
                    item,
                    delivery_count,
                    last_error: reason,
                    dead_lettered_at: Timestamp::now(),
                })
                .await?;
            self.wal.append(&WalRecord::DeadLettered(id)).await
        } else {
            let delay = self.retry_policy.delay_for(delivery_count);
            let mut state = self.state.lock().await;
            state.delayed.push(Delayed {
                id,
                item,
                delivery_count,
                available_at: Instant::now() + delay,
                idempotency_key,
                checkpoint,
            });
            Ok(())
        }
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
                    idempotency_key: entry.idempotency_key,
                    checkpoint: entry.checkpoint,
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

    /// The idempotency key a [`Claim`] should carry: `explicit` if the
    /// producer supplied one, otherwise one derived from `id` — a UUID's
    /// string form is never empty, so this can't fail the way a
    /// caller-supplied string might have to be validated against.
    fn effective_idempotency_key(
        id: MessageId,
        explicit: Option<IdempotencyKey>,
    ) -> IdempotencyKey {
        explicit.unwrap_or_else(|| {
            IdempotencyKey::new(id.to_string()).expect("a UUID's string form is never empty")
        })
    }

    /// Every message currently sitting in this group's dead-letter
    /// queue, in no particular order.
    pub async fn dead_letters(&self) -> Vec<DeadLetter<T>>
    where
        T: Clone,
    {
        self.dlq.list().await
    }

    /// Takes the dead letter with `id` out of the DLQ and durably
    /// re-enqueues it into this group's live queue — at the back, as a
    /// fresh delivery cycle (`delivery_count` starts over at 0), but
    /// keeping its original [`MessageId`] so it stays traceable across
    /// the round trip through the DLQ.
    ///
    /// Returns `Ok(false)` if no dead letter with `id` exists — already
    /// reprocessed, already purged, or never dead-lettered at all.
    ///
    /// # Errors
    ///
    /// Returns an error if either WAL write fails (removing it from the
    /// DLQ, or re-enqueueing it into the live queue).
    pub async fn reprocess_dead_letter(&self, id: MessageId) -> io::Result<bool> {
        let Some(dead_letter) = self.dlq.take(id).await? else {
            return Ok(false);
        };
        // Deliberately not enqueue_with_id_and_key — see
        // write_enqueue_record's docs for why going through the
        // dedup-checking path here would wrongly no-op the reprocess.
        self.write_enqueue_record(dead_letter.id, dead_letter.item, dead_letter.idempotency_key)
            .await?;
        Ok(true)
    }

    /// Permanently discards the dead letter with `id` — it will not be
    /// reprocessed. Its idempotency key (if any) is released, so a
    /// future enqueue reusing it is treated as a new message rather than
    /// deduplicated against the discarded one. Returns `Ok(false)` if no
    /// dead letter with `id` exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the DLQ's WAL write fails.
    pub async fn purge_dead_letter(&self, id: MessageId) -> io::Result<bool> {
        let Some(dead_letter) = self.dlq.take(id).await? else {
            return Ok(false);
        };
        if let Some(key) = &dead_letter.idempotency_key {
            self.state.lock().await.dedup.remove(key);
        }
        Ok(true)
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
        max_attempts: None,
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
    async fn lease_token_round_trips_through_as_u64_and_from_u64() {
        // Proves the escape hatch `feature/mcp-server-interface` needs —
        // a token handed to an out-of-process caller as a plain integer,
        // then handed back — actually resolves the same lease it came
        // from, not just that the two integers happen to be equal.
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let claim = group.claim().await;
        let carried = claim.token.as_u64();
        let reconstructed = super::LeaseToken::from_u64(carried);

        assert!(group.ack(claim.id, reconstructed).await.unwrap());
    }

    #[tokio::test]
    async fn a_first_claim_has_no_checkpoint() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let claim = group.claim().await;
        assert_eq!(claim.checkpoint, None);
    }

    #[tokio::test]
    async fn a_checkpoint_is_visible_on_the_next_claim_after_an_explicit_nack() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let first = group.claim().await;
        let progress = serde_json::json!({"step": 2, "of": 5});
        assert!(group.checkpoint(first.id, first.token, progress.clone()).await.unwrap());

        // A pause, not a failure — this branch's answer to
        // human-in-the-loop waits is to release via the existing `nack`
        // path rather than a dedicated "pause" primitive, with the
        // checkpoint already saved carrying the progress forward.
        assert!(group.nack(first.id, first.token, None).await.unwrap());

        let second = group.claim().await;
        assert_eq!(second.id, first.id);
        assert_eq!(second.checkpoint, Some(progress));
    }

    #[tokio::test]
    async fn only_the_latest_checkpoint_is_kept_not_a_history_of_all_of_them() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let claim = group.claim().await;
        group.checkpoint(claim.id, claim.token, serde_json::json!({"step": 1})).await.unwrap();
        group.checkpoint(claim.id, claim.token, serde_json::json!({"step": 2})).await.unwrap();
        assert!(group.nack(claim.id, claim.token, None).await.unwrap());

        let redelivered = group.claim().await;
        assert_eq!(redelivered.checkpoint, Some(serde_json::json!({"step": 2})));
    }

    #[tokio::test]
    async fn a_checkpoint_survives_a_restart_the_same_way_the_message_itself_does() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let progress = serde_json::json!({"step": "chained-llm-call-3"});
        let id = {
            let group =
                ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT, NO_RETRY_DELAY).await.unwrap();
            group.enqueue(1).await.unwrap();
            let claim = group.claim().await;
            assert!(group.checkpoint(claim.id, claim.token, progress.clone()).await.unwrap());
            claim.id
            // Dropped without ack/nack — simulates the consumer crashing
            // mid-workflow, the exact scenario this branch is for.
        };

        let reopened =
            ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT, NO_RETRY_DELAY).await.unwrap();
        let resumed = reopened.claim().await;
        assert_eq!(resumed.id, id);
        assert_eq!(resumed.checkpoint, Some(progress));
    }

    #[tokio::test]
    async fn checkpointing_with_a_stale_token_is_rejected_without_effect() {
        let dir = tempdir().unwrap();
        let group = open(&dir, SHORT_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let first = group.claim().await;
        tokio::time::sleep(SHORT_TIMEOUT * 3).await;
        let second = group.claim().await;
        assert_eq!(second.id, first.id, "same message, redelivered after the first lease expired");

        // `first.token` no longer names an active lease — this must not
        // silently attribute progress to `second`'s delivery.
        assert!(!group.checkpoint(first.id, first.token, serde_json::json!("late")).await.unwrap());

        // Prove it didn't corrupt `second`'s lease rather than just
        // trusting the `false` return: a legitimate checkpoint against
        // `second`'s own token should still work normally, and the
        // stale attempt's value must not show up anywhere afterward.
        let legitimate = serde_json::json!("second's own progress");
        assert!(group.checkpoint(second.id, second.token, legitimate.clone()).await.unwrap());
        assert!(group.nack(second.id, second.token, None).await.unwrap());
        let redelivered = group.claim().await;
        assert_eq!(redelivered.checkpoint, Some(legitimate));
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
            max_attempts: None,
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
            max_attempts: None,
        };
        let dir = tempdir().unwrap();
        let group = ConsumerGroup::<i32>::open(dir.path().join("wal.log"), LONG_TIMEOUT, policy)
            .await
            .unwrap();
        group.enqueue(1).await.unwrap();

        let claim = group.claim().await;
        assert!(group.nack(claim.id, claim.token, None).await.unwrap());

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
            max_attempts: None,
        };
        let dir = tempdir().unwrap();
        let group = ConsumerGroup::<i32>::open(dir.path().join("wal.log"), LONG_TIMEOUT, policy)
            .await
            .unwrap();
        group.enqueue(1).await.unwrap();

        let first = group.claim().await;
        assert!(
            group.nack(first.id, first.token, Some("downstream 429".to_string())).await.unwrap()
        );

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

        assert!(!group.nack(first.id, first.token, None).await.unwrap());
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

    /// Zero delay (so these tests run fast) but a real `max_attempts`,
    /// so a message dead-letters on its second failed delivery.
    const EXHAUST_AFTER_TWO: RetryPolicy = RetryPolicy {
        base_delay: Duration::ZERO,
        max_delay: Duration::ZERO,
        multiplier: 1.0,
        jitter: 0.0,
        max_attempts: Some(2),
    };

    #[tokio::test]
    async fn nacking_past_max_attempts_dead_letters_the_message_with_its_last_reason() {
        let dir = tempdir().unwrap();
        let group =
            ConsumerGroup::<i32>::open(dir.path().join("wal.log"), LONG_TIMEOUT, EXHAUST_AFTER_TWO)
                .await
                .unwrap();
        group.enqueue(42).await.unwrap();

        let first = group.claim().await;
        assert_eq!(first.delivery_count, 1);
        assert!(
            group.nack(first.id, first.token, Some("first failure".to_string())).await.unwrap()
        );

        // Not exhausted yet (1 < 2): back in the live queue, not the DLQ.
        assert!(group.dead_letters().await.is_empty());

        let second = group.claim().await;
        assert_eq!(second.delivery_count, 2);
        assert!(
            group.nack(second.id, second.token, Some("second failure".to_string())).await.unwrap()
        );

        // Exhausted now (2 >= 2): gone from the live queue, present in
        // the DLQ with the reason from this last failure.
        assert!(group.is_empty().await);
        let dead_letters = group.dead_letters().await;
        assert_eq!(dead_letters.len(), 1);
        assert_eq!(dead_letters[0].id, first.id);
        assert_eq!(dead_letters[0].item, 42);
        assert_eq!(dead_letters[0].delivery_count, 2);
        assert_eq!(dead_letters[0].last_error.as_deref(), Some("second failure"));
    }

    #[tokio::test]
    async fn a_dead_lettered_message_does_not_resurrect_as_pending_after_a_restart() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        {
            let group =
                ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT, EXHAUST_AFTER_TWO).await.unwrap();
            group.enqueue(42).await.unwrap();
            let first = group.claim().await;
            group.nack(first.id, first.token, Some("f1".to_string())).await.unwrap();
            let second = group.claim().await;
            group.nack(second.id, second.token, Some("f2".to_string())).await.unwrap();
            assert!(group.is_empty().await);
            assert_eq!(group.dead_letters().await.len(), 1);
        }

        // Before the DeadLettered WAL record existed, this resurrected the
        // message as pending on top of it still sitting in the DLQ — a
        // dead-lettered message is supposed to be a terminal, durable
        // outcome, not one that only lasts until the next restart.
        let reopened =
            ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT, EXHAUST_AFTER_TWO).await.unwrap();
        assert_eq!(reopened.len().await, 0);
        assert_eq!(reopened.dead_letters().await.len(), 1);
    }

    #[tokio::test]
    async fn an_expired_lease_that_exhausts_retries_is_dead_lettered_with_a_synthetic_reason() {
        let policy = RetryPolicy { max_attempts: Some(1), ..EXHAUST_AFTER_TWO };
        let dir = tempdir().unwrap();
        let group = ConsumerGroup::<i32>::open(dir.path().join("wal.log"), SHORT_TIMEOUT, policy)
            .await
            .unwrap();
        group.enqueue(1).await.unwrap();

        let _first = group.claim().await; // delivery_count 1; never acked or nacked

        // Lease expiry is only reclaimed as a side effect of something
        // calling `claim` — there's no background sweeper — so polling
        // `dead_letters` alone would hang forever. Bounded `claim` calls
        // both drive that reclaim sweep and double as the timeout: once
        // max_attempts is 1, the single failed delivery is already
        // exhausted, so this expired lease goes straight to the DLQ
        // rather than becoming claimable again — every one of these
        // calls is expected to time out with nothing to claim.
        //
        // The inner bound is deliberately generous, not tight: dead-
        // lettering does a real fsync (~tens of ms in this environment
        // in isolation — see Wal's docs on the durability-over-
        // throughput tradeoff — but observably over 200ms under the
        // contention of the full suite running in parallel, which
        // caused exactly the flake this comment now warns against). And
        // claim's own docs warn that a timeout shorter than the actual
        // write latency risks cancelling it mid-flight and losing the
        // message — a tight bound here wouldn't just be flaky, it would
        // flakily reproduce that documented edge case instead of
        // exercising the behavior this test is actually checking. 1s
        // comfortably covers realistic contention; the 15s outer bound
        // gives room for several such inner cycles before this is
        // treated as a genuine hang rather than just slow.
        tokio::time::timeout(Duration::from_secs(15), async {
            while group.dead_letters().await.is_empty() {
                let _ = tokio::time::timeout(Duration::from_secs(1), group.claim()).await;
            }
        })
        .await
        .expect("expired lease should have been dead-lettered");

        assert!(group.is_empty().await);
        let dead_letters = group.dead_letters().await;
        assert_eq!(dead_letters[0].last_error.as_deref(), Some("visibility timeout expired"));
    }

    #[tokio::test]
    async fn reprocessing_a_dead_letter_returns_it_to_the_live_queue_with_a_fresh_delivery_count() {
        let dir = tempdir().unwrap();
        let group =
            ConsumerGroup::<i32>::open(dir.path().join("wal.log"), LONG_TIMEOUT, EXHAUST_AFTER_TWO)
                .await
                .unwrap();
        group.enqueue(7).await.unwrap();

        let first = group.claim().await;
        group.nack(first.id, first.token, None).await.unwrap();
        let second = group.claim().await;
        group.nack(second.id, second.token, None).await.unwrap();
        assert_eq!(group.dead_letters().await.len(), 1);

        assert!(group.reprocess_dead_letter(first.id).await.unwrap());
        assert!(group.dead_letters().await.is_empty());
        assert_eq!(group.len().await, 1);

        let reclaimed = group.claim().await;
        assert_eq!(reclaimed.id, first.id, "reprocessing keeps the original message id");
        assert_eq!(reclaimed.item, 7);
        assert_eq!(
            reclaimed.delivery_count, 1,
            "reprocessing is a fresh delivery cycle, not a continuation of the exhausted one"
        );
    }

    #[tokio::test]
    async fn purging_a_dead_letter_discards_it_instead_of_returning_it() {
        let dir = tempdir().unwrap();
        let group =
            ConsumerGroup::<i32>::open(dir.path().join("wal.log"), LONG_TIMEOUT, EXHAUST_AFTER_TWO)
                .await
                .unwrap();
        group.enqueue(1).await.unwrap();
        let first = group.claim().await;
        group.nack(first.id, first.token, None).await.unwrap();
        let second = group.claim().await;
        group.nack(second.id, second.token, None).await.unwrap();

        assert!(group.purge_dead_letter(first.id).await.unwrap());
        assert!(group.dead_letters().await.is_empty());
        assert!(group.is_empty().await, "a purged message must not come back anywhere");
        assert!(!group.reprocess_dead_letter(first.id).await.unwrap());
    }

    #[tokio::test]
    async fn reprocessing_or_purging_an_unknown_id_returns_false() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;
        let bogus_id = qaas_types::MessageId::new();

        assert!(!group.reprocess_dead_letter(bogus_id).await.unwrap());
        assert!(!group.purge_dead_letter(bogus_id).await.unwrap());
    }

    fn key(s: &str) -> qaas_types::IdempotencyKey {
        qaas_types::IdempotencyKey::new(s).unwrap()
    }

    #[tokio::test]
    async fn a_retried_enqueue_with_the_same_key_returns_the_original_id_not_a_duplicate() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;

        let first_id = group.enqueue_with_key(1, key("order-42")).await.unwrap();
        let second_id = group.enqueue_with_key(1, key("order-42")).await.unwrap();

        assert_eq!(first_id, second_id);
        assert_eq!(group.len().await, 1, "a retried enqueue must not create a second message");
    }

    #[tokio::test]
    async fn different_keys_create_separate_messages() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;

        let a = group.enqueue_with_key(1, key("a")).await.unwrap();
        let b = group.enqueue_with_key(2, key("b")).await.unwrap();

        assert_ne!(a, b);
        assert_eq!(group.len().await, 2);
    }

    #[tokio::test]
    async fn a_claim_always_has_an_idempotency_key_even_without_one_from_the_producer() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;
        group.enqueue(1).await.unwrap();

        let claim = group.claim().await;
        assert_eq!(claim.idempotency_key, key(&claim.id.to_string()));
    }

    #[tokio::test]
    async fn a_producer_supplied_key_is_what_the_claim_carries() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;
        group.enqueue_with_key(1, key("order-42")).await.unwrap();

        let claim = group.claim().await;
        assert_eq!(claim.idempotency_key, key("order-42"));
    }

    #[tokio::test]
    async fn the_idempotency_key_is_identical_across_a_redelivery() {
        let dir = tempdir().unwrap();
        let group = open(&dir, SHORT_TIMEOUT).await;
        group.enqueue_with_key(1, key("order-42")).await.unwrap();

        let first = group.claim().await;
        let second = tokio::time::timeout(Duration::from_secs(5), group.claim()).await.unwrap();

        assert_eq!(first.idempotency_key, second.idempotency_key);
        assert_eq!(second.idempotency_key, key("order-42"));
    }

    #[tokio::test]
    async fn acking_releases_the_key_so_a_later_enqueue_can_reuse_it() {
        let dir = tempdir().unwrap();
        let group = open(&dir, LONG_TIMEOUT).await;

        let first_id = group.enqueue_with_key(1, key("order-42")).await.unwrap();
        let claim = group.claim().await;
        assert!(group.ack(claim.id, claim.token).await.unwrap());

        // Not a retry of the same logical operation any more, as far as
        // this group is concerned — acking means it's done — so a new
        // enqueue with the same key is a genuinely new message.
        let second_id = group.enqueue_with_key(2, key("order-42")).await.unwrap();
        assert_ne!(first_id, second_id);
        assert_eq!(group.len().await, 1);
    }

    #[tokio::test]
    async fn dead_lettering_does_not_release_the_key_but_purging_does() {
        let dir = tempdir().unwrap();
        let group =
            ConsumerGroup::<i32>::open(dir.path().join("wal.log"), LONG_TIMEOUT, EXHAUST_AFTER_TWO)
                .await
                .unwrap();

        let first_id = group.enqueue_with_key(1, key("order-42")).await.unwrap();
        let first = group.claim().await;
        group.nack(first.id, first.token, None).await.unwrap();
        let second = group.claim().await;
        group.nack(second.id, second.token, None).await.unwrap();
        assert_eq!(group.dead_letters().await.len(), 1);

        // Still dead-lettered, not gone — a retried enqueue with the
        // same key must still resolve to it, not create a fresh
        // duplicate that would also just end up exhausted.
        let retried_id = group.enqueue_with_key(1, key("order-42")).await.unwrap();
        assert_eq!(retried_id, first_id);
        assert_eq!(group.dead_letters().await.len(), 1, "must not have created a duplicate");

        assert!(group.purge_dead_letter(first_id).await.unwrap());
        // Now genuinely gone — a new enqueue with the same key is a new
        // message.
        let fresh_id = group.enqueue_with_key(1, key("order-42")).await.unwrap();
        assert_ne!(fresh_id, first_id);
    }

    #[tokio::test]
    async fn reprocessing_keeps_the_key_registered_against_the_reprocessed_message() {
        let dir = tempdir().unwrap();
        let group =
            ConsumerGroup::<i32>::open(dir.path().join("wal.log"), LONG_TIMEOUT, EXHAUST_AFTER_TWO)
                .await
                .unwrap();

        let original_id = group.enqueue_with_key(1, key("order-42")).await.unwrap();
        let first = group.claim().await;
        group.nack(first.id, first.token, None).await.unwrap();
        let second = group.claim().await;
        group.nack(second.id, second.token, None).await.unwrap();

        assert!(group.reprocess_dead_letter(original_id).await.unwrap());

        // A retried enqueue now resolves to the reprocessed (live again)
        // message, not a new one.
        let retried_id = group.enqueue_with_key(1, key("order-42")).await.unwrap();
        assert_eq!(retried_id, original_id);
        assert_eq!(group.len().await, 1);
    }

    #[tokio::test]
    async fn dedup_survives_reopening_the_same_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let original_id = {
            let group =
                ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT, NO_RETRY_DELAY).await.unwrap();
            group.enqueue_with_key(1, key("order-42")).await.unwrap()
        };

        let recovered =
            ConsumerGroup::<i32>::open(&path, LONG_TIMEOUT, NO_RETRY_DELAY).await.unwrap();
        let retried_id = recovered.enqueue_with_key(2, key("order-42")).await.unwrap();

        assert_eq!(
            retried_id, original_id,
            "a producer retry after a restart must still be deduplicated"
        );
        assert_eq!(recovered.len().await, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_enqueues_with_the_same_key_produce_exactly_one_message() {
        let dir = tempdir().unwrap();
        let group = Arc::new(open(&dir, LONG_TIMEOUT).await);

        let mut attempts = Vec::new();
        for i in 0..16 {
            let group = Arc::clone(&group);
            attempts.push(tokio::spawn(async move {
                group.enqueue_with_key(i, key("order-42")).await.unwrap()
            }));
        }

        let mut ids = Vec::new();
        for attempt in attempts {
            ids.push(attempt.await.unwrap());
        }

        ids.sort_unstable();
        ids.dedup();
        assert_eq!(ids.len(), 1, "every concurrent attempt must resolve to the same single id");
        assert_eq!(group.len().await, 1);
    }
}
