//! A durable holding area for messages that ran out of retries.
//!
//! This module has no idea what "reprocessing" means, and doesn't touch
//! a live queue at all — [`DeadLetterQueue`] is purely a durable set of
//! [`DeadLetter`] records, keyed by message id, with a durable remove.
//! [`ConsumerGroup`](crate::ConsumerGroup) is what gives it meaning: it
//! decides when [`RetryPolicy`](crate::RetryPolicy) considers a message
//! exhausted, and it's the one that knows how to put a dead letter's
//! item back into a live queue on reprocessing. Keeping this type
//! ignorant of all that is what let it double as the foundation for
//! `feature/llm-assisted-dlq-triage` (Phase 4): a triage agent lists,
//! inspects, and resolves dead letters, which was already this type's
//! whole API — [`annotate`](DeadLetterQueue::annotate) is the one
//! genuinely new operation that branch adds, letting a verdict attach to
//! an entry *without* resolving it, the same "record something durably
//! without ending this entry's life" shape
//! [`ConsumerGroup::checkpoint`](crate::ConsumerGroup::checkpoint)
//! already has for a live message's lease.
//!
//! [`DeadLetter::checkpoint`] is the other piece `feature/llm-assisted-dlq-triage`
//! adds: whatever a workflow last saved via `ConsumerGroup::checkpoint`
//! before this delivery exhausted its retries, carried onto the dead
//! letter itself. `feature/durable-agent-workflows` deliberately dropped
//! this on the exhausted path when it first threaded `checkpoint` through
//! — see `ConsumerGroup::resolve_failed_delivery`'s docs — precisely
//! because *this* is the branch whose entire point is making dead letters
//! diagnosable: knowing a workflow died on step 4 of 5 (an API call) versus
//! step 1 (argument validation) is exactly the signal a triage
//! classification needs, human or agent.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use qaas_types::{IdempotencyKey, MessageId, Timestamp};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Mutex;

use crate::wal::Wal;

/// A message that exhausted its retry policy, with enough context to
/// let an operator — or an LLM triage agent — decide what to do with it:
/// inspect why it failed, reprocess it, or discard it for good.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter<T> {
    /// The message's original identity, preserved from its first
    /// enqueue. Reprocessing keeps this id rather than minting a new
    /// one, so a message stays traceable across the round trip through
    /// the DLQ.
    pub id: MessageId,
    /// The producer-supplied idempotency key this message was enqueued
    /// with, if any — carried along so
    /// [`ConsumerGroup::reprocess_dead_letter`](crate::ConsumerGroup::reprocess_dead_letter)
    /// can re-register it and
    /// [`ConsumerGroup::purge_dead_letter`](crate::ConsumerGroup::purge_dead_letter)
    /// can release it; see `feature/idempotent-delivery`.
    pub idempotency_key: Option<IdempotencyKey>,
    /// The message payload.
    pub item: T,
    /// How many times this message was delivered in total before it was
    /// dead-lettered.
    pub delivery_count: u32,
    /// The failure reason from whichever delivery attempt exhausted the
    /// retry policy: the caller's own nack reason, or a synthetic one
    /// ("visibility timeout expired") if it was a silent lease expiry
    /// rather than an explicit nack. `None` if that final nack simply
    /// didn't provide one.
    pub last_error: Option<String>,
    /// When this message was dead-lettered.
    pub dead_lettered_at: Timestamp,
    /// Whatever was last saved via
    /// [`ConsumerGroup::checkpoint`](crate::ConsumerGroup::checkpoint)
    /// for this message before the delivery that exhausted its retries —
    /// `None` if it was never checkpointed. See this module's own docs
    /// for why this matters for triage specifically.
    pub checkpoint: Option<Value>,
    /// A triage agent's (or a human's) verdict on this failure, if one
    /// has been recorded via [`DeadLetterQueue::annotate`]. `None` until
    /// something triages it.
    pub triage: Option<TriageVerdict>,
}

/// Whether a dead-lettered failure looks worth retrying automatically, or
/// needs a person to look at it before anything happens to it again.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TriageClassification {
    /// Looks like a failure that might well succeed on a fresh attempt —
    /// a rate limit, a timeout, a downstream blip. Safe to retry
    /// automatically; this classification is what
    /// `qaas-server`'s `triage_dead_letter` tool treats as "auto-apply
    /// reprocessing."
    Transient,
    /// Looks like a failure that will keep failing no matter how many
    /// times it's retried — bad input, a genuine bug, a business-rule
    /// rejection. Deliberately never auto-resolved by this crate: a
    /// permanent failure is held, annotated, in the DLQ until a human
    /// decides to reprocess it anyway or purge it — never silently
    /// discarded just because something classified it this way. See
    /// `qaas-server`'s `triage_dead_letter` docs for why auto-purge
    /// specifically is off the table.
    Permanent,
}

/// A recorded triage verdict on a [`DeadLetter`] — see
/// [`DeadLetterQueue::annotate`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriageVerdict {
    /// The classification itself.
    pub classification: TriageClassification,
    /// Why — whatever explanation the triage agent or human gave.
    /// Free-form; this crate has no opinion on its shape.
    pub reason: String,
    /// When this verdict was recorded.
    pub triaged_at: Timestamp,
}

/// One entry in a [`DeadLetterQueue`]'s WAL.
#[derive(Serialize, Deserialize)]
enum WalRecord<T> {
    DeadLetter(DeadLetter<T>),
    /// The dead letter with this id left the queue — reprocessed back
    /// into a live queue, or purged outright. Replay doesn't need to
    /// distinguish which: both mean "this id is no longer a dead
    /// letter," and preserving *why* it left is an audit-log concern
    /// (`feature/structured-audit-logging`), not this branch's.
    Remove(MessageId),
    /// A triage verdict was attached to an existing dead letter, via
    /// [`DeadLetterQueue::annotate`]. Replayed as a no-op if the entry
    /// it names isn't present — see that method's own docs on the one
    /// narrow, harmless race that can produce exactly that.
    Annotate(MessageId, TriageVerdict),
}

/// A durable, inspectable set of [`DeadLetter`] records.
pub struct DeadLetterQueue<T> {
    entries: Mutex<HashMap<MessageId, DeadLetter<T>>>,
    wal: Wal<WalRecord<T>>,
}

impl<T: Serialize + DeserializeOwned> DeadLetterQueue<T> {
    /// Opens the WAL at `path` (creating it if it doesn't exist) and
    /// replays it to rebuild the current set of dead letters.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as
    /// [`Wal::open`](crate::wal::Wal::open).
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let (wal, records) = Wal::open(path).await?;
        let mut entries = HashMap::new();
        for record in records {
            match record {
                WalRecord::DeadLetter(dead_letter) => {
                    entries.insert(dead_letter.id, dead_letter);
                }
                WalRecord::Remove(id) => {
                    entries.remove(&id);
                }
                WalRecord::Annotate(id, verdict) => {
                    if let Some(entry) = entries.get_mut(&id) {
                        entry.triage = Some(verdict);
                    }
                }
            }
        }
        Ok(Self { entries: Mutex::new(entries), wal })
    }

    /// Durably records `dead_letter`.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails; `dead_letter` is not
    /// recorded in that case.
    pub async fn record(&self, dead_letter: DeadLetter<T>) -> io::Result<()> {
        let record = WalRecord::DeadLetter(dead_letter);
        self.wal.append(&record).await?;

        let WalRecord::DeadLetter(dead_letter) = record else {
            unreachable!("record was just constructed as DeadLetter")
        };
        self.entries.lock().await.insert(dead_letter.id, dead_letter);
        Ok(())
    }

    /// Every dead letter currently held, in no particular order.
    pub async fn list(&self) -> Vec<DeadLetter<T>>
    where
        T: Clone,
    {
        self.entries.lock().await.values().cloned().collect()
    }

    /// The id and idempotency key (if any) of every dead letter
    /// currently held. Doesn't need `T: Clone` the way
    /// [`list`](Self::list) does — for a caller that only needs to know
    /// *which* messages and keys are dead-lettered, not their payloads
    /// (`ConsumerGroup::open` rebuilding its dedup map after a restart,
    /// specifically), this avoids requiring a bound the type it's
    /// holding might not have.
    pub async fn ids_and_keys(&self) -> Vec<(MessageId, Option<IdempotencyKey>)> {
        self.entries
            .lock()
            .await
            .values()
            .map(|dead_letter| (dead_letter.id, dead_letter.idempotency_key.clone()))
            .collect()
    }

    /// The number of dead letters currently held.
    pub async fn len(&self) -> usize {
        self.entries.lock().await.len()
    }

    /// Whether there are no dead letters at all.
    pub async fn is_empty(&self) -> bool {
        self.entries.lock().await.is_empty()
    }

    /// Durably removes and returns the dead letter with `id`, if any.
    /// The caller decides what that removal means — reprocessing it
    /// elsewhere, or discarding it for good.
    ///
    /// Removes from the in-memory set before writing the durable
    /// `Remove` record, not after — consistent with how every other
    /// WAL-backed type in this crate favors "a message can resurrect
    /// after a crash" over "a message can be silently lost": if the WAL
    /// write below fails, the in-memory removal has already happened
    /// (so this process won't see `id` in [`list`](Self::list) again),
    /// but nothing durably recorded it leaving, so a restart correctly
    /// brings it back rather than losing it for good.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails, after the entry has
    /// already been removed from memory (see above).
    pub async fn take(&self, id: MessageId) -> io::Result<Option<DeadLetter<T>>> {
        let dead_letter = self.entries.lock().await.remove(&id);
        let Some(dead_letter) = dead_letter else {
            return Ok(None);
        };
        self.wal.append(&WalRecord::Remove(id)).await?;
        Ok(Some(dead_letter))
    }

    /// Durably attaches `verdict` to the dead letter `id`, without
    /// resolving it — it stays exactly where it was, just with a triage
    /// verdict now visible on it. Returns `Ok(false)` without effect if
    /// no dead letter with `id` currently exists (already reprocessed or
    /// purged, or never existed).
    ///
    /// There's one narrow, harmless race here, the same shape documented
    /// on [`ConsumerGroup::checkpoint`](crate::ConsumerGroup::checkpoint):
    /// `id` is checked before the (async, fsync-backed) WAL write and
    /// again after, and if a concurrent [`take`](Self::take) removed it
    /// in between, this still returns `Ok(false)` — the entry genuinely
    /// wasn't annotated — even though `verdict` was already durably
    /// written by then. That stray record is harmless: replay only
    /// applies an `Annotate` record to an entry that still exists (see
    /// [`open`](Self::open)), so a dangling one for an id that's since
    /// been removed simply does nothing.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails; `verdict` is not
    /// recorded in that case, and the entry (if it still exists) is left
    /// exactly as it was before this call.
    pub async fn annotate(&self, id: MessageId, verdict: TriageVerdict) -> io::Result<bool> {
        {
            let entries = self.entries.lock().await;
            if !entries.contains_key(&id) {
                return Ok(false);
            }
        }

        self.wal.append(&WalRecord::Annotate(id, verdict.clone())).await?;

        let mut entries = self.entries.lock().await;
        match entries.get_mut(&id) {
            Some(entry) => {
                entry.triage = Some(verdict);
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{DeadLetter, DeadLetterQueue, TriageClassification, TriageVerdict};
    use qaas_types::{MessageId, Timestamp};

    fn sample(id: MessageId, item: i32) -> DeadLetter<i32> {
        DeadLetter {
            id,
            idempotency_key: None,
            item,
            delivery_count: 5,
            last_error: Some("downstream returned 429".to_string()),
            dead_lettered_at: Timestamp::now(),
            checkpoint: None,
            triage: None,
        }
    }

    fn verdict(classification: TriageClassification, reason: &str) -> TriageVerdict {
        TriageVerdict { classification, reason: reason.to_string(), triaged_at: Timestamp::now() }
    }

    #[tokio::test]
    async fn recorded_dead_letters_appear_in_list() {
        let dir = tempdir().unwrap();
        let dlq = DeadLetterQueue::open(dir.path().join("dlq.log")).await.unwrap();
        let id = MessageId::new();
        dlq.record(sample(id, 42)).await.unwrap();

        assert_eq!(dlq.len().await, 1);
        let listed = dlq.list().await;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].id, id);
        assert_eq!(listed[0].item, 42);
        assert_eq!(listed[0].delivery_count, 5);
        assert_eq!(listed[0].last_error.as_deref(), Some("downstream returned 429"));
    }

    #[tokio::test]
    async fn take_removes_and_returns_the_entry() {
        let dir = tempdir().unwrap();
        let dlq = DeadLetterQueue::open(dir.path().join("dlq.log")).await.unwrap();
        let id = MessageId::new();
        dlq.record(sample(id, 1)).await.unwrap();

        let taken = dlq.take(id).await.unwrap();
        assert_eq!(taken.unwrap().id, id);
        assert!(dlq.is_empty().await);
    }

    #[tokio::test]
    async fn taking_an_unknown_id_returns_none_without_error() {
        let dir = tempdir().unwrap();
        let dlq = DeadLetterQueue::<i32>::open(dir.path().join("dlq.log")).await.unwrap();
        assert!(dlq.take(MessageId::new()).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn state_survives_reopening_the_same_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dlq.log");
        let (kept_id, taken_id) = (MessageId::new(), MessageId::new());

        {
            let dlq = DeadLetterQueue::open(&path).await.unwrap();
            dlq.record(sample(kept_id, 1)).await.unwrap();
            dlq.record(sample(taken_id, 2)).await.unwrap();
            dlq.take(taken_id).await.unwrap();
        }

        let recovered = DeadLetterQueue::<i32>::open(&path).await.unwrap();
        assert_eq!(recovered.len().await, 1);
        let listed = recovered.list().await;
        assert_eq!(listed[0].id, kept_id);
    }

    #[tokio::test]
    async fn ids_and_keys_reports_every_entry_without_requiring_clone() {
        let dir = tempdir().unwrap();
        let dlq = DeadLetterQueue::open(dir.path().join("dlq.log")).await.unwrap();
        let id = MessageId::new();
        let key = qaas_types::IdempotencyKey::new("order-42").unwrap();

        let mut with_key = sample(id, 1);
        with_key.idempotency_key = Some(key.clone());
        dlq.record(with_key).await.unwrap();

        let entries = dlq.ids_and_keys().await;
        assert_eq!(entries, vec![(id, Some(key))]);
    }

    #[tokio::test]
    async fn annotate_attaches_a_verdict_without_removing_the_entry() {
        let dir = tempdir().unwrap();
        let dlq = DeadLetterQueue::open(dir.path().join("dlq.log")).await.unwrap();
        let id = MessageId::new();
        dlq.record(sample(id, 1)).await.unwrap();

        let applied = dlq
            .annotate(id, verdict(TriageClassification::Transient, "looks like a rate limit"))
            .await
            .unwrap();
        assert!(applied);

        assert_eq!(dlq.len().await, 1, "annotating must not resolve the entry");
        let listed = dlq.list().await;
        let triage = listed[0].triage.as_ref().unwrap();
        assert_eq!(triage.classification, TriageClassification::Transient);
        assert_eq!(triage.reason, "looks like a rate limit");
    }

    #[tokio::test]
    async fn annotating_an_unknown_id_is_a_false_not_an_error() {
        let dir = tempdir().unwrap();
        let dlq = DeadLetterQueue::<i32>::open(dir.path().join("dlq.log")).await.unwrap();
        let applied = dlq
            .annotate(MessageId::new(), verdict(TriageClassification::Permanent, "no such entry"))
            .await
            .unwrap();
        assert!(!applied);
    }

    #[tokio::test]
    async fn a_later_annotation_replaces_an_earlier_one() {
        let dir = tempdir().unwrap();
        let dlq = DeadLetterQueue::open(dir.path().join("dlq.log")).await.unwrap();
        let id = MessageId::new();
        dlq.record(sample(id, 1)).await.unwrap();

        dlq.annotate(id, verdict(TriageClassification::Transient, "first guess")).await.unwrap();
        dlq.annotate(id, verdict(TriageClassification::Permanent, "actually, no")).await.unwrap();

        let listed = dlq.list().await;
        let triage = listed[0].triage.as_ref().unwrap();
        assert_eq!(triage.classification, TriageClassification::Permanent);
        assert_eq!(triage.reason, "actually, no");
    }

    #[tokio::test]
    async fn a_triage_verdict_survives_reopening_the_same_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("dlq.log");
        let id = MessageId::new();
        {
            let dlq = DeadLetterQueue::open(&path).await.unwrap();
            dlq.record(sample(id, 1)).await.unwrap();
            dlq.annotate(id, verdict(TriageClassification::Permanent, "bad input")).await.unwrap();
        }

        let recovered = DeadLetterQueue::<i32>::open(&path).await.unwrap();
        let listed = recovered.list().await;
        let triage = listed[0].triage.as_ref().unwrap();
        assert_eq!(triage.classification, TriageClassification::Permanent);
        assert_eq!(triage.reason, "bad input");
    }

    #[tokio::test]
    async fn a_dangling_annotation_for_an_already_removed_entry_replays_as_a_no_op() {
        // Reproduces the race `annotate`'s own docs describe: a WAL can
        // end up with an `Annotate` record for an id that's since been
        // removed. Replay must not resurrect the entry or panic on it.
        let dir = tempdir().unwrap();
        let path = dir.path().join("dlq.log");
        let id = MessageId::new();
        {
            let dlq = DeadLetterQueue::open(&path).await.unwrap();
            dlq.record(sample(id, 1)).await.unwrap();
            dlq.annotate(id, verdict(TriageClassification::Transient, "will be removed"))
                .await
                .unwrap();
            dlq.take(id).await.unwrap();
        }

        let recovered = DeadLetterQueue::<i32>::open(&path).await.unwrap();
        assert!(recovered.is_empty().await);
    }

    #[tokio::test]
    async fn checkpoint_carries_onto_the_dead_letter() {
        let dir = tempdir().unwrap();
        let dlq = DeadLetterQueue::open(dir.path().join("dlq.log")).await.unwrap();
        let id = MessageId::new();

        let mut entry = sample(id, 1);
        entry.checkpoint = Some(serde_json::json!({"completed_steps": ["fetch"]}));
        dlq.record(entry).await.unwrap();

        let listed = dlq.list().await;
        assert_eq!(listed[0].checkpoint, Some(serde_json::json!({"completed_steps": ["fetch"]})));
    }
}
