//! A durable holding area for messages that ran out of retries.
//!
//! This module has no idea what "reprocessing" means, and doesn't touch
//! a live queue at all — [`DeadLetterQueue`] is purely a durable set of
//! [`DeadLetter`] records, keyed by message id, with a durable remove.
//! [`ConsumerGroup`](crate::ConsumerGroup) is what gives it meaning: it
//! decides when [`RetryPolicy`](crate::RetryPolicy) considers a message
//! exhausted, and it's the one that knows how to put a dead letter's
//! item back into a live queue on reprocessing. Keeping this type
//! ignorant of all that is what lets it double as the foundation for
//! `feature/llm-assisted-dlq-triage` (Phase 4) later — a triage agent
//! wants to list, inspect, and resolve dead letters, which is exactly
//! this type's whole API, without needing to know anything about
//! `ConsumerGroup`'s internals either.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use qaas_types::{MessageId, Timestamp};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::wal::Wal;

/// A message that exhausted its retry policy, with enough context to
/// let an operator — or, eventually, an LLM triage agent — decide what
/// to do with it: inspect why it failed, reprocess it, or discard it for
/// good.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetter<T> {
    /// The message's original identity, preserved from its first
    /// enqueue. Reprocessing keeps this id rather than minting a new
    /// one, so a message stays traceable across the round trip through
    /// the DLQ.
    pub id: MessageId,
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
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{DeadLetter, DeadLetterQueue};
    use qaas_types::{MessageId, Timestamp};

    fn sample(id: MessageId, item: i32) -> DeadLetter<i32> {
        DeadLetter {
            id,
            item,
            delivery_count: 5,
            last_error: Some("downstream returned 429".to_string()),
            dead_lettered_at: Timestamp::now(),
        }
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
}
