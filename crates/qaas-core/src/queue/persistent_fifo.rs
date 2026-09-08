use std::io;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::FifoQueue;
use crate::wal::Wal;

/// One entry in a [`PersistentFifoQueue`]'s WAL: not the state itself,
/// but the operation that produced it. Replaying every `Enqueue` and
/// `Dequeue` in order, against an empty queue, reconstructs exactly the
/// state the queue was in — this is what makes it a *write-ahead* log
/// rather than a snapshot: the log is the source of truth, and the
/// in-memory queue is only ever a cache rebuilt from it.
///
/// `Dequeue` carries no payload. It doesn't need one — replaying it just
/// means "pop and discard whatever's at the front" — and logging it at
/// all matters: without it, every item this queue ever dequeued before a
/// crash would reappear on the next restart, since nothing on disk would
/// say it had ever left.
#[derive(Serialize, Deserialize)]
enum WalRecord<T> {
    Enqueue(T),
    Dequeue,
}

/// A [`FifoQueue`] whose enqueues and dequeues survive process restarts
/// and crashes, backed by a [`Wal`].
///
/// This is deliberately a wrapper around `FifoQueue`, not a modification
/// of it: `FifoQueue` stays usable on its own (in tests, or anywhere
/// durability genuinely isn't needed) with none of the I/O or fsync cost,
/// and this type adds exactly the durability layer on top without
/// touching it.
///
/// There's no delivery-semantics story here yet — no ack, no redelivery
/// tracking, no visibility timeout (`feature/consumer-groups`). A crash
/// between this queue handing an item to a caller and that caller
/// finishing whatever it was doing with it means the item comes back on
/// the next restart, because the corresponding `Dequeue` record was
/// never durably written. That's at-least-once behavior by construction,
/// not a gap to close here — closing it with proper acknowledgement is
/// what `feature/consumer-groups` is for.
pub struct PersistentFifoQueue<T> {
    queue: FifoQueue<T>,
    wal: Wal<WalRecord<T>>,
}

impl<T: Serialize + DeserializeOwned> PersistentFifoQueue<T> {
    /// Opens the WAL at `path` (creating it if it doesn't exist),
    /// replays it to rebuild in-memory state, and returns a queue ready
    /// for use — already containing whatever items were left enqueued
    /// the last time this process (or a previous one) ran.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as
    /// [`Wal::open`](crate::wal::Wal::open).
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let (wal, records) = Wal::open(path).await?;
        let queue = FifoQueue::unbounded();
        for record in records {
            match record {
                WalRecord::Enqueue(item) => queue.enqueue(item).await,
                WalRecord::Dequeue => {
                    queue.try_dequeue().await;
                }
            }
        }
        Ok(Self { queue, wal })
    }

    /// The number of items currently in the queue.
    pub async fn len(&self) -> usize {
        self.queue.len().await
    }

    /// Whether the queue currently holds no items.
    pub async fn is_empty(&self) -> bool {
        self.queue.is_empty().await
    }

    /// Durably logs and then enqueues `item`. Only returns once the WAL
    /// write backing this enqueue has been fsynced — on `Ok`, `item` will
    /// still be here after a crash, even one that happens immediately
    /// after this call returns.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails; `item` is not enqueued in
    /// that case.
    pub async fn enqueue(&self, item: T) -> io::Result<()> {
        let record = WalRecord::Enqueue(item);
        self.wal.append(&record).await?;

        // Move `item` back out of the record we just wrote, rather than
        // requiring `T: Clone` to keep a copy around for the in-memory
        // queue. The WAL write already borrowed it by reference — it was
        // never actually consumed until this point.
        let WalRecord::Enqueue(item) = record else {
            unreachable!("record was just constructed as Enqueue")
        };
        self.queue.enqueue(item).await;
        Ok(())
    }

    /// Dequeues the item at the front of the queue, waiting until one is
    /// available, then durably logs that it left.
    ///
    /// The item is removed from the in-memory queue and handed back to
    /// the caller *before* the corresponding `Dequeue` record is fsynced.
    /// A crash in that window means the item is still recoverable — the
    /// on-disk log never recorded it leaving, so the next
    /// [`open`](Self::open) replays it right back into the queue. See
    /// this type's own docs for why that's intended, not a bug.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails. The item has already
    /// been removed from the in-memory queue at that point and is not
    /// put back — it's still recoverable on the next restart, per the
    /// at-least-once behavior described above, but it is gone from this
    /// running process.
    pub async fn dequeue(&self) -> io::Result<T> {
        let item = self.queue.dequeue().await;
        self.wal.append(&WalRecord::Dequeue).await?;
        Ok(item)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::PersistentFifoQueue;

    #[tokio::test]
    async fn enqueue_then_dequeue_round_trips_in_fifo_order() {
        let dir = tempdir().unwrap();
        let queue = PersistentFifoQueue::open(dir.path().join("wal.log")).await.unwrap();

        queue.enqueue(1).await.unwrap();
        queue.enqueue(2).await.unwrap();
        queue.enqueue(3).await.unwrap();

        assert_eq!(queue.dequeue().await.unwrap(), 1);
        assert_eq!(queue.dequeue().await.unwrap(), 2);
        assert_eq!(queue.len().await, 1);
    }

    #[tokio::test]
    async fn state_survives_reopening_the_same_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let queue = PersistentFifoQueue::<String>::open(&path).await.unwrap();
            queue.enqueue("a".to_string()).await.unwrap();
            queue.enqueue("b".to_string()).await.unwrap();
            queue.enqueue("c".to_string()).await.unwrap();
            // One of the three is consumed and durably logged as such
            // before this scope ends — it must not come back.
            assert_eq!(queue.dequeue().await.unwrap(), "a");
        }

        let recovered = PersistentFifoQueue::<String>::open(&path).await.unwrap();
        assert_eq!(recovered.len().await, 2);
        assert_eq!(recovered.dequeue().await.unwrap(), "b");
        assert_eq!(recovered.dequeue().await.unwrap(), "c");
    }

    #[tokio::test]
    async fn a_torn_final_record_is_dropped_and_the_file_is_healed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let queue = PersistentFifoQueue::open(&path).await.unwrap();
            queue.enqueue(1).await.unwrap();
            queue.enqueue(2).await.unwrap();
        }
        let len_after_two_good_records = tokio::fs::metadata(&path).await.unwrap().len();

        // Simulate a crash mid-append: a few garbage bytes land after the
        // two good records — shorter than any real header, let alone a
        // full header+payload, so replay must treat it as torn.
        {
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new().append(true).open(&path).await.unwrap();
            file.write_all(&[0xAA; 6]).await.unwrap();
        }

        let recovered = PersistentFifoQueue::<i32>::open(&path).await.unwrap();
        assert_eq!(recovered.len().await, 2);

        // The torn tail must actually have been truncated away by `open`
        // itself, not just skipped in memory — otherwise this heals once
        // and then breaks replay again on the *next* restart. Check this
        // before doing anything else with `recovered`: every dequeue
        // below appends its own WAL record and grows the file again, so
        // this assertion has to happen first or it's checking the wrong
        // thing.
        let healed_len = tokio::fs::metadata(&path).await.unwrap().len();
        assert_eq!(healed_len, len_after_two_good_records);

        assert_eq!(recovered.dequeue().await.unwrap(), 1);
        assert_eq!(recovered.dequeue().await.unwrap(), 2);
    }
}
