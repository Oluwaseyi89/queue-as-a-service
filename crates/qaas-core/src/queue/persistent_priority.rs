use std::io;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::{Priority, PriorityQueue};
use crate::wal::Wal;

/// One entry in a [`PersistentPriorityQueue`]'s WAL. Same shape and same
/// reasoning as [`PersistentFifoQueue`](super::PersistentFifoQueue)'s
/// own (private) WAL record type — see that type's docs — except
/// `Enqueue` also carries the [`Priority`] it was enqueued with, since
/// replay has to reproduce the exact ordering, not just the set of
/// items.
#[derive(Serialize, Deserialize)]
enum WalRecord<T> {
    Enqueue(T, Priority),
    Dequeue,
}

/// A [`PriorityQueue`] whose enqueues and dequeues survive process
/// restarts and crashes, backed by a [`Wal`].
///
/// See [`PersistentFifoQueue`](super::PersistentFifoQueue)'s docs for the
/// design this mirrors: a wrapper rather than a modification of
/// `PriorityQueue`, and the same at-least-once behavior around dequeue
/// (no ack/redelivery tracking here — that's `feature/consumer-groups`).
pub struct PersistentPriorityQueue<T> {
    queue: PriorityQueue<T>,
    wal: Wal<WalRecord<T>>,
}

impl<T: Serialize + DeserializeOwned> PersistentPriorityQueue<T> {
    /// Opens the WAL at `path` (creating it if it doesn't exist),
    /// replays it to rebuild in-memory state, and returns a queue ready
    /// for use.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as
    /// [`Wal::open`](crate::wal::Wal::open).
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let (wal, records) = Wal::open(path).await?;
        let queue = PriorityQueue::unbounded();
        for record in records {
            match record {
                WalRecord::Enqueue(item, priority) => queue.enqueue(item, priority).await,
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

    /// Durably logs and then enqueues `item` at the given priority. Only
    /// returns once the WAL write backing this enqueue has been fsynced.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails; `item` is not enqueued in
    /// that case.
    pub async fn enqueue(&self, item: T, priority: Priority) -> io::Result<()> {
        let record = WalRecord::Enqueue(item, priority);
        self.wal.append(&record).await?;

        let WalRecord::Enqueue(item, priority) = record else {
            unreachable!("record was just constructed as Enqueue")
        };
        self.queue.enqueue(item, priority).await;
        Ok(())
    }

    /// Dequeues the highest-priority item, waiting until one is
    /// available, then durably logs that it left.
    ///
    /// See [`PersistentFifoQueue::dequeue`](super::PersistentFifoQueue::dequeue)
    /// for why the item can still be recoverable on the next restart even
    /// after this call has returned it.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails. The item has already
    /// been removed from the in-memory queue at that point.
    pub async fn dequeue(&self) -> io::Result<T> {
        let item = self.queue.dequeue().await;
        self.wal.append(&WalRecord::Dequeue).await?;
        Ok(item)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::PersistentPriorityQueue;
    use crate::queue::Priority;

    #[tokio::test]
    async fn dequeues_highest_priority_first() {
        let dir = tempdir().unwrap();
        let queue =
            PersistentPriorityQueue::<String>::open(dir.path().join("wal.log")).await.unwrap();

        queue.enqueue("low".to_string(), Priority(1)).await.unwrap();
        queue.enqueue("high".to_string(), Priority(10)).await.unwrap();

        assert_eq!(queue.dequeue().await.unwrap(), "high");
        assert_eq!(queue.dequeue().await.unwrap(), "low");
    }

    #[tokio::test]
    async fn priority_and_order_both_survive_reopening_the_same_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal.log");

        {
            let queue = PersistentPriorityQueue::<String>::open(&path).await.unwrap();
            queue.enqueue("first-low".to_string(), Priority(1)).await.unwrap();
            queue.enqueue("second-low".to_string(), Priority(1)).await.unwrap();
            queue.enqueue("urgent".to_string(), Priority(9)).await.unwrap();
            assert_eq!(queue.dequeue().await.unwrap(), "urgent");
        }

        let recovered = PersistentPriorityQueue::<String>::open(&path).await.unwrap();
        assert_eq!(recovered.len().await, 2);
        assert_eq!(recovered.dequeue().await.unwrap(), "first-low");
        assert_eq!(recovered.dequeue().await.unwrap(), "second-low");
    }
}
