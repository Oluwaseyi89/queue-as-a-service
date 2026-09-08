use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

use tokio::sync::{Mutex, Notify};

/// A message's priority within a [`PriorityQueue`]. Higher values are
/// dequeued before lower ones; `Priority(0)` (the default) is the lowest
/// priority a message can have.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Priority(pub u8);

/// A single slot in the heap: the payload plus enough bookkeeping to
/// order it. `sequence` breaks ties between equal priorities — without
/// it, `BinaryHeap` gives no ordering guarantee at all among equal
/// elements, so two same-priority messages could come back out in either
/// order, including reversed from how they were enqueued.
struct Entry<T> {
    priority: Priority,
    sequence: u64,
    item: T,
}

impl<T> PartialEq for Entry<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.sequence == other.sequence
    }
}

impl<T> Eq for Entry<T> {}

impl<T> PartialOrd for Entry<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for Entry<T> {
    fn cmp(&self, other: &Self) -> Ordering {
        // `BinaryHeap` is a max-heap, so higher priority must compare as
        // "greater". Within equal priority, the *earlier* sequence number
        // must compare as "greater" so it's popped first (FIFO among
        // equal priorities) — hence comparing `other.sequence` against
        // `self.sequence`, the reverse of the natural order.
        self.priority.cmp(&other.priority).then_with(|| other.sequence.cmp(&self.sequence))
    }
}

/// An async-safe, in-memory priority queue.
///
/// Dequeues the highest-[`Priority`] item first; among items of equal
/// priority, dequeues them in the order they were enqueued (FIFO), the
/// same tie-breaking behavior most callers expect from "priority queue"
/// even though a bare `BinaryHeap` doesn't provide it.
///
/// Shares [`FifoQueue`](super::FifoQueue)'s design: `&self`-only methods
/// meant to be used behind an `Arc`, and optional bounded capacity that
/// makes `enqueue` apply backpressure instead of growing without limit.
pub struct PriorityQueue<T> {
    items: Mutex<BinaryHeap<Entry<T>>>,
    capacity: Option<NonZeroUsize>,
    next_sequence: AtomicU64,
    not_empty: Notify,
    not_full: Notify,
}

impl<T> PriorityQueue<T> {
    /// Creates a queue with no capacity limit.
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            items: Mutex::new(BinaryHeap::new()),
            capacity: None,
            next_sequence: AtomicU64::new(0),
            not_empty: Notify::new(),
            not_full: Notify::new(),
        }
    }

    /// Creates a queue that holds at most `capacity` items. `enqueue`
    /// waits for room once the queue is full, instead of growing past it.
    #[must_use]
    pub fn bounded(capacity: NonZeroUsize) -> Self {
        Self {
            items: Mutex::new(BinaryHeap::new()),
            capacity: Some(capacity),
            next_sequence: AtomicU64::new(0),
            not_empty: Notify::new(),
            not_full: Notify::new(),
        }
    }

    /// This queue's capacity limit, or `None` if it's unbounded.
    #[must_use]
    pub fn capacity(&self) -> Option<NonZeroUsize> {
        self.capacity
    }

    /// The number of items currently in the queue.
    pub async fn len(&self) -> usize {
        self.items.lock().await.len()
    }

    /// Whether the queue currently holds no items.
    pub async fn is_empty(&self) -> bool {
        self.items.lock().await.is_empty()
    }

    /// Pushes `item` onto the queue at the given priority, waiting for
    /// room if the queue is at capacity.
    ///
    /// See [`FifoQueue::enqueue`](super::FifoQueue::enqueue) for why this
    /// is a check-then-wait loop.
    pub async fn enqueue(&self, item: T, priority: Priority) {
        loop {
            {
                let mut guard = self.items.lock().await;
                if self.has_room(guard.len()) {
                    guard.push(self.entry(item, priority));
                    drop(guard);
                    self.not_empty.notify_one();
                    return;
                }
            }
            self.not_full.notified().await;
        }
    }

    /// Pushes `item` onto the queue at the given priority without
    /// waiting.
    ///
    /// # Errors
    ///
    /// Returns `item` back, unmodified, if the queue is at capacity.
    pub async fn try_enqueue(&self, item: T, priority: Priority) -> Result<(), T> {
        let mut guard = self.items.lock().await;
        if self.has_room(guard.len()) {
            guard.push(self.entry(item, priority));
            drop(guard);
            self.not_empty.notify_one();
            Ok(())
        } else {
            Err(item)
        }
    }

    /// Pops the highest-priority item, waiting until one is available if
    /// the queue is currently empty.
    pub async fn dequeue(&self) -> T {
        loop {
            {
                let mut guard = self.items.lock().await;
                if let Some(entry) = guard.pop() {
                    drop(guard);
                    self.not_full.notify_one();
                    return entry.item;
                }
            }
            self.not_empty.notified().await;
        }
    }

    /// Pops the highest-priority item without waiting. Returns `None` if
    /// the queue is currently empty.
    pub async fn try_dequeue(&self) -> Option<T> {
        let mut guard = self.items.lock().await;
        let entry = guard.pop();
        if entry.is_some() {
            drop(guard);
            self.not_full.notify_one();
        }
        entry.map(|entry| entry.item)
    }

    fn has_room(&self, current_len: usize) -> bool {
        self.capacity.is_none_or(|cap| current_len < cap.get())
    }

    fn entry(&self, item: T, priority: Priority) -> Entry<T> {
        Entry { priority, sequence: self.next_sequence.fetch_add(1, AtomicOrdering::Relaxed), item }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use super::{Priority, PriorityQueue};

    #[tokio::test]
    async fn dequeues_highest_priority_first() {
        let queue = PriorityQueue::unbounded();
        queue.enqueue("low", Priority(1)).await;
        queue.enqueue("high", Priority(10)).await;
        queue.enqueue("medium", Priority(5)).await;

        assert_eq!(queue.dequeue().await, "high");
        assert_eq!(queue.dequeue().await, "medium");
        assert_eq!(queue.dequeue().await, "low");
    }

    #[tokio::test]
    async fn equal_priority_items_dequeue_in_fifo_order() {
        let queue = PriorityQueue::unbounded();
        queue.enqueue(1, Priority(5)).await;
        queue.enqueue(2, Priority(5)).await;
        queue.enqueue(3, Priority(5)).await;

        assert_eq!(queue.dequeue().await, 1);
        assert_eq!(queue.dequeue().await, 2);
        assert_eq!(queue.dequeue().await, 3);
    }

    #[tokio::test]
    async fn a_later_higher_priority_item_still_jumps_the_queue() {
        let queue = PriorityQueue::unbounded();
        queue.enqueue("first-low", Priority(1)).await;
        queue.enqueue("second-low", Priority(1)).await;
        queue.enqueue("urgent", Priority(9)).await;

        assert_eq!(queue.dequeue().await, "urgent");
        assert_eq!(queue.dequeue().await, "first-low");
        assert_eq!(queue.dequeue().await, "second-low");
    }

    #[tokio::test]
    async fn len_and_is_empty_track_contents() {
        let queue = PriorityQueue::unbounded();
        assert!(queue.is_empty().await);

        queue.enqueue(1, Priority::default()).await;
        assert!(!queue.is_empty().await);
        assert_eq!(queue.len().await, 1);

        queue.dequeue().await;
        assert!(queue.is_empty().await);
    }

    #[tokio::test]
    async fn try_dequeue_on_empty_queue_returns_none() {
        let queue: PriorityQueue<i32> = PriorityQueue::unbounded();
        assert_eq!(queue.try_dequeue().await, None);
    }

    #[tokio::test]
    async fn try_enqueue_at_capacity_returns_the_item_back() {
        let queue = PriorityQueue::bounded(NonZeroUsize::new(1).unwrap());
        queue.try_enqueue(1, Priority::default()).await.unwrap();

        assert_eq!(queue.try_enqueue(2, Priority::default()).await, Err(2));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enqueue_blocks_until_capacity_is_available() {
        let queue = Arc::new(PriorityQueue::bounded(NonZeroUsize::new(1).unwrap()));
        queue.enqueue(1, Priority::default()).await;

        let blocked = Arc::clone(&queue);
        let enqueue_task =
            tokio::spawn(async move { blocked.enqueue(2, Priority::default()).await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!enqueue_task.is_finished());

        assert_eq!(queue.dequeue().await, 1);
        enqueue_task.await.unwrap();
        assert_eq!(queue.dequeue().await, 2);
    }
}
