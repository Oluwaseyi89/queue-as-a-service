use std::collections::VecDeque;
use std::num::NonZeroUsize;

use tokio::sync::{Mutex, Notify};

/// An async-safe, in-memory, strictly first-in-first-out queue.
///
/// Meant to be shared across tasks behind an `Arc` — every method takes
/// `&self`, not `&mut self`, so cloning the queue itself isn't how you
/// share it. There is no owned-payload draining API: the only way to get
/// an item out is [`dequeue`](FifoQueue::dequeue) or
/// [`try_dequeue`](FifoQueue::try_dequeue).
///
/// Capacity is optional. An unbounded queue never blocks on `enqueue` and
/// can grow without limit; a bounded one applies backpressure by making
/// `enqueue` wait for room instead of failing or growing past its limit.
/// Real production queues want bounded capacity — an unbounded queue in
/// front of a slow consumer is an unbounded-memory-growth bug waiting to
/// happen — but the choice is left to the caller rather than baked in,
/// since tests and short-lived examples often don't care.
pub struct FifoQueue<T> {
    items: Mutex<VecDeque<T>>,
    capacity: Option<NonZeroUsize>,
    // Signaled after a successful `enqueue`; `dequeue` waits on this when
    // the queue is empty. Signaled after a successful `dequeue`; `enqueue`
    // waits on this when the queue is at capacity.
    not_empty: Notify,
    not_full: Notify,
}

impl<T> FifoQueue<T> {
    /// Creates a queue with no capacity limit.
    #[must_use]
    pub fn unbounded() -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            capacity: None,
            not_empty: Notify::new(),
            not_full: Notify::new(),
        }
    }

    /// Creates a queue that holds at most `capacity` items. `enqueue`
    /// waits for room once the queue is full, instead of growing past it.
    #[must_use]
    pub fn bounded(capacity: NonZeroUsize) -> Self {
        Self {
            items: Mutex::new(VecDeque::new()),
            capacity: Some(capacity),
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

    /// Pushes `item` onto the back of the queue, waiting for room if the
    /// queue is at capacity.
    ///
    /// This loops on a check-then-wait pattern (lock, check capacity,
    /// either push or wait on `not_full` and retry) rather than a single
    /// wait, which is the pattern `tokio::sync::Notify`'s own
    /// documentation recommends: it makes the wait level-triggered
    /// instead of edge-triggered, so a spurious or coalesced wakeup just
    /// re-checks the condition instead of being lost.
    pub async fn enqueue(&self, item: T) {
        loop {
            {
                let mut guard = self.items.lock().await;
                if self.has_room(guard.len()) {
                    guard.push_back(item);
                    drop(guard);
                    self.not_empty.notify_one();
                    return;
                }
            }
            // The lock above is already dropped before we wait — never
            // hold it across an `.await`, or every other task blocks on
            // the mutex, not just on capacity.
            self.not_full.notified().await;
            // Loop back around and re-check: another task may have
            // grabbed the room we were just notified about.
        }
    }

    /// Pushes `item` onto the back of the queue without waiting.
    ///
    /// # Errors
    ///
    /// Returns `item` back, unmodified, if the queue is at capacity.
    pub async fn try_enqueue(&self, item: T) -> Result<(), T> {
        let mut guard = self.items.lock().await;
        if self.has_room(guard.len()) {
            guard.push_back(item);
            drop(guard);
            self.not_empty.notify_one();
            Ok(())
        } else {
            Err(item)
        }
    }

    /// Pops the item at the front of the queue, waiting until one is
    /// available if the queue is currently empty.
    ///
    /// See [`enqueue`](Self::enqueue) for why this is a check-then-wait
    /// loop rather than a single wait.
    pub async fn dequeue(&self) -> T {
        loop {
            {
                let mut guard = self.items.lock().await;
                if let Some(item) = guard.pop_front() {
                    drop(guard);
                    self.not_full.notify_one();
                    return item;
                }
            }
            self.not_empty.notified().await;
        }
    }

    /// Pops the item at the front of the queue without waiting. Returns
    /// `None` if the queue is currently empty.
    pub async fn try_dequeue(&self) -> Option<T> {
        let mut guard = self.items.lock().await;
        let item = guard.pop_front();
        if item.is_some() {
            drop(guard);
            self.not_full.notify_one();
        }
        item
    }

    fn has_room(&self, current_len: usize) -> bool {
        self.capacity.is_none_or(|cap| current_len < cap.get())
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use super::FifoQueue;

    #[tokio::test]
    async fn dequeues_in_fifo_order() {
        let queue = FifoQueue::unbounded();
        queue.enqueue(1).await;
        queue.enqueue(2).await;
        queue.enqueue(3).await;

        assert_eq!(queue.dequeue().await, 1);
        assert_eq!(queue.dequeue().await, 2);
        assert_eq!(queue.dequeue().await, 3);
    }

    #[tokio::test]
    async fn len_and_is_empty_track_contents() {
        let queue = FifoQueue::unbounded();
        assert!(queue.is_empty().await);
        assert_eq!(queue.len().await, 0);

        queue.enqueue("a").await;
        queue.enqueue("b").await;
        assert!(!queue.is_empty().await);
        assert_eq!(queue.len().await, 2);

        queue.dequeue().await;
        assert_eq!(queue.len().await, 1);
    }

    #[tokio::test]
    async fn try_dequeue_on_empty_queue_returns_none() {
        let queue: FifoQueue<i32> = FifoQueue::unbounded();
        assert_eq!(queue.try_dequeue().await, None);
    }

    #[tokio::test]
    async fn try_enqueue_at_capacity_returns_the_item_back() {
        let queue = FifoQueue::bounded(NonZeroUsize::new(1).unwrap());
        queue.try_enqueue(1).await.unwrap();

        assert_eq!(queue.try_enqueue(2).await, Err(2));
        assert_eq!(queue.len().await, 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn enqueue_blocks_until_capacity_is_available() {
        let queue = Arc::new(FifoQueue::bounded(NonZeroUsize::new(1).unwrap()));
        queue.enqueue(1).await;

        let blocked = Arc::clone(&queue);
        let enqueue_task = tokio::spawn(async move { blocked.enqueue(2).await });

        // The second enqueue has room for nothing: give it a moment to
        // prove it's actually waiting rather than having (incorrectly)
        // returned immediately.
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!enqueue_task.is_finished());

        assert_eq!(queue.dequeue().await, 1);
        enqueue_task.await.unwrap();
        assert_eq!(queue.dequeue().await, 2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dequeue_blocks_until_an_item_is_enqueued() {
        let queue = Arc::new(FifoQueue::unbounded());

        let waiting = Arc::clone(&queue);
        let dequeue_task = tokio::spawn(async move { waiting.dequeue().await });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!dequeue_task.is_finished());

        queue.enqueue(42).await;
        assert_eq!(dequeue_task.await.unwrap(), 42);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_producers_and_consumers_deliver_every_item_exactly_once() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        const PRODUCERS: usize = 4;
        const ITEMS_PER_PRODUCER: usize = 200;
        const TOTAL_ITEMS: usize = PRODUCERS * ITEMS_PER_PRODUCER;

        let queue = Arc::new(FifoQueue::bounded(NonZeroUsize::new(8).unwrap()));

        let mut producers = Vec::new();
        for producer_id in 0..PRODUCERS {
            let queue = Arc::clone(&queue);
            producers.push(tokio::spawn(async move {
                for i in 0..ITEMS_PER_PRODUCER {
                    queue.enqueue(producer_id * ITEMS_PER_PRODUCER + i).await;
                }
            }));
        }

        // Each consumer claims one "slot" from a shared counter before
        // calling `dequeue`, so the number of `dequeue` calls issued
        // across all consumers combined exactly matches the number of
        // items produced. Without this, a consumer looping until it
        // personally sees `TOTAL_ITEMS` deadlocks the moment more than
        // one consumer task is running.
        let remaining = Arc::new(AtomicUsize::new(TOTAL_ITEMS));
        let mut consumers = Vec::new();
        for _ in 0..3 {
            let queue = Arc::clone(&queue);
            let remaining = Arc::clone(&remaining);
            consumers.push(tokio::spawn(async move {
                let mut received = Vec::new();
                while remaining
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| (n > 0).then(|| n - 1))
                    .is_ok()
                {
                    received.push(queue.dequeue().await);
                }
                received
            }));
        }

        for producer in producers {
            producer.await.unwrap();
        }

        let mut all_received = Vec::new();
        for consumer in consumers {
            all_received.extend(consumer.await.unwrap());
        }

        all_received.sort_unstable();
        let expected: Vec<usize> = (0..TOTAL_ITEMS).collect();
        assert_eq!(all_received, expected);
    }
}
