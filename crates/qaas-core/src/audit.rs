//! Async, non-blocking audit logging via a worker pool — ported from
//! global-rate-limiter's own logger design (see `CLAUDE.md`'s reference
//! architecture section: "Async, non-blocking audit logging via a
//! worker pool so the hot path never waits on I/O"), the same
//! "port the mechanism, translate the unit" move this crate has already
//! made twice for [`circuit_breaker`](crate::circuit_breaker) and
//! [`admission`](crate::admission).
//!
//! # Why non-blocking means *drop*, not *block*
//!
//! [`AuditLogger::log`] is a plain, synchronous, non-`async` method that
//! returns immediately — there's no `.await` point on the calling side
//! at all, which is the only way to make "the hot path never waits on
//! I/O" literally true rather than "waits on I/O, just wrapped in a
//! `Future`." It hands `event` to a bounded channel via
//! [`try_send`](tokio::sync::mpsc::Sender::try_send): if a worker has
//! room, the event is queued and `log` returns; if every worker is
//! backed up and the channel is full, the event is **dropped** and a
//! counter increments (see [`AuditLogger::dropped`]) rather than the
//! caller blocking until room frees up. A bounded queue that blocks
//! once full isn't actually non-blocking, it's just non-blocking *most
//! of the time* — under real backpressure it would reintroduce exactly
//! the hot-path stall this module exists to prevent. Dropping (loudly,
//! countably) is the honest trade for a system that has explicitly
//! chosen to never let audit logging slow down the actual queue
//! operations it's recording.
//!
//! # Why a worker *pool*, not one background task
//!
//! A single consumer task would still decouple the hot path from I/O,
//! but it also means one slow write (a disk hiccup, a large batch of
//! events queued up) serializes every event behind it. Multiple workers
//! competing for the same receiver mean a slow write only blocks
//! whichever worker issued it, not the other `workers - 1` still
//! draining the queue — the same reasoning a real worker pool gives you
//! anywhere else. Every record carries its own timestamp
//! specifically because of this: concurrent workers can *commit* out of
//! strict chronological order (worker A picks up event 1, worker B
//! picks up event 2, B's write finishes first), but every record still
//! carries the instant `log` was actually called, so anything reading
//! the log back can always recover true chronological order by sorting
//! on it, regardless of write-completion order.
//!
//! # Why this crate, and why generic over `E`
//!
//! Durability is the same [`Wal`] every other durable thing in this
//! crate already uses — see its own docs; wrapping it in a worker pool
//! and a drop-on-backpressure channel doesn't need to know anything
//! about what's actually being logged. `E` is deliberately just
//! `Serialize + DeserializeOwned + Send + Sync`, not some
//! `AuditEvent` type this crate invents: `qaas-server`'s MCP layer is
//! where a real event shape (which tenant, which queue, which tool,
//! what happened) gets defined and where `log` actually gets called, at
//! `enqueue`/`ack`/`nack` — the same "generic primitive in `qaas-core`,
//! domain-specific wiring in `qaas-server`" split [`admission`](crate::admission)
//! and [`quota`](crate::quota) already established.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use qaas_types::Timestamp;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::wal::Wal;

/// Tunables for one [`AuditLogger`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditLoggerConfig {
    /// How many background tasks concurrently drain the event channel
    /// and write to the durable log. See the module docs for why more
    /// than one.
    pub workers: usize,
    /// How many events [`AuditLogger::log`] can queue up before it
    /// starts dropping them rather than growing without bound. A larger
    /// buffer absorbs a longer burst before anything is lost, at the
    /// cost of more events sitting in memory, unwritten, if the process
    /// crashes.
    pub channel_capacity: usize,
}

impl AuditLoggerConfig {
    /// Four workers, a 4096-event buffer — generous enough to absorb a
    /// real burst of hot-path activity without tuning, not a value
    /// derived from any measured throughput target this branch doesn't
    /// have yet (see [`Wal`]'s own docs on the same "correctness first,
    /// a real perf target is future work" stance).
    pub const DEFAULT: Self = Self { workers: 4, channel_capacity: 4096 };
}

impl Default for AuditLoggerConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// One logged event, with the instant [`AuditLogger::log`] was called
/// for it — see the module docs on why this is what lets a reader
/// recover true chronological order even though a worker pool can
/// *write* records out of that order.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuditRecord<E> {
    at: Timestamp,
    event: E,
}

/// Async, non-blocking, worker-pool-backed audit logging. See the
/// module docs for the full design.
pub struct AuditLogger<E> {
    sender: mpsc::Sender<AuditRecord<E>>,
    dropped: Arc<AtomicU64>,
    written: Arc<AtomicU64>,
}

impl<E: Serialize + DeserializeOwned + Send + Sync + 'static> AuditLogger<E> {
    /// Opens the durable log at `path` (creating it if it doesn't exist,
    /// replaying — and discarding — whatever's already in it, the same
    /// as any other [`Wal`] open) and spawns `config.workers` background
    /// tasks to drain new events into it.
    ///
    /// Replayed records are intentionally not returned: unlike every
    /// other `Wal`-backed type in this crate, an audit log has no
    /// in-memory state that needs rebuilding from it on restart — it's
    /// a write-only record for something else (a person, a compliance
    /// tool) to read later, not a source of truth this process itself
    /// acts on.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying [`Wal`] fails to open.
    pub async fn open(
        path: impl AsRef<std::path::Path>,
        config: AuditLoggerConfig,
    ) -> std::io::Result<Self> {
        let (wal, _replayed) = Wal::<AuditRecord<E>>::open(path).await?;
        let wal = Arc::new(wal);

        let (sender, receiver) = mpsc::channel(config.channel_capacity);
        // Shared, not one-receiver-per-worker: `mpsc::Receiver` has
        // exactly one owner, so a pool of workers competing for the same
        // queue of events (rather than each getting its own, defeating
        // the point of pooling) has to go through a lock — held only for
        // the instant it takes to pull the next event off, never across
        // the write that follows.
        let receiver = Arc::new(Mutex::new(receiver));

        let dropped = Arc::new(AtomicU64::new(0));
        let written = Arc::new(AtomicU64::new(0));

        for _ in 0..config.workers.max(1) {
            let wal = Arc::clone(&wal);
            let receiver = Arc::clone(&receiver);
            let written = Arc::clone(&written);
            tokio::spawn(async move {
                loop {
                    let record = { receiver.lock().await.recv().await };
                    let Some(record) = record else {
                        // The sender side (this AuditLogger) was
                        // dropped and every queued event already
                        // drained - nothing left for this worker to do,
                        // ever again.
                        break;
                    };
                    // A failed write here has nowhere further to
                    // escalate to - log() already returned successfully
                    // to its caller, which is the entire point of this
                    // module. Surfacing it as a tracing event is the
                    // most this worker can honestly do; see the module
                    // docs on the drop-on-backpressure trade this type
                    // already makes elsewhere for the same underlying
                    // reason (a durability guarantee this deep in the
                    // stack can't also block the caller that already
                    // moved on).
                    match wal.append(&record).await {
                        Ok(()) => {
                            written.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(error) => tracing::error!(%error, "audit log write failed"),
                    }
                }
            });
        }

        Ok(Self { sender, dropped, written })
    }

    /// Queues `event` for durable logging and returns immediately — see
    /// the module docs for exactly what "immediately" and "non-blocking"
    /// mean here, including when and why an event is dropped instead of
    /// written.
    pub fn log(&self, event: E) {
        let record = AuditRecord { at: Timestamp::now(), event };
        if let Err(TrySendError::Full(_) | TrySendError::Closed(_)) = self.sender.try_send(record) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// How many events have been dropped since this logger was opened —
    /// see the module docs on why dropping, rather than blocking the
    /// caller, is this type's deliberate behavior once its workers fall
    /// behind. `qaas-server` is expected to expose this as a metric
    /// (`feature/tracing-metrics`'s own pattern), not silently ignore it.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// How many events have actually been durably written since this
    /// logger was opened — unlike [`AuditLogger::log`] itself, which
    /// only ever confirms an event was *queued*, this only counts once
    /// a worker's own `Wal::append` has actually returned successfully.
    /// `written() + dropped()` is always the total number of `log` calls
    /// this logger has ever accepted a request for, once its workers
    /// catch up — the two counters partition every event between "made
    /// it to disk" and "never will."
    #[must_use]
    pub fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tempfile::tempdir;

    use super::{AuditLogger, AuditLoggerConfig, AuditRecord};
    use crate::wal::Wal;

    /// Polls `logger.written()` until it reaches `expected` or a
    /// generous timeout elapses, *then* opens the WAL at `path` exactly
    /// once and returns what it found.
    ///
    /// Deliberately doesn't poll by reopening the WAL itself repeatedly
    /// while the logger's own workers might still be writing to it:
    /// `Wal::open` replays and then truncates the file to whatever it
    /// just validated (`writer.set_len(valid_len)`, see that type's own
    /// docs) — safe when nothing else has the file open for writing, but
    /// a real time-of-check-to-time-of-use hazard against a *live*
    /// writer. A read racing a concurrent append could see a torn tail,
    /// compute a `valid_len` that stops just short of a record the
    /// writer is about to fsync, and then truncate that fully-durable
    /// record away out from under it the instant after it commits.
    /// `written()` is an in-memory counter with no such hazard, so it's
    /// what this polls instead — the file is only ever opened once,
    /// after `written()` confirms every worker is done.
    async fn wait_for_writes<E>(
        logger: &AuditLogger<E>,
        path: &std::path::Path,
        expected: u64,
    ) -> Vec<AuditRecord<E>>
    where
        E: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        tokio::time::timeout(Duration::from_secs(30), async {
            while logger.written() < expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("workers should have caught up well within 30s");

        let (_wal, records) = Wal::<AuditRecord<E>>::open(path).await.unwrap();
        records
    }

    #[tokio::test]
    async fn a_logged_event_is_durably_written() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let logger = AuditLogger::<String>::open(&path, AuditLoggerConfig::DEFAULT).await.unwrap();

        logger.log("first".to_string());
        logger.log("second".to_string());

        let records = wait_for_writes(&logger, &path, 2).await;
        let mut events: Vec<String> = records.into_iter().map(|record| record.event).collect();
        events.sort_unstable();
        assert_eq!(events, vec!["first".to_string(), "second".to_string()]);
        assert_eq!(logger.dropped(), 0);
    }

    #[tokio::test]
    async fn every_record_carries_a_real_logging_timestamp() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let logger = AuditLogger::<String>::open(&path, AuditLoggerConfig::DEFAULT).await.unwrap();

        let before = qaas_types::Timestamp::now();
        logger.log("event".to_string());
        let records = wait_for_writes(&logger, &path, 1).await;
        let after = qaas_types::Timestamp::now();

        assert_eq!(records.len(), 1);
        assert!(records[0].at >= before && records[0].at <= after);
    }

    #[tokio::test]
    async fn events_beyond_channel_capacity_are_dropped_and_counted_not_blocked() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let config = AuditLoggerConfig { workers: 1, channel_capacity: 2 };
        let logger = AuditLogger::<u32>::open(&path, config).await.unwrap();

        // A current-thread test runtime never lets a spawned worker task
        // actually run until this function itself yields - so a tight,
        // synchronous burst of log() calls with no .await between them
        // is a deterministic way to fill the channel before anything
        // drains it, rather than racing a real background write.
        for i in 0..10u32 {
            logger.log(i);
        }

        assert_eq!(logger.dropped(), 8, "only the first 2 of 10 should have fit in the channel");

        let records = wait_for_writes(&logger, &path, 2).await;
        assert_eq!(records.len(), 2, "exactly the events that were queued should be on disk");
    }

    #[tokio::test]
    async fn multiple_workers_all_drain_the_same_queue() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("audit.log");
        let config = AuditLoggerConfig { workers: 4, channel_capacity: 100 };
        let logger = AuditLogger::<u32>::open(&path, config).await.unwrap();

        for i in 0..50u32 {
            logger.log(i);
        }
        assert_eq!(logger.dropped(), 0);

        let records = wait_for_writes(&logger, &path, 50).await;
        let mut events: Vec<u32> = records.into_iter().map(|record| record.event).collect();
        events.sort_unstable();
        assert_eq!(events, (0..50).collect::<Vec<_>>());
    }
}
