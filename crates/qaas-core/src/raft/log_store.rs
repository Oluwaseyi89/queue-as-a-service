//! An in-memory Raft log store. See [`super`]'s module docs for why this
//! isn't backed by this crate's [`Wal`](crate::wal::Wal) and what that
//! means for real deployment.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{LogId, OptionalSend, StorageError, Vote};
use tokio::sync::RwLock;

use super::{NodeId, TypeConfig};

/// An in-memory, non-durable Raft log.
///
/// Cheap to clone — every field is an `Arc`, so a clone shares the same
/// underlying log with the original, which is exactly what
/// [`RaftLogStorage::get_log_reader`] needs: a reader that observes
/// concurrent appends rather than a frozen copy.
#[derive(Debug, Clone, Default)]
pub struct LogStore {
    log: Arc<RwLock<BTreeMap<u64, openraft::Entry<TypeConfig>>>>,
    vote: Arc<RwLock<Option<Vote<NodeId>>>>,
    last_purged: Arc<RwLock<Option<LogId<NodeId>>>>,
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<openraft::Entry<TypeConfig>>, StorageError<NodeId>> {
        let log = self.log.read().await;
        Ok(log.range(range).map(|(_, entry)| entry.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let log = self.log.read().await;
        let last_purged = *self.last_purged.read().await;
        let last_log_id = log.values().next_back().map(|entry| entry.log_id).or(last_purged);
        Ok(LogState { last_purged_log_id: last_purged, last_log_id })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut log = self.log.write().await;
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        drop(log);
        // Nothing here is actually asynchronous I/O to wait on — this
        // store has no disk to flush to — so the flush callback fires
        // immediately, reporting success as soon as the in-memory
        // insert above completes.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.log.write().await.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        {
            let mut last_purged = self.last_purged.write().await;
            if last_purged.is_none_or(|purged| purged < log_id) {
                *last_purged = Some(log_id);
            }
        }
        let mut log = self.log.write().await;
        let kept = log.split_off(&(log_id.index + 1));
        *log = kept;
        Ok(())
    }
}
