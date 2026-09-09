//! A small key-value state machine — see [`super`]'s module docs for why
//! this is deliberately a toy domain rather than `ConsumerGroup`'s own
//! operations.

use std::collections::BTreeMap;
use std::io::Cursor;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use openraft::storage::{RaftStateMachine, Snapshot};
use openraft::{
    EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use super::{NodeId, TypeConfig};

/// A write submitted to the cluster through [`super::Raft::client_write`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    /// Set `key` to `value` in the replicated key-value store.
    Set {
        /// The key to set.
        key: String,
        /// The value to associate with `key`.
        value: String,
    },
}

/// What applying a [`Request`] to the state machine returns.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Response {
    /// The value at the affected key immediately after this request was
    /// applied.
    pub value: Option<String>,
}

/// The state machine's actual data: the key-value map plus the metadata
/// `openraft` requires every state machine to track — how far its
/// content reflects the log, and the last membership it has seen.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StateMachineData {
    last_applied_log: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    kv: BTreeMap<String, String>,
}

/// A previously built snapshot, kept so
/// [`get_current_snapshot`](RaftStateMachine::get_current_snapshot) has
/// something to return without rebuilding it.
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

/// The `openraft` state machine for this crate's [`TypeConfig`].
///
/// `RaftStateMachine`'s methods take `&mut self`, but this crate hands
/// `openraft` an `Arc<StateMachineStore>` (see the `RaftStateMachine`
/// impl below) rather than the bare type, specifically so a second
/// `Arc` clone kept outside the `Raft` instance can still read the
/// current key-value contents for tests and observability — `&mut self`
/// on an `Arc<T>` only needs unique access to the *pointer*, not `T`
/// itself, which is exactly what the interior `RwLock` is for.
#[derive(Debug, Default)]
pub struct StateMachineStore {
    data: RwLock<StateMachineData>,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    /// Monotonic counter giving each built snapshot a distinct id, since
    /// two snapshots built from the same log id are otherwise
    /// indistinguishable by content alone.
    snapshot_idx: AtomicU64,
}

impl StateMachineStore {
    /// The current value for `key`, if any. Not part of `RaftStateMachine`
    /// — this is this crate's own read path into the state machine.
    pub async fn get(&self, key: &str) -> Option<String> {
        self.data.read().await.kv.get(key).cloned()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let (last_applied_log, last_membership, kv) = {
            let data = self.data.read().await;
            (data.last_applied_log, data.last_membership.clone(), data.kv.clone())
        };

        let data =
            serde_json::to_vec(&kv).map_err(|error| StorageIOError::write_state_machine(&error))?;

        let snapshot_id = format!(
            "{}-{}",
            last_applied_log.map_or_else(|| "none".to_string(), |id| id.to_string()),
            self.snapshot_idx.fetch_add(1, Ordering::Relaxed)
        );
        let meta = SnapshotMeta { last_log_id: last_applied_log, last_membership, snapshot_id };

        *self.current_snapshot.write().await =
            Some(StoredSnapshot { meta: meta.clone(), data: data.clone() });

        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<NodeId>>, StoredMembership<NodeId, openraft::BasicNode>),
        StorageError<NodeId>,
    > {
        let data = self.data.read().await;
        Ok((data.last_applied_log, data.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut data = self.data.write().await;
        let mut responses = Vec::new();

        for entry in entries {
            data.last_applied_log = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => Response::default(),
                EntryPayload::Normal(Request::Set { key, value }) => {
                    data.kv.insert(key.clone(), value.clone());
                    Response { value: Some(value) }
                }
                EntryPayload::Membership(membership) => {
                    data.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    Response::default()
                }
            };
            responses.push(response);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::default())
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let bytes = snapshot.into_inner();
        let kv: BTreeMap<String, String> = serde_json::from_slice(&bytes)
            .map_err(|error| StorageIOError::read_snapshot(Some(meta.signature()), &error))?;

        {
            let mut data = self.data.write().await;
            data.last_applied_log = meta.last_log_id;
            data.last_membership = meta.last_membership.clone();
            data.kv = kv;
        }
        *self.current_snapshot.write().await =
            Some(StoredSnapshot { meta: meta.clone(), data: bytes });
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(self.current_snapshot.read().await.as_ref().map(|stored| Snapshot {
            meta: stored.meta.clone(),
            snapshot: Box::new(Cursor::new(stored.data.clone())),
        }))
    }
}
