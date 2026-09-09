//! Two different kinds of proof this module actually works, not just
//! compiles:
//!
//! - [`storage_passes_the_openraft_conformance_suite`] runs `openraft`'s
//!   own, exhaustive correctness test suite against [`LogStore`] and
//!   [`StateMachineStore`] — this is the real bar the getting-started
//!   guide sets for a storage implementation, not a substitute for it.
//! - The rest are integration tests standing up real, multi-node
//!   clusters (communicating through [`InProcessNetworkHub`], per this
//!   module's documented scope) and checking actual consensus outcomes:
//!   a leader gets elected, a write submitted to it reaches every node's
//!   state machine, and losing the leader triggers a real re-election
//!   among the survivors.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, ServerState};

use super::{InProcessNetworkHub, LogStore, NodeId, Raft, Request, StateMachineStore, TypeConfig};

struct StoreBuilder;

impl openraft::testing::StoreBuilder<TypeConfig, LogStore, Arc<StateMachineStore>>
    for StoreBuilder
{
    async fn build(
        &self,
    ) -> Result<((), LogStore, Arc<StateMachineStore>), openraft::StorageError<NodeId>> {
        Ok(((), LogStore::default(), Arc::new(StateMachineStore::default())))
    }
}

// `StorageError` is `openraft`'s own type, sized by its own error variants
// (each carrying a `LogId`/`SnapshotSignature`/etc.) — not something this
// crate controls or should box away just to satisfy this lint on a test.
#[allow(clippy::result_large_err)]
#[test]
fn storage_passes_the_openraft_conformance_suite() -> Result<(), openraft::StorageError<NodeId>> {
    openraft::testing::Suite::test_all(StoreBuilder)
}

/// Starts a node with default config, backed by this module's in-memory
/// log store and KV state machine, and registers it with `hub` so other
/// nodes in the same test can reach it. Returns the `Raft` handle and a
/// separate `Arc` clone of the state machine so the test can read its
/// contents directly (see [`StateMachineStore`]'s docs for why that's
/// safe to keep alongside the clone `openraft` owns).
async fn start_node(hub: &InProcessNetworkHub, id: NodeId) -> (Raft, Arc<StateMachineStore>) {
    let config = Arc::new(Config::default().validate().expect("default config is valid"));
    let log_store = LogStore::default();
    let state_machine = Arc::new(StateMachineStore::default());

    let raft = Raft::new(id, config, hub.clone(), log_store, Arc::clone(&state_machine))
        .await
        .expect("starting a Raft node is only fallible if the core task can't be spawned");

    hub.register(id, raft.clone()).await;
    (raft, state_machine)
}

fn three_node_membership() -> BTreeMap<NodeId, BasicNode> {
    [(1, BasicNode::default()), (2, BasicNode::default()), (3, BasicNode::default())]
        .into_iter()
        .collect()
}

/// Polls `sm.get(key)` until it equals `Some(expected)` or `timeout`
/// elapses. Replication to followers is asynchronous even after the
/// leader commits a write, so a test checking a follower's state has to
/// wait for it to catch up rather than read once immediately.
async fn wait_for_value(sm: &StateMachineStore, key: &str, expected: &str, timeout: Duration) {
    tokio::time::timeout(timeout, async {
        loop {
            if sm.get(key).await.as_deref() == Some(expected) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!("{key:?} should have replicated to {expected:?} within {timeout:?}")
    });
}

#[tokio::test]
async fn a_three_node_cluster_elects_a_leader_and_replicates_writes() {
    let hub = InProcessNetworkHub::new();
    let (raft1, sm1) = start_node(&hub, 1).await;
    let (_raft2, sm2) = start_node(&hub, 2).await;
    let (_raft3, sm3) = start_node(&hub, 3).await;

    raft1.initialize(three_node_membership()).await.unwrap();
    raft1
        .wait(Some(Duration::from_secs(5)))
        .state(
            ServerState::Leader,
            "node 1 should become leader after initializing a single-node-proposed cluster",
        )
        .await
        .unwrap();

    raft1
        .client_write(Request::Set { key: "hello".to_string(), value: "world".to_string() })
        .await
        .unwrap();

    // The leader's own state machine is updated synchronously as part of
    // committing the write, so this one should already be correct — but
    // checking it the same way as the followers keeps the assertion
    // uniform and still catches a regression in the leader's own apply
    // path.
    for sm in [&sm1, &sm2, &sm3] {
        wait_for_value(sm, "hello", "world", Duration::from_secs(5)).await;
    }
}

#[tokio::test]
async fn losing_the_leader_triggers_reelection_among_the_survivors() {
    let hub = InProcessNetworkHub::new();
    let (raft1, _sm1) = start_node(&hub, 1).await;
    let (raft2, _sm2) = start_node(&hub, 2).await;
    let (raft3, _sm3) = start_node(&hub, 3).await;

    raft1.initialize(three_node_membership()).await.unwrap();
    raft1
        .wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "node 1 becomes leader")
        .await
        .unwrap();

    // Shutting down node 1's core task stops it from sending the
    // AppendEntries heartbeats that keep followers from starting their
    // own election — this is what a crashed leader looks like from the
    // survivors' point of view, not a graceful "step down" message.
    raft1.shutdown().await.unwrap();

    let new_leader = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for raft in [&raft2, &raft3] {
                if let Some(leader) = raft.current_leader().await
                    && leader != 1
                {
                    return leader;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("one of the two remaining nodes should have elected a new leader");

    assert!(
        new_leader == 2 || new_leader == 3,
        "the new leader must be one of the surviving nodes, not the shut-down one"
    );
}
