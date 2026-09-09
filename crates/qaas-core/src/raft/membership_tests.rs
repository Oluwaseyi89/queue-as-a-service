//! Integration tests for [`super::membership`]: real multi-node clusters
//! whose membership is grown and shrunk by a [`MembershipWatcher`]
//! reconciling against a [`MembershipSource`], not by hand-driving
//! `openraft`'s add-learner-then-promote protocol directly the way
//! `feature/raft-replication`'s own tests do.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, Config, ServerState};
use tempfile::tempdir;

use super::{
    FileMembershipSource, InProcessNetworkHub, LogStore, MembershipSource, MembershipWatcher,
    NodeId, Raft, StateMachineStore, StaticMembershipSource,
};

/// Same shape as `tests.rs`'s own `start_node` — duplicated rather than
/// shared, since the two test modules aren't nested under one another
/// and this is a handful of lines, not worth threading a visibility
/// workaround through for.
async fn start_node(hub: &InProcessNetworkHub, id: NodeId) -> Raft {
    let config = Arc::new(Config::default().validate().expect("default config is valid"));
    let log_store = LogStore::default();
    let state_machine = Arc::new(StateMachineStore::default());

    let raft = Raft::new(id, config, hub.clone(), log_store, state_machine)
        .await
        .expect("starting a Raft node is only fallible if the core task can't be spawned");

    hub.register(id, raft.clone()).await;
    raft
}

fn voter_ids(raft: &Raft) -> Vec<NodeId> {
    raft.metrics().borrow().membership_config.voter_ids().collect()
}

#[tokio::test]
async fn static_membership_source_returns_what_it_was_given() {
    let members: BTreeMap<NodeId, BasicNode> = [(1, BasicNode::default())].into_iter().collect();
    let source = StaticMembershipSource::new(members.clone());
    assert_eq!(source.members().await.unwrap(), members);
}

#[tokio::test]
async fn file_membership_source_reads_json_from_disk() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("members.json");
    tokio::fs::write(&path, r#"{"1": "127.0.0.1:21001", "2": "127.0.0.1:21002"}"#).await.unwrap();

    let source = FileMembershipSource::new(&path);
    let members = source.members().await.unwrap();

    assert_eq!(members.len(), 2);
    assert_eq!(members[&1].addr, "127.0.0.1:21001");
    assert_eq!(members[&2].addr, "127.0.0.1:21002");
}

#[tokio::test]
async fn file_membership_source_reflects_edits_without_restarting_anything() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("members.json");
    tokio::fs::write(&path, r#"{"1": "127.0.0.1:21001"}"#).await.unwrap();
    let source = FileMembershipSource::new(&path);

    assert_eq!(source.members().await.unwrap().len(), 1);

    // The whole point: no restart, no re-construction of `source` — just
    // edit the file a running watcher is already pointed at.
    tokio::fs::write(&path, r#"{"1": "127.0.0.1:21001", "2": "127.0.0.1:21002"}"#).await.unwrap();
    assert_eq!(source.members().await.unwrap().len(), 2);
}

#[tokio::test]
async fn reconciling_adds_a_new_node_as_a_voter_and_it_receives_replicated_writes() {
    let hub = InProcessNetworkHub::new();
    let raft1 = start_node(&hub, 1).await;
    raft1
        .initialize([(1, BasicNode::default())].into_iter().collect::<BTreeMap<_, _>>())
        .await
        .unwrap();
    raft1
        .wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "node 1 becomes leader")
        .await
        .unwrap();

    // Node 2 exists and is reachable through the hub, but isn't part of
    // the cluster's membership yet — this is the "a new server process
    // started and registered itself, now it needs to actually join"
    // situation this module exists to handle.
    let _raft2 = start_node(&hub, 2).await;

    let desired: BTreeMap<NodeId, BasicNode> =
        [(1, BasicNode::default()), (2, BasicNode::default())].into_iter().collect();
    let watcher = MembershipWatcher::new(raft1.clone(), StaticMembershipSource::new(desired));

    let diff = watcher.reconcile_once().await.unwrap();
    assert_eq!(diff.joined, [2].into_iter().collect());
    assert!(diff.left.is_empty());

    let voters = voter_ids(&raft1);
    assert!(
        voters.contains(&1) && voters.contains(&2),
        "both nodes should now be voters: {voters:?}"
    );
}

#[tokio::test]
async fn reconciling_removes_a_node_that_dropped_off_the_source() {
    let hub = InProcessNetworkHub::new();
    let raft1 = start_node(&hub, 1).await;
    let _raft2 = start_node(&hub, 2).await;
    let _raft3 = start_node(&hub, 3).await;

    let three: BTreeMap<NodeId, BasicNode> =
        [(1, BasicNode::default()), (2, BasicNode::default()), (3, BasicNode::default())]
            .into_iter()
            .collect();
    raft1.initialize(three).await.unwrap();
    raft1
        .wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "node 1 becomes leader")
        .await
        .unwrap();

    // Node 3 "scaled down": the source no longer lists it.
    let two: BTreeMap<NodeId, BasicNode> =
        [(1, BasicNode::default()), (2, BasicNode::default())].into_iter().collect();
    let watcher = MembershipWatcher::new(raft1.clone(), StaticMembershipSource::new(two));

    let diff = watcher.reconcile_once().await.unwrap();
    assert_eq!(diff.left, [3].into_iter().collect());
    assert!(diff.joined.is_empty());

    let voters = voter_ids(&raft1);
    assert!(!voters.contains(&3), "node 3 should no longer be a voter: {voters:?}");
    assert!(voters.contains(&1) && voters.contains(&2));
}

#[tokio::test]
async fn reconcile_is_a_noop_when_membership_already_matches_the_source() {
    let hub = InProcessNetworkHub::new();
    let raft1 = start_node(&hub, 1).await;
    raft1
        .initialize([(1, BasicNode::default())].into_iter().collect::<BTreeMap<_, _>>())
        .await
        .unwrap();
    raft1
        .wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "node 1 becomes leader")
        .await
        .unwrap();

    let watcher = MembershipWatcher::new(
        raft1.clone(),
        StaticMembershipSource::new([(1, BasicNode::default())].into_iter().collect()),
    );

    let diff = watcher.reconcile_once().await.unwrap();
    assert!(diff.is_empty(), "membership already matched the source: {diff:?}");
}

#[tokio::test]
async fn run_stops_promptly_once_the_shutdown_future_resolves() {
    let hub = InProcessNetworkHub::new();
    let raft1 = start_node(&hub, 1).await;
    raft1
        .initialize([(1, BasicNode::default())].into_iter().collect::<BTreeMap<_, _>>())
        .await
        .unwrap();
    raft1
        .wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "node 1 becomes leader")
        .await
        .unwrap();

    let watcher = MembershipWatcher::new(
        raft1.clone(),
        StaticMembershipSource::new([(1, BasicNode::default())].into_iter().collect()),
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let run_handle = tokio::spawn(async move {
        watcher
            .run(Duration::from_secs(60), async { shutdown_rx.await.ok().map_or((), |()| ()) })
            .await;
    });

    // `run`'s poll interval is a full minute — if shutdown weren't
    // handled by a concurrent `select!` arm, waiting for it would hang
    // this test for that long. Bounding it tightly proves shutdown is
    // actually racing the sleep, not just eventually happening to line
    // up with it.
    shutdown_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(1), run_handle)
        .await
        .expect("run() should return promptly once shutdown resolves")
        .unwrap();
}
