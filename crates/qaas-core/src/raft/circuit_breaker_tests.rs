//! Proves [`crate::circuit_breaker`]'s pattern against a real dependency
//! this crate has, rather than only a synthetic one — the same
//! discipline [`super::tests`] applies to [`super::log_store`] and
//! [`super::state_machine`] (openraft's own conformance suite, real
//! multi-node elections) instead of trusting a smaller unit test alone.
//!
//! The scenario: a producer sends writes through [`HybridGuard::call`],
//! targeting one specific node in a three-node cluster. While that node
//! is the leader, writes succeed and are served fresh. When the producer
//! is left pointed at a node that can no longer accept writes — a
//! follower after leadership moved elsewhere, standing in for "the
//! replica or downstream dependency" `Plan.md`'s line for this branch
//! names — the guard's breaker opens after enough consecutive failures,
//! and the producer keeps getting *a* value (the last one that actually
//! succeeded) instead of hard failures, until it's pointed back at a
//! node that can actually serve writes and the circuit recovers.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, Config, ServerState};

use super::{InProcessNetworkHub, LogStore, NodeId, Raft, Request, StateMachineStore};
use crate::circuit_breaker::{CircuitBreakerConfig, CircuitState, HybridGuard, HybridOutcome};

/// Same shape as `tests.rs`'s and `membership_tests.rs`'s own
/// `start_node` — duplicated again rather than shared, for the same
/// reason `membership_tests.rs` gives: these test modules aren't nested
/// under one another, and it's a handful of lines.
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

/// A breaker that trips and recovers fast, so this test isn't stuck
/// waiting real seconds for a cooldown the way `CircuitBreakerConfig::DEFAULT`
/// would — same reasoning as `circuit_breaker::tests::fast_config`, just
/// duplicated locally rather than making that test-only helper `pub`.
fn fast_breaker_config() -> CircuitBreakerConfig {
    CircuitBreakerConfig {
        failure_threshold: 2,
        open_duration: Duration::from_millis(100),
        half_open_success_threshold: 1,
    }
}

/// Submits `Request::Set { key, value }` through `raft`, returning the
/// value written on success — this is `HybridGuard::call`'s `primary`
/// closure body, factored out only because it's called from more than
/// one place below with different `raft` handles (the healthy leader,
/// then the unreachable follower).
async fn write_through(
    raft: &Raft,
    key: &str,
    value: &str,
) -> Result<String, RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>> {
    raft.client_write(Request::Set { key: key.to_string(), value: value.to_string() }).await?;
    Ok(value.to_string())
}

#[tokio::test]
async fn a_producer_keeps_getting_the_last_good_value_while_its_replica_cant_write() {
    let hub = InProcessNetworkHub::new();
    let leader = start_node(&hub, 1).await;
    let follower = start_node(&hub, 2).await;
    let _third = start_node(&hub, 3).await;

    let membership: BTreeMap<NodeId, BasicNode> =
        [(1, BasicNode::default()), (2, BasicNode::default()), (3, BasicNode::default())]
            .into_iter()
            .collect();
    leader.initialize(membership).await.unwrap();
    leader
        .wait(Some(Duration::from_secs(5)))
        .state(ServerState::Leader, "node 1 becomes leader")
        .await
        .unwrap();

    let guard: HybridGuard<String, String> =
        HybridGuard::new(fast_breaker_config(), Duration::from_secs(10));

    // Healthy path: the producer talks to the actual leader and gets a
    // fresh answer, which also seeds the cache for the degraded path
    // below.
    let outcome = guard.call("producer:1".to_string(), || write_through(&leader, "k", "v1")).await;
    assert!(matches!(outcome, HybridOutcome::Fresh(ref v) if v == "v1"), "{outcome:?}");

    // Now the producer is pointed at `follower` instead — every write it
    // sends is rejected with `ForwardToLeader`, exactly what "a replica
    // is unhealthy [for writes]" looks like from a caller stuck talking
    // to the wrong node. `fast_breaker_config`'s threshold is two
    // failures, so the second call here should trip the breaker.
    for attempt in 0..2u32 {
        let outcome =
            guard.call("producer:1".to_string(), || write_through(&follower, "k", "v2")).await;
        // Cached fallback was already seeded, so the producer degrades
        // to `Cached("v1")` rather than seeing the raw `ForwardToLeader`
        // error — this is the entire point of the pattern: the caller
        // keeps working.
        assert!(
            matches!(outcome, HybridOutcome::Cached(ref v) if v == "v1"),
            "attempt {attempt}: {outcome:?}"
        );
    }
    assert_eq!(guard.breaker_state().await, CircuitState::Open);

    // Circuit open: further calls must not even attempt the follower —
    // proven the same way `circuit_breaker::tests` proves it, by
    // counting attempts on the closure itself.
    let attempts = AtomicU32::new(0);
    let outcome = guard
        .call("producer:1".to_string(), || {
            attempts.fetch_add(1, Ordering::SeqCst);
            write_through(&follower, "k", "v3")
        })
        .await;
    assert!(matches!(outcome, HybridOutcome::Cached(ref v) if v == "v1"));
    assert_eq!(attempts.load(Ordering::SeqCst), 0, "breaker open: follower must not be called");

    // Past the cooldown, and pointed back at the real leader: the trial
    // succeeds and the producer is back to fresh values, not stuck
    // serving `"v1"` forever.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(guard.breaker_state().await, CircuitState::HalfOpen);

    let outcome = guard.call("producer:1".to_string(), || write_through(&leader, "k", "v4")).await;
    assert!(matches!(outcome, HybridOutcome::Fresh(ref v) if v == "v4"), "{outcome:?}");
    assert_eq!(guard.breaker_state().await, CircuitState::Closed);
}
