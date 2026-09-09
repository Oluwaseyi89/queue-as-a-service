//! Config-based node discovery and dynamic membership changes.
//!
//! "Gossip- or config-based," per the branch plan — this is config-based:
//! a [`MembershipSource`] answers "who should be in the cluster right
//! now," and [`MembershipWatcher`] polls it and reconciles Raft's actual
//! membership to match, adding newly-listed nodes as voters and removing
//! ones that dropped off the list. Editing a [`FileMembershipSource`]'s
//! backing file — or pointing at a different [`MembershipSource`]
//! implementation entirely — and letting a running watcher pick it up is
//! the whole "scale up/down without manual reconfiguration" story this
//! branch delivers, echoing the rate limiter's `docker-setup.sh scale`
//! workflow without needing a gossip protocol to get there.
//!
//! What this doesn't do: discover peers automatically the way a gossip
//! protocol would (nodes finding each other by broadcasting on the
//! network), or start a brand-new node process for you. Both need real
//! networking — this project doesn't have any yet (see
//! [`raft`](super)'s own module docs) — so *starting* a node and
//! registering it with the cluster's [`InProcessNetworkHub`](super::InProcessNetworkHub)
//! stays the caller's job, same as in `feature/raft-replication`'s
//! tests. What this module owns is deciding *which* nodes should be
//! members and driving Raft's own two-step add-learner-then-promote (and
//! its reverse, for removal) protocol to get there — the part that's
//! genuinely reusable regardless of how a node's address was discovered
//! or how it was started.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use openraft::error::{ClientWriteError, RaftError};
use openraft::{BasicNode, ChangeMembers};

use super::{NodeId, Raft};

/// The set of nodes a [`MembershipSource`] believes should currently be
/// voting members of the cluster.
pub type ClusterMembers = BTreeMap<NodeId, BasicNode>;

/// Where a [`MembershipWatcher`] learns which nodes should be in the
/// cluster right now.
pub trait MembershipSource: Send + Sync {
    /// Returns the desired cluster membership.
    ///
    /// # Errors
    ///
    /// Returns an error if the membership can't currently be determined
    /// (a config file is missing or malformed, for
    /// [`FileMembershipSource`]).
    fn members(&self) -> impl Future<Output = std::io::Result<ClusterMembers>> + Send;
}

/// A fixed, in-memory membership — mainly for tests, or a deployment
/// simple enough that the member set genuinely never changes at runtime.
#[derive(Debug, Clone)]
pub struct StaticMembershipSource {
    members: ClusterMembers,
}

impl StaticMembershipSource {
    /// Always reports `members` as the desired cluster membership.
    #[must_use]
    pub fn new(members: ClusterMembers) -> Self {
        Self { members }
    }
}

impl MembershipSource for StaticMembershipSource {
    async fn members(&self) -> std::io::Result<ClusterMembers> {
        Ok(self.members.clone())
    }
}

/// Reads the desired cluster membership from a JSON file on disk each
/// time it's asked — `{"1": "127.0.0.1:21001", "2": "127.0.0.1:21002"}`,
/// node id to address. Editing the file and waiting for a
/// [`MembershipWatcher`]'s next poll to notice is what "scale up/down
/// without manual reconfiguration" means concretely for this source: no
/// process restart, no admin command, just an edit to a file every node
/// already knows how to read.
pub struct FileMembershipSource {
    path: PathBuf,
}

impl FileMembershipSource {
    /// Reads from `path` on every [`members`](MembershipSource::members)
    /// call — not cached, so an external edit is visible on the very
    /// next read.
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl MembershipSource for FileMembershipSource {
    async fn members(&self) -> std::io::Result<ClusterMembers> {
        let bytes = tokio::fs::read(&self.path).await?;
        let raw: BTreeMap<NodeId, String> = serde_json::from_slice(&bytes)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
        Ok(raw.into_iter().map(|(id, addr)| (id, BasicNode::new(addr))).collect())
    }
}

/// What changed as a result of a single [`MembershipWatcher::reconcile_once`]
/// call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MembershipDiff {
    /// Nodes that were added as voters this round.
    pub joined: BTreeSet<NodeId>,
    /// Nodes that were removed from the voter set this round.
    pub left: BTreeSet<NodeId>,
}

impl MembershipDiff {
    /// Whether this round changed nothing — the cluster's actual
    /// membership already matched what the source wanted.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.joined.is_empty() && self.left.is_empty()
    }
}

/// A failure while reconciling the cluster's membership to a
/// [`MembershipSource`]'s desired state.
#[derive(Debug, thiserror::Error)]
pub enum ReconcileError {
    /// The membership source itself couldn't be read.
    #[error("failed to read desired cluster membership: {0}")]
    Source(#[source] std::io::Error),
    /// Adding a node as a learner (the required first step before it can
    /// become a voter) failed.
    #[error("failed to add node {node_id} as a learner: {source}")]
    AddLearner {
        /// The node that couldn't be added.
        node_id: NodeId,
        /// The underlying `openraft` error.
        #[source]
        source: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
    },
    /// Promoting learners to voters, or removing voters, failed.
    #[error("failed to change cluster membership: {0}")]
    ChangeMembership(#[source] RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>),
}

/// Polls a [`MembershipSource`] and reconciles a [`Raft`] node's actual
/// membership to match.
///
/// The `raft` handle given to a watcher must currently be the cluster
/// leader for [`reconcile_once`](Self::reconcile_once) to succeed — like
/// any other Raft write, a membership change can only be proposed by the
/// leader. A watcher pointed at a follower gets a leader-forwarding
/// error back from `openraft` on every attempt rather than silently
/// doing nothing; this module doesn't chase the leader across nodes
/// automatically, so retrying against a specific node until it becomes
/// leader (or re-pointing the watcher once you know who the leader is)
/// is the caller's job.
pub struct MembershipWatcher<S> {
    raft: Raft,
    source: S,
}

impl<S: MembershipSource> MembershipWatcher<S> {
    /// Creates a watcher that will reconcile `raft`'s membership against
    /// `source`.
    pub fn new(raft: Raft, source: S) -> Self {
        Self { raft, source }
    }

    /// Reads the desired membership once and reconciles the cluster to
    /// match: nodes present in the source but not yet voters are added
    /// as learners and then promoted; voters no longer present in the
    /// source are removed outright (not demoted to learner — a node
    /// that scaled down isn't coming back as a learner waiting to
    /// rejoin, it's gone).
    ///
    /// # Errors
    ///
    /// Returns an error on the first failed step; some nodes may have
    /// already been added as learners even if promoting them (or a
    /// later removal) then fails; a subsequent call will retry whatever
    /// didn't complete, since it re-reads the source and re-compares
    /// against whatever the actual membership ended up being.
    pub async fn reconcile_once(&self) -> Result<MembershipDiff, ReconcileError> {
        let desired = self.source.members().await.map_err(ReconcileError::Source)?;
        let current_voter_ids: BTreeSet<NodeId> =
            self.raft.metrics().borrow().membership_config.voter_ids().collect();

        let joining: ClusterMembers = desired
            .iter()
            .filter(|(id, _)| !current_voter_ids.contains(id))
            .map(|(id, node)| (*id, node.clone()))
            .collect();
        let leaving: BTreeSet<NodeId> =
            current_voter_ids.iter().filter(|id| !desired.contains_key(id)).copied().collect();

        if joining.is_empty() && leaving.is_empty() {
            return Ok(MembershipDiff::default());
        }

        for (node_id, node) in &joining {
            self.raft
                .add_learner(*node_id, node.clone(), true)
                .await
                .map_err(|source| ReconcileError::AddLearner { node_id: *node_id, source })?;
        }
        if !joining.is_empty() {
            self.raft
                .change_membership(ChangeMembers::AddVoters(joining.clone()), true)
                .await
                .map_err(ReconcileError::ChangeMembership)?;
        }
        if !leaving.is_empty() {
            self.raft
                .change_membership(ChangeMembers::RemoveVoters(leaving.clone()), false)
                .await
                .map_err(ReconcileError::ChangeMembership)?;
        }

        Ok(MembershipDiff { joined: joining.into_keys().collect(), left: leaving })
    }

    /// Calls [`reconcile_once`](Self::reconcile_once) every
    /// `poll_interval`, forever, until `shutdown` resolves. A
    /// reconciliation error is logged and does not stop the loop — the
    /// next poll simply tries again, since a transient failure (this
    /// node briefly not being leader, a config file mid-write) is
    /// exactly the kind of condition polling is meant to recover from
    /// without operator intervention.
    pub async fn run(&self, poll_interval: Duration, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        loop {
            tokio::select! {
                () = &mut shutdown => return,
                () = tokio::time::sleep(poll_interval) => {
                    if let Err(error) = self.reconcile_once().await {
                        tracing::warn!(%error, "membership reconciliation failed; will retry next poll");
                    }
                }
            }
        }
    }
}
