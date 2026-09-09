//! An in-process Raft network — see [`super`]'s module docs for why
//! there's no real network transport here yet.

use std::collections::HashMap;
use std::sync::Arc;

use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tokio::sync::RwLock;

use super::{NodeId, Raft, TypeConfig};

/// A node this hub was asked to reach isn't registered with it.
///
/// The only way this happens is a test or caller asking the hub to route
/// to a node id that was never [`register`](InProcessNetworkHub::register)ed
/// — in a real network this would instead be a connection failure, which
/// is exactly the [`Unreachable`] error this gets wrapped in.
#[derive(Debug, thiserror::Error)]
#[error("node {0} is not registered with this in-process network hub")]
struct NodeNotRegistered(NodeId);

/// A shared registry mapping node id to a running [`Raft`] instance
/// within the *same process*, standing in for a real network directory
/// service. Every node in an in-process test cluster is constructed with
/// a clone of the same hub (cheap — it's one `Arc` inside) and
/// [`register`](Self::register)s itself after starting up.
///
/// Implements [`RaftNetworkFactory`] directly: creating a "connection" to
/// a peer is just remembering which node id to look up in this map when
/// an RPC is actually sent, exactly as a real factory would remember an
/// address without connecting yet.
#[derive(Clone, Default)]
pub struct InProcessNetworkHub {
    nodes: Arc<RwLock<HashMap<NodeId, Raft>>>,
}

impl InProcessNetworkHub {
    /// Creates an empty hub with no nodes registered yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes `raft` reachable at `id` through this hub.
    pub async fn register(&self, id: NodeId, raft: Raft) {
        self.nodes.write().await.insert(id, raft);
    }

    async fn get(&self, id: NodeId) -> Option<Raft> {
        self.nodes.read().await.get(&id).cloned()
    }
}

impl RaftNetworkFactory<TypeConfig> for InProcessNetworkHub {
    type Network = InProcessNetwork;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        InProcessNetwork { hub: self.clone(), target }
    }
}

/// A [`RaftNetwork`] connection to one target node, routed through an
/// [`InProcessNetworkHub`] instead of a real socket.
pub struct InProcessNetwork {
    hub: InProcessNetworkHub,
    target: NodeId,
}

impl InProcessNetwork {
    async fn target_raft(&self) -> Result<Raft, Unreachable> {
        self.hub
            .get(self.target)
            .await
            .ok_or_else(|| Unreachable::new(&NodeNotRegistered(self.target)))
    }
}

impl RaftNetwork<TypeConfig> for InProcessNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.target_raft().await?;
        raft.append_entries(rpc).await.map_err(|error| RemoteError::new(self.target, error).into())
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        let raft = self.target_raft().await?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|error| RemoteError::new(self.target, error).into())
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.target_raft().await?;
        raft.vote(rpc).await.map_err(|error| RemoteError::new(self.target, error).into())
    }
}
