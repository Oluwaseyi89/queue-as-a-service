//! Exposes the queue as MCP tools: `enqueue`, `claim`, `ack`, `nack`,
//! `checkpoint`.
//!
//! This is the branch's whole point — an agent runtime that already
//! speaks MCP (Claude Code, Claude Desktop, any other MCP client) can
//! drive this queue by calling tools directly, with `qaas-server` itself
//! as the MCP server. No bridge process translating some other wire
//! protocol into ours, and no separate adapter binary to keep in sync
//! with `qaas-core`'s actual API.
//!
//! # Why [`rmcp`]
//!
//! It's the SDK published under `github.com/modelcontextprotocol`, the
//! same org that owns the protocol spec — the "verify against the real
//! thing" discipline this project applies to `openraft` and `cargo-deny`
//! (see `CLAUDE.md`) points at the *official* SDK here too, not a
//! smaller third-party crate that might drift from the spec.
//!
//! # Why four tools, not the three `Plan.md` names
//!
//! `Plan.md`'s line for this branch says "enqueue, claim, and ack." It
//! doesn't mention `nack` — but `nack` already exists on
//! [`ConsumerGroup`], built and tested two
//! phases ago specifically to drive the retry/backoff/dead-letter
//! pipeline (`feature/retry-and-backoff-policies`,
//! `feature/dead-letter-queue`). Leaving it out wouldn't reduce scope —
//! it would silently disable that pipeline for every message that ever
//! passes through MCP, since the only other way a claimed message
//! returns to the pool is its lease expiring on its own. Exposing it
//! costs one more tool method around an operation this crate already
//! ships; not exposing it would make this interface structurally
//! incomplete for real agent failure handling.
//!
//! # Multiple named queues
//!
//! Every tool takes a `queue` argument. [`QueueRegistry`] opens (and
//! keeps open) one [`ConsumerGroup`] per
//! distinct name, lazily, the first time any tool references it — there
//! is no separate "create a queue" step, matching how nothing else in
//! this crate has needed an explicit provisioning call before use.
//! `queue` names are validated before ever touching the filesystem (see
//! [`validate_queue_name`]): they come straight from agent-supplied tool
//! arguments and are used to build a WAL file path, so this is a real
//! path-traversal boundary, not a cosmetic check.
//!
//! # Transport: stdio only
//!
//! This branch wires up [`rmcp::transport::stdio`] — the standard way an
//! agent runtime spawns and talks to a *local* MCP server, no listening
//! socket involved, cleanly satisfying "no bridge process" with zero
//! network attack surface. A network-reachable transport (streamable
//! HTTP, which `rmcp` also supports) is deliberately left for later:
//! `feature/api-auth` is the very next phase, and standing up an
//! unauthenticated network listener before that branch exists would be
//! a real, not hypothetical, security hole.
//!
//! # Every queue gets the same visibility timeout and retry policy
//!
//! [`VISIBILITY_TIMEOUT`] and [`RetryPolicy::DEFAULT`] apply to every
//! queue this server opens; there's no per-queue override in this
//! branch. `ConsumerGroup::open` itself has no such override either
//! (see its docs) — this interface doesn't add configurability its
//! underlying engine doesn't already have.
//!
//! # `checkpoint` (`feature/durable-agent-workflows`)
//!
//! Durably saves progress against a currently-claimed message without
//! resolving it — the primitive a multi-step agent workflow (chained LLM
//! calls, a human-in-the-loop pause) needs to resume where it left off
//! instead of starting over after a crash or a deliberate pause. `claim`
//! now returns whatever was last checkpointed for a message (`null` if
//! nothing has been), and a caller pauses a long-running step the same
//! way it always has — by calling `nack` — with its progress already
//! saved. See `qaas_core::ConsumerGroup`'s own module docs for the full
//! reasoning, including why this reuses `nack` rather than adding a
//! dedicated pause tool.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use qaas_core::{ConsumerGroup, LeaseToken, RetryPolicy};
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::{ErrorData, ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

/// How long a `claim` tool call waits for a message to become available
/// before reporting `available: false`, if the caller doesn't specify
/// `wait_ms`.
const DEFAULT_CLAIM_WAIT: Duration = Duration::from_secs(5);

/// Hard ceiling on `wait_ms`, regardless of what a caller asks for. An
/// MCP tool call blocking indefinitely is a bad citizen on a connection
/// other tool calls may be waiting to use — see this module's docs on
/// concurrent tool-call handling being the transport's job, not this
/// server's, but bounding any single call's worst case is still this
/// server's responsibility.
const MAX_CLAIM_WAIT: Duration = Duration::from_secs(60);

/// The visibility timeout every queue this server opens is given. Not
/// configurable per-queue in this branch — see the module docs.
const VISIBILITY_TIMEOUT: Duration = Duration::from_secs(30);

/// Rejects a queue name that would resolve to anything other than a
/// single file directly under the registry's data directory.
///
/// `name` comes straight from an agent's tool-call arguments and is
/// about to be used to build a filesystem path — `..`, `/`, and a null
/// byte all need to be off the table before that path is ever
/// constructed, not caught after the fact by however the filesystem
/// happens to react to them. Restricting to a plain, short, ASCII
/// identifier is simpler to get right than trying to enumerate every
/// unsafe character individually.
fn validate_queue_name(name: &str) -> Result<(), ErrorData> {
    let valid = !name.is_empty()
        && name.len() <= 128
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if valid {
        Ok(())
    } else {
        Err(ErrorData::invalid_params(
            format!(
                "queue name {name:?} is invalid: must be 1-128 characters of ASCII letters, \
                 digits, '-', or '_'"
            ),
            None,
        ))
    }
}

/// Parses a `message_id` tool argument back into a
/// [`qaas_types::MessageId`], reusing its existing `Deserialize` impl
/// (which in turn reuses `Uuid`'s) rather than adding a new string-parsing
/// method to `qaas-types` for this one call site.
fn parse_message_id(value: &str) -> Result<qaas_types::MessageId, ErrorData> {
    serde_json::from_value(serde_json::Value::String(value.to_string())).map_err(|_| {
        ErrorData::invalid_params(format!("{value:?} is not a valid message id"), None)
    })
}

/// Maps a `qaas-core` I/O failure (a WAL write that failed) onto the
/// MCP error shape. Always [`ErrorCode::INTERNAL_ERROR`](rmcp::model::ErrorCode)
/// — a failed durable write is this server's problem, not a malformed
/// request, so it doesn't belong under `INVALID_PARAMS`.
fn io_error_to_mcp(error: &std::io::Error) -> ErrorData {
    ErrorData::internal_error(error.to_string(), None)
}

/// Lazily opens and holds one [`ConsumerGroup`] per queue name.
///
/// A `ConsumerGroup` owns a WAL file and does its own internal locking
/// once opened, so this registry's own lock is only ever held for the
/// brief moment of looking up or inserting an `Arc` — never across a
/// `ConsumerGroup` operation itself. Re-opening a `ConsumerGroup` on
/// every tool call (rather than caching it here) would mean replaying
/// its WAL from scratch every time, which defeats the entire point of
/// it being durable, in-process state.
struct QueueRegistry {
    data_dir: PathBuf,
    groups: Mutex<HashMap<String, Arc<ConsumerGroup<serde_json::Value>>>>,
}

impl QueueRegistry {
    fn new(data_dir: PathBuf) -> Self {
        Self { data_dir, groups: Mutex::new(HashMap::new()) }
    }

    /// Returns the queue named `name`, opening it (creating its WAL file
    /// under `data_dir` if this is the first reference to it, in this
    /// process or ever) if it isn't already held.
    async fn get(&self, name: &str) -> Result<Arc<ConsumerGroup<serde_json::Value>>, ErrorData> {
        validate_queue_name(name)?;

        let mut groups = self.groups.lock().await;
        if let Some(group) = groups.get(name) {
            return Ok(Arc::clone(group));
        }

        let path = self.data_dir.join(format!("{name}.wal"));
        let group = ConsumerGroup::open(path, VISIBILITY_TIMEOUT, RetryPolicy::DEFAULT)
            .await
            .map_err(|error| {
                ErrorData::internal_error(format!("failed to open queue {name:?}: {error}"), None)
            })?;
        let group = Arc::new(group);
        groups.insert(name.to_string(), Arc::clone(&group));
        Ok(group)
    }
}

/// Arguments for the `enqueue` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct EnqueueParams {
    /// Which queue to enqueue onto. Opened automatically if it doesn't
    /// exist yet — there's no separate "create queue" step.
    queue: String,
    /// The message payload. Any JSON value; QaaS doesn't interpret it.
    payload: serde_json::Value,
    /// Optional caller-supplied deduplication key. An enqueue reusing a
    /// key already present in this queue (pending, leased, or
    /// dead-lettered) is a no-op that returns the *original* message's
    /// id — safe to retry a timed-out enqueue call without risking a
    /// duplicate message.
    idempotency_key: Option<String>,
}

/// Result of the `enqueue` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct EnqueueResult {
    /// The enqueued message's id. A later `claim` returns this same id
    /// when it delivers the message; pass it to `ack`/`nack` then.
    message_id: String,
}

/// Arguments for the `claim` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct ClaimParams {
    /// Which queue to claim from.
    queue: String,
    /// How long to wait for a message to become available, in
    /// milliseconds, before returning `available: false`. Defaults to
    /// 5000; capped at 60000 regardless of what's requested.
    wait_ms: Option<u64>,
}

/// Result of the `claim` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct ClaimResult {
    /// `false` means the wait window elapsed with nothing claimable —
    /// an empty queue, or everything currently leased to other
    /// consumers. Not an error; every other field is `null` in that
    /// case.
    available: bool,
    /// The claimed message's id. Present only when `available` is true.
    message_id: Option<String>,
    /// This delivery's lease token. Pass it back to `ack`/`nack`
    /// exactly as returned — it identifies *this* delivery, not the
    /// message itself, and does not carry over to a redelivery.
    lease_token: Option<u64>,
    /// A stable idempotency key for this message, safe to pass through
    /// to a downstream call that itself supports idempotency keys —
    /// always present (even for a message enqueued without one), see
    /// [`qaas_core::Claim`]'s own docs for why.
    idempotency_key: Option<String>,
    /// The message payload, exactly as enqueued.
    payload: Option<serde_json::Value>,
    /// How many times this message has now been delivered, including
    /// this delivery. Starts at 1.
    delivery_count: Option<u32>,
    /// The latest progress saved via `checkpoint` for this message, if
    /// any — `null` for a message's first-ever claim, or one that was
    /// never checkpointed. Present so a resuming workflow can pick up
    /// where a previous delivery (crashed, or paused via `nack`) left
    /// off instead of redoing already-completed steps.
    checkpoint: Option<serde_json::Value>,
}

/// Arguments for the `ack` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct AckParams {
    /// Which queue the message was claimed from.
    queue: String,
    /// The message id from a prior `claim` call.
    message_id: String,
    /// The lease token from that same `claim` call.
    lease_token: u64,
}

/// Result of the `ack` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct AckResult {
    /// `false` means this id/token pair no longer matched an active
    /// lease — most likely it already expired and the message was
    /// redelivered to someone else. Not an error: acking a lease that's
    /// already gone is an expected race, not a caller mistake.
    acked: bool,
}

/// Arguments for the `nack` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct NackParams {
    /// Which queue the message was claimed from.
    queue: String,
    /// The message id from a prior `claim` call.
    message_id: String,
    /// The lease token from that same `claim` call.
    lease_token: u64,
    /// Why this delivery failed, recorded on the dead letter if this is
    /// the delivery that exhausts the queue's retry policy.
    reason: Option<String>,
}

/// Result of the `nack` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct NackResult {
    /// Same stale-lease semantics as [`AckResult::acked`].
    nacked: bool,
}

/// Arguments for the `checkpoint` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct CheckpointParams {
    /// Which queue the message was claimed from.
    queue: String,
    /// The message id from a prior `claim` call.
    message_id: String,
    /// The lease token from that same `claim` call.
    lease_token: u64,
    /// This workflow's current progress, any JSON value. Shape it as a
    /// full snapshot of where the task is, not a delta — only the
    /// latest checkpoint per message is kept, and it entirely replaces
    /// whatever was saved before.
    state: serde_json::Value,
}

/// Result of the `checkpoint` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct CheckpointResult {
    /// Same stale-lease semantics as [`AckResult::acked`] — `false`
    /// means this id/token pair no longer matched an active lease, and
    /// `state` was not associated with this delivery. Note that in one
    /// narrow race (the lease expiring in the moment between this call's
    /// internal checks) `state` may still have been durably recorded
    /// even though this returns `false` — see `qaas_core::ConsumerGroup::checkpoint`'s
    /// own docs.
    checkpointed: bool,
}

/// The MCP server itself. Cheap to clone — the only state is an `Arc`'d
/// [`QueueRegistry`] — which `rmcp` relies on internally when handling
/// more than one tool call concurrently over the same connection.
#[derive(Clone)]
pub struct QaasMcpServer {
    registry: Arc<QueueRegistry>,
}

#[tool_router]
impl QaasMcpServer {
    /// Creates a server that opens queue WAL files under `data_dir`
    /// (creating the directory itself is the caller's job — see
    /// `main.rs`), ready to be handed to
    /// [`ServiceExt::serve`](rmcp::ServiceExt::serve).
    #[must_use]
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self { registry: Arc::new(QueueRegistry::new(data_dir.into())) }
    }

    /// Durably enqueues a message onto a named queue, creating the queue
    /// on first use.
    #[tool(
        description = "Durably enqueue a message onto a named queue, creating the queue on first use."
    )]
    async fn enqueue(
        &self,
        Parameters(params): Parameters<EnqueueParams>,
    ) -> Result<Json<EnqueueResult>, ErrorData> {
        let group = self.registry.get(&params.queue).await?;

        let idempotency_key =
            params.idempotency_key.map(qaas_types::IdempotencyKey::new).transpose().map_err(
                |_| ErrorData::invalid_params("idempotency_key must not be empty", None),
            )?;

        let message_id = match idempotency_key {
            Some(key) => group.enqueue_with_key(params.payload, key).await,
            None => group.enqueue(params.payload).await,
        }
        .map_err(|error| io_error_to_mcp(&error))?;

        Ok(Json(EnqueueResult { message_id: message_id.to_string() }))
    }

    /// Claims the next available message from a named queue, waiting up
    /// to `wait_ms` (default 5000, max 60000) for one to become
    /// available.
    #[tool(
        description = "Claim the next available message from a named queue, waiting up to wait_ms \
                        (default 5000, max 60000) for one to become available. Call ack or nack with \
                        the returned message_id and lease_token once you're done with it."
    )]
    async fn claim(
        &self,
        Parameters(params): Parameters<ClaimParams>,
    ) -> Result<Json<ClaimResult>, ErrorData> {
        let group = self.registry.get(&params.queue).await?;
        let wait =
            params.wait_ms.map_or(DEFAULT_CLAIM_WAIT, Duration::from_millis).min(MAX_CLAIM_WAIT);

        match tokio::time::timeout(wait, group.claim()).await {
            Ok(claim) => Ok(Json(ClaimResult {
                available: true,
                message_id: Some(claim.id.to_string()),
                lease_token: Some(claim.token.as_u64()),
                idempotency_key: Some(claim.idempotency_key.as_str().to_string()),
                payload: Some(claim.item),
                delivery_count: Some(claim.delivery_count),
                checkpoint: claim.checkpoint,
            })),
            Err(_elapsed) => Ok(Json(ClaimResult {
                available: false,
                message_id: None,
                lease_token: None,
                idempotency_key: None,
                payload: None,
                delivery_count: None,
                checkpoint: None,
            })),
        }
    }

    /// Permanently removes a claimed message, resolving it successfully.
    #[tool(description = "Permanently remove a claimed message, resolving it successfully.")]
    async fn ack(
        &self,
        Parameters(params): Parameters<AckParams>,
    ) -> Result<Json<AckResult>, ErrorData> {
        let group = self.registry.get(&params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let token = LeaseToken::from_u64(params.lease_token);

        let acked = group.ack(message_id, token).await.map_err(|error| io_error_to_mcp(&error))?;
        Ok(Json(AckResult { acked }))
    }

    /// Releases a claimed message back for retry, or dead-letters it if
    /// this delivery exhausts the queue's retry policy.
    #[tool(description = "Release a claimed message back for retry (or dead-letter it, if this \
                        delivery exhausts the queue's retry policy), optionally recording why it failed.")]
    async fn nack(
        &self,
        Parameters(params): Parameters<NackParams>,
    ) -> Result<Json<NackResult>, ErrorData> {
        let group = self.registry.get(&params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let token = LeaseToken::from_u64(params.lease_token);

        let nacked = group
            .nack(message_id, token, params.reason)
            .await
            .map_err(|error| io_error_to_mcp(&error))?;
        Ok(Json(NackResult { nacked }))
    }

    /// Durably saves progress against a still-claimed message, without
    /// resolving it — the lease keeps running unchanged.
    #[tool(
        description = "Durably save progress against a still-claimed message, without resolving \
                        it - the lease keeps running unchanged. Call this after each step of a \
                        multi-step task so a future claim (after a crash, or after you nack to \
                        pause) can resume from here instead of starting over. Only the latest \
                        state per message is kept."
    )]
    async fn checkpoint(
        &self,
        Parameters(params): Parameters<CheckpointParams>,
    ) -> Result<Json<CheckpointResult>, ErrorData> {
        let group = self.registry.get(&params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let token = LeaseToken::from_u64(params.lease_token);

        let checkpointed = group
            .checkpoint(message_id, token, params.state)
            .await
            .map_err(|error| io_error_to_mcp(&error))?;
        Ok(Json(CheckpointResult { checkpointed }))
    }
}

#[tool_handler(
    instructions = "Durable, agent-native message queue. Call enqueue to add work to a named \
                    queue (created automatically on first use), claim to receive the next \
                    available message (waiting up to wait_ms), and ack once you've finished it \
                    successfully — or nack to release it for retry, optionally recording why it \
                    failed. Every queue is independent; pick a queue name that groups related \
                    work. For a multi-step task, call checkpoint after each step to durably save \
                    progress; a later claim of the same message (after a crash, or after you \
                    nack to pause) returns that progress so you can resume instead of starting \
                    over."
)]
impl ServerHandler for QaasMcpServer {}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::tempdir;

    use super::{QaasMcpServer, validate_queue_name};

    fn server(dir: &tempfile::TempDir) -> QaasMcpServer {
        QaasMcpServer::new(dir.path())
    }

    #[test]
    fn queue_names_reject_path_traversal_and_separators() {
        for bad in ["", "../escape", "a/b", "a\0b", &"x".repeat(129)] {
            assert!(validate_queue_name(bad).is_err(), "{bad:?} should be rejected");
        }
        for good in ["orders", "orders-2", "orders_2", "A1"] {
            assert!(validate_queue_name(good).is_ok(), "{good:?} should be accepted");
        }
    }

    #[tokio::test]
    async fn enqueue_then_claim_then_ack_round_trips_through_the_tool_methods() {
        let dir = tempdir().unwrap();
        let server = server(&dir);

        let enqueued = server
            .enqueue(rmcp::handler::server::wrapper::Parameters(super::EnqueueParams {
                queue: "orders".to_string(),
                payload: json!({"item": "widget"}),
                idempotency_key: None,
            }))
            .await
            .unwrap()
            .0;

        let claimed = server
            .claim(rmcp::handler::server::wrapper::Parameters(super::ClaimParams {
                queue: "orders".to_string(),
                wait_ms: Some(100),
            }))
            .await
            .unwrap()
            .0;
        assert!(claimed.available);
        assert_eq!(claimed.message_id, Some(enqueued.message_id.clone()));
        assert_eq!(claimed.payload, Some(json!({"item": "widget"})));
        assert_eq!(claimed.delivery_count, Some(1));
        assert_eq!(claimed.checkpoint, None);

        let acked = server
            .ack(rmcp::handler::server::wrapper::Parameters(super::AckParams {
                queue: "orders".to_string(),
                message_id: claimed.message_id.unwrap(),
                lease_token: claimed.lease_token.unwrap(),
            }))
            .await
            .unwrap()
            .0;
        assert!(acked.acked);
    }

    #[tokio::test]
    async fn a_checkpoint_is_visible_on_the_next_claim_after_a_pausing_nack() {
        let dir = tempdir().unwrap();
        let server = server(&dir);

        server
            .enqueue(rmcp::handler::server::wrapper::Parameters(super::EnqueueParams {
                queue: "workflows".to_string(),
                payload: json!("start the task"),
                idempotency_key: None,
            }))
            .await
            .unwrap();

        let first = server
            .claim(rmcp::handler::server::wrapper::Parameters(super::ClaimParams {
                queue: "workflows".to_string(),
                wait_ms: Some(100),
            }))
            .await
            .unwrap()
            .0;

        let progress = json!({"completed_steps": ["fetch", "summarize"]});
        let checkpointed = server
            .checkpoint(rmcp::handler::server::wrapper::Parameters(super::CheckpointParams {
                queue: "workflows".to_string(),
                message_id: first.message_id.clone().unwrap(),
                lease_token: first.lease_token.unwrap(),
                state: progress.clone(),
            }))
            .await
            .unwrap()
            .0;
        assert!(checkpointed.checkpointed);

        // Pausing — e.g. waiting on a human — releases the message the
        // same way a failure does (see the module docs for why), but the
        // checkpoint already saved should still be there for whoever
        // claims it next.
        let nacked = server
            .nack(rmcp::handler::server::wrapper::Parameters(super::NackParams {
                queue: "workflows".to_string(),
                message_id: first.message_id.unwrap(),
                lease_token: first.lease_token.unwrap(),
                reason: None,
            }))
            .await
            .unwrap()
            .0;
        assert!(nacked.nacked);

        // The registry opens queues with `RetryPolicy::DEFAULT`, whose
        // 500ms base backoff delay means the nacked message isn't
        // immediately claimable again — `wait_ms` here needs to be
        // comfortably longer than that, not just longer than zero, or
        // this becomes exactly the kind of timing-sensitive test that's
        // flaky under load rather than one that's reliably correct.
        let resumed = server
            .claim(rmcp::handler::server::wrapper::Parameters(super::ClaimParams {
                queue: "workflows".to_string(),
                wait_ms: Some(2000),
            }))
            .await
            .unwrap()
            .0;
        assert!(resumed.available, "should not still be waiting out the retry backoff");
        assert_eq!(resumed.checkpoint, Some(progress));
    }

    #[tokio::test]
    async fn checkpointing_with_a_stale_lease_token_is_rejected() {
        let dir = tempdir().unwrap();
        let server = server(&dir);

        let result = server
            .checkpoint(rmcp::handler::server::wrapper::Parameters(super::CheckpointParams {
                queue: "workflows".to_string(),
                message_id: qaas_types::MessageId::new().to_string(),
                lease_token: 0,
                state: json!("never claimed"),
            }))
            .await
            .unwrap()
            .0;

        assert!(!result.checkpointed);
    }

    #[tokio::test]
    async fn claim_reports_unavailable_rather_than_blocking_forever_on_an_empty_queue() {
        let dir = tempdir().unwrap();
        let server = server(&dir);

        let claimed = server
            .claim(rmcp::handler::server::wrapper::Parameters(super::ClaimParams {
                queue: "empty".to_string(),
                wait_ms: Some(20),
            }))
            .await
            .unwrap()
            .0;

        assert!(!claimed.available);
        assert_eq!(claimed.message_id, None);
        assert_eq!(claimed.lease_token, None);
    }

    #[tokio::test]
    async fn nack_without_a_matching_lease_is_a_false_not_an_error() {
        let dir = tempdir().unwrap();
        let server = server(&dir);

        let result = server
            .nack(rmcp::handler::server::wrapper::Parameters(super::NackParams {
                queue: "orders".to_string(),
                message_id: qaas_types::MessageId::new().to_string(),
                lease_token: 0,
                reason: Some("simulated failure".to_string()),
            }))
            .await
            .unwrap()
            .0;

        assert!(!result.nacked);
    }

    #[tokio::test]
    async fn enqueue_with_a_reused_idempotency_key_does_not_create_a_second_message() {
        let dir = tempdir().unwrap();
        let server = server(&dir);
        let params = || super::EnqueueParams {
            queue: "orders".to_string(),
            payload: json!("payload"),
            idempotency_key: Some("dedup-1".to_string()),
        };

        let first =
            server.enqueue(rmcp::handler::server::wrapper::Parameters(params())).await.unwrap().0;
        let second =
            server.enqueue(rmcp::handler::server::wrapper::Parameters(params())).await.unwrap().0;

        assert_eq!(first.message_id, second.message_id);
    }

    #[tokio::test]
    async fn an_invalid_queue_name_is_rejected_before_touching_the_filesystem() {
        let dir = tempdir().unwrap();
        let server = server(&dir);

        let result = server
            .enqueue(rmcp::handler::server::wrapper::Parameters(super::EnqueueParams {
                queue: "../escape".to_string(),
                payload: json!("x"),
                idempotency_key: None,
            }))
            .await;

        // `Json<EnqueueResult>` isn't `Debug` (it's `rmcp`'s wrapper
        // type, not one of ours), so `unwrap_err` isn't available here —
        // matching directly is just as clear and doesn't need it.
        let Err(error) = result else {
            panic!("expected an error, got a successful enqueue");
        };
        assert_eq!(error.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }
}
