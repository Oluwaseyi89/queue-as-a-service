//! Exposes the queue as MCP tools: `enqueue`, `claim`, `ack`, `nack`,
//! `checkpoint`, `configure_admission`, `admission_status`,
//! `configure_route`, `list_dead_letters`, `triage_dead_letter`,
//! `reprocess_dead_letter`, `purge_dead_letter`, `publish_partial_result`,
//! `stream_partial_results`.
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
//!
//! # Token/cost admission control (`feature/token-cost-aware-admission`)
//!
//! Every queue gets its own [`qaas_core::AdmissionController`] (see that
//! module's docs for the sliding-window algorithm), starting unlimited —
//! admission control is opt-in per queue via `configure_admission`, not a
//! surprise default every queue this server already had suddenly has to
//! satisfy. Two different checks against the *same* window, not one:
//!
//! - `enqueue` takes optional `estimated_tokens`/`estimated_cost`
//!   arguments — a producer's up-front estimate of what a task will cost
//!   — and admits (recording them) or refuses the enqueue outright if
//!   either would exceed the queue's ceiling this window. Omitting both
//!   defaults to zero cost, so existing callers that don't know or care
//!   about token economics are unaffected.
//! - `claim` checks, read-only, whether the queue is *currently* within
//!   both ceilings before even attempting to claim anything, refusing
//!   (`available: false, throttled: true`) without waiting out `wait_ms`
//!   at all if not. This is deliberately not a second deduction of the
//!   claimed message's own cost — `enqueue` already recorded that
//!   estimate — it exists to catch an operator having *lowered* a
//!   queue's budget live (`configure_admission`) below what's already
//!   been admitted this window: `enqueue` alone wouldn't stop a consumer
//!   from continuing to burn through work that was admitted under a more
//!   generous, since-tightened ceiling.
//!
//! `configure_admission` and `admission_status` are the only tools this
//! server has that aren't producer/consumer delivery operations — the
//! closest thing to an admin surface this project has, since there's no
//! HTTP control plane yet (`feature/api-auth`, later, is what a real one
//! would need first).
//!
//! # Semantic dedup and routing (`feature/semantic-dedup-routing`)
//!
//! `enqueue`'s `queue` argument is now optional, and it gains a new
//! optional `embedding` argument — a caller-computed vector (this crate
//! has no embedding model of its own; see [`qaas_core::semantic`]'s own
//! docs for why) representing the task's content. Two independent uses
//! of the same idea, each backed by its own
//! [`qaas_core::EmbeddingIndex`]:
//!
//! - **Dedup**: when `embedding` is given, it's checked against every
//!   other embedding recently seen on the *target* queue (within
//!   [`DEDUP_TTL`], regardless of whether the message that submitted it
//!   is still pending, leased, or even already acked — see this
//!   module's own registry docs for exactly what "recently" bounds).
//!   A match at or above [`DEDUP_SIMILARITY_THRESHOLD`] collapses the
//!   enqueue: nothing new is written, and the *existing* near-duplicate's
//!   id comes back instead, flagged `deduplicated: true` — the same
//!   "safe to retry, no duplicate created" contract
//!   [`enqueue_with_key`](qaas_core::ConsumerGroup::enqueue_with_key)
//!   already gives an exact idempotency-key match, just approximate
//!   instead of exact, and catching duplicates across callers that never
//!   shared an idempotency key in the first place — precisely the
//!   "multiple agents independently queue overlapping work" scenario
//!   `Plan.md`'s line for this branch names.
//! - **Routing**: when `queue` is omitted, `embedding` is required, and
//!   is matched instead against every queue's own descriptor embedding
//!   (set via `configure_route`) — the enqueue lands in whichever
//!   registered queue's descriptor is closest, with no threshold (unlike
//!   dedup, routing always picks *something* if any route exists at
//!   all — there's no "not similar enough" outcome for "which specialized
//!   consumer should get this," only "which is the best of the ones
//!   available"). Omitting `queue` with no routes registered, or with
//!   no `embedding` given either, is a request the server can't fulfill
//!   and rejects outright.
//!
//! `DEDUP_SIMILARITY_THRESHOLD` and `DEDUP_TTL` are fixed constants in
//! this branch, not configurable per queue — the same "doesn't add
//! configurability its underlying engine doesn't already have" stance
//! `VISIBILITY_TIMEOUT` already takes.
//!
//! # LLM-assisted DLQ triage (`feature/llm-assisted-dlq-triage`)
//!
//! Every prior branch that mentioned the DLQ pointed here — `ConsumerGroup`
//! has had `dead_letters`/`reprocess_dead_letter`/`purge_dead_letter`
//! since `feature/dead-letter-queue`, but nothing exposed them over MCP
//! until now. Four new tools:
//!
//! - `list_dead_letters` — every dead letter on a queue, including its
//!   `checkpoint` (`feature/durable-agent-workflows`'s workflow state,
//!   finally carried onto the dead letter itself — see
//!   `qaas_core::dead_letter`'s own docs) and any existing `triage`
//!   verdict. What a triage agent (or a human) reads before deciding
//!   anything.
//! - `triage_dead_letter` — the tool this branch is named for: records a
//!   classification (`transient` or `permanent`) and a reason via
//!   `qaas_core::ConsumerGroup::annotate_dead_letter`, then *auto-applies
//!   the matching policy* — `transient` immediately calls
//!   `reprocess_dead_letter` internally; `permanent` does nothing further,
//!   leaving the entry annotated and in the DLQ. Deliberately asymmetric:
//!   reprocessing is reversible (a message that shouldn't have been
//!   retried just fails and comes back to the DLQ again), so auto-applying
//!   it from a classification is a reasonable bet. Purging is not
//!   reversible, so this tool never does it, no matter how confident a
//!   `permanent` classification is — `Plan.md`'s own wording for this
//!   branch calls that bucket "needs-human," not "needs deletion," and a
//!   destructive action a person didn't directly trigger has no place
//!   here. A human (or an agent a human is supervising) who agrees a
//!   `permanent` entry is worth discarding calls `purge_dead_letter`
//!   separately, as its own explicit action.
//! - `reprocess_dead_letter` / `purge_dead_letter` — direct MCP exposure
//!   of the `ConsumerGroup` methods of the same name, for a human (or an
//!   agent) that's already decided without going through triage's
//!   classify-then-explain ceremony.
//!
//! # Streaming delivery (`feature/streaming-delivery`)
//!
//! `publish_partial_result` and `stream_partial_results` expose
//! `qaas_core::ConsumerGroup`'s new partial-result methods directly —
//! see that module's own docs for why the underlying primitive is
//! deliberately ephemeral (in-memory only, gone once the lease that
//! produced it ends) rather than durable like everything else this
//! server persists.
//!
//! This is "SSE/WebSocket delivery" in spirit, not on the wire: this
//! project has had exactly one network listener — none — since
//! `feature/mcp-server-interface` deliberately deferred one until
//! `feature/api-auth` exists, and this branch lands *before* that one.
//! Opening a raw HTTP/SSE socket now would mean the first unauthenticated
//! network listener in the project's history, for a project whose own
//! prior branch called that "a real, not hypothetical, security hole."
//! Instead, streaming happens over the exact same stdio MCP connection
//! every other tool here already uses: `stream_partial_results` is a
//! bounded-wait poll (`wait_ms`, same shape as `claim`'s) rather than a
//! server-initiated push, which is what actually makes incremental
//! delivery possible over a synchronous request/response tool-call
//! transport without inventing a second connection or a notification
//! protocol this SDK doesn't hand us for free. A caller that wants
//! near-real-time updates just calls it in a loop, passing back the
//! highest `sequence` it's already seen each time.
//!
//! # Multi-tenant quotas (`feature/multi-tenant-quotas`)
//!
//! `feature/api-auth` made two tenants' queues fully isolated — never
//! seeing or touching each other's messages — but said nothing about how
//! much of the *shared* process either one gets to use. A noisy tenant,
//! or a single runaway agent loop, could still open an unbounded number
//! of queues, leave an unbounded number of messages pending across them,
//! or hammer tool calls fast enough to starve every other tenant's
//! actual work of CPU and lock time — isolation alone doesn't prevent
//! any of that. This branch closes that gap with three independent,
//! per-tenant ceilings, all opt-in (a tenant nobody has configured a
//! quota for is never throttled) and all exempt for the trusted stdio
//! connection, the same as every tenant-scoped check `feature/api-auth`
//! already added:
//!
//! - `max_queues` — `enqueue`, `claim`, and every other tool that opens a
//!   queue on first reference (see [`QueueRegistry::get`]) refuses to
//!   open a tenant's next-over-the-limit queue. An already-open queue
//!   never becomes invalid just because the ceiling was lowered since.
//! - `max_pending_messages` — `enqueue` refuses a message that would
//!   push a tenant's total unacknowledged count, summed across every
//!   queue it has open, past the ceiling. Checked after dedup (a
//!   collapsed enqueue creates no new pending message) but before the
//!   per-queue token/cost admission check — the coarser, tenant-wide gate
//!   comes first.
//! - `requests_per_window` — every tool call, not just `enqueue`, spends
//!   one request of the calling tenant's rate quota via a new
//!   [`QaasMcpServer::authorize`] method that every `#[tool]`-annotated
//!   method now calls instead of the bare [`tenant_from`] `feature/api-auth`
//!   introduced. Backed by [`qaas_core::TenantQuota`] — the same
//!   trailing-window algorithm [`qaas_core::AdmissionController`] already
//!   ports from global-rate-limiter, just counting plain requests again
//!   instead of tokens and dollars. See that module's own docs for why
//!   it's a sibling primitive rather than a generalization of
//!   `AdmissionController`.
//!
//! Two new tools: `configure_tenant_quota` sets all three ceilings for a
//! named tenant, restricted to the trusted stdio connection the same way
//! `create_api_key` is — an operator action, not something a tenant
//! credential should be able to do to itself or anyone else.
//! `tenant_quota_status` is the self-service counterpart: any
//! HTTP-authenticated tenant can call it with no arguments to see its own
//! configuration and live usage (queues open, pending messages, requests
//! this window), while the stdio connection must name which tenant to
//! report on, since it has no tenant of its own to default to.
//!
//! # Tracing and metrics (`feature/tracing-metrics`)
//!
//! Every `#[tool]`-dispatched method's `_impl` sibling now carries
//! `#[tracing::instrument(skip_all, fields(tenant = ...))]` — a real span
//! per tool call, nested under whatever `rmcp` itself already emits, with
//! the resolved tenant (`"none"` for stdio) as a field. `qaas_core`'s own
//! `ConsumerGroup` methods gained the same attribute this branch (see
//! that crate's docs), so a single agent's `claim` → work → `ack` already
//! shows up as a proper parent/child span chain with zero extra plumbing
//! here — `main.rs`'s [`crate::telemetry::init_tracing`] is what turns
//! those spans into exported `OpenTelemetry` traces, opt-in via
//! `QAAS_OTEL_ENDPOINT`.
//!
//! Three Prometheus metrics, recorded via [`record_queue_metrics`] and,
//! for the third, directly in [`QaasMcpServer::ack_impl`] — every one
//! labeled by `queue` and `tenant`, `Plan.md`'s own "per-queue and
//! per-agent" breakdown, where `tenant` is the closest thing this project
//! has to an agent identity axis (see `crate::telemetry`'s own docs on
//! why that's the right reading):
//!
//! - `qaas_queue_depth` — a gauge, [`qaas_core::ConsumerGroup::len`]
//!   re-recorded after every `enqueue`/`claim`/`ack`/`nack` that could
//!   have changed it.
//! - `qaas_queue_oldest_pending_age_seconds` — a gauge, this branch's
//!   consumer-lag signal: how long the oldest still-claimable message on
//!   a queue has been waiting, from the new
//!   [`qaas_core::ConsumerGroup::oldest_pending_age`].
//! - `qaas_message_latency_seconds` — a histogram, this branch's
//!   "latency percentiles" signal: end-to-end time from a message's
//!   enqueue to its successful `ack`, read straight off the acked
//!   message's own id (`MessageId` embeds its own generation time — see
//!   [`qaas_types::MessageId::timestamp`]) rather than a separately
//!   tracked "claimed at" field. Recorded only on a real ack, not a
//!   nack — a nacked message isn't done yet, so "how long did it take"
//!   has no answer until whichever ack eventually resolves it.
//!
//! Read as "the queue itself," not "the RPC layer," deliberately: a
//! fourth, tool-call-duration histogram (mirroring the rate limiter's
//! own proxied-request latency more literally) was considered and left
//! out — `Plan.md`'s line names exactly these three queueing-theory
//! metrics, and inventing a fourth un-requested one is exactly the kind
//! of scope creep this project's branches otherwise avoid. Depth, lag,
//! and message latency already jointly describe a queue's health the
//! way Little's law relates them; per-call RPC timing is a reasonable
//! future addition, not a gap this branch leaves half-finished.
//!
//! Metrics are served from their own listener
//! ([`crate::telemetry::init_metrics`]), not a route on this module's own
//! HTTP transport — see that module's docs for why: this data is
//! inherently cross-tenant and operator-facing, not something a single
//! tenant's API key should unlock.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use qaas_core::{
    AdmissionConfig, AdmissionController, AdmissionDecision, ApiKeyStore, ConsumerGroup, Embedding,
    EmbeddingIndex, LeaseToken, PartialResultsPoll, QuotaConfig, QuotaDecision, RetryPolicy,
    TenantId, TenantQuota, TriageClassification, TriageVerdict,
};
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool, tool_handler, tool_router};
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

/// Cosine similarity at or above which two tasks on the same queue are
/// treated as near-duplicates. `0.95` is a common real-world starting
/// point for "these two embeddings represent the same underlying
/// content" with typical sentence/document embedding models — high
/// enough that genuinely different tasks essentially never collide,
/// while still catching paraphrased near-duplicates that an exact
/// idempotency-key match never would.
const DEDUP_SIMILARITY_THRESHOLD: f32 = 0.95;

/// How long an enqueued task's embedding stays eligible for dedup
/// matching. Ten minutes: long enough to catch the scenario `Plan.md`'s
/// line for this branch actually names — several agents independently
/// noticing the same work and queuing it within a short window of each
/// other — without keeping every task's embedding around indefinitely.
/// See [`EmbeddingIndex`]'s own docs for why a TTL exists here at all
/// (this is dedup's safety net for messages this server can't observe
/// leaving the live queue any other way).
const DEDUP_TTL: Duration = Duration::from_secs(600);

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

/// The authenticated tenant for this tool call, if any — `None` for a
/// call that arrived over the trusted local stdio connection (no
/// authentication happens there at all; see this module's own docs), or
/// `Some` for one that arrived over HTTP, where `crate::http_auth`'s
/// middleware has *already* verified a bearer credential and inserted
/// the [`TenantId`] it resolved to into the request's own extensions
/// before this call was ever dispatched — this function only reads that
/// back, it never itself decides whether a caller is who it claims.
///
/// Reads through two layers, both documented (and demonstrated) in
/// `rmcp`'s own `StreamableHttpService` docs: `ctx.extensions` carries
/// the raw `http::request::Parts` for an HTTP-transport call (absent
/// entirely for stdio, which is exactly what makes the `None` case work
/// without any transport-specific branching here); `Parts.extensions`
/// is where a tower/axum `Extension` layer — `http_auth`'s middleware —
/// puts application state onto the request itself.
fn tenant_from(ctx: &RequestContext<RoleServer>) -> Option<TenantId> {
    ctx.extensions.get::<http::request::Parts>()?.extensions.get::<TenantId>().cloned()
}

/// Maps an [`AdmissionDecision::Denied`] or a [`QuotaDecision::Denied`]
/// onto the MCP error shape — both share the exact same `reason` /
/// `retry_after` structure by design (see [`quota`](qaas_core::quota)'s
/// own docs on why it's a sibling of
/// [`admission`](qaas_core::admission) rather than a rewrite), so one
/// mapper serves both call sites instead of two near-duplicates.
/// [`ErrorCode::INVALID_REQUEST`](rmcp::model::ErrorCode) rather than
/// `INVALID_PARAMS`: the request itself is well-formed, just not
/// currently admissible — the same distinction a real HTTP 429 draws
/// from a 400. `retry_after` travels in the structured `data` field
/// (seconds, or absent if retrying can never help) so a calling agent
/// can act on it programmatically instead of having to parse it back out
/// of `reason`'s prose.
fn throttled_to_mcp(reason: &str, retry_after: Option<Duration>) -> ErrorData {
    ErrorData::invalid_request(
        reason.to_string(),
        Some(serde_json::json!({ "retry_after_seconds": retry_after.map(|d| d.as_secs_f64()) })),
    )
}

/// The tenant-axis label used on every metric this module records —
/// `"none"` for the trusted stdio connection, an owned copy of the
/// tenant id otherwise. `metrics`' own macros require a label value that
/// resolves to `String` or `&'static str` (see their doc comments); a
/// borrowed `&str` tied to `tenant`'s own lifetime satisfies neither, so
/// this always allocates rather than trying to thread a borrow through
/// call sites that frequently don't have one long enough to give.
fn tenant_label(tenant: Option<&TenantId>) -> String {
    tenant.map_or_else(|| "none".to_string(), |tenant| tenant.as_str().to_string())
}

/// Updates this module's two queue-shaped gauges for `queue` — see the
/// module docs for what each measures. Every tool that can change a
/// queue's depth or its oldest-pending message's age (`enqueue`,
/// `claim`, `ack`, `nack`) calls this once it's done, so a scrape always
/// reflects the queue's state as of the most recent operation against
/// it rather than needing a separate polling loop to keep these gauges
/// current.
async fn record_queue_metrics(
    tenant: Option<&TenantId>,
    queue: &str,
    group: &ConsumerGroup<serde_json::Value>,
) {
    let tenant = tenant_label(tenant);
    let queue = queue.to_string();

    // `usize`/`u64` -> `f64`: a queue depth or an age in seconds could in
    // principle lose precision above 2^53, a scale this project will
    // never see a single queue reach — narrower than pretending the
    // conversion could realistically fail.
    #[allow(clippy::cast_precision_loss)]
    let depth = group.len().await as f64;
    metrics::gauge!("qaas_queue_depth", "queue" => queue.clone(), "tenant" => tenant.clone())
        .set(depth);

    let lag = group.oldest_pending_age().await.map_or(0.0, |age| age.as_secs_f64());
    metrics::gauge!("qaas_queue_oldest_pending_age_seconds", "queue" => queue, "tenant" => tenant)
        .set(lag);
}

/// Lazily opens and holds one [`ConsumerGroup`], one
/// [`AdmissionController`], and one dedup [`EmbeddingIndex`] per
/// *tenant-scoped* queue name, plus one routing [`EmbeddingIndex`] and
/// one [`TenantQuota`] per tenant.
///
/// Every lookup here takes `tenant: Option<&TenantId>` — `None` for a
/// call that came in over the trusted local stdio connection (see this
/// module's own docs on why that connection stays unauthenticated),
/// `Some` for one that arrived over HTTP and was authenticated to a
/// specific tenant by [`crate::http_auth`]'s middleware before ever
/// reaching a tool method. `Some(a)` and `Some(b)` (or `None`) never see
/// or touch each other's queues, even if they both ask for a queue
/// literally named `"orders"` — this is the actual trust boundary
/// `feature/api-auth` exists to build, not just bookkeeping: isolation
/// is total and unconditional, not a permission a tenant could be
/// granted or denied. `feature/multi-tenant-quotas` is what finally gives
/// an operator a knob for the question isolation alone never answered —
/// how much of the *shared* process a given tenant gets to use: `Self::get`
/// refuses to open a tenant's `max_queues`-plus-first queue, `enqueue`
/// (see [`QaasMcpServer::enqueue_impl`]) refuses one that would push a
/// tenant's total pending messages past `max_pending_messages`, and
/// [`QaasMcpServer::authorize`] refuses any tool call once a tenant is
/// over its `requests_per_window`. Stdio is exempt from all three, same
/// as it's exempt from tenant scoping in the first place.
///
/// A `ConsumerGroup` owns a WAL file and does its own internal locking
/// once opened, so this registry's own lock is only ever held for the
/// brief moment of looking up or inserting an `Arc` — never across a
/// `ConsumerGroup` operation itself. Re-opening a `ConsumerGroup` on
/// every tool call (rather than caching it here) would mean replaying
/// its WAL from scratch every time, which defeats the entire point of
/// it being durable, in-process state. `AdmissionController` and
/// `EmbeddingIndex` have no WAL to replay — both are deliberately
/// in-memory only (see their own docs) — but are cached here for the
/// same reason: a fresh, empty one on every tool call would mean neither
/// ever actually did anything.
struct QueueRegistry {
    data_dir: PathBuf,
    groups: Mutex<HashMap<String, Arc<ConsumerGroup<serde_json::Value>>>>,
    admission: Mutex<HashMap<String, Arc<AdmissionController>>>,
    dedup: Mutex<HashMap<String, Arc<EmbeddingIndex<qaas_types::MessageId>>>>,
    routes: Mutex<HashMap<String, Arc<EmbeddingIndex<String>>>>,
    /// One [`TenantQuota`] per tenant, never per queue — `max_queues` and
    /// `max_pending_messages` are ceilings on a tenant's *total*
    /// footprint across every queue it has, not a per-queue limit, and
    /// `requests_per_window` throttles the tenant's tool calls generally,
    /// not calls against any one queue. Keyed by plain tenant id (there's
    /// no `None` entry — `authorize` never consults this map for the
    /// trusted stdio connection at all, rather than storing an always-
    /// unlimited entry for it that nothing would ever meaningfully use).
    tenant_quotas: Mutex<HashMap<String, Arc<TenantQuota>>>,
}

impl QueueRegistry {
    fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            groups: Mutex::new(HashMap::new()),
            admission: Mutex::new(HashMap::new()),
            dedup: Mutex::new(HashMap::new()),
            routes: Mutex::new(HashMap::new()),
            tenant_quotas: Mutex::new(HashMap::new()),
        }
    }

    /// The key every per-queue map here is actually keyed by: `name`
    /// alone for `tenant: None`, or `name` prefixed with the tenant id
    /// otherwise. `\0` (a null byte) as the separator, not `/` or `:` —
    /// both `TenantId` and a validated queue name are restricted to
    /// `[A-Za-z0-9_-]`, so a null byte can never appear in either half
    /// and can never be ambiguous between "tenant `a`, queue `b/c`" and
    /// "tenant `a/b`, queue `c`" the way a printable separator might
    /// invite someone to assume.
    fn scope_key(tenant: Option<&TenantId>, name: &str) -> String {
        match tenant {
            Some(tenant) => format!("{}\0{name}", tenant.as_str()),
            None => name.to_string(),
        }
    }

    /// Same idea as [`scope_key`](Self::scope_key), but for `routes`,
    /// which isn't keyed by queue name at all (a route registry spans
    /// every queue name within one tenant) — just by tenant.
    fn tenant_key(tenant: Option<&TenantId>) -> String {
        tenant.map_or_else(String::new, |tenant| tenant.as_str().to_string())
    }

    /// The prefix every `scope_key` belonging to `tenant` starts with —
    /// used to count or sum across all of a tenant's queues at once
    /// (`max_queues`, `max_pending_messages`), rather than one queue at a
    /// time the way `scope_key` itself addresses a single entry.
    fn tenant_prefix(tenant: &TenantId) -> String {
        format!("{}\0", tenant.as_str())
    }

    /// Returns `tenant`'s queue named `name`, opening it (creating its
    /// WAL file under `data_dir` — under a `tenant`-named subdirectory
    /// when `tenant` is `Some`, so two tenants' `"orders"` queues are
    /// physically different files on disk, not just different map
    /// entries — if this is the first reference to it, in this process
    /// or ever) if it isn't already held.
    async fn get(
        &self,
        tenant: Option<&TenantId>,
        name: &str,
    ) -> Result<Arc<ConsumerGroup<serde_json::Value>>, ErrorData> {
        validate_queue_name(name)?;
        let key = Self::scope_key(tenant, name);

        let mut groups = self.groups.lock().await;
        if let Some(group) = groups.get(&key) {
            return Ok(Arc::clone(group));
        }

        // Only a genuinely *new* queue is checked against max_queues - an
        // already-open one never becomes invalid just because a ceiling
        // was lowered since, the same "already-admitted usage isn't
        // retroactively undone" stance every other ceiling in this file
        // takes (see AdmissionController::set_config's own docs).
        if let Some(tenant) = tenant
            && let Some(max_queues) = self.quota(tenant).await.config().await.max_queues
        {
            let open = Self::count_matching(&groups, tenant);
            if open >= max_queues {
                return Err(ErrorData::invalid_request(
                    format!(
                        "tenant {:?} already has {open} queues open, at its {max_queues}-queue \
                         limit - configure_tenant_quota can raise it",
                        tenant.as_str()
                    ),
                    None,
                ));
            }
        }

        let dir = match tenant {
            Some(tenant) => self.data_dir.join(tenant.as_str()),
            None => self.data_dir.clone(),
        };
        // `main.rs` only ever creates `data_dir` itself - a per-tenant
        // subdirectory is this registry's own responsibility, created
        // lazily the same way the `ConsumerGroup` it's about to hold is:
        // the first tool call for a given tenant is also the first time
        // anything needs this directory to exist at all.
        tokio::fs::create_dir_all(&dir).await.map_err(|error| {
            ErrorData::internal_error(format!("failed to open queue {name:?}: {error}"), None)
        })?;
        let path = dir.join(format!("{name}.wal"));
        let group = ConsumerGroup::open(path, VISIBILITY_TIMEOUT, RetryPolicy::DEFAULT)
            .await
            .map_err(|error| {
                ErrorData::internal_error(format!("failed to open queue {name:?}: {error}"), None)
            })?;
        let group = Arc::new(group);
        groups.insert(key, Arc::clone(&group));
        Ok(group)
    }

    /// Returns `tenant`'s admission controller for `name`, creating one
    /// with [`AdmissionConfig::UNLIMITED`] on first reference — a queue
    /// no one has ever called `configure_admission` on is never
    /// throttled, not throttled by some undocumented default.
    async fn admission(
        &self,
        tenant: Option<&TenantId>,
        name: &str,
    ) -> Result<Arc<AdmissionController>, ErrorData> {
        validate_queue_name(name)?;
        let key = Self::scope_key(tenant, name);

        let mut controllers = self.admission.lock().await;
        if let Some(controller) = controllers.get(&key) {
            return Ok(Arc::clone(controller));
        }

        let controller = Arc::new(AdmissionController::new(AdmissionConfig::UNLIMITED));
        controllers.insert(key, Arc::clone(&controller));
        Ok(controller)
    }

    /// Returns `tenant`'s dedup index for `name`, creating an empty,
    /// [`DEDUP_TTL`]'d one on first reference.
    async fn dedup(
        &self,
        tenant: Option<&TenantId>,
        name: &str,
    ) -> Result<Arc<EmbeddingIndex<qaas_types::MessageId>>, ErrorData> {
        validate_queue_name(name)?;
        let key = Self::scope_key(tenant, name);

        let mut indexes = self.dedup.lock().await;
        if let Some(index) = indexes.get(&key) {
            return Ok(Arc::clone(index));
        }

        let index = Arc::new(EmbeddingIndex::new(Some(DEDUP_TTL)));
        indexes.insert(key, Arc::clone(&index));
        Ok(index)
    }

    /// Returns `tenant`'s route registry, creating an empty one on first
    /// reference. Never validated against `validate_queue_name` — this
    /// isn't keyed by a single queue name at all.
    async fn routes(&self, tenant: Option<&TenantId>) -> Arc<EmbeddingIndex<String>> {
        let key = Self::tenant_key(tenant);

        let mut registries = self.routes.lock().await;
        if let Some(routes) = registries.get(&key) {
            return Arc::clone(routes);
        }

        let routes = Arc::new(EmbeddingIndex::new(None));
        registries.insert(key, Arc::clone(&routes));
        routes
    }

    /// Returns `tenant`'s quota, creating one with [`QuotaConfig::UNLIMITED`]
    /// on first reference — a tenant no one has ever called
    /// `configure_tenant_quota` on is never throttled, not throttled by
    /// some undocumented default. No `Option<&TenantId>` parameter here,
    /// unlike every other accessor: the trusted stdio connection never
    /// has a quota to look up in the first place, so every call site
    /// already knows it only has a `&TenantId` to offer.
    async fn quota(&self, tenant: &TenantId) -> Arc<TenantQuota> {
        let key = tenant.as_str().to_string();

        let mut quotas = self.tenant_quotas.lock().await;
        if let Some(quota) = quotas.get(&key) {
            return Arc::clone(quota);
        }

        let quota = Arc::new(TenantQuota::new(QuotaConfig::UNLIMITED));
        quotas.insert(key, Arc::clone(&quota));
        quota
    }

    /// How many of `groups`' keys belong to `tenant` — shared by
    /// [`Self::get`] (checking `max_queues` while already holding the
    /// lock `groups` is a guard for) and [`Self::tenant_queue_count`]
    /// (reporting the same number for `tenant_quota_status`, without
    /// already holding it).
    fn count_matching(
        groups: &HashMap<String, Arc<ConsumerGroup<serde_json::Value>>>,
        tenant: &TenantId,
    ) -> usize {
        let prefix = Self::tenant_prefix(tenant);
        groups.keys().filter(|key| key.starts_with(&prefix)).count()
    }

    /// How many queues `tenant` currently has open — what `max_queues`
    /// is actually checked against, and what `tenant_quota_status`
    /// reports back.
    async fn tenant_queue_count(&self, tenant: &TenantId) -> usize {
        let groups = self.groups.lock().await;
        Self::count_matching(&groups, tenant)
    }

    /// Total unacknowledged messages across every queue `tenant`
    /// currently has open — what `max_pending_messages` is actually
    /// checked against. Only counts queues this process has already
    /// opened in [`Self::get`]; a queue that exists on disk but hasn't
    /// been referenced yet contributes nothing, the same "lazily opens
    /// and holds" stance the rest of this registry already takes (see
    /// its own struct docs) - consistent with `max_queues` counting only
    /// entries in this same map, not a filesystem scan.
    async fn tenant_pending_total(&self, tenant: &TenantId) -> u64 {
        let prefix = Self::tenant_prefix(tenant);
        // Snapshot the matching Arcs and drop the registry's own lock
        // before awaiting each ConsumerGroup's own `len()` - `len()`
        // takes that group's *own* internal lock, and holding two locks
        // across an await for no reason is worth avoiding on principle
        // even though nothing else in this file ever acquires them in
        // the reverse order.
        let matching: Vec<_> = {
            let groups = self.groups.lock().await;
            groups
                .iter()
                .filter(|(key, _)| key.starts_with(&prefix))
                .map(|(_, group)| Arc::clone(group))
                .collect()
        };

        let mut total: u64 = 0;
        for group in matching {
            let len = u64::try_from(group.len().await).unwrap_or(u64::MAX);
            total = total.saturating_add(len);
        }
        total
    }
}

/// Arguments for the `enqueue` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct EnqueueParams {
    /// Which queue to enqueue onto. Opened automatically if it doesn't
    /// exist yet — there's no separate "create queue" step. Optional if
    /// `embedding` is given: omitting `queue` asks the server to route
    /// this task to whichever registered queue's descriptor (see
    /// `configure_route`) is the closest match instead of naming one
    /// directly.
    queue: Option<String>,
    /// The message payload. Any JSON value; QaaS doesn't interpret it.
    payload: serde_json::Value,
    /// Optional caller-supplied deduplication key. An enqueue reusing a
    /// key already present in this queue (pending, leased, or
    /// dead-lettered) is a no-op that returns the *original* message's
    /// id — safe to retry a timed-out enqueue call without risking a
    /// duplicate message.
    idempotency_key: Option<String>,
    /// Estimated tokens this task is expected to cost once claimed and
    /// executed — checked against the queue's token budget (see
    /// `configure_admission`) before the enqueue is allowed to proceed.
    /// Defaults to 0 (no contribution to, or gating by, the token
    /// budget) if omitted.
    estimated_tokens: Option<u64>,
    /// Estimated dollar cost this task is expected to incur — checked
    /// against the queue's cost ceiling the same way `estimated_tokens`
    /// is checked against its token budget. Defaults to 0.0 if omitted.
    estimated_cost: Option<f64>,
    /// A caller-computed embedding representing this task's content —
    /// any non-empty vector of finite numbers, from whatever embedding
    /// model the caller already has access to. Used for near-duplicate
    /// detection on the target queue, and — if `queue` is omitted — to
    /// pick which queue that is in the first place. See the module docs
    /// for the full behavior.
    embedding: Option<Vec<f32>>,
}

/// Result of the `enqueue` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct EnqueueResult {
    /// The id of the message this call resolved to — either genuinely
    /// new, or (if `deduplicated` is true) the existing near-duplicate
    /// task it collapsed into. Either way, a later `claim` returns this
    /// same id when it delivers the message; pass it to `ack`/`nack`
    /// then.
    message_id: String,
    /// Which queue this task actually landed in — always worth checking
    /// when `queue` was omitted from the request and the server picked
    /// one via routing instead.
    queue: String,
    /// `true` if `embedding` matched an existing task closely enough
    /// (see the module docs on `DEDUP_SIMILARITY_THRESHOLD`) that this
    /// call was collapsed into it rather than creating a new message —
    /// `message_id` is that existing task's id, and no new message was
    /// written.
    deduplicated: bool,
    /// The cosine similarity score that triggered dedup, if
    /// `deduplicated` is true (`null` otherwise).
    similarity: Option<f32>,
    /// Tokens left in the queue's budget for the rest of this window
    /// after admitting this enqueue, if a token budget is configured for
    /// it (`null` otherwise). `null` when `deduplicated` is true, too —
    /// a collapsed enqueue was never checked against admission at all.
    tokens_remaining: Option<u64>,
    /// Dollars left in the queue's cost ceiling for the rest of this
    /// window after admitting this enqueue, if a cost ceiling is
    /// configured for it (`null` otherwise). Same `deduplicated`
    /// exception as `tokens_remaining`.
    cost_remaining: Option<f64>,
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
    /// `false` means nothing was claimed — either the wait window
    /// elapsed with nothing claimable (an empty queue, or everything
    /// currently leased to other consumers), or the queue is currently
    /// over its token/cost budget (`throttled` distinguishes the two).
    /// Not an error either way; every other field is `null` in both
    /// cases.
    available: bool,
    /// `true` means `available: false` is because the queue is
    /// currently over its admission budget, not because there was
    /// nothing to claim — `wait_ms` wasn't even waited out in this case,
    /// since there's no reason to poll a queue that's throttled
    /// regardless of what becomes claimable. See `configure_admission`.
    throttled: bool,
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

/// Arguments for the `publish_partial_result` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct PublishPartialResultParams {
    /// Which queue the message was claimed from.
    queue: String,
    /// The message id from a prior `claim` call.
    message_id: String,
    /// The lease token from that same `claim` call.
    lease_token: u64,
    /// This chunk's content, any JSON value — a token, a line, a partial
    /// object, whatever unit makes sense for what's being streamed.
    data: serde_json::Value,
    /// Whether this is the last chunk you'll publish for this delivery.
    /// Defaults to `false` if omitted — most published chunks are
    /// intermediate, so a caller streaming many of them only needs to
    /// say this once, on the last one.
    is_final: Option<bool>,
}

/// Result of the `publish_partial_result` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct PublishPartialResultResult {
    /// `false` means this id/token pair no longer matched an active
    /// lease, and `data` was not recorded — same stale-lease semantics
    /// as `ack`/`nack`/`checkpoint`.
    published: bool,
    /// This chunk's position in the stream (starting at 1), if
    /// published. `null` if not.
    sequence: Option<u64>,
}

/// Arguments for the `stream_partial_results` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct StreamPartialResultsParams {
    /// Which queue the message was claimed from.
    queue: String,
    /// The message id to watch. No lease token needed — watching a
    /// stream doesn't require holding the lease, only publishing to one
    /// does.
    message_id: String,
    /// Only return chunks with a sequence number greater than this.
    /// Defaults to 0 (everything published so far) if omitted; pass the
    /// highest `sequence` you've already seen to only get what's new.
    after_sequence: Option<u64>,
    /// How long to wait for at least one new chunk if none are
    /// available yet, in milliseconds. Defaults to 5000; capped at
    /// 60000 — the same bounds `claim`'s `wait_ms` has, for the same
    /// reason.
    wait_ms: Option<u64>,
}

/// One chunk in `stream_partial_results`' result.
#[derive(Debug, Serialize, JsonSchema)]
struct PartialResultChunkSummary {
    /// This chunk's position in the stream, starting at 1.
    sequence: u64,
    /// The chunk's own content, exactly as published.
    data: serde_json::Value,
    /// Whether the publisher marked this as the last chunk it would
    /// publish. Advisory, not enforced — see
    /// `qaas_core::StreamChunk::is_final`'s own docs.
    is_final: bool,
}

/// Result of the `stream_partial_results` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct StreamPartialResultsResult {
    /// Whether `message_id` currently names an active lease. `false`
    /// means either it's never been claimed yet, or whatever delivery
    /// held it has already ended (acked, nacked, or expired) — `chunks`
    /// is always empty in that case. `true` with empty `chunks` means
    /// the lease is active but nothing new arrived within `wait_ms`;
    /// call again with the same `after_sequence`.
    active: bool,
    /// New chunks since `after_sequence`, in order. Empty if none
    /// arrived in time, or if `active` is `false`.
    chunks: Vec<PartialResultChunkSummary>,
}

/// Arguments for the `configure_admission` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct ConfigureAdmissionParams {
    /// Which queue to configure. Opened automatically if it doesn't
    /// exist yet, same as every other tool here.
    queue: String,
    /// Maximum total tokens allowed within any trailing window on this
    /// queue. `null` (the default if omitted) means no token ceiling.
    tokens_per_window: Option<u64>,
    /// Maximum total dollars allowed within any trailing window on this
    /// queue. `null` (the default if omitted) means no cost ceiling.
    cost_ceiling_per_window: Option<f64>,
    /// How far back "within the window" looks, in seconds. Defaults to
    /// 3600 (one hour) if omitted.
    window_seconds: Option<u64>,
}

/// Result of the `configure_admission` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct ConfigureAdmissionResult {
    /// The token ceiling now in effect for this queue (`null` if none).
    tokens_per_window: Option<u64>,
    /// The cost ceiling now in effect for this queue (`null` if none).
    cost_ceiling_per_window: Option<f64>,
    /// The window, in seconds, now in effect for this queue.
    window_seconds: u64,
}

/// Arguments for the `admission_status` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct AdmissionStatusParams {
    /// Which queue to report on.
    queue: String,
}

/// Result of the `admission_status` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct AdmissionStatusResult {
    /// The token ceiling currently configured for this queue (`null` if
    /// none — never throttled on tokens).
    tokens_per_window: Option<u64>,
    /// The cost ceiling currently configured for this queue (`null` if
    /// none — never throttled on cost).
    cost_ceiling_per_window: Option<f64>,
    /// The window, in seconds, currently configured for this queue.
    window_seconds: u64,
    /// Tokens admitted within the current window, as of now.
    tokens_used: u64,
    /// Dollars admitted within the current window, as of now.
    cost_used: f64,
    /// Tokens still available this window (`null` if no token ceiling
    /// is configured — nothing to be "remaining" against).
    tokens_remaining: Option<u64>,
    /// Dollars still available this window (`null` if no cost ceiling
    /// is configured).
    cost_remaining: Option<f64>,
}

/// Arguments for the `configure_route` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct ConfigureRouteParams {
    /// Which queue this route points at. Opened automatically if it
    /// doesn't exist yet, same as every other tool here — registering a
    /// route doesn't require the queue to have ever been enqueued to.
    queue: String,
    /// This queue's descriptor embedding: a caller-computed vector
    /// representing the *kind* of task this queue's consumers specialize
    /// in (e.g. an embedding of "code review tasks" for a code-review
    /// queue). An `enqueue` call that omits `queue` is routed to
    /// whichever registered queue's descriptor is closest to its own
    /// embedding.
    embedding: Vec<f32>,
}

/// Result of the `configure_route` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct ConfigureRouteResult {
    /// Every queue name currently registered as a route, including this
    /// call's — useful for seeing the full picture without a separate
    /// listing tool.
    registered_routes: Vec<String>,
}

/// Arguments for the `list_dead_letters` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct ListDeadLettersParams {
    /// Which queue's dead-letter queue to list.
    queue: String,
}

/// One entry in `list_dead_letters`' result.
#[derive(Debug, Serialize, JsonSchema)]
struct DeadLetterSummary {
    /// This message's id — pass it to `triage_dead_letter`,
    /// `reprocess_dead_letter`, or `purge_dead_letter`.
    message_id: String,
    /// The message payload, exactly as originally enqueued.
    payload: serde_json::Value,
    /// How many times this message was delivered in total before it was
    /// dead-lettered.
    delivery_count: u32,
    /// The failure reason from whichever delivery attempt exhausted the
    /// retry policy — the caller's own `nack` reason, or a synthetic one
    /// if it was a silent lease expiry. `null` if none was given.
    last_error: Option<String>,
    /// When this message was dead-lettered, in milliseconds since the
    /// Unix epoch.
    dead_lettered_at_ms: u64,
    /// Whatever this workflow last saved via `checkpoint` before the
    /// delivery that exhausted its retries — `null` if it was never
    /// checkpointed. Often the most useful triage signal there is: a
    /// task that died on a late step reads very differently from one
    /// that died on the first.
    checkpoint: Option<serde_json::Value>,
    /// An existing triage verdict, if `triage_dead_letter` has already
    /// classified this entry — `null` otherwise.
    triage: Option<TriageSummary>,
}

/// A recorded triage verdict, as returned by `list_dead_letters`.
#[derive(Debug, Serialize, JsonSchema)]
struct TriageSummary {
    /// `"transient"` or `"permanent"`.
    classification: String,
    /// Why.
    reason: String,
    /// When this verdict was recorded, in milliseconds since the Unix
    /// epoch.
    triaged_at_ms: u64,
}

/// Arguments for the `triage_dead_letter` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct TriageDeadLetterParams {
    /// Which queue the dead letter is on.
    queue: String,
    /// The dead letter's id, from `list_dead_letters`.
    message_id: String,
    /// Whether this failure looks worth retrying automatically
    /// (`"transient"`) or needs a human to look at it before anything
    /// happens to it again (`"permanent"`). See this module's docs for
    /// exactly what each one auto-applies.
    classification: TriageClassificationParam,
    /// Why — whatever explanation led to this classification. Recorded
    /// alongside the verdict for whoever looks at this entry next.
    reason: String,
}

/// Wire form of [`TriageClassification`] — kept separate from the
/// `qaas-core` type rather than deriving `schemars::JsonSchema` directly
/// on it, matching every other tool parameter in this module: `qaas-core`
/// doesn't depend on `schemars`, and shouldn't just to satisfy this one
/// call site's wire schema.
#[derive(Debug, Clone, Copy, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum TriageClassificationParam {
    Transient,
    Permanent,
}

impl From<TriageClassificationParam> for TriageClassification {
    fn from(value: TriageClassificationParam) -> Self {
        match value {
            TriageClassificationParam::Transient => Self::Transient,
            TriageClassificationParam::Permanent => Self::Permanent,
        }
    }
}

/// Result of the `triage_dead_letter` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct TriageDeadLetterResult {
    /// `false` means no dead letter with `message_id` exists on this
    /// queue (already resolved, or never dead-lettered) — the verdict
    /// was not recorded and nothing was reprocessed. Not an error: the
    /// same "already gone" stance `ack`/`nack` already take on a stale
    /// lease.
    annotated: bool,
    /// `true` if `classification` was `"transient"` and this call also
    /// auto-applied `reprocess_dead_letter` — the message is back in the
    /// live queue and no longer in the DLQ. Always `false` for
    /// `"permanent"`, and for a `"transient"` verdict recorded on an
    /// entry that (in a narrow race) was resolved by something else
    /// between the annotation and the reprocess attempt.
    reprocessed: bool,
}

/// Arguments shared by `reprocess_dead_letter` and `purge_dead_letter`.
#[derive(Debug, Deserialize, JsonSchema)]
struct DeadLetterActionParams {
    /// Which queue the dead letter is on.
    queue: String,
    /// The dead letter's id, from `list_dead_letters`.
    message_id: String,
}

/// Result of the `reprocess_dead_letter` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct ReprocessDeadLetterResult {
    /// `false` means no dead letter with `message_id` exists on this
    /// queue — same "already gone" stance as everywhere else in this
    /// module.
    reprocessed: bool,
}

/// Result of the `purge_dead_letter` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct PurgeDeadLetterResult {
    /// `false` means no dead letter with `message_id` exists on this
    /// queue.
    purged: bool,
}

/// Arguments for the `create_api_key` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct CreateApiKeyParams {
    /// The tenant this key authenticates as once used over HTTP. Created
    /// on first use — there's no separate "register a tenant" step,
    /// matching how a queue name needs no separate provisioning either.
    tenant_id: String,
}

/// Result of the `create_api_key` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct CreateApiKeyResult {
    /// The raw credential — shown exactly once, here. QaaS only ever
    /// stores its SHA-256 hash (see `qaas_core::auth`'s own docs), so
    /// there is no way to recover it later if this response is lost;
    /// minting a replacement is the only remedy.
    api_key: String,
    /// The tenant this key authenticates as, echoed back for
    /// confirmation.
    tenant_id: String,
}

/// Arguments for the `revoke_api_key` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct RevokeApiKeyParams {
    /// The raw API key to revoke, exactly as returned by
    /// `create_api_key`.
    api_key: String,
}

/// Result of the `revoke_api_key` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct RevokeApiKeyResult {
    /// `false` means this key was already revoked, or never existed —
    /// same "already gone" stance every other resolve-once operation in
    /// this module takes.
    revoked: bool,
}

/// Arguments for the `configure_tenant_quota` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct ConfigureTenantQuotaParams {
    /// Which tenant this quota applies to.
    tenant_id: String,
    /// Maximum queues this tenant may have open at once. Omit to leave
    /// unlimited.
    max_queues: Option<usize>,
    /// Maximum total unacknowledged messages this tenant may have
    /// outstanding across all of its queues at once. Omit to leave
    /// unlimited.
    max_pending_messages: Option<u64>,
    /// Maximum MCP tool calls this tenant may make within
    /// `window_seconds`. Omit to leave unlimited.
    requests_per_window: Option<u64>,
    /// The rolling window `requests_per_window` is measured over, in
    /// seconds. Defaults to 60 if omitted; irrelevant while
    /// `requests_per_window` is omitted too.
    window_seconds: Option<u64>,
}

/// Result of the `configure_tenant_quota` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct ConfigureTenantQuotaResult {
    /// Echoed back for confirmation.
    tenant_id: String,
    /// The configuration now in effect, immediately - see
    /// `TenantQuota::set_config`'s own docs on what "immediately" does
    /// and doesn't retroactively change.
    max_queues: Option<usize>,
    max_pending_messages: Option<u64>,
    requests_per_window: Option<u64>,
    window_seconds: u64,
}

/// Arguments for the `tenant_quota_status` tool.
#[derive(Debug, Deserialize, JsonSchema)]
struct TenantQuotaStatusParams {
    /// Which tenant to report on. Required for a caller on the trusted
    /// stdio connection - the operator can ask about any tenant. Ignored
    /// for an HTTP-authenticated caller, who always gets their own
    /// status back regardless of what (if anything) this names - no
    /// tenant can query another's usage this way.
    tenant_id: Option<String>,
}

/// Result of the `tenant_quota_status` tool.
#[derive(Debug, Serialize, JsonSchema)]
struct TenantQuotaStatusResult {
    /// Which tenant this status is for.
    tenant_id: String,
    /// Configured queue-count ceiling, `null` if unlimited.
    max_queues: Option<usize>,
    /// How many queues this tenant currently has open.
    queues_open: usize,
    /// Configured pending-message ceiling, `null` if unlimited.
    max_pending_messages: Option<u64>,
    /// Total unacknowledged messages across this tenant's queues right
    /// now.
    pending_messages: u64,
    /// Configured request-rate ceiling, `null` if unlimited.
    requests_per_window: Option<u64>,
    /// How many tool calls this tenant has made within the current
    /// window. Includes this very call when an HTTP-authenticated tenant
    /// checks its own status (authorized through the exact same path as
    /// every other tool); doesn't, when the trusted stdio connection
    /// checks a tenant's status on its behalf, since that request never
    /// authorized *as* the tenant it's asking about.
    requests_in_window: u64,
    /// The rolling window `requests_per_window` (and
    /// `requests_in_window`) are measured over.
    window_seconds: u64,
}

/// The MCP server itself. Cheap to clone — the only state is two
/// `Arc`'d pieces, [`QueueRegistry`] and [`ApiKeyStore`] — which `rmcp`
/// relies on internally when handling more than one tool call
/// concurrently over the same connection, and which `crate::http_auth`'s
/// middleware also needs a handle to, independently of any particular
/// connection.
#[derive(Clone)]
pub struct QaasMcpServer {
    registry: Arc<QueueRegistry>,
    /// Public so `crate::http_auth`'s middleware — which runs *before*
    /// any tool method, as part of routing an HTTP request to this
    /// server at all — can authenticate a bearer credential against it
    /// directly, without going through a tool call to do so.
    pub api_keys: Arc<ApiKeyStore>,
}

#[tool_router]
impl QaasMcpServer {
    /// Opens (creating if necessary) queue WAL files under `data_dir`
    /// and the API-key store at `data_dir/api_keys.log`, returning a
    /// server ready to be handed to
    /// [`ServiceExt::serve`](rmcp::ServiceExt::serve) — for stdio, and,
    /// wrapped in `crate::http_auth`'s middleware first, for the
    /// streamable-HTTP transport `main.rs` also stands up.
    ///
    /// # Errors
    ///
    /// Returns an error if opening the API-key store's WAL fails — see
    /// [`ApiKeyStore::open`](qaas_core::ApiKeyStore::open).
    pub async fn new(data_dir: impl Into<PathBuf>) -> std::io::Result<Self> {
        let data_dir = data_dir.into();
        let api_keys = ApiKeyStore::open(data_dir.join("api_keys.log")).await?;
        Ok(Self { registry: Arc::new(QueueRegistry::new(data_dir)), api_keys: Arc::new(api_keys) })
    }

    /// The single entry point every tool method uses instead of calling
    /// [`tenant_from`] directly: resolves the caller's tenant *and*
    /// spends one request of its rate quota, in one place, so there's no
    /// way for a new tool to accidentally skip the check the way sixteen
    /// separate copies of it invite. Stdio (`None`) is exempt entirely —
    /// see the module docs on why that connection stays trusted and
    /// unthrottled — matching every other tenant-scoped check in this
    /// file.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorCode::INVALID_REQUEST`](rmcp::model::ErrorCode) if
    /// the resolved tenant is currently over its configured
    /// `requests_per_window` — see [`quota::QuotaDecision::Denied`](qaas_core::quota::QuotaDecision::Denied).
    async fn authorize(
        &self,
        ctx: &RequestContext<RoleServer>,
    ) -> Result<Option<TenantId>, ErrorData> {
        self.authorize_impl(tenant_from(ctx)).await
    }

    /// The actual `authorize` logic, taking `tenant` directly rather than
    /// a [`RequestContext`] — see [`Self::enqueue_impl`]'s docs on why
    /// every dispatch-layer method in this file has a `_impl` sibling
    /// tests can call without a real MCP connection to construct a
    /// context from.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn authorize_impl(
        &self,
        tenant: Option<TenantId>,
    ) -> Result<Option<TenantId>, ErrorData> {
        if let Some(tenant) = &tenant {
            match self.registry.quota(tenant).await.try_acquire().await {
                QuotaDecision::Allowed { .. } => {}
                QuotaDecision::Denied { reason, retry_after } => {
                    return Err(throttled_to_mcp(&reason, retry_after));
                }
            }
        }
        Ok(tenant)
    }

    /// Durably enqueues a message onto a named queue, creating the queue
    /// on first use.
    #[tool(
        description = "Durably enqueue a message onto a named queue, creating the queue on first use."
    )]
    async fn enqueue(
        &self,
        Parameters(params): Parameters<EnqueueParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<EnqueueResult>, ErrorData> {
        self.enqueue_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `enqueue` logic, taking `tenant` directly rather than a
    /// [`RequestContext`] — split out so tests can call it (including
    /// with an explicit `Some(tenant)`, to exercise isolation) without
    /// needing a real MCP connection to construct a context from, since
    /// `rmcp` gives no public way to build one outside of actually
    /// serving a client.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn enqueue_impl(
        &self,
        tenant: Option<TenantId>,
        params: EnqueueParams,
    ) -> Result<Json<EnqueueResult>, ErrorData> {
        let embedding = params.embedding.map(Embedding::new).transpose().map_err(|_| {
            ErrorData::invalid_params(
                "embedding must be non-empty and contain only finite values",
                None,
            )
        })?;

        let queue_name =
            self.resolve_target_queue(tenant.as_ref(), params.queue, embedding.as_ref()).await?;
        let group = self.registry.get(tenant.as_ref(), &queue_name).await?;
        let admission = self.registry.admission(tenant.as_ref(), &queue_name).await?;
        let dedup = match &embedding {
            Some(_) => Some(self.registry.dedup(tenant.as_ref(), &queue_name).await?),
            None => None,
        };

        // Validated before admission (or dedup) is consulted: a
        // malformed request shouldn't spend any of the queue's budget,
        // or get compared against other tasks' embeddings, on its way
        // to being rejected anyway.
        let idempotency_key =
            params.idempotency_key.map(qaas_types::IdempotencyKey::new).transpose().map_err(
                |_| ErrorData::invalid_params("idempotency_key must not be empty", None),
            )?;

        // Dedup runs before admission, not after: a collapsed enqueue
        // creates no new work, so it shouldn't cost any of the queue's
        // token/cost budget either — see EnqueueResult::tokens_remaining's
        // own docs on why that field is `null` when `deduplicated` is
        // `true`.
        if let (Some(dedup), Some(embedding)) = (&dedup, &embedding)
            && let Some((existing_id, similarity)) = dedup.nearest(embedding).await
            && similarity >= DEDUP_SIMILARITY_THRESHOLD
        {
            return Ok(Json(EnqueueResult {
                message_id: existing_id.to_string(),
                queue: queue_name,
                deduplicated: true,
                similarity: Some(similarity),
                tokens_remaining: None,
                cost_remaining: None,
            }));
        }

        // Same "dedup runs first" ordering as admission below: a
        // collapsed enqueue creates no new pending message, so it
        // shouldn't be blocked by a tenant that's already at its pending-
        // message ceiling either. Checked before the per-queue
        // token/cost budget, not after - this is the coarser, tenant-wide
        // gate `feature/multi-tenant-quotas` adds on top of a budget
        // that only ever knew about one queue at a time.
        if let Some(tenant) = &tenant
            && let Some(max_pending) =
                self.registry.quota(tenant).await.config().await.max_pending_messages
        {
            let current = self.registry.tenant_pending_total(tenant).await;
            if current >= max_pending {
                return Err(ErrorData::invalid_request(
                    format!(
                        "tenant {:?} already has {current} pending messages across its queues, \
                         at its {max_pending}-message limit - configure_tenant_quota can raise it",
                        tenant.as_str()
                    ),
                    None,
                ));
            }
        }

        let (tokens_remaining, cost_remaining) = match admission
            .try_admit(params.estimated_tokens.unwrap_or(0), params.estimated_cost.unwrap_or(0.0))
            .await
        {
            AdmissionDecision::Admitted { tokens_remaining, cost_remaining } => {
                (tokens_remaining, cost_remaining)
            }
            AdmissionDecision::Denied { reason, retry_after } => {
                return Err(throttled_to_mcp(&reason, retry_after));
            }
        };

        let message_id = match idempotency_key {
            Some(key) => group.enqueue_with_key(params.payload, key).await,
            None => group.enqueue(params.payload).await,
        }
        .map_err(|error| io_error_to_mcp(&error))?;

        if let (Some(dedup), Some(embedding)) = (dedup, embedding) {
            dedup.insert(message_id, embedding).await;
        }

        record_queue_metrics(tenant.as_ref(), &queue_name, &group).await;

        Ok(Json(EnqueueResult {
            message_id: message_id.to_string(),
            queue: queue_name,
            deduplicated: false,
            similarity: None,
            tokens_remaining,
            cost_remaining,
        }))
    }

    /// Resolves `enqueue`'s target queue name: `explicit_queue` directly
    /// if given, otherwise the nearest registered route to `embedding`.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorCode::INVALID_PARAMS`](rmcp::model::ErrorCode) if
    /// both `explicit_queue` and `embedding` are absent (nothing to
    /// route on), or if `embedding` is given but no route has ever been
    /// registered via `configure_route` to match it against.
    async fn resolve_target_queue(
        &self,
        tenant: Option<&TenantId>,
        explicit_queue: Option<String>,
        embedding: Option<&Embedding>,
    ) -> Result<String, ErrorData> {
        if let Some(queue) = explicit_queue {
            return Ok(queue);
        }
        let Some(embedding) = embedding else {
            return Err(ErrorData::invalid_params(
                "queue is required unless embedding is given, for routing",
                None,
            ));
        };
        self.registry
            .routes(tenant)
            .await
            .nearest(embedding)
            .await
            .map(|(queue, _similarity)| queue)
            .ok_or_else(|| {
                ErrorData::invalid_params(
                    "queue was omitted and no routes are registered - call configure_route \
                     first, or specify queue directly",
                    None,
                )
            })
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
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<ClaimResult>, ErrorData> {
        self.claim_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `claim` logic — see [`Self::enqueue_impl`]'s docs on
    /// why this is split out from the `#[tool]`-annotated method.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn claim_impl(
        &self,
        tenant: Option<TenantId>,
        params: ClaimParams,
    ) -> Result<Json<ClaimResult>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let admission = self.registry.admission(tenant.as_ref(), &params.queue).await?;

        // Checked before attempting to claim at all, and not waited out
        // the way an empty queue is — see the module docs for why this
        // is a read-only re-check of enqueue's own admission, not a
        // second deduction of the same task's cost.
        if !admission.has_headroom().await {
            return Ok(Json(ClaimResult {
                available: false,
                throttled: true,
                message_id: None,
                lease_token: None,
                idempotency_key: None,
                payload: None,
                delivery_count: None,
                checkpoint: None,
            }));
        }

        let wait =
            params.wait_ms.map_or(DEFAULT_CLAIM_WAIT, Duration::from_millis).min(MAX_CLAIM_WAIT);

        match tokio::time::timeout(wait, group.claim()).await {
            Ok(claim) => {
                // Total len() doesn't change on a claim (the message
                // just moves from pending to leased), but which message
                // is now the *oldest pending* one does - worth
                // refreshing the lag gauge even though the depth gauge
                // it shares a call with won't actually move.
                record_queue_metrics(tenant.as_ref(), &params.queue, &group).await;
                Ok(Json(ClaimResult {
                    available: true,
                    throttled: false,
                    message_id: Some(claim.id.to_string()),
                    lease_token: Some(claim.token.as_u64()),
                    idempotency_key: Some(claim.idempotency_key.as_str().to_string()),
                    payload: Some(claim.item),
                    delivery_count: Some(claim.delivery_count),
                    checkpoint: claim.checkpoint,
                }))
            }
            Err(_elapsed) => Ok(Json(ClaimResult {
                available: false,
                throttled: false,
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
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<AckResult>, ErrorData> {
        self.ack_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `ack` logic — see [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn ack_impl(
        &self,
        tenant: Option<TenantId>,
        params: AckParams,
    ) -> Result<Json<AckResult>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let token = LeaseToken::from_u64(params.lease_token);

        let acked = group.ack(message_id, token).await.map_err(|error| io_error_to_mcp(&error))?;

        if acked {
            // Precise cleanup for the common case — a message acked
            // quickly shouldn't keep blocking dedup for the rest of
            // DEDUP_TTL just because nothing told the index it's gone.
            // Not load-bearing for correctness (the TTL is what actually
            // bounds the gap for every other way a message leaves the
            // live queue — nack, dead-lettering, a crashed consumer —
            // that this server can't observe directly), just tighter
            // than waiting on it here.
            self.registry.dedup(tenant.as_ref(), &params.queue).await?.remove(&message_id).await;

            // The "latency percentiles" metric: end-to-end time from
            // enqueue to a *successful* resolution, read straight off
            // message_id's own embedded generation time (see
            // MessageId::timestamp's own docs) rather than a separately
            // tracked "claimed at" field this server doesn't otherwise
            // need. Recorded only here, not in nack - a nack means the
            // message isn't actually done yet (it's retrying, or being
            // dead-lettered), so "how long did it take" isn't answerable
            // until whichever ack eventually resolves it for good.
            let elapsed_ms =
                qaas_types::Timestamp::now().0.saturating_sub(message_id.timestamp().0);
            #[allow(clippy::cast_precision_loss)]
            let elapsed_secs = elapsed_ms as f64 / 1000.0;
            metrics::histogram!(
                "qaas_message_latency_seconds",
                "queue" => params.queue.clone(),
                "tenant" => tenant_label(tenant.as_ref()),
            )
            .record(elapsed_secs);

            record_queue_metrics(tenant.as_ref(), &params.queue, &group).await;
        }
        Ok(Json(AckResult { acked }))
    }

    /// Releases a claimed message back for retry, or dead-letters it if
    /// this delivery exhausts the queue's retry policy.
    #[tool(description = "Release a claimed message back for retry (or dead-letter it, if this \
                        delivery exhausts the queue's retry policy), optionally recording why it failed.")]
    async fn nack(
        &self,
        Parameters(params): Parameters<NackParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<NackResult>, ErrorData> {
        self.nack_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `nack` logic — see [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn nack_impl(
        &self,
        tenant: Option<TenantId>,
        params: NackParams,
    ) -> Result<Json<NackResult>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let token = LeaseToken::from_u64(params.lease_token);

        let nacked = group
            .nack(message_id, token, params.reason)
            .await
            .map_err(|error| io_error_to_mcp(&error))?;

        if nacked {
            record_queue_metrics(tenant.as_ref(), &params.queue, &group).await;
        }
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
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<CheckpointResult>, ErrorData> {
        self.checkpoint_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `checkpoint` logic — see [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn checkpoint_impl(
        &self,
        tenant: Option<TenantId>,
        params: CheckpointParams,
    ) -> Result<Json<CheckpointResult>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let token = LeaseToken::from_u64(params.lease_token);

        let checkpointed = group
            .checkpoint(message_id, token, params.state)
            .await
            .map_err(|error| io_error_to_mcp(&error))?;
        Ok(Json(CheckpointResult { checkpointed }))
    }

    /// Publishes the next chunk of a still-claimed message's partial
    /// results, without resolving it.
    #[tool(description = "Publish the next chunk of a still-claimed message's partial results - \
                        e.g. one token or line at a time from a streaming LLM call - without \
                        resolving it. Mark the last chunk with is_final: true. Watchers see \
                        chunks via stream_partial_results.")]
    async fn publish_partial_result(
        &self,
        Parameters(params): Parameters<PublishPartialResultParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<PublishPartialResultResult>, ErrorData> {
        self.publish_partial_result_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `publish_partial_result` logic — see
    /// [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn publish_partial_result_impl(
        &self,
        tenant: Option<TenantId>,
        params: PublishPartialResultParams,
    ) -> Result<Json<PublishPartialResultResult>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let token = LeaseToken::from_u64(params.lease_token);

        let sequence = group
            .publish_partial_result(
                message_id,
                token,
                params.data,
                params.is_final.unwrap_or(false),
            )
            .await;
        Ok(Json(PublishPartialResultResult { published: sequence.is_some(), sequence }))
    }

    /// Waits for and returns new partial-result chunks for a message.
    #[tool(
        description = "Wait for (up to wait_ms) and return new partial-result chunks published \
                        for a message since after_sequence - no lease token needed, watching \
                        doesn't require holding the lease. active: false means the message was \
                        never claimed, or the delivery that held it has already ended; \
                        active: true with empty chunks means still in progress with nothing new \
                        yet. Call again with the highest sequence you've seen to keep watching."
    )]
    async fn stream_partial_results(
        &self,
        Parameters(params): Parameters<StreamPartialResultsParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<StreamPartialResultsResult>, ErrorData> {
        self.stream_partial_results_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `stream_partial_results` logic — see
    /// [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn stream_partial_results_impl(
        &self,
        tenant: Option<TenantId>,
        params: StreamPartialResultsParams,
    ) -> Result<Json<StreamPartialResultsResult>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let after = params.after_sequence.unwrap_or(0);
        let wait =
            params.wait_ms.map_or(DEFAULT_CLAIM_WAIT, Duration::from_millis).min(MAX_CLAIM_WAIT);

        let (active, chunks) = match tokio::time::timeout(
            wait,
            group.partial_results_since(message_id, after),
        )
        .await
        {
            Ok(PartialResultsPoll::Chunks(chunks)) => (true, chunks),
            Ok(PartialResultsPoll::NotActive) => (false, Vec::new()),
            // Timed out waiting: the poll only ever blocks once it's
            // confirmed the lease is active (`NotActive` returns
            // immediately, never reaching the timeout) - so a
            // timeout here always means "active, nothing new yet."
            Err(_elapsed) => (true, Vec::new()),
        };

        Ok(Json(StreamPartialResultsResult {
            active,
            chunks: chunks
                .into_iter()
                .map(|chunk| PartialResultChunkSummary {
                    sequence: chunk.sequence,
                    data: chunk.data,
                    is_final: chunk.is_final,
                })
                .collect(),
        }))
    }

    /// Sets or clears a queue's token/cost admission budget.
    #[tool(description = "Set (or clear) a queue's token/cost admission budget: a sliding window \
                        that continuously refills as time passes, not a one-time allowance. \
                        Omitting both tokens_per_window and cost_ceiling_per_window disables \
                        throttling for this queue. Takes effect immediately, including for usage \
                        already admitted this window - see admission_status to inspect current usage.")]
    async fn configure_admission(
        &self,
        Parameters(params): Parameters<ConfigureAdmissionParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<ConfigureAdmissionResult>, ErrorData> {
        self.configure_admission_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `configure_admission` logic — see
    /// [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn configure_admission_impl(
        &self,
        tenant: Option<TenantId>,
        params: ConfigureAdmissionParams,
    ) -> Result<Json<ConfigureAdmissionResult>, ErrorData> {
        let admission = self.registry.admission(tenant.as_ref(), &params.queue).await?;
        let window = Duration::from_secs(params.window_seconds.unwrap_or(3600));
        let config = AdmissionConfig {
            tokens_per_window: params.tokens_per_window,
            cost_ceiling_per_window: params.cost_ceiling_per_window,
            window,
        };
        admission.set_config(config).await;

        Ok(Json(ConfigureAdmissionResult {
            tokens_per_window: config.tokens_per_window,
            cost_ceiling_per_window: config.cost_ceiling_per_window,
            window_seconds: window.as_secs(),
        }))
    }

    /// Reports a queue's current admission configuration and usage.
    #[tool(description = "Report a queue's current token/cost admission configuration and usage \
                        within the active window - how much budget is configured, how much has \
                        been used, and how much remains.")]
    async fn admission_status(
        &self,
        Parameters(params): Parameters<AdmissionStatusParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<AdmissionStatusResult>, ErrorData> {
        self.admission_status_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `admission_status` logic — see
    /// [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn admission_status_impl(
        &self,
        tenant: Option<TenantId>,
        params: AdmissionStatusParams,
    ) -> Result<Json<AdmissionStatusResult>, ErrorData> {
        let admission = self.registry.admission(tenant.as_ref(), &params.queue).await?;
        let config = admission.config().await;
        let usage = admission.usage().await;

        Ok(Json(AdmissionStatusResult {
            tokens_per_window: config.tokens_per_window,
            cost_ceiling_per_window: config.cost_ceiling_per_window,
            window_seconds: config.window.as_secs(),
            tokens_used: usage.tokens_used,
            cost_used: usage.cost_used,
            tokens_remaining: config
                .tokens_per_window
                .map(|limit| limit.saturating_sub(usage.tokens_used)),
            cost_remaining: config
                .cost_ceiling_per_window
                .map(|ceiling| (ceiling - usage.cost_used).max(0.0)),
        }))
    }

    /// Registers (or replaces) a queue's routing descriptor embedding.
    #[tool(
        description = "Register (or replace) a queue's routing descriptor embedding - a vector \
                        representing the kind of task that queue's consumers specialize in. An \
                        enqueue call that omits queue is routed to whichever registered queue's \
                        descriptor is closest to its own embedding."
    )]
    async fn configure_route(
        &self,
        Parameters(params): Parameters<ConfigureRouteParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<ConfigureRouteResult>, ErrorData> {
        self.configure_route_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `configure_route` logic — see
    /// [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn configure_route_impl(
        &self,
        tenant: Option<TenantId>,
        params: ConfigureRouteParams,
    ) -> Result<Json<ConfigureRouteResult>, ErrorData> {
        validate_queue_name(&params.queue)?;
        let embedding = Embedding::new(params.embedding).map_err(|_| {
            ErrorData::invalid_params(
                "embedding must be non-empty and contain only finite values",
                None,
            )
        })?;

        let routes = self.registry.routes(tenant.as_ref()).await;
        routes.insert(params.queue, embedding).await;
        let mut registered_routes = routes.keys().await;
        registered_routes.sort_unstable();

        Ok(Json(ConfigureRouteResult { registered_routes }))
    }

    /// Lists every dead letter currently on a queue.
    #[tool(description = "List every dead letter currently on a queue, including its last \
                        checkpoint (if any) and any existing triage verdict - what a triage \
                        agent reads before deciding anything.")]
    async fn list_dead_letters(
        &self,
        Parameters(params): Parameters<ListDeadLettersParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<Vec<DeadLetterSummary>>, ErrorData> {
        self.list_dead_letters_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `list_dead_letters` logic — see
    /// [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn list_dead_letters_impl(
        &self,
        tenant: Option<TenantId>,
        params: ListDeadLettersParams,
    ) -> Result<Json<Vec<DeadLetterSummary>>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let dead_letters = group.dead_letters().await;

        Ok(Json(
            dead_letters
                .into_iter()
                .map(|dead_letter| DeadLetterSummary {
                    message_id: dead_letter.id.to_string(),
                    payload: dead_letter.item,
                    delivery_count: dead_letter.delivery_count,
                    last_error: dead_letter.last_error,
                    dead_lettered_at_ms: dead_letter.dead_lettered_at.0,
                    checkpoint: dead_letter.checkpoint,
                    triage: dead_letter.triage.map(|verdict| TriageSummary {
                        classification: match verdict.classification {
                            TriageClassification::Transient => "transient".to_string(),
                            TriageClassification::Permanent => "permanent".to_string(),
                        },
                        reason: verdict.reason,
                        triaged_at_ms: verdict.triaged_at.0,
                    }),
                })
                .collect(),
        ))
    }

    /// Classifies a dead letter and auto-applies the matching policy.
    #[tool(
        description = "Classify a dead letter as transient (worth retrying) or permanent (needs \
                        a human), with a reason, and auto-apply the matching policy: transient \
                        immediately reprocesses the message back into the live queue; permanent \
                        leaves it annotated in the DLQ for a human - never auto-purged, \
                        regardless of confidence."
    )]
    async fn triage_dead_letter(
        &self,
        Parameters(params): Parameters<TriageDeadLetterParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<TriageDeadLetterResult>, ErrorData> {
        self.triage_dead_letter_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `triage_dead_letter` logic — see
    /// [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn triage_dead_letter_impl(
        &self,
        tenant: Option<TenantId>,
        params: TriageDeadLetterParams,
    ) -> Result<Json<TriageDeadLetterResult>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let classification: TriageClassification = params.classification.into();
        let verdict = TriageVerdict {
            classification,
            reason: params.reason,
            triaged_at: qaas_types::Timestamp::now(),
        };

        let annotated = group
            .annotate_dead_letter(message_id, verdict)
            .await
            .map_err(|error| io_error_to_mcp(&error))?;
        if !annotated {
            return Ok(Json(TriageDeadLetterResult { annotated: false, reprocessed: false }));
        }

        let reprocessed = if matches!(classification, TriageClassification::Transient) {
            group
                .reprocess_dead_letter(message_id)
                .await
                .map_err(|error| io_error_to_mcp(&error))?
        } else {
            false
        };

        Ok(Json(TriageDeadLetterResult { annotated: true, reprocessed }))
    }

    /// Reprocesses a dead letter directly, without going through triage.
    #[tool(
        description = "Reprocess a dead letter directly, without going through triage - puts it \
                        back in the live queue for another delivery attempt."
    )]
    async fn reprocess_dead_letter(
        &self,
        Parameters(params): Parameters<DeadLetterActionParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<ReprocessDeadLetterResult>, ErrorData> {
        self.reprocess_dead_letter_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `reprocess_dead_letter` logic — see
    /// [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn reprocess_dead_letter_impl(
        &self,
        tenant: Option<TenantId>,
        params: DeadLetterActionParams,
    ) -> Result<Json<ReprocessDeadLetterResult>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let reprocessed = group
            .reprocess_dead_letter(message_id)
            .await
            .map_err(|error| io_error_to_mcp(&error))?;
        Ok(Json(ReprocessDeadLetterResult { reprocessed }))
    }

    /// Permanently discards a dead letter.
    #[tool(
        description = "Permanently discard a dead letter - it will not be reprocessed. The only \
                        destructive DLQ action this server has; nothing auto-applies it."
    )]
    async fn purge_dead_letter(
        &self,
        Parameters(params): Parameters<DeadLetterActionParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<PurgeDeadLetterResult>, ErrorData> {
        self.purge_dead_letter_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `purge_dead_letter` logic — see
    /// [`Self::enqueue_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(tenant.as_ref())))]
    async fn purge_dead_letter_impl(
        &self,
        tenant: Option<TenantId>,
        params: DeadLetterActionParams,
    ) -> Result<Json<PurgeDeadLetterResult>, ErrorData> {
        let group = self.registry.get(tenant.as_ref(), &params.queue).await?;
        let message_id = parse_message_id(&params.message_id)?;
        let purged =
            group.purge_dead_letter(message_id).await.map_err(|error| io_error_to_mcp(&error))?;
        Ok(Json(PurgeDeadLetterResult { purged }))
    }

    /// Mints a new API key for a tenant. Restricted to the trusted local
    /// stdio connection — see the module docs on why key management
    /// itself isn't reachable over the boundary it exists to defend.
    #[tool(
        description = "Mint a new API key for a tenant, for authenticating future HTTP calls to \
                        this server as that tenant. The raw key is returned exactly once here - \
                        only its hash is ever stored, so losing this response means minting a \
                        replacement. Only available over the trusted local stdio connection."
    )]
    async fn create_api_key(
        &self,
        Parameters(params): Parameters<CreateApiKeyParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<CreateApiKeyResult>, ErrorData> {
        self.create_api_key_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `create_api_key` logic — see [`Self::enqueue_impl`]'s
    /// docs on why this is split out. `caller_tenant` is the tenant an
    /// HTTP caller already authenticated as, if any — `None` means this
    /// call arrived over stdio, the only place this tool is allowed to
    /// be reached from.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(caller_tenant.as_ref())))]
    async fn create_api_key_impl(
        &self,
        caller_tenant: Option<TenantId>,
        params: CreateApiKeyParams,
    ) -> Result<Json<CreateApiKeyResult>, ErrorData> {
        // An HTTP-originated call always carries a resolved TenantId (the
        // auth middleware guarantees that before dispatch ever happens),
        // so `Some` here unambiguously means "not stdio" - an
        // already-authenticated tenant credential has no business minting
        // more credentials, for itself or anyone else. Least privilege,
        // not a defense against a compromised middleware.
        if caller_tenant.is_some() {
            return Err(ErrorData::invalid_request(
                "create_api_key is only available over the trusted local stdio connection",
                None,
            ));
        }
        let tenant = TenantId::new(params.tenant_id)
            .map_err(|error| ErrorData::invalid_params(error.to_string(), None))?;
        let api_key =
            self.api_keys.mint(tenant.clone()).await.map_err(|error| io_error_to_mcp(&error))?;
        Ok(Json(CreateApiKeyResult { api_key, tenant_id: tenant.as_str().to_string() }))
    }

    /// Revokes an API key. Same stdio-only restriction as `create_api_key`.
    #[tool(
        description = "Revoke an API key so it can no longer authenticate any future call. Only \
                        available over the trusted local stdio connection."
    )]
    async fn revoke_api_key(
        &self,
        Parameters(params): Parameters<RevokeApiKeyParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<RevokeApiKeyResult>, ErrorData> {
        self.revoke_api_key_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `revoke_api_key` logic — see
    /// [`Self::create_api_key_impl`]'s docs.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(caller_tenant.as_ref())))]
    async fn revoke_api_key_impl(
        &self,
        caller_tenant: Option<TenantId>,
        params: RevokeApiKeyParams,
    ) -> Result<Json<RevokeApiKeyResult>, ErrorData> {
        if caller_tenant.is_some() {
            return Err(ErrorData::invalid_request(
                "revoke_api_key is only available over the trusted local stdio connection",
                None,
            ));
        }
        let revoked =
            self.api_keys.revoke(&params.api_key).await.map_err(|error| io_error_to_mcp(&error))?;
        Ok(Json(RevokeApiKeyResult { revoked }))
    }

    /// Sets a tenant's queue-count, pending-message, and request-rate
    /// quotas. Restricted to the trusted local stdio connection — same
    /// least-privilege reasoning as `create_api_key`: this configures
    /// limits for *other* tenants, an operator action a tenant credential
    /// itself has no business taking, for itself or anyone else.
    #[tool(
        description = "Set a tenant's resource quotas: max_queues (how many queues it may have \
                        open at once), max_pending_messages (total unacknowledged messages across \
                        all its queues), and requests_per_window (MCP tool calls per \
                        window_seconds, default 60). Omitting a field leaves it unlimited. Takes \
                        effect immediately, including for usage already counted. Only available \
                        over the trusted local stdio connection."
    )]
    async fn configure_tenant_quota(
        &self,
        Parameters(params): Parameters<ConfigureTenantQuotaParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<ConfigureTenantQuotaResult>, ErrorData> {
        self.configure_tenant_quota_impl(self.authorize(&ctx).await?, params).await
    }

    /// The actual `configure_tenant_quota` logic — see
    /// [`Self::create_api_key_impl`]'s docs on why this takes
    /// `caller_tenant` directly rather than a `RequestContext`.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(caller_tenant.as_ref())))]
    async fn configure_tenant_quota_impl(
        &self,
        caller_tenant: Option<TenantId>,
        params: ConfigureTenantQuotaParams,
    ) -> Result<Json<ConfigureTenantQuotaResult>, ErrorData> {
        if caller_tenant.is_some() {
            return Err(ErrorData::invalid_request(
                "configure_tenant_quota is only available over the trusted local stdio connection",
                None,
            ));
        }
        let tenant = TenantId::new(params.tenant_id)
            .map_err(|error| ErrorData::invalid_params(error.to_string(), None))?;

        let window = Duration::from_secs(params.window_seconds.unwrap_or(60));
        let config = QuotaConfig {
            max_queues: params.max_queues,
            max_pending_messages: params.max_pending_messages,
            requests_per_window: params.requests_per_window,
            window,
        };
        self.registry.quota(&tenant).await.set_config(config).await;

        Ok(Json(ConfigureTenantQuotaResult {
            tenant_id: tenant.as_str().to_string(),
            max_queues: config.max_queues,
            max_pending_messages: config.max_pending_messages,
            requests_per_window: config.requests_per_window,
            window_seconds: window.as_secs(),
        }))
    }

    /// Reports a tenant's current quota configuration and live usage.
    #[tool(description = "Report a tenant's current resource-quota configuration and live usage: \
                        how many queues it has open, how many messages are pending across them, \
                        and how many tool calls it's made within the current rate-limit window. \
                        An HTTP-authenticated caller always gets its own status regardless of \
                        tenant_id; the trusted stdio connection must pass tenant_id to name which \
                        tenant to report on.")]
    async fn tenant_quota_status(
        &self,
        Parameters(params): Parameters<TenantQuotaStatusParams>,
        ctx: RequestContext<RoleServer>,
    ) -> Result<Json<TenantQuotaStatusResult>, ErrorData> {
        let caller_tenant = self.authorize(&ctx).await?;
        self.tenant_quota_status_impl(caller_tenant, params).await
    }

    /// The actual `tenant_quota_status` logic — see
    /// [`Self::enqueue_impl`]'s docs on why this is split out.
    #[tracing::instrument(skip_all, fields(tenant = %tenant_label(caller_tenant.as_ref())))]
    async fn tenant_quota_status_impl(
        &self,
        caller_tenant: Option<TenantId>,
        params: TenantQuotaStatusParams,
    ) -> Result<Json<TenantQuotaStatusResult>, ErrorData> {
        let tenant = if let Some(tenant) = caller_tenant {
            tenant
        } else {
            let Some(tenant_id) = params.tenant_id else {
                return Err(ErrorData::invalid_params(
                    "tenant_id is required when calling tenant_quota_status over the trusted \
                     stdio connection",
                    None,
                ));
            };
            TenantId::new(tenant_id)
                .map_err(|error| ErrorData::invalid_params(error.to_string(), None))?
        };

        let quota = self.registry.quota(&tenant).await;
        let config = quota.config().await;
        let requests_in_window = quota.requests_in_window().await;
        let queues_open = self.registry.tenant_queue_count(&tenant).await;
        let pending_messages = self.registry.tenant_pending_total(&tenant).await;

        Ok(Json(TenantQuotaStatusResult {
            tenant_id: tenant.as_str().to_string(),
            max_queues: config.max_queues,
            queues_open,
            max_pending_messages: config.max_pending_messages,
            pending_messages,
            requests_per_window: config.requests_per_window,
            requests_in_window,
            window_seconds: config.window.as_secs(),
        }))
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
                    over. Use configure_admission to set a queue's token/cost budget (a sliding \
                    window, off by default) and admission_status to check current usage; enqueue \
                    can decline a task that would exceed the budget (pass estimated_tokens / \
                    estimated_cost to be checked), and claim reports throttled: true instead of \
                    waiting when the queue is currently over budget. Pass embedding (a vector from \
                    your own embedding model) to enqueue to collapse near-duplicate tasks into an \
                    existing one automatically, or omit queue entirely to have the task routed to \
                    the closest match among queues registered via configure_route. Use \
                    list_dead_letters to see what's failed permanently, and triage_dead_letter to \
                    classify each one (transient/permanent, with a reason) - transient \
                    auto-reprocesses back into the live queue, permanent stays held for a human; \
                    reprocess_dead_letter and purge_dead_letter are also available directly \
                    without going through triage. For a long-running task, call \
                    publish_partial_result after each incremental step (e.g. each token from a \
                    streaming LLM call) without resolving the message; another caller watches \
                    with stream_partial_results, passing after_sequence to only get what's new \
                    and calling it again in a loop for near-real-time updates. If you're being \
                    denied with a message about queue, pending-message, or request-rate limits, \
                    call tenant_quota_status (no arguments needed) to see your current \
                    configuration and usage before retrying."
)]
impl ServerHandler for QaasMcpServer {}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::time::Duration;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use serde_json::json;
    use tempfile::tempdir;

    use super::{QaasMcpServer, TenantId, validate_queue_name};

    async fn server(dir: &tempfile::TempDir) -> QaasMcpServer {
        QaasMcpServer::new(dir.path()).await.unwrap()
    }

    /// Finds the recorded value for the metric named `name` carrying
    /// exactly `labels` (order-independent) in `snapshot` (a
    /// [`metrics_util::debugging::Snapshot`], already unpacked via
    /// `into_vec` so a single snapshot can be queried more than once), or
    /// `None` if nothing matches — a metric this branch never recorded
    /// under those labels, not a test failure any other way a test could
    /// observe, since `metrics::gauge!`/`histogram!` themselves never
    /// report "not called."
    fn metric_value(
        snapshot: &[(
            metrics_util::CompositeKey,
            Option<metrics::Unit>,
            Option<metrics::SharedString>,
            DebugValue,
        )],
        name: &str,
        labels: &[(&str, &str)],
    ) -> Option<DebugValue> {
        let expected: BTreeSet<(&str, &str)> = labels.iter().copied().collect();
        snapshot.iter().find_map(|(composite_key, _, _, value)| {
            let key = composite_key.key();
            if key.name() != name {
                return None;
            }
            let actual: BTreeSet<(&str, &str)> =
                key.labels().map(|label| (label.key(), label.value())).collect();
            (actual == expected).then(|| value_clone(value))
        })
    }

    /// [`DebugValue`] doesn't implement `Clone`, but every variant this
    /// module actually asserts on (`Gauge`, `Histogram`) is trivially
    /// reconstructible from its own inner data — cheaper than
    /// restructuring [`metric_value`] to return a borrow tied to
    /// `snapshot`'s lifetime for what's only ever used in test
    /// assertions.
    fn value_clone(value: &DebugValue) -> DebugValue {
        match value {
            DebugValue::Counter(count) => DebugValue::Counter(*count),
            DebugValue::Gauge(gauge) => DebugValue::Gauge(*gauge),
            DebugValue::Histogram(values) => DebugValue::Histogram(values.clone()),
        }
    }

    /// Dead-letters a message directly through `qaas-core`, bypassing
    /// the MCP layer entirely — the registry always opens queues with
    /// `RetryPolicy::DEFAULT` (five attempts, real backoff delays), and
    /// walking a message through five real claim/nack cycles just to
    /// test triage would make every test here take several real seconds
    /// for no reason. Opening the same WAL path the registry would lazily
    /// open on first reference, with a policy that exhausts after one
    /// attempt instead, gets a real dead letter (not a faked-up struct)
    /// sitting on disk before the server ever touches this queue —
    /// `list_dead_letters`/`triage_dead_letter`/etc. find it exactly the
    /// way they'd find one that arrived through real MCP traffic.
    async fn seed_a_dead_letter(
        dir: &tempfile::TempDir,
        queue: &str,
        payload: serde_json::Value,
        checkpoint: Option<serde_json::Value>,
    ) -> String {
        let path = dir.path().join(format!("{queue}.wal"));
        let exhausts_immediately = super::RetryPolicy {
            base_delay: Duration::ZERO,
            max_delay: Duration::ZERO,
            multiplier: 1.0,
            jitter: 0.0,
            max_attempts: Some(1),
        };
        let group: super::ConsumerGroup<serde_json::Value> =
            super::ConsumerGroup::open(&path, Duration::from_secs(60), exhausts_immediately)
                .await
                .unwrap();
        let id = group.enqueue(payload).await.unwrap();
        let claim = group.claim().await;
        if let Some(checkpoint) = checkpoint {
            group.checkpoint(claim.id, claim.token, checkpoint).await.unwrap();
        }
        group.nack(claim.id, claim.token, Some("simulated failure".to_string())).await.unwrap();
        id.to_string()
    }

    /// Every `EnqueueParams` field except `queue`/`payload`, defaulted to
    /// "not participating in this feature" — most tests only care about
    /// one or two fields and would otherwise have to spell out every
    /// admission/dedup/routing field just to get a bare enqueue.
    fn enqueue_params(queue: &str, payload: serde_json::Value) -> super::EnqueueParams {
        super::EnqueueParams {
            queue: Some(queue.to_string()),
            payload,
            idempotency_key: None,
            estimated_tokens: None,
            estimated_cost: None,
            embedding: None,
        }
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
        let server = server(&dir).await;

        let enqueued = server
            .enqueue_impl(None, enqueue_params("orders", json!({"item": "widget"})))
            .await
            .unwrap()
            .0;
        assert_eq!(enqueued.queue, "orders");
        assert!(!enqueued.deduplicated);

        let claimed = server
            .claim_impl(
                None,
                super::ClaimParams { queue: "orders".to_string(), wait_ms: Some(100) },
            )
            .await
            .unwrap()
            .0;
        assert!(claimed.available);
        assert_eq!(claimed.message_id, Some(enqueued.message_id.clone()));
        assert_eq!(claimed.payload, Some(json!({"item": "widget"})));
        assert_eq!(claimed.delivery_count, Some(1));
        assert_eq!(claimed.checkpoint, None);

        let acked = server
            .ack_impl(
                None,
                super::AckParams {
                    queue: "orders".to_string(),
                    message_id: claimed.message_id.unwrap(),
                    lease_token: claimed.lease_token.unwrap(),
                },
            )
            .await
            .unwrap()
            .0;
        assert!(acked.acked);
    }

    #[tokio::test]
    async fn a_checkpoint_is_visible_on_the_next_claim_after_a_pausing_nack() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        server
            .enqueue_impl(None, enqueue_params("workflows", json!("start the task")))
            .await
            .unwrap();

        let first = server
            .claim_impl(
                None,
                super::ClaimParams { queue: "workflows".to_string(), wait_ms: Some(100) },
            )
            .await
            .unwrap()
            .0;

        let progress = json!({"completed_steps": ["fetch", "summarize"]});
        let checkpointed = server
            .checkpoint_impl(
                None,
                super::CheckpointParams {
                    queue: "workflows".to_string(),
                    message_id: first.message_id.clone().unwrap(),
                    lease_token: first.lease_token.unwrap(),
                    state: progress.clone(),
                },
            )
            .await
            .unwrap()
            .0;
        assert!(checkpointed.checkpointed);

        // Pausing — e.g. waiting on a human — releases the message the
        // same way a failure does (see the module docs for why), but the
        // checkpoint already saved should still be there for whoever
        // claims it next.
        let nacked = server
            .nack_impl(
                None,
                super::NackParams {
                    queue: "workflows".to_string(),
                    message_id: first.message_id.unwrap(),
                    lease_token: first.lease_token.unwrap(),
                    reason: None,
                },
            )
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
            .claim_impl(
                None,
                super::ClaimParams { queue: "workflows".to_string(), wait_ms: Some(2000) },
            )
            .await
            .unwrap()
            .0;
        assert!(resumed.available, "should not still be waiting out the retry backoff");
        assert_eq!(resumed.checkpoint, Some(progress));
    }

    #[tokio::test]
    async fn checkpointing_with_a_stale_lease_token_is_rejected() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let result = server
            .checkpoint_impl(
                None,
                super::CheckpointParams {
                    queue: "workflows".to_string(),
                    message_id: qaas_types::MessageId::new().to_string(),
                    lease_token: 0,
                    state: json!("never claimed"),
                },
            )
            .await
            .unwrap()
            .0;

        assert!(!result.checkpointed);
    }

    #[tokio::test]
    async fn claim_reports_unavailable_rather_than_blocking_forever_on_an_empty_queue() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let claimed = server
            .claim_impl(None, super::ClaimParams { queue: "empty".to_string(), wait_ms: Some(20) })
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
        let server = server(&dir).await;

        let result = server
            .nack_impl(
                None,
                super::NackParams {
                    queue: "orders".to_string(),
                    message_id: qaas_types::MessageId::new().to_string(),
                    lease_token: 0,
                    reason: Some("simulated failure".to_string()),
                },
            )
            .await
            .unwrap()
            .0;

        assert!(!result.nacked);
    }

    #[tokio::test]
    async fn enqueue_with_a_reused_idempotency_key_does_not_create_a_second_message() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let params = || super::EnqueueParams {
            idempotency_key: Some("dedup-1".to_string()),
            ..enqueue_params("orders", json!("payload"))
        };

        let first = server.enqueue_impl(None, params()).await.unwrap().0;
        let second = server.enqueue_impl(None, params()).await.unwrap().0;

        assert_eq!(first.message_id, second.message_id);
    }

    #[tokio::test]
    async fn an_invalid_queue_name_is_rejected_before_touching_the_filesystem() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let result = server.enqueue_impl(None, enqueue_params("../escape", json!("x"))).await;

        // `Json<EnqueueResult>` isn't `Debug` (it's `rmcp`'s wrapper
        // type, not one of ours), so `unwrap_err` isn't available here —
        // matching directly is just as clear and doesn't need it.
        let Err(error) = result else {
            panic!("expected an error, got a successful enqueue");
        };
        assert_eq!(error.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn an_unconfigured_queue_is_never_throttled() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let enqueued = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    estimated_tokens: Some(1_000_000_000),
                    estimated_cost: Some(1_000_000.0),
                    ..enqueue_params("orders", json!("x"))
                },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(enqueued.tokens_remaining, None);
        assert_eq!(enqueued.cost_remaining, None);
    }

    #[tokio::test]
    async fn enqueue_is_denied_once_the_configured_token_budget_would_be_exceeded() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        server
            .configure_admission_impl(
                None,
                super::ConfigureAdmissionParams {
                    queue: "orders".to_string(),
                    tokens_per_window: Some(100),
                    cost_ceiling_per_window: None,
                    window_seconds: Some(60),
                },
            )
            .await
            .unwrap();

        let first = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    estimated_tokens: Some(80),
                    ..enqueue_params("orders", json!("first"))
                },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(first.tokens_remaining, Some(20));

        let result = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    estimated_tokens: Some(50),
                    ..enqueue_params("orders", json!("second"))
                },
            )
            .await;

        let Err(error) = result else {
            panic!("expected the second enqueue to be denied");
        };
        assert_eq!(error.code, rmcp::model::ErrorCode::INVALID_REQUEST);
        assert!(error.message.contains("130 tokens"), "{}", error.message);
    }

    #[tokio::test]
    async fn claim_reports_throttled_instead_of_waiting_when_the_queue_is_over_budget() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        server
            .enqueue_impl(
                None,
                enqueue_params("orders", json!("a real message, sitting right there")),
            )
            .await
            .unwrap();

        server
            .configure_admission_impl(
                None,
                super::ConfigureAdmissionParams {
                    queue: "orders".to_string(),
                    tokens_per_window: Some(0),
                    cost_ceiling_per_window: None,
                    window_seconds: Some(60),
                },
            )
            .await
            .unwrap();

        let claimed = server
            .claim_impl(None, super::ClaimParams { queue: "orders".to_string(), wait_ms: Some(50) })
            .await
            .unwrap()
            .0;

        // A real message is sitting there, unclaimed by anyone — this is
        // specifically the throttled outcome, not "the queue was empty."
        assert!(!claimed.available);
        assert!(claimed.throttled);
    }

    #[tokio::test]
    async fn admission_status_reports_configuration_and_live_usage() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        server
            .configure_admission_impl(
                None,
                super::ConfigureAdmissionParams {
                    queue: "orders".to_string(),
                    tokens_per_window: Some(1000),
                    cost_ceiling_per_window: Some(5.0),
                    window_seconds: Some(120),
                },
            )
            .await
            .unwrap();

        server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    estimated_tokens: Some(200),
                    estimated_cost: Some(1.5),
                    ..enqueue_params("orders", json!("x"))
                },
            )
            .await
            .unwrap();

        let status = server
            .admission_status_impl(
                None,
                super::AdmissionStatusParams { queue: "orders".to_string() },
            )
            .await
            .unwrap()
            .0;

        assert_eq!(status.tokens_per_window, Some(1000));
        assert_eq!(status.cost_ceiling_per_window, Some(5.0));
        assert_eq!(status.window_seconds, 120);
        assert_eq!(status.tokens_used, 200);
        assert!((status.cost_used - 1.5).abs() < f64::EPSILON, "{}", status.cost_used);
        assert_eq!(status.tokens_remaining, Some(800));
        assert_eq!(status.cost_remaining, Some(3.5));
    }

    #[tokio::test]
    async fn near_duplicate_enqueues_collapse_into_the_original() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let first = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    embedding: Some(vec![1.0, 0.0, 0.0]),
                    ..enqueue_params("support", json!("please reset my password"))
                },
            )
            .await
            .unwrap()
            .0;
        assert!(!first.deduplicated);

        // Slightly different wording, near-identical meaning (a tiny
        // nudge off the same direction) — this is the whole scenario
        // Plan.md's line names: another agent independently queuing the
        // same underlying task.
        let second = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    embedding: Some(vec![0.999, 0.001, 0.0]),
                    ..enqueue_params("support", json!("can you reset my password please"))
                },
            )
            .await
            .unwrap()
            .0;

        assert!(second.deduplicated);
        assert_eq!(second.message_id, first.message_id);
        assert!(second.similarity.unwrap() >= super::DEDUP_SIMILARITY_THRESHOLD);
        assert_eq!(second.tokens_remaining, None, "a collapsed enqueue shouldn't touch admission");

        // Only one message actually made it into the queue.
        let group = server.registry.get(None, "support").await.unwrap();
        assert_eq!(group.len().await, 1);
    }

    #[tokio::test]
    async fn dissimilar_embeddings_do_not_collapse() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    embedding: Some(vec![1.0, 0.0]),
                    ..enqueue_params("support", json!("reset my password"))
                },
            )
            .await
            .unwrap();

        let second = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    embedding: Some(vec![0.0, 1.0]),
                    ..enqueue_params("support", json!("cancel my subscription"))
                },
            )
            .await
            .unwrap()
            .0;

        assert!(!second.deduplicated);
        let group = server.registry.get(None, "support").await.unwrap();
        assert_eq!(group.len().await, 2);
    }

    #[tokio::test]
    async fn acking_a_message_frees_its_embedding_for_reuse_immediately() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let first = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    embedding: Some(vec![1.0, 0.0]),
                    ..enqueue_params("support", json!("reset my password"))
                },
            )
            .await
            .unwrap()
            .0;

        let claimed = server
            .claim_impl(
                None,
                super::ClaimParams { queue: "support".to_string(), wait_ms: Some(100) },
            )
            .await
            .unwrap()
            .0;
        server
            .ack_impl(
                None,
                super::AckParams {
                    queue: "support".to_string(),
                    message_id: claimed.message_id.unwrap(),
                    lease_token: claimed.lease_token.unwrap(),
                },
            )
            .await
            .unwrap();

        // The first task is done and gone — a near-identical *new* task
        // must not be silently swallowed as a "duplicate" of it.
        let second = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    embedding: Some(vec![1.0, 0.0]),
                    ..enqueue_params("support", json!("reset my password again"))
                },
            )
            .await
            .unwrap()
            .0;

        assert!(!second.deduplicated);
        assert_ne!(second.message_id, first.message_id);
    }

    #[tokio::test]
    async fn enqueue_without_a_queue_or_embedding_is_rejected() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let result = server
            .enqueue_impl(
                None,
                super::EnqueueParams { queue: None, ..enqueue_params("unused", json!("x")) },
            )
            .await;

        let Err(error) = result else {
            panic!("expected an error with neither queue nor embedding given");
        };
        assert_eq!(error.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn enqueue_without_a_queue_routes_to_the_closest_registered_descriptor() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let routes = server
            .configure_route_impl(
                None,
                super::ConfigureRouteParams {
                    queue: "billing".to_string(),
                    embedding: vec![1.0, 0.0],
                },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(routes.registered_routes, vec!["billing".to_string()]);

        server
            .configure_route_impl(
                None,
                super::ConfigureRouteParams {
                    queue: "support".to_string(),
                    embedding: vec![0.0, 1.0],
                },
            )
            .await
            .unwrap();

        let enqueued = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    queue: None,
                    embedding: Some(vec![0.9, 0.1]),
                    ..enqueue_params("unused", json!("a billing question"))
                },
            )
            .await
            .unwrap()
            .0;

        assert_eq!(enqueued.queue, "billing");
        let group = server.registry.get(None, "billing").await.unwrap();
        assert_eq!(group.len().await, 1);
    }

    #[tokio::test]
    async fn routing_with_no_routes_registered_is_rejected() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let result = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    queue: None,
                    embedding: Some(vec![1.0, 0.0]),
                    ..enqueue_params("unused", json!("x"))
                },
            )
            .await;

        let Err(error) = result else {
            panic!("expected an error with no routes registered");
        };
        assert_eq!(error.code, rmcp::model::ErrorCode::INVALID_PARAMS);
    }

    #[tokio::test]
    async fn list_dead_letters_reports_payload_checkpoint_and_failure_context() {
        let dir = tempdir().unwrap();
        let id = seed_a_dead_letter(
            &dir,
            "doomed",
            json!({"task": "will fail"}),
            Some(json!({"completed_steps": ["fetch"]})),
        )
        .await;

        let server = server(&dir).await;
        let listed = server
            .list_dead_letters_impl(
                None,
                super::ListDeadLettersParams { queue: "doomed".to_string() },
            )
            .await
            .unwrap()
            .0;

        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].message_id, id);
        assert_eq!(listed[0].payload, json!({"task": "will fail"}));
        assert_eq!(listed[0].delivery_count, 1);
        assert_eq!(listed[0].last_error.as_deref(), Some("simulated failure"));
        assert_eq!(listed[0].checkpoint, Some(json!({"completed_steps": ["fetch"]})));
        assert!(listed[0].triage.is_none());
    }

    #[tokio::test]
    async fn triaging_as_transient_auto_reprocesses_into_the_live_queue() {
        let dir = tempdir().unwrap();
        let id = seed_a_dead_letter(&dir, "doomed", json!("will fail"), None).await;
        let server = server(&dir).await;

        let result = server
            .triage_dead_letter_impl(
                None,
                super::TriageDeadLetterParams {
                    queue: "doomed".to_string(),
                    message_id: id.clone(),
                    classification: super::TriageClassificationParam::Transient,
                    reason: "looks like a rate limit".to_string(),
                },
            )
            .await
            .unwrap()
            .0;

        assert!(result.annotated);
        assert!(result.reprocessed);

        // Gone from the DLQ, and genuinely claimable again in the live
        // queue - not just marked somehow.
        let listed = server
            .list_dead_letters_impl(
                None,
                super::ListDeadLettersParams { queue: "doomed".to_string() },
            )
            .await
            .unwrap()
            .0;
        assert!(listed.is_empty());

        let claimed = server
            .claim_impl(
                None,
                super::ClaimParams { queue: "doomed".to_string(), wait_ms: Some(500) },
            )
            .await
            .unwrap()
            .0;
        assert!(claimed.available);
        assert_eq!(claimed.message_id, Some(id));
    }

    #[tokio::test]
    async fn triaging_as_permanent_holds_and_annotates_without_reprocessing() {
        let dir = tempdir().unwrap();
        let id = seed_a_dead_letter(&dir, "doomed", json!("will fail"), None).await;
        let server = server(&dir).await;

        let result = server
            .triage_dead_letter_impl(
                None,
                super::TriageDeadLetterParams {
                    queue: "doomed".to_string(),
                    message_id: id.clone(),
                    classification: super::TriageClassificationParam::Permanent,
                    reason: "malformed input, will never succeed".to_string(),
                },
            )
            .await
            .unwrap()
            .0;

        assert!(result.annotated);
        assert!(!result.reprocessed, "permanent must never auto-reprocess");

        let listed = server
            .list_dead_letters_impl(
                None,
                super::ListDeadLettersParams { queue: "doomed".to_string() },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(listed.len(), 1, "must still be held in the DLQ, not discarded");
        let triage = listed[0].triage.as_ref().unwrap();
        assert_eq!(triage.classification, "permanent");
        assert_eq!(triage.reason, "malformed input, will never succeed");
    }

    #[tokio::test]
    async fn triaging_an_unknown_id_is_not_annotated() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let result = server
            .triage_dead_letter_impl(
                None,
                super::TriageDeadLetterParams {
                    queue: "doomed".to_string(),
                    message_id: qaas_types::MessageId::new().to_string(),
                    classification: super::TriageClassificationParam::Transient,
                    reason: "no such entry".to_string(),
                },
            )
            .await
            .unwrap()
            .0;

        assert!(!result.annotated);
        assert!(!result.reprocessed);
    }

    #[tokio::test]
    async fn reprocess_dead_letter_works_directly_without_triage() {
        let dir = tempdir().unwrap();
        let id = seed_a_dead_letter(&dir, "doomed", json!("will fail"), None).await;
        let server = server(&dir).await;

        let result = server
            .reprocess_dead_letter_impl(
                None,
                super::DeadLetterActionParams { queue: "doomed".to_string(), message_id: id },
            )
            .await
            .unwrap()
            .0;
        assert!(result.reprocessed);

        let listed = server
            .list_dead_letters_impl(
                None,
                super::ListDeadLettersParams { queue: "doomed".to_string() },
            )
            .await
            .unwrap()
            .0;
        assert!(listed.is_empty());
    }

    #[tokio::test]
    async fn purge_dead_letter_discards_it_for_good() {
        let dir = tempdir().unwrap();
        let id = seed_a_dead_letter(&dir, "doomed", json!("will fail"), None).await;
        let server = server(&dir).await;

        let result = server
            .purge_dead_letter_impl(
                None,
                super::DeadLetterActionParams { queue: "doomed".to_string(), message_id: id },
            )
            .await
            .unwrap()
            .0;
        assert!(result.purged);

        let listed = server
            .list_dead_letters_impl(
                None,
                super::ListDeadLettersParams { queue: "doomed".to_string() },
            )
            .await
            .unwrap()
            .0;
        assert!(listed.is_empty());

        // Purging is the one action nothing in this module auto-applies
        // from a mere classification — confirmed here only by the fact
        // that reaching this state required calling purge_dead_letter
        // directly, not triage_dead_letter with any classification.
    }

    #[tokio::test]
    async fn purging_an_unknown_id_is_a_false_not_an_error() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let result = server
            .purge_dead_letter_impl(
                None,
                super::DeadLetterActionParams {
                    queue: "doomed".to_string(),
                    message_id: qaas_types::MessageId::new().to_string(),
                },
            )
            .await
            .unwrap()
            .0;
        assert!(!result.purged);
    }

    #[tokio::test]
    async fn published_chunks_are_returned_in_order_by_stream_partial_results() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        server.enqueue_impl(None, enqueue_params("narration", json!("start"))).await.unwrap();
        let claimed = server
            .claim_impl(
                None,
                super::ClaimParams { queue: "narration".to_string(), wait_ms: Some(500) },
            )
            .await
            .unwrap()
            .0;

        for (chunk, is_final) in [("once", false), ("upon", false), ("a time", true)] {
            let result = server
                .publish_partial_result_impl(
                    None,
                    super::PublishPartialResultParams {
                        queue: "narration".to_string(),
                        message_id: claimed.message_id.clone().unwrap(),
                        lease_token: claimed.lease_token.unwrap(),
                        data: json!(chunk),
                        is_final: Some(is_final),
                    },
                )
                .await
                .unwrap()
                .0;
            assert!(result.published);
        }

        let streamed = server
            .stream_partial_results_impl(
                None,
                super::StreamPartialResultsParams {
                    queue: "narration".to_string(),
                    message_id: claimed.message_id.clone().unwrap(),
                    after_sequence: None,
                    wait_ms: Some(200),
                },
            )
            .await
            .unwrap()
            .0;

        assert!(streamed.active);
        assert_eq!(streamed.chunks.len(), 3);
        assert_eq!(streamed.chunks[0].sequence, 1);
        assert_eq!(streamed.chunks[0].data, json!("once"));
        assert!(!streamed.chunks[0].is_final);
        assert_eq!(streamed.chunks[2].sequence, 3);
        assert!(streamed.chunks[2].is_final);
    }

    #[tokio::test]
    async fn stream_partial_results_only_returns_chunks_newer_than_after_sequence() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        server.enqueue_impl(None, enqueue_params("narration", json!("x"))).await.unwrap();
        let claimed = server
            .claim_impl(
                None,
                super::ClaimParams { queue: "narration".to_string(), wait_ms: Some(500) },
            )
            .await
            .unwrap()
            .0;

        for chunk in ["one", "two"] {
            server
                .publish_partial_result_impl(
                    None,
                    super::PublishPartialResultParams {
                        queue: "narration".to_string(),
                        message_id: claimed.message_id.clone().unwrap(),
                        lease_token: claimed.lease_token.unwrap(),
                        data: json!(chunk),
                        is_final: None,
                    },
                )
                .await
                .unwrap();
        }

        let streamed = server
            .stream_partial_results_impl(
                None,
                super::StreamPartialResultsParams {
                    queue: "narration".to_string(),
                    message_id: claimed.message_id.unwrap(),
                    after_sequence: Some(1),
                    wait_ms: Some(200),
                },
            )
            .await
            .unwrap()
            .0;

        assert_eq!(streamed.chunks.len(), 1);
        assert_eq!(streamed.chunks[0].sequence, 2);
        assert_eq!(streamed.chunks[0].data, json!("two"));
    }

    #[tokio::test]
    async fn stream_partial_results_reports_inactive_for_a_never_claimed_message() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let streamed = server
            .stream_partial_results_impl(
                None,
                super::StreamPartialResultsParams {
                    queue: "narration".to_string(),
                    message_id: qaas_types::MessageId::new().to_string(),
                    after_sequence: None,
                    wait_ms: Some(50),
                },
            )
            .await
            .unwrap()
            .0;

        assert!(!streamed.active);
        assert!(streamed.chunks.is_empty());
    }

    #[tokio::test]
    async fn stream_partial_results_reports_inactive_once_the_lease_ends() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        server.enqueue_impl(None, enqueue_params("narration", json!("x"))).await.unwrap();
        let claimed = server
            .claim_impl(
                None,
                super::ClaimParams { queue: "narration".to_string(), wait_ms: Some(500) },
            )
            .await
            .unwrap()
            .0;

        server
            .publish_partial_result_impl(
                None,
                super::PublishPartialResultParams {
                    queue: "narration".to_string(),
                    message_id: claimed.message_id.clone().unwrap(),
                    lease_token: claimed.lease_token.unwrap(),
                    data: json!("in progress"),
                    is_final: None,
                },
            )
            .await
            .unwrap();
        server
            .ack_impl(
                None,
                super::AckParams {
                    queue: "narration".to_string(),
                    message_id: claimed.message_id.clone().unwrap(),
                    lease_token: claimed.lease_token.unwrap(),
                },
            )
            .await
            .unwrap();

        let streamed = server
            .stream_partial_results_impl(
                None,
                super::StreamPartialResultsParams {
                    queue: "narration".to_string(),
                    message_id: claimed.message_id.unwrap(),
                    after_sequence: None,
                    wait_ms: Some(50),
                },
            )
            .await
            .unwrap()
            .0;

        assert!(!streamed.active, "the lease that produced this stream is gone");
        assert!(streamed.chunks.is_empty());
    }

    #[tokio::test]
    async fn stream_partial_results_waits_for_a_chunk_published_after_the_call_starts() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        server.enqueue_impl(None, enqueue_params("narration", json!("x"))).await.unwrap();
        let claimed = server
            .claim_impl(
                None,
                super::ClaimParams { queue: "narration".to_string(), wait_ms: Some(500) },
            )
            .await
            .unwrap()
            .0;

        let watcher = {
            let server = server.clone();
            let message_id = claimed.message_id.clone().unwrap();
            tokio::spawn(async move {
                server
                    .stream_partial_results_impl(
                        None,
                        super::StreamPartialResultsParams {
                            queue: "narration".to_string(),
                            message_id,
                            after_sequence: None,
                            wait_ms: Some(5000),
                        },
                    )
                    .await
                    .unwrap()
                    .0
            })
        };

        tokio::time::sleep(Duration::from_millis(50)).await;
        server
            .publish_partial_result_impl(
                None,
                super::PublishPartialResultParams {
                    queue: "narration".to_string(),
                    message_id: claimed.message_id.unwrap(),
                    lease_token: claimed.lease_token.unwrap(),
                    data: json!("finally"),
                    is_final: Some(true),
                },
            )
            .await
            .unwrap();

        let streamed = tokio::time::timeout(Duration::from_secs(5), watcher)
            .await
            .expect("stream_partial_results should have woken up once a chunk was published")
            .unwrap();
        assert_eq!(streamed.chunks.len(), 1);
        assert_eq!(streamed.chunks[0].data, json!("finally"));
    }

    #[tokio::test]
    async fn publishing_with_a_stale_lease_token_has_no_effect() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let result = server
            .publish_partial_result_impl(
                None,
                super::PublishPartialResultParams {
                    queue: "narration".to_string(),
                    message_id: qaas_types::MessageId::new().to_string(),
                    lease_token: 0,
                    data: json!("nobody's listening"),
                    is_final: None,
                },
            )
            .await
            .unwrap()
            .0;

        assert!(!result.published);
        assert_eq!(result.sequence, None);
    }

    #[tokio::test]
    async fn tenants_with_the_same_queue_name_never_see_each_others_messages() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant_a = TenantId::new("tenant-a").unwrap();
        let tenant_b = TenantId::new("tenant-b").unwrap();

        server
            .enqueue_impl(Some(tenant_a.clone()), enqueue_params("orders", json!("a's task")))
            .await
            .unwrap();
        server
            .enqueue_impl(Some(tenant_b.clone()), enqueue_params("orders", json!("b's task")))
            .await
            .unwrap();

        // Tenant B's claim on "orders" must return B's own task, never A's.
        let claimed_by_b = server
            .claim_impl(
                Some(tenant_b),
                super::ClaimParams { queue: "orders".to_string(), wait_ms: Some(50) },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(claimed_by_b.payload, Some(json!("b's task")));

        // The trusted stdio connection (tenant: None) has its own, third
        // "orders" queue - distinct from either tenant's, not a shared
        // fallback either of them can reach.
        let claimed_over_stdio = server
            .claim_impl(None, super::ClaimParams { queue: "orders".to_string(), wait_ms: Some(20) })
            .await
            .unwrap()
            .0;
        assert!(!claimed_over_stdio.available);

        // Tenant A's own "orders" queue still has exactly its own task
        // waiting - untouched by B's enqueue or claim.
        let claimed_by_a = server
            .claim_impl(
                Some(tenant_a),
                super::ClaimParams { queue: "orders".to_string(), wait_ms: Some(20) },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(claimed_by_a.payload, Some(json!("a's task")));
    }

    #[tokio::test]
    async fn a_tenant_can_never_claim_a_message_enqueued_by_a_different_tenant() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant_a = TenantId::new("tenant-a").unwrap();
        let tenant_b = TenantId::new("tenant-b").unwrap();

        server
            .enqueue_impl(Some(tenant_b), enqueue_params("orders", json!("b's task")))
            .await
            .unwrap();

        let claimed_by_a = server
            .claim_impl(
                Some(tenant_a),
                super::ClaimParams { queue: "orders".to_string(), wait_ms: Some(20) },
            )
            .await
            .unwrap()
            .0;
        assert!(!claimed_by_a.available, "tenant A must not see tenant B's message");
    }

    #[tokio::test]
    async fn routing_descriptors_are_scoped_per_tenant() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant_a = TenantId::new("tenant-a").unwrap();

        server
            .configure_route_impl(
                Some(tenant_a.clone()),
                super::ConfigureRouteParams {
                    queue: "support".to_string(),
                    embedding: vec![1.0, 0.0],
                },
            )
            .await
            .unwrap();

        // The same descriptor is unregistered for a different caller
        // (here, the trusted stdio connection) - routing must fail for
        // it rather than silently reusing tenant A's route table.
        let stdio_routing_result = server
            .enqueue_impl(
                None,
                super::EnqueueParams {
                    queue: None,
                    embedding: Some(vec![1.0, 0.0]),
                    ..enqueue_params("unused", json!("x"))
                },
            )
            .await;
        assert!(stdio_routing_result.is_err());

        // Tenant A's own enqueue, by contrast, resolves via the route it
        // just registered.
        let routed = server
            .enqueue_impl(
                Some(tenant_a),
                super::EnqueueParams {
                    queue: None,
                    embedding: Some(vec![1.0, 0.0]),
                    ..enqueue_params("unused", json!("x"))
                },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(routed.queue, "support");
    }

    #[tokio::test]
    async fn create_api_key_is_rejected_for_a_caller_already_authenticated_over_http() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let already_authenticated = TenantId::new("tenant-a").unwrap();

        let result = server
            .create_api_key_impl(
                Some(already_authenticated),
                super::CreateApiKeyParams { tenant_id: "tenant-b".to_string() },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn create_api_key_over_stdio_mints_a_credential_that_authenticates_as_its_tenant() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let minted = server
            .create_api_key_impl(
                None,
                super::CreateApiKeyParams { tenant_id: "tenant-a".to_string() },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(minted.tenant_id, "tenant-a");

        let resolved = server.api_keys.authenticate(&minted.api_key).await;
        assert_eq!(resolved, Some(TenantId::new("tenant-a").unwrap()));
    }

    #[tokio::test]
    async fn revoke_api_key_is_rejected_for_a_caller_already_authenticated_over_http() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let already_authenticated = TenantId::new("tenant-a").unwrap();

        let minted = server
            .create_api_key_impl(
                None,
                super::CreateApiKeyParams { tenant_id: "tenant-a".to_string() },
            )
            .await
            .unwrap()
            .0;

        let result = server
            .revoke_api_key_impl(
                Some(already_authenticated),
                super::RevokeApiKeyParams { api_key: minted.api_key },
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn revoke_api_key_over_stdio_makes_the_key_stop_authenticating() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let minted = server
            .create_api_key_impl(
                None,
                super::CreateApiKeyParams { tenant_id: "tenant-a".to_string() },
            )
            .await
            .unwrap()
            .0;

        let revoked = server
            .revoke_api_key_impl(
                None,
                super::RevokeApiKeyParams { api_key: minted.api_key.clone() },
            )
            .await
            .unwrap()
            .0;
        assert!(revoked.revoked);

        let resolved = server.api_keys.authenticate(&minted.api_key).await;
        assert_eq!(resolved, None);
    }

    /// Every `ConfigureTenantQuotaParams` field except `tenant_id`,
    /// defaulted to "unlimited" - same reasoning as `enqueue_params`.
    fn quota_params(tenant_id: &str) -> super::ConfigureTenantQuotaParams {
        super::ConfigureTenantQuotaParams {
            tenant_id: tenant_id.to_string(),
            max_queues: None,
            max_pending_messages: None,
            requests_per_window: None,
            window_seconds: None,
        }
    }

    #[tokio::test]
    async fn a_tenant_is_refused_a_queue_beyond_its_configured_limit() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant = TenantId::new("tenant-a").unwrap();

        server
            .configure_tenant_quota_impl(
                None,
                super::ConfigureTenantQuotaParams {
                    max_queues: Some(1),
                    ..quota_params("tenant-a")
                },
            )
            .await
            .unwrap();

        server
            .enqueue_impl(Some(tenant.clone()), enqueue_params("first", json!("a")))
            .await
            .unwrap();

        let second =
            server.enqueue_impl(Some(tenant.clone()), enqueue_params("second", json!("b"))).await;
        assert!(second.is_err(), "a second distinct queue should exceed the 1-queue limit");

        // The already-open first queue is unaffected - the limit only
        // ever gates *opening a new* queue.
        let again = server.enqueue_impl(Some(tenant), enqueue_params("first", json!("c"))).await;
        assert!(again.is_ok());
    }

    #[tokio::test]
    async fn a_tenant_is_refused_a_message_beyond_its_pending_limit() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant = TenantId::new("tenant-a").unwrap();

        server
            .configure_tenant_quota_impl(
                None,
                super::ConfigureTenantQuotaParams {
                    max_pending_messages: Some(1),
                    ..quota_params("tenant-a")
                },
            )
            .await
            .unwrap();

        server
            .enqueue_impl(Some(tenant.clone()), enqueue_params("orders", json!("first")))
            .await
            .unwrap();

        let denied = server
            .enqueue_impl(Some(tenant.clone()), enqueue_params("orders", json!("second")))
            .await;
        assert!(denied.is_err(), "a second pending message should exceed the 1-message limit");

        // Acking the first message frees a slot for another.
        let claimed = server
            .claim_impl(
                Some(tenant.clone()),
                super::ClaimParams { queue: "orders".to_string(), wait_ms: Some(50) },
            )
            .await
            .unwrap()
            .0;
        server
            .ack_impl(
                Some(tenant.clone()),
                super::AckParams {
                    queue: "orders".to_string(),
                    message_id: claimed.message_id.unwrap(),
                    lease_token: claimed.lease_token.unwrap(),
                },
            )
            .await
            .unwrap();

        let after_ack =
            server.enqueue_impl(Some(tenant), enqueue_params("orders", json!("third"))).await;
        assert!(after_ack.is_ok(), "acking the first message should free a pending slot");
    }

    #[tokio::test]
    async fn a_deduplicated_enqueue_never_counts_against_the_pending_limit() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant = TenantId::new("tenant-a").unwrap();

        server
            .configure_tenant_quota_impl(
                None,
                super::ConfigureTenantQuotaParams {
                    max_pending_messages: Some(1),
                    ..quota_params("tenant-a")
                },
            )
            .await
            .unwrap();

        let embedding = vec![1.0, 0.0, 0.0];
        server
            .enqueue_impl(
                Some(tenant.clone()),
                super::EnqueueParams {
                    embedding: Some(embedding.clone()),
                    ..enqueue_params("orders", json!("first"))
                },
            )
            .await
            .unwrap();

        // A near-duplicate of the first task collapses into it rather
        // than creating a new pending message, so it must not be refused
        // by a pending limit already fully used by the original.
        let collapsed = server
            .enqueue_impl(
                Some(tenant),
                super::EnqueueParams {
                    embedding: Some(embedding),
                    ..enqueue_params("orders", json!("near-duplicate"))
                },
            )
            .await
            .unwrap()
            .0;
        assert!(collapsed.deduplicated);
    }

    #[tokio::test]
    async fn a_tenant_is_throttled_once_its_request_rate_limit_is_exhausted() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant = TenantId::new("tenant-a").unwrap();

        server
            .configure_tenant_quota_impl(
                None,
                super::ConfigureTenantQuotaParams {
                    requests_per_window: Some(1),
                    window_seconds: Some(60),
                    ..quota_params("tenant-a")
                },
            )
            .await
            .unwrap();

        assert!(server.authorize_impl(Some(tenant.clone())).await.is_ok());
        let denied = server.authorize_impl(Some(tenant)).await;
        assert!(denied.is_err(), "a second request within the window should be throttled");
    }

    #[tokio::test]
    async fn the_trusted_stdio_connection_is_never_rate_limited() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        // No quota was ever configured for `None` - there's nothing to
        // configure it *as* - so every call succeeds regardless of how
        // many times it's made.
        for _ in 0..5 {
            assert_eq!(server.authorize_impl(None).await.unwrap(), None);
        }
    }

    #[tokio::test]
    async fn configure_tenant_quota_is_rejected_for_a_caller_already_authenticated_over_http() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let already_authenticated = TenantId::new("tenant-a").unwrap();

        let result = server
            .configure_tenant_quota_impl(Some(already_authenticated), quota_params("tenant-b"))
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tenant_quota_status_requires_a_tenant_id_over_stdio() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let result = server
            .tenant_quota_status_impl(None, super::TenantQuotaStatusParams { tenant_id: None })
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn tenant_quota_status_reports_live_usage_for_the_calling_tenant() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant = TenantId::new("tenant-a").unwrap();

        server
            .configure_tenant_quota_impl(
                None,
                super::ConfigureTenantQuotaParams {
                    max_queues: Some(5),
                    max_pending_messages: Some(100),
                    requests_per_window: Some(50),
                    window_seconds: Some(30),
                    ..quota_params("tenant-a")
                },
            )
            .await
            .unwrap();

        server
            .enqueue_impl(Some(tenant.clone()), enqueue_params("orders", json!("a")))
            .await
            .unwrap();
        server
            .enqueue_impl(Some(tenant.clone()), enqueue_params("orders", json!("b")))
            .await
            .unwrap();

        let status = server
            .tenant_quota_status_impl(
                Some(tenant.clone()),
                super::TenantQuotaStatusParams { tenant_id: None },
            )
            .await
            .unwrap()
            .0;

        assert_eq!(status.tenant_id, "tenant-a");
        assert_eq!(status.max_queues, Some(5));
        assert_eq!(status.queues_open, 1);
        assert_eq!(status.max_pending_messages, Some(100));
        assert_eq!(status.pending_messages, 2);
        assert_eq!(status.requests_per_window, Some(50));
        assert_eq!(status.window_seconds, 30);

        // A second tenant, entirely unconfigured, is unaffected.
        let other_status = server
            .tenant_quota_status_impl(
                Some(TenantId::new("tenant-b").unwrap()),
                super::TenantQuotaStatusParams { tenant_id: None },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(other_status.max_queues, None);
        assert_eq!(other_status.queues_open, 0);
        assert_eq!(other_status.pending_messages, 0);
    }

    #[tokio::test]
    async fn tenant_quota_status_over_stdio_reports_on_the_named_tenant() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant = TenantId::new("tenant-a").unwrap();

        server.enqueue_impl(Some(tenant), enqueue_params("orders", json!("a"))).await.unwrap();

        let status = server
            .tenant_quota_status_impl(
                None,
                super::TenantQuotaStatusParams { tenant_id: Some("tenant-a".to_string()) },
            )
            .await
            .unwrap()
            .0;
        assert_eq!(status.tenant_id, "tenant-a");
        assert_eq!(status.queues_open, 1);
        assert_eq!(status.pending_messages, 1);
    }

    #[tokio::test]
    async fn enqueue_records_the_queue_depth_gauge() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant = TenantId::new("tenant-a").unwrap();

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);

        server.enqueue_impl(Some(tenant), enqueue_params("orders", json!("first"))).await.unwrap();

        let snapshot = snapshotter.snapshot().into_vec();
        let depth = metric_value(
            &snapshot,
            "qaas_queue_depth",
            &[("queue", "orders"), ("tenant", "tenant-a")],
        );
        assert_eq!(depth, Some(DebugValue::Gauge(1.0.into())));
    }

    #[tokio::test]
    async fn ack_records_a_lower_queue_depth_and_a_message_latency_sample() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant = TenantId::new("tenant-a").unwrap();

        server
            .enqueue_impl(Some(tenant.clone()), enqueue_params("orders", json!("first")))
            .await
            .unwrap();
        let claimed = server
            .claim_impl(
                Some(tenant.clone()),
                super::ClaimParams { queue: "orders".to_string(), wait_ms: Some(50) },
            )
            .await
            .unwrap()
            .0;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);

        server
            .ack_impl(
                Some(tenant.clone()),
                super::AckParams {
                    queue: "orders".to_string(),
                    message_id: claimed.message_id.unwrap(),
                    lease_token: claimed.lease_token.unwrap(),
                },
            )
            .await
            .unwrap();

        let snapshot = snapshotter.snapshot().into_vec();
        let depth = metric_value(
            &snapshot,
            "qaas_queue_depth",
            &[("queue", "orders"), ("tenant", "tenant-a")],
        );
        assert_eq!(depth, Some(DebugValue::Gauge(0.0.into())));

        let latency = metric_value(
            &snapshot,
            "qaas_message_latency_seconds",
            &[("queue", "orders"), ("tenant", "tenant-a")],
        );
        let Some(DebugValue::Histogram(samples)) = latency else {
            panic!("expected a recorded latency histogram sample, got {latency:?}");
        };
        assert_eq!(samples.len(), 1);
        assert!(
            samples[0].0 >= 0.0 && samples[0].0 < 5.0,
            "latency sample {:?} should be small and non-negative, just measured",
            samples[0]
        );
    }

    #[tokio::test]
    async fn ack_never_records_metrics_for_a_stale_lease() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;
        let tenant = TenantId::new("tenant-a").unwrap();

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);

        server
            .ack_impl(
                Some(tenant),
                super::AckParams {
                    queue: "orders".to_string(),
                    message_id: qaas_types::MessageId::new().to_string(),
                    lease_token: 0,
                },
            )
            .await
            .unwrap();

        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(
            metric_value(
                &snapshot,
                "qaas_message_latency_seconds",
                &[("queue", "orders"), ("tenant", "tenant-a")]
            ),
            None,
            "acking a lease that was never real should record nothing"
        );
    }

    #[tokio::test]
    async fn different_tenants_record_metrics_under_different_labels() {
        let dir = tempdir().unwrap();
        let server = server(&dir).await;

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        let _guard = metrics::set_default_local_recorder(&recorder);

        server
            .enqueue_impl(
                Some(TenantId::new("tenant-a").unwrap()),
                enqueue_params("orders", json!("a")),
            )
            .await
            .unwrap();
        server
            .enqueue_impl(
                Some(TenantId::new("tenant-b").unwrap()),
                enqueue_params("orders", json!("b1")),
            )
            .await
            .unwrap();
        server
            .enqueue_impl(
                Some(TenantId::new("tenant-b").unwrap()),
                enqueue_params("orders", json!("b2")),
            )
            .await
            .unwrap();

        let snapshot = snapshotter.snapshot().into_vec();
        assert_eq!(
            metric_value(
                &snapshot,
                "qaas_queue_depth",
                &[("queue", "orders"), ("tenant", "tenant-a")]
            ),
            Some(DebugValue::Gauge(1.0.into())),
        );
        assert_eq!(
            metric_value(
                &snapshot,
                "qaas_queue_depth",
                &[("queue", "orders"), ("tenant", "tenant-b")]
            ),
            Some(DebugValue::Gauge(2.0.into())),
        );
    }
}
