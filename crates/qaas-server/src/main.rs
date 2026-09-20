//! Entry point for the QaaS broker process.
//!
//! `feature/mcp-server-interface` is the first branch to actually give
//! this binary something to serve: it now starts [`mcp::QaasMcpServer`]
//! over stdio (see that module's docs for why stdio, why no auth yet,
//! and why the tool surface is enqueue/claim/ack/nack rather than the
//! three verbs `Plan.md`'s line names). Every earlier branch's "no
//! network listeners wired up yet" is no longer quite true — MCP over
//! stdio isn't a listening socket, but it is a real protocol a real
//! client can drive this process through.
//!
//! It still waits on the stdio server's own run loop rather than exiting
//! immediately, same as the scaffold this replaces — `RunningService::waiting`
//! resolves when the client disconnects (stdin closes) or the process is
//! asked to shut down, which is the natural lifetime for a subprocess an
//! agent runtime spawns and eventually tears down itself.
//!
//! # `feature/api-auth`: a second, authenticated transport
//!
//! No branch in `Plan.md` ever adds a network listener explicitly, but
//! `feature/api-auth`'s whole point — a real trust boundary for API keys
//! and JWTs to defend — needs one to exist. This branch adds it:
//! [`serve_http`] stands up `rmcp`'s own streamable-HTTP transport
//! alongside stdio, with [`http_auth::require_tenant`] guarding every
//! request. Unlike stdio, this listener defaults to loopback-only
//! (`QAAS_HTTP_ADDR`, `127.0.0.1:8080` unless overridden) — a real
//! multi-tenant deployment has to opt into being reachable from beyond
//! this host, not get it by default from running the binary. The stdio
//! transport itself stays exactly as unauthenticated as it always was:
//! it's a local subprocess connection, not a network boundary, and
//! nothing about adding a second, network-facing transport changes what
//! the first one already was.
//!
//! # `feature/tracing-metrics`: a third listener, for operators only
//!
//! [`telemetry::init_metrics`] starts a Prometheus scrape endpoint on
//! its own address (`QAAS_METRICS_ADDR`) — deliberately not a route on
//! the same listener [`serve_http`] already binds, and not behind
//! [`http_auth::require_tenant`] either. See `telemetry`'s own module
//! docs for why: metrics here are cross-tenant, operator-facing data,
//! not something a tenant's own API key was ever meant to unlock.
//! [`telemetry::init_tracing`] replaces the bare
//! `tracing_subscriber::fmt().init()` this file used to call directly —
//! same stderr-writing behavior, now also wired to export spans via
//! `OpenTelemetry` if `QAAS_OTEL_ENDPOINT` is set.

mod http_auth;
mod mcp;
mod telemetry;

use std::sync::Arc;

use rmcp::ServiceExt;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};

/// Where queue WAL files live, overridable via `QAAS_DATA_DIR` — the
/// default keeps a fresh checkout runnable with no configuration, the
/// override is what `docker compose` (`feature/dev-environment-docker`)
/// and any real deployment actually need.
const DEFAULT_DATA_DIR: &str = "./data/queues";

/// Where the streamable-HTTP MCP transport listens, overridable via
/// `QAAS_HTTP_ADDR`. Loopback-only by default — see the module docs on
/// why that's a deliberate default rather than `0.0.0.0`.
const DEFAULT_HTTP_ADDR: &str = "127.0.0.1:8080";

#[tokio::main]
async fn main() {
    telemetry::init_tracing();

    let data_dir = std::env::var("QAAS_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
    if let Err(error) = tokio::fs::create_dir_all(&data_dir).await {
        tracing::error!(%error, %data_dir, "failed to create queue data directory");
        return;
    }

    let server = match mcp::QaasMcpServer::new(data_dir).await {
        Ok(server) => server,
        Err(error) => {
            tracing::error!(%error, "failed to open queue data / API-key store");
            return;
        }
    };

    let metrics_addr = std::env::var("QAAS_METRICS_ADDR")
        .unwrap_or_else(|_| telemetry::DEFAULT_METRICS_ADDR.to_string());
    match metrics_addr.parse() {
        Ok(addr) => {
            if let Err(error) = telemetry::init_metrics(addr) {
                tracing::error!(%error, %metrics_addr, "failed to start Prometheus metrics listener");
            } else {
                tracing::info!(%metrics_addr, "starting Prometheus metrics listener");
            }
        }
        Err(error) => {
            tracing::error!(%error, %metrics_addr, "QAAS_METRICS_ADDR is not a valid address");
        }
    }

    let http_addr =
        std::env::var("QAAS_HTTP_ADDR").unwrap_or_else(|_| DEFAULT_HTTP_ADDR.to_string());
    let http_server = server.clone();
    tokio::spawn(async move {
        if let Err(error) = serve_http(http_server, &http_addr).await {
            tracing::error!(%error, %http_addr, "streamable-HTTP MCP server failed");
        }
    });

    tracing::info!("starting QaaS MCP server over stdio");
    let running = match server.serve(rmcp::transport::stdio()).await {
        Ok(running) => running,
        Err(error) => {
            tracing::error!(%error, "failed to start MCP server");
            return;
        }
    };

    match running.waiting().await {
        Ok(reason) => tracing::info!(?reason, "MCP server stopped"),
        Err(error) => tracing::error!(%error, "MCP server task panicked"),
    }
}

/// Binds and serves `rmcp`'s streamable-HTTP MCP transport on `addr`,
/// with [`http_auth::require_tenant`] wrapped around every request via
/// an axum middleware layer — a request that doesn't resolve to a tenant
/// never reaches `service`, and so never reaches a single MCP tool call.
/// `LocalSessionManager` (an in-memory session store, `rmcp`'s own
/// default) is enough here: this branch adds authentication, not
/// clustering, so there's no reason yet for MCP session state to
/// outlive this one process — see `qaas-core::raft`'s own docs for where
/// actual cross-node state eventually belongs.
///
/// Runs until the listener itself fails; there's no separate shutdown
/// signal in this branch, matching how the stdio transport's own
/// lifetime is just "the process is still running."
async fn serve_http(server: mcp::QaasMcpServer, addr: &str) -> std::io::Result<()> {
    let auth_state = http_auth::AuthState::new(Arc::clone(&server.api_keys));
    let service: StreamableHttpService<mcp::QaasMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(server.clone()),
            Arc::new(LocalSessionManager::default()),
            StreamableHttpServerConfig::default(),
        );
    let router = axum::Router::new()
        .nest_service("/mcp", service)
        .layer(axum::middleware::from_fn_with_state(auth_state, http_auth::require_tenant));

    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "starting QaaS MCP server over streamable HTTP");
    axum::serve(listener, router).await
}
