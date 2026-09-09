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
//! It still waits on the server's own run loop rather than exiting
//! immediately, same as the scaffold this replaces — `RunningService::waiting`
//! resolves when the client disconnects (stdin closes) or the process is
//! asked to shut down, which is the natural lifetime for a subprocess an
//! agent runtime spawns and eventually tears down itself.

mod mcp;

use rmcp::ServiceExt;

/// Where queue WAL files live, overridable via `QAAS_DATA_DIR` — the
/// default keeps a fresh checkout runnable with no configuration, the
/// override is what `docker compose` (`feature/dev-environment-docker`)
/// and any real deployment actually need.
const DEFAULT_DATA_DIR: &str = "./data/queues";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let data_dir = std::env::var("QAAS_DATA_DIR").unwrap_or_else(|_| DEFAULT_DATA_DIR.to_string());
    if let Err(error) = tokio::fs::create_dir_all(&data_dir).await {
        tracing::error!(%error, %data_dir, "failed to create queue data directory");
        return;
    }

    tracing::info!(%data_dir, "starting QaaS MCP server over stdio");
    let server = mcp::QaasMcpServer::new(data_dir);

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
