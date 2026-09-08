//! Entry point for the QaaS broker process.
//!
//! Right now this only brings up structured logging and confirms the
//! async runtime starts cleanly — it doesn't bind a socket or talk to
//! `qaas-core` yet. The point of standing up a runnable binary this early
//! is that CI (`feature/cicd-pipeline`) has something real to build, and
//! every later branch that adds a listener (gRPC, HTTP, MCP) is extending
//! an existing entry point instead of creating the first one.
//!
//! It does wait on a shutdown signal before exiting, rather than running
//! `main` to completion immediately. That's not queue functionality — a
//! process that exits the instant it starts isn't useful inside
//! `docker compose up` (`feature/dev-environment-docker`), where the
//! whole point is a long-running container contributors can leave up
//! alongside Prometheus and Grafana. The actual listener work is still
//! deferred to the branches that add one.

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("qaas-server scaffold starting — no network listeners wired up yet");

    match tokio::signal::ctrl_c().await {
        Ok(()) => tracing::info!("received shutdown signal, exiting"),
        Err(error) => tracing::error!(%error, "failed to listen for shutdown signal"),
    }
}
