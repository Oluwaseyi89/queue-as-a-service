//! Entry point for the QaaS broker process.
//!
//! Right now this only brings up structured logging and confirms the
//! async runtime starts cleanly — it doesn't bind a socket or talk to
//! `qaas-core` yet. The point of standing up a runnable binary this early
//! is that CI (`feature/cicd-pipeline`) has something real to build, and
//! every later branch that adds a listener (gRPC, HTTP, MCP) is extending
//! an existing entry point instead of creating the first one.

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    tracing::info!("qaas-server scaffold starting — no network listeners wired up yet");
}
