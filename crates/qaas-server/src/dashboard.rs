//! Read-only operator dashboard: queue depth, DLQ counts, token/cost
//! spend, and a tenant leaderboard — `Plan.md`'s "real-time web
//! dashboard ... analogous to the rate limiter's analytics API," making
//! agent workload behavior visible without grepping logs.
//!
//! # Its own listener, unauthenticated, same reasoning as `/metrics`
//!
//! This data is the same shape as `feature/tracing-metrics`'s Prometheus
//! metrics — cross-tenant, operator-facing aggregates, not something a
//! single tenant's API key was ever meant to unlock, and there's no
//! "authenticate as this tenant" concept for a human operator loading a
//! dashboard page to offer anyway. `crate::telemetry`'s own module docs
//! already settled this exact question for `/metrics`; this module just
//! applies the same answer rather than re-litigating it. It can't
//! literally share that listener, though — `metrics-exporter-prometheus`
//! owns its own internal HTTP server with no routes of its own to add
//! to — so this is a fourth listener (`QAAS_DASHBOARD_ADDR`), loopback
//! by default, matching every other operator-facing address this
//! project binds.
//!
//! # Real-time via polling, not `SSE` or a `WebSocket`
//!
//! The embedded page (`dashboard.html`, served as-is at `/`) polls
//! [`snapshot`] (`GET /api/snapshot`) every two seconds and redraws
//! itself — the same "prefer the simpler mechanism the transport
//! already supports over inventing a push protocol" call
//! `stream_partial_results` made in `feature/streaming-delivery`,
//! applied here to a browser instead of an MCP tool call. A dashboard
//! refreshing every couple of seconds reads as "real-time" to an
//! operator the same way Grafana's own default scrape-and-redraw
//! cadence does.
//!
//! # No server-side history
//!
//! [`crate::mcp::DashboardSnapshot`] is assembled fresh on every
//! request from this process's *current* live state — nothing here
//! samples on a timer or retains a time series of its own. The trend
//! sparklines a viewer sees are computed entirely client-side, from
//! whatever snapshots that one browser tab has personally polled since
//! it was opened; a second viewer, or the same one after a reload, starts
//! its own history from empty. That's a deliberate scope cut for a first
//! dashboard, not an oversight: real persistent history is exactly what
//! the Prometheus metrics `feature/tracing-metrics` already exports are
//! for, via whatever long-term storage a real Prometheus server
//! scraping this process provides — this module doesn't need to
//! duplicate that.

use std::net::SocketAddr;

use axum::Json;
use axum::extract::State;
use axum::response::Html;
use axum::routing::get;

use crate::mcp::{DashboardSnapshot, QaasMcpServer};

/// The dashboard's own HTML/CSS/JS, embedded at compile time — no
/// external assets, no CDN, no build step. Works fully offline, which
/// matters for a tool meant to run alongside a local or air-gapped
/// deployment, not just one with internet access to fetch a JS
/// framework from.
const DASHBOARD_HTML: &str = include_str!("dashboard.html");

/// Binds and serves the dashboard on `addr` — the embedded page at `/`,
/// its JSON data source at `/api/snapshot`. Runs until the listener
/// itself fails, matching every other listener `main.rs` starts.
///
/// # Errors
///
/// Returns an error if `addr` can't be bound.
pub async fn serve(server: QaasMcpServer, addr: SocketAddr) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "starting operator dashboard");
    axum::serve(listener, router(server)).await
}

/// Builds the dashboard's routes without binding a socket — split out
/// from [`serve`] so tests can drive it directly (via
/// `tower::ServiceExt::oneshot`) without a real listener.
fn router(server: QaasMcpServer) -> axum::Router {
    axum::Router::new()
        .route("/", get(index))
        .route("/api/snapshot", get(snapshot))
        .with_state(server)
}

/// Serves the embedded dashboard page.
async fn index() -> Html<&'static str> {
    Html(DASHBOARD_HTML)
}

/// The dashboard page's own data source — the same
/// [`QaasMcpServer::dashboard_snapshot`] a test or any other future
/// caller could use directly; this handler is a thin JSON wrapper
/// around it, nothing more.
async fn snapshot(State(server): State<QaasMcpServer>) -> Json<DashboardSnapshot> {
    Json(server.dashboard_snapshot().await)
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    use super::{DashboardSnapshot, router};
    use crate::mcp::QaasMcpServer;

    // Deliberately exercises only this module's own job — that the
    // router serves the embedded page and correctly wraps
    // QaasMcpServer::dashboard_snapshot as JSON at the right paths with
    // the right content type. Whether a snapshot's *contents* are
    // correct for real queue/tenant state is mcp.rs's own
    // dashboard_snapshot_* tests' job, thoroughly covered there against
    // a server that isn't reachable from this module (EnqueueParams and
    // enqueue_impl are private to mcp.rs) — no need to duplicate that
    // coverage here against an empty server standing in for "any state
    // at all."
    #[tokio::test]
    async fn index_serves_the_embedded_page() {
        let dir = tempfile::tempdir().unwrap();
        let server = QaasMcpServer::new(dir.path()).await.unwrap();
        let app = router(server);

        let response =
            app.oneshot(Request::builder().uri("/").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let page = String::from_utf8(body.to_vec()).unwrap();
        assert!(page.contains("<title>QaaS Dashboard</title>"));
        assert!(page.contains("/api/snapshot"), "the page must actually poll its own API");
    }

    #[tokio::test]
    async fn snapshot_endpoint_serves_json_matching_dashboard_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let server = QaasMcpServer::new(dir.path()).await.unwrap();
        let app = router(server);

        let response = app
            .oneshot(Request::builder().uri("/api/snapshot").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(axum::http::header::CONTENT_TYPE).unwrap(),
            "application/json"
        );

        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let snapshot: DashboardSnapshot = serde_json::from_slice(&body).unwrap();
        assert!(snapshot.queues.is_empty());
        assert!(snapshot.tenants.is_empty());
    }
}
