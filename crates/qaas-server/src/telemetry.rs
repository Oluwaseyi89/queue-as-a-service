//! Observability bootstrap: the `tracing` subscriber (logs, and —
//! opt-in — `OpenTelemetry` distributed tracing) and the Prometheus
//! metrics exporter.
//!
//! `feature/tracing-metrics` gives operators "the same P95/P99
//! visibility the rate limiter had" (`Plan.md`'s own wording), broken
//! down per-queue and per-tenant — the closest thing this project has to
//! "per-agent," since there's no finer-grained caller identity than
//! [`TenantId`](qaas_core::TenantId) (see `feature/api-auth`) for an
//! HTTP-authenticated caller, and the trusted stdio connection has none
//! at all. Three signals, deliberately kept separate rather than merged
//! into one system:
//!
//! - **Logs and traces** (this module's [`init_tracing`]) travel through
//!   the `tracing` facade every crate in this workspace already uses —
//!   [`qaas_core::consumer_group`] gained `#[tracing::instrument]` on its
//!   key methods this branch, and [`crate::mcp`]'s tool methods gain it
//!   here, so every MCP tool call becomes a real span, nested under
//!   whatever `rmcp` itself already emits. Those spans go to stderr as
//!   before (unchanged from `feature/api-auth`'s own fix for why not
//!   stdout); they *also* export as `OpenTelemetry` spans to an OTLP
//!   collector if `QAAS_OTEL_ENDPOINT` is set — see that constant's own
//!   docs for why this is opt-in rather than defaulting to some
//!   well-known local address.
//! - **Metrics** ([`init_metrics`]) are a separate signal with a
//!   separate transport: a small set of named Prometheus gauges/
//!   histograms (queue depth, oldest-pending-message age, and
//!   end-to-end message latency — see [`crate::mcp`]'s own module docs
//!   for exactly what each measures and why), recorded via the `metrics`
//!   facade at the handful of call sites in `mcp.rs` that actually
//!   change a queue's state, and served from their own dedicated
//!   listener.
//!
//! # Why metrics get their own listener, not a route on the MCP one
//!
//! Confirmed with the project owner rather than assumed: metrics here
//! are inherently cross-tenant, operator-facing data — a queue's depth
//! broken down per tenant is exactly the kind of aggregate view a single
//! tenant's API key was never meant to unlock, and Prometheus itself has
//! no concept of "authenticate as this tenant" to offer at scrape time
//! anyway. Rather than carve an unauthenticated exception into the same
//! router `feature/api-auth` built specifically to require a tenant
//! credential on everything reaching it — which would mean a
//! `QAAS_HTTP_ADDR` widened for real multi-tenant traffic also
//! widening `/metrics`' own audience — this listens on its own address
//! (`QAAS_METRICS_ADDR`), off by default from the tenant-facing surface
//! entirely, matching how Prometheus exporters are conventionally
//! deployed in the first place: their own port, scraped from inside a
//! trusted network, never exposed alongside the service's real traffic.

use std::net::SocketAddr;

use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::WithExportConfig;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// If set (and non-empty), `OpenTelemetry` spans are exported via OTLP/
/// gRPC to this address (e.g. `http://localhost:4317`, the standard
/// local-collector default). Unset means no exporter is built at all —
/// tracing keeps working exactly as it already did (stderr logs, spans
/// that just don't leave this process) — rather than defaulting to a
/// well-known collector address that, for anyone not also running one,
/// would mean every span export silently fails in the background. The
/// same "absent config is a feature being off, not a startup failure or
/// a silently broken default" stance `http_auth::AuthState::new` already
/// takes for `QAAS_JWT_SECRET`.
const OTEL_ENDPOINT_ENV: &str = "QAAS_OTEL_ENDPOINT";

/// Where the Prometheus scrape endpoint listens, overridable via
/// `QAAS_METRICS_ADDR`. Loopback-only by default, same reasoning as
/// `QAAS_HTTP_ADDR` in `main.rs` — and a different default *port*
/// (`9090`, Prometheus's own conventional exporter port) specifically so
/// running both listeners with only `QAAS_HTTP_ADDR` overridden doesn't
/// silently collide.
pub const DEFAULT_METRICS_ADDR: &str = "127.0.0.1:9090";

/// Initializes the global `tracing` subscriber: an `EnvFilter` (`RUST_LOG`,
/// defaulting to `info` when unset — this branch is what first wires
/// `RUST_LOG` up at all; every earlier branch's bare
/// `tracing_subscriber::fmt().init()` silently ignored it in favor of a
/// fixed `LevelFilter::INFO`, found while restructuring this exact call
/// to add the layers below) feeding both a stderr-writing fmt layer (the
/// `feature/api-auth` fix for why not stdout still applies) and,
/// conditionally, an `OpenTelemetry` layer — see [`OTEL_ENDPOINT_ENV`].
///
/// Must be called exactly once, before anything else in this process
/// logs or opens a span — every earlier branch called
/// `tracing_subscriber::fmt().init()` as `main`'s first line for the
/// same reason.
pub fn init_tracing() {
    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);

    // Built inline, not as a separate helper returning `Option<OpenTelemetryLayer<...>>`:
    // that type is generic over the exact subscriber it's layered onto
    // (`S` in `OpenTelemetryLayer<S, T>`), which at the `.with(otel_layer)`
    // call below is the concrete `Layered<FmtLayer, Layered<EnvFilter,
    // Registry>>` stack built so far, not `Registry` alone — a type this
    // function only learns by being written at the actual call site and
    // left to inference, not one a standalone helper could name up front.
    let otel_layer = otel_endpoint().and_then(|endpoint| {
        let exporter = match opentelemetry_otlp::SpanExporter::builder()
            .with_tonic()
            .with_endpoint(&endpoint)
            .build()
        {
            Ok(exporter) => exporter,
            Err(error) => {
                eprintln!(
                    "{OTEL_ENDPOINT_ENV} set to {endpoint:?} but the OTLP exporter failed to \
                     build: {error} - continuing without OpenTelemetry export"
                );
                return None;
            }
        };

        let resource =
            opentelemetry_sdk::Resource::builder().with_service_name("qaas-server").build();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_resource(resource)
            .with_batch_exporter(exporter)
            .build();
        let tracer = provider.tracer("qaas-server");

        Some(tracing_opentelemetry::layer().with_tracer(tracer))
    });

    tracing_subscriber::registry().with(env_filter).with(fmt_layer).with(otel_layer).init();
}

/// [`OTEL_ENDPOINT_ENV`] if set to a non-empty value, `None` otherwise.
fn otel_endpoint() -> Option<String> {
    std::env::var(OTEL_ENDPOINT_ENV).ok().filter(|value| !value.is_empty())
}

/// Installs the global `metrics` recorder and starts the Prometheus
/// scrape listener on `addr` — see the module docs for why this is a
/// separate listener from the MCP HTTP transport, not a route on it.
///
/// Also registers human-readable descriptions for every metric
/// `crate::mcp` records, up front, so a scrape includes `# HELP`/`# TYPE`
/// lines even before the first data point exists for a given
/// queue/tenant label combination.
///
/// # Errors
///
/// Returns an error if `addr` can't be bound, or if a global recorder is
/// somehow already installed (this function is meant to be called
/// exactly once, from `main`).
pub fn init_metrics(addr: SocketAddr) -> Result<(), metrics_exporter_prometheus::BuildError> {
    metrics::describe_gauge!(
        "qaas_queue_depth",
        "Messages not yet acknowledged on a queue (pending, leased, or delayed)."
    );
    metrics::describe_gauge!(
        "qaas_queue_oldest_pending_age_seconds",
        "How long the oldest currently-claimable message on a queue has been waiting."
    );
    metrics::describe_histogram!(
        "qaas_message_latency_seconds",
        "End-to-end time from a message's enqueue to its successful ack."
    );

    metrics_exporter_prometheus::PrometheusBuilder::new().with_http_listener(addr).install()
}
