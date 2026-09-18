//! Authentication for the streamable-HTTP MCP transport.
//!
//! `feature/mcp-server-interface` deliberately left this project with
//! exactly one transport — stdio — and no listening socket, because
//! standing up a network-reachable one with nothing guarding it would
//! have been "a real, not hypothetical, security hole" (see `mcp`'s own
//! module docs). `feature/api-auth` is what removes that caveat: this
//! module is the one piece of the server allowed to say yes or no to an
//! HTTP request before it ever becomes an MCP tool call.
//!
//! # Two credential kinds, one outcome
//!
//! A request authenticates with an `Authorization: Bearer <credential>`
//! header, tried as an API key first (a hash lookup against
//! [`qaas_core::ApiKeyStore`], durable, minted via the `create_api_key`
//! MCP tool) and, only if that fails, as an HS256 JWT (verified against
//! [`JWT_SECRET_ENV`], with a `tenant_id` claim naming the tenant).
//! Either way the *only* thing this module ever produces is a
//! [`TenantId`] — nothing downstream of this middleware can tell which
//! credential kind actually authenticated a given request, because
//! nothing downstream needs to. JWT support is opt-in at the deployment
//! level: a server started with no `QAAS_JWT_SECRET` set accepts API
//! keys only, rather than refusing to start or (far worse) silently
//! trusting a token it can't actually verify.
//!
//! # Where the resolved tenant goes
//!
//! [`require_tenant`] inserts the resolved [`TenantId`] into the
//! request's own extensions before calling `next` — exactly where
//! `rmcp`'s `StreamableHttpService` looks when it later splits the
//! request into [`http::request::Parts`] and hands those to a tool call's
//! `RequestContext` (see that type's own doc-comment example, and
//! `mcp::tenant_from`, which reads it back out on the other side). A
//! request that never reaches a tenant never reaches MCP dispatch at
//! all — this middleware returns `401 Unauthorized` itself rather than
//! letting an unauthenticated call through to be rejected by some later
//! layer, which is the actual trust boundary `feature/api-auth` exists
//! to build, not just a convenience for tool code.
//!
//! # Why this isn't in `qaas-core`
//!
//! [`qaas_core::auth`] owns identity (`TenantId`) and the durable
//! API-key credential, because both are meaningful with no network
//! involved at all. Verifying a JWT's signature and reading an HTTP
//! header are neither: JWT verification needs real cryptography this
//! crate already pulls in for other reasons (`qaas-core` deliberately
//! doesn't), and an `Authorization` header only exists because this is
//! the one crate in the workspace allowed to speak HTTP in the first
//! place (see `CLAUDE.md`'s workspace-boundary table).

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use qaas_core::{ApiKeyStore, TenantId};
use serde::Deserialize;

/// Environment variable holding the HS256 secret used to verify JWTs.
/// Unset (or empty) disables JWT verification entirely — see the module
/// docs on why that's a deliberate opt-in rather than a startup failure.
const JWT_SECRET_ENV: &str = "QAAS_JWT_SECRET";

/// The one claim this project actually reads out of a JWT. `exp` isn't
/// listed here — `jsonwebtoken` validates it against the token's raw
/// claims independently of whatever struct the caller deserializes into
/// (verified against its own source: `decode`'s `validate` call runs
/// against a separate `ClaimsForValidation` value, not `T`), and
/// [`Validation::new`] already requires it be present by default. Adding
/// a field here would only duplicate a check that already happens.
#[derive(Debug, Deserialize)]
struct Claims {
    /// Which tenant this token authenticates as. Not a registered JWT
    /// claim (RFC 7519 defines none for multi-tenancy) — this project's
    /// own.
    tenant_id: String,
}

/// Shared state [`require_tenant`] needs on every request. Cheap to
/// clone: the API-key store is already `Arc`'d by `mcp::QaasMcpServer`
/// itself, and the JWT key (if any) is `Arc`'d here for the same reason.
#[derive(Clone)]
pub struct AuthState {
    api_keys: Arc<ApiKeyStore>,
    jwt_key: Option<Arc<DecodingKey>>,
}

impl AuthState {
    /// Builds the middleware state from `api_keys` and whatever
    /// [`JWT_SECRET_ENV`] currently holds in the process environment.
    #[must_use]
    pub fn new(api_keys: Arc<ApiKeyStore>) -> Self {
        let jwt_key = std::env::var(JWT_SECRET_ENV)
            .ok()
            .filter(|secret| !secret.is_empty())
            .map(|secret| Arc::new(DecodingKey::from_secret(secret.as_bytes())));
        Self { api_keys, jwt_key }
    }
}

/// Axum middleware guarding every route it's layered onto: requires a
/// bearer credential that resolves to a tenant, or responds
/// `401 Unauthorized` itself without ever calling `next`. See the module
/// docs for the full resolution order and where the resolved tenant ends
/// up.
pub async fn require_tenant(
    State(state): State<AuthState>,
    mut request: Request,
    next: Next,
) -> Response {
    let Some(credential) = bearer_credential(&request) else {
        return unauthorized("missing or malformed Authorization header");
    };

    let tenant = match state.api_keys.authenticate(&credential).await {
        Some(tenant) => Some(tenant),
        None => state.jwt_key.as_deref().and_then(|key| verify_jwt(&credential, key)),
    };

    let Some(tenant) = tenant else {
        return unauthorized("credential did not resolve to a known tenant");
    };

    request.extensions_mut().insert(tenant);
    next.run(request).await
}

/// Extracts the bearer credential from an `Authorization: Bearer <value>`
/// header. `None` if the header is missing, isn't valid UTF-8, or
/// doesn't use the `Bearer` scheme — all three are simply "no usable
/// credential," not distinguished any further (see [`unauthorized`]'s
/// own docs on why).
fn bearer_credential(request: &Request) -> Option<String> {
    let value = request.headers().get(header::AUTHORIZATION)?.to_str().ok()?;
    value.strip_prefix("Bearer ").map(str::to_string)
}

/// Verifies `token`'s HS256 signature and required `exp` claim against
/// `key`, returning the tenant its `tenant_id` claim names.
///
/// `None` for any failure — bad signature, expired, malformed, or a
/// `tenant_id` that isn't itself a valid [`TenantId`] — deliberately
/// collapsed into one outcome rather than distinguished, same as an
/// unknown API key: this boundary tells a caller "not authenticated,"
/// never *why*, so a probing attacker never learns whether they had the
/// right secret, an expired token, or a malformed tenant name.
fn verify_jwt(token: &str, key: &DecodingKey) -> Option<TenantId> {
    let claims =
        jsonwebtoken::decode::<Claims>(token, key, &Validation::new(Algorithm::HS256)).ok()?.claims;
    TenantId::new(claims.tenant_id).ok()
}

/// Builds a `401 Unauthorized` response with `reason` as a plain-text
/// body — deliberately plain text, not an MCP-shaped JSON-RPC error:
/// this rejection happens before a request is ever recognized as an MCP
/// call at all, at the HTTP layer in front of it.
fn unauthorized(reason: &'static str) -> Response {
    (StatusCode::UNAUTHORIZED, reason).into_response()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use axum::routing::get;
    use jsonwebtoken::{EncodingKey, Header};
    use qaas_core::ApiKeyStore;
    use tower::ServiceExt;

    use super::{AuthState, Claims, TenantId, bearer_credential, require_tenant, verify_jwt};

    /// A terminal handler standing in for the MCP service `require_tenant`
    /// actually guards in production: echoes back the [`TenantId`]
    /// `require_tenant` resolved and inserted, so a test can assert not
    /// just the status code but *which* tenant a credential resolved to.
    async fn echo_tenant(axum::Extension(tenant): axum::Extension<TenantId>) -> String {
        tenant.as_str().to_string()
    }

    fn router_with(state: AuthState) -> Router {
        Router::new()
            .route("/probe", get(echo_tenant))
            .layer(axum::middleware::from_fn_with_state(state, require_tenant))
    }

    async fn open_store() -> (tempfile::TempDir, Arc<ApiKeyStore>) {
        let dir = tempfile::tempdir().unwrap();
        let store = ApiKeyStore::open(dir.path().join("api_keys.log")).await.unwrap();
        (dir, Arc::new(store))
    }

    fn request(auth_header: Option<&str>) -> Request<Body> {
        let mut builder = Request::builder().uri("/probe");
        if let Some(value) = auth_header {
            builder = builder.header(header::AUTHORIZATION, value);
        }
        builder.body(Body::empty()).unwrap()
    }

    /// `AuthState::new` reads `QAAS_JWT_SECRET` from the real process
    /// environment, which every test binary shares — and, as of the
    /// toolchain this project pins (`rust-toolchain.toml`, 1.88+),
    /// mutating it needs `unsafe`, which this workspace denies outright
    /// (`unsafe_code = "deny"` — see `CLAUDE.md`/root `Cargo.toml`). So
    /// JWT verification is tested directly against [`verify_jwt`], which
    /// takes its [`jsonwebtoken::DecodingKey`] as a plain argument rather
    /// than through the environment, and every full round-trip test below
    /// goes through the API-key path instead, which needs no such
    /// workaround.
    fn hs256_token(secret: &[u8], tenant_id: &str, expires_in: i64) -> String {
        let now_secs =
            i64::try_from(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()).unwrap();
        let exp = now_secs + expires_in;
        let claims = serde_json::json!({ "tenant_id": tenant_id, "exp": exp });
        jsonwebtoken::encode(&Header::default(), &claims, &EncodingKey::from_secret(secret))
            .unwrap()
    }

    #[test]
    fn a_well_formed_bearer_header_yields_its_credential() {
        let req = request(Some("Bearer abc123"));
        assert_eq!(bearer_credential(&req).as_deref(), Some("abc123"));
    }

    #[test]
    fn a_non_bearer_scheme_yields_no_credential() {
        let req = request(Some("Basic abc123"));
        assert_eq!(bearer_credential(&req), None);
    }

    #[test]
    fn a_missing_header_yields_no_credential() {
        let req = request(None);
        assert_eq!(bearer_credential(&req), None);
    }

    #[test]
    fn a_valid_jwt_resolves_to_its_tenant_claim() {
        let secret = b"test-secret";
        let token = hs256_token(secret, "tenant-a", 3600);
        let tenant = verify_jwt(&token, &jsonwebtoken::DecodingKey::from_secret(secret)).unwrap();
        assert_eq!(tenant.as_str(), "tenant-a");
    }

    #[test]
    fn an_expired_jwt_is_rejected() {
        let secret = b"test-secret";
        let token = hs256_token(secret, "tenant-a", -3600);
        assert!(verify_jwt(&token, &jsonwebtoken::DecodingKey::from_secret(secret)).is_none());
    }

    #[test]
    fn a_jwt_signed_with_the_wrong_secret_is_rejected() {
        let token = hs256_token(b"correct-secret", "tenant-a", 3600);
        let wrong_key = jsonwebtoken::DecodingKey::from_secret(b"wrong-secret");
        assert!(verify_jwt(&token, &wrong_key).is_none());
    }

    #[test]
    fn a_jwt_with_an_invalid_tenant_id_claim_is_rejected() {
        let secret = b"test-secret";
        // Empty string fails TenantId::new's own validation - verify_jwt
        // must propagate that rejection rather than trusting the claim
        // blindly just because the signature checked out.
        let token = hs256_token(secret, "", 3600);
        assert!(verify_jwt(&token, &jsonwebtoken::DecodingKey::from_secret(secret)).is_none());
    }

    #[test]
    fn claims_deserializes_the_tenant_id_field() {
        let claims: Claims = serde_json::from_value(serde_json::json!({
            "tenant_id": "tenant-a",
        }))
        .unwrap();
        assert_eq!(claims.tenant_id, "tenant-a");
    }

    #[tokio::test]
    async fn a_request_with_no_authorization_header_is_rejected_before_dispatch() {
        let (_dir, api_keys) = open_store().await;
        let app = router_with(AuthState::new(api_keys));

        let response = app.oneshot(request(None)).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn an_unknown_api_key_is_rejected() {
        let (_dir, api_keys) = open_store().await;
        let app = router_with(AuthState::new(api_keys));

        let response = app.oneshot(request(Some("Bearer qaas_nonexistent"))).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn a_valid_api_key_resolves_to_its_tenant_and_reaches_the_inner_service() {
        let (_dir, api_keys) = open_store().await;
        let tenant = TenantId::new("tenant-a").unwrap();
        let raw_key = api_keys.mint(tenant).await.unwrap();
        let app = router_with(AuthState::new(Arc::clone(&api_keys)));

        let response = app.oneshot(request(Some(&format!("Bearer {raw_key}")))).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(&body[..], b"tenant-a");
    }

    #[tokio::test]
    async fn a_revoked_api_key_is_rejected() {
        let (_dir, api_keys) = open_store().await;
        let tenant = TenantId::new("tenant-a").unwrap();
        let raw_key = api_keys.mint(tenant).await.unwrap();
        api_keys.revoke(&raw_key).await.unwrap();
        let app = router_with(AuthState::new(api_keys));

        let response = app.oneshot(request(Some(&format!("Bearer {raw_key}")))).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
}
