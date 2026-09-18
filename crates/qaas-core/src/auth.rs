//! Tenant identity and durable API-key credentials.
//!
//! `feature/api-auth` is the first branch to give this project a caller
//! it doesn't automatically trust — every branch before this one has run
//! over `qaas-server`'s stdio MCP transport, a single local pipe with no
//! "wrong caller connects" scenario to defend against. [`TenantId`] is
//! the identity this crate now has a name for, and [`ApiKeyStore`] is
//! one way (of two `feature/api-auth` adds; see `qaas-server`'s own auth
//! module for the other, JWT verification) to prove you're allowed to
//! claim one.
//!
//! Deliberately durable, unlike most of what this crate has added since
//! Phase 4: an API key is a long-lived credential an operator hands to a
//! team, and a team expects it to keep working across a routine restart
//! or redeploy — unlike a per-queue route descriptor or admission
//! budget, which this project has always treated as in-memory,
//! reconfigure-after-restart state. Losing every tenant's ability to
//! authenticate on every deploy would be a real, surprising regression,
//! not an acceptable simplification.
//!
//! What lives here versus in `qaas-server`: this module owns the
//! *identity* concept (`TenantId`) and the *API-key* credential, because
//! neither needs anything beyond what this crate already has (a `Wal`,
//! `serde`, a cryptographic hash). JWT verification needs a real crypto
//! library for signature checking and is fundamentally about an HTTP
//! `Authorization` header — a wire-protocol concern belonging to
//! `qaas-server`, the one crate allowed to speak a wire protocol at all,
//! the same boundary `schemars`-based tool schemas already respect.

use std::collections::HashMap;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::wal::Wal;

/// The longest a [`TenantId`] (or a raw API key's tenant-facing
/// component) is allowed to be — generous for any real identifier, tight
/// enough that nothing built from one balloons a WAL record for no
/// reason.
const MAX_TENANT_ID_LEN: usize = 128;

/// Who a request is acting on behalf of, once authenticated.
///
/// Validated at construction the same way
/// [`IdempotencyKey`](qaas_types::IdempotencyKey) is: non-empty, and
/// restricted to a plain ASCII identifier (letters, digits, `-`, `_`).
/// This isn't just tidiness — a `TenantId` ends up as a filesystem path
/// component (`qaas-server` namespaces each tenant's queues under a
/// directory named after it), so the same "no `..`, no `/`, no path
/// traversal" reasoning `qaas-server`'s own queue-name validation
/// already applies now applies here too, one layer earlier.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TenantId(String);

/// `value` passed to [`TenantId::new`] was empty, too long, or contained
/// a character outside `[A-Za-z0-9_-]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("tenant id must be 1-{MAX_TENANT_ID_LEN} characters of ASCII letters, digits, '-', or '_'")]
pub struct InvalidTenantId;

impl TenantId {
    /// Validates and wraps `value`.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidTenantId`] if `value` is empty, longer than
    /// `MAX_TENANT_ID_LEN`, or contains anything outside ASCII letters,
    /// digits, `-`, or `_`.
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidTenantId> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= MAX_TENANT_ID_LEN
            && value.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
        if valid { Ok(Self(value)) } else { Err(InvalidTenantId) }
    }

    /// This tenant id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// One entry in an [`ApiKeyStore`]'s WAL. Keyed by the SHA-256 hash of
/// the raw key, never the raw key itself — see [`ApiKeyStore`]'s own
/// docs for why.
#[derive(Serialize, Deserialize)]
enum WalRecord {
    Mint { key_hash: String, tenant: TenantId },
    Revoke { key_hash: String },
}

/// SHA-256 of `raw_key`, hex-encoded — the only form of an API key this
/// module ever stores or compares against durably. An API key is a
/// high-entropy random secret, not a human password: there's no
/// brute-forcing concern a slow, salted password hash exists to defend
/// against, so a plain cryptographic hash is the right tool, not
/// over-engineering. What it *does* defend against is a memory dump, a
/// debug log, or a WAL file on disk leaking directly usable credentials
/// — comparing hashes instead of raw keys throughout means none of those
/// ever have to hold one.
fn hash_key(raw_key: &str) -> String {
    let digest = Sha256::digest(raw_key.as_bytes());
    hex_encode(&digest)
}

/// Minimal hex encoder — not worth a dependency for something this
/// small and this rarely called (once per mint, once per authenticate).
fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;

    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

/// Generates a new, high-entropy raw API key.
///
/// Built from two random `UUIDv4`s rather than pulling in a
/// general-purpose CSPRNG crate this workspace doesn't otherwise need:
/// `uuid`'s `v4` feature (already a dependency, used throughout this
/// crate for message and trace ids) is itself backed by a
/// cryptographically secure source, and two of them concatenated give
/// 256 bits of randomness — comfortably enough for a bearer credential.
/// The `qaas_` prefix is only for humans skimming a credentials file or
/// a log line, the same convention API providers commonly use for their
/// own keys — it carries no security meaning of its own.
fn generate_raw_key() -> String {
    format!("qaas_{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

/// A durable set of API-key credentials, each naming exactly one
/// [`TenantId`].
///
/// Never stores a raw key — only its SHA-256 hash. A newly
/// [`mint`](Self::mint)ed key is returned to the caller exactly once;
/// after that, this store (and `qaas-server`, and the WAL on disk) only
/// ever holds its hash. If it's lost, it's gone — the same "show once"
/// contract every real API-key system uses, not a limitation specific
/// to this one.
pub struct ApiKeyStore {
    // key_hash -> tenant.
    keys: Mutex<HashMap<String, TenantId>>,
    wal: Wal<WalRecord>,
}

impl ApiKeyStore {
    /// Opens the WAL at `path` (creating it if it doesn't exist) and
    /// replays it to rebuild the current set of valid key hashes.
    ///
    /// # Errors
    ///
    /// Returns an error under the same conditions as
    /// [`Wal::open`](crate::wal::Wal::open).
    pub async fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let (wal, records) = Wal::open(path).await?;
        let mut keys = HashMap::new();
        for record in records {
            match record {
                WalRecord::Mint { key_hash, tenant } => {
                    keys.insert(key_hash, tenant);
                }
                WalRecord::Revoke { key_hash } => {
                    keys.remove(&key_hash);
                }
            }
        }
        Ok(Self { keys: Mutex::new(keys), wal })
    }

    /// Durably mints a new API key for `tenant`, returning the raw key —
    /// this is the only time it's ever available; only its hash is kept
    /// from here on.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails; the key is not valid in
    /// that case.
    pub async fn mint(&self, tenant: TenantId) -> io::Result<String> {
        let raw_key = generate_raw_key();
        let key_hash = hash_key(&raw_key);
        self.wal
            .append(&WalRecord::Mint { key_hash: key_hash.clone(), tenant: tenant.clone() })
            .await?;
        self.keys.lock().await.insert(key_hash, tenant);
        Ok(raw_key)
    }

    /// Durably revokes `raw_key`. Returns `Ok(false)` without effect if
    /// it doesn't currently name a valid key.
    ///
    /// # Errors
    ///
    /// Returns an error if the WAL write fails; `raw_key` remains valid
    /// in that case.
    pub async fn revoke(&self, raw_key: &str) -> io::Result<bool> {
        let key_hash = hash_key(raw_key);
        if !self.keys.lock().await.contains_key(&key_hash) {
            return Ok(false);
        }
        self.wal.append(&WalRecord::Revoke { key_hash: key_hash.clone() }).await?;
        Ok(self.keys.lock().await.remove(&key_hash).is_some())
    }

    /// Resolves `raw_key` to the tenant it authenticates, if it's
    /// currently valid. Read-only — no WAL write, so this can't fail the
    /// way `mint`/`revoke` can.
    pub async fn authenticate(&self, raw_key: &str) -> Option<TenantId> {
        let key_hash = hash_key(raw_key);
        self.keys.lock().await.get(&key_hash).cloned()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{ApiKeyStore, InvalidTenantId, TenantId};

    #[test]
    fn rejects_an_empty_tenant_id() {
        assert_eq!(TenantId::new(""), Err(InvalidTenantId));
    }

    #[test]
    fn rejects_characters_outside_the_allowed_set() {
        for bad in ["has space", "has/slash", "has..dots", "has\0null"] {
            assert_eq!(TenantId::new(bad), Err(InvalidTenantId), "{bad:?} should be rejected");
        }
    }

    #[test]
    fn rejects_an_overly_long_tenant_id() {
        assert_eq!(TenantId::new("x".repeat(129)), Err(InvalidTenantId));
    }

    #[test]
    fn accepts_a_normal_tenant_id() {
        assert!(TenantId::new("acme-corp_1").is_ok());
    }

    #[tokio::test]
    async fn a_minted_key_authenticates_as_its_tenant() {
        let dir = tempdir().unwrap();
        let store = ApiKeyStore::open(dir.path().join("keys.log")).await.unwrap();
        let tenant = TenantId::new("acme").unwrap();

        let raw_key = store.mint(tenant.clone()).await.unwrap();
        assert_eq!(store.authenticate(&raw_key).await, Some(tenant));
    }

    #[tokio::test]
    async fn an_unknown_key_does_not_authenticate() {
        let dir = tempdir().unwrap();
        let store = ApiKeyStore::open(dir.path().join("keys.log")).await.unwrap();
        assert_eq!(store.authenticate("qaas_not_a_real_key").await, None);
    }

    #[tokio::test]
    async fn two_mints_produce_two_different_keys() {
        let dir = tempdir().unwrap();
        let store = ApiKeyStore::open(dir.path().join("keys.log")).await.unwrap();
        let tenant = TenantId::new("acme").unwrap();

        let first = store.mint(tenant.clone()).await.unwrap();
        let second = store.mint(tenant).await.unwrap();
        assert_ne!(first, second);
    }

    #[tokio::test]
    async fn revoking_a_key_stops_it_from_authenticating() {
        let dir = tempdir().unwrap();
        let store = ApiKeyStore::open(dir.path().join("keys.log")).await.unwrap();
        let raw_key = store.mint(TenantId::new("acme").unwrap()).await.unwrap();

        assert!(store.revoke(&raw_key).await.unwrap());
        assert_eq!(store.authenticate(&raw_key).await, None);
    }

    #[tokio::test]
    async fn revoking_an_unknown_key_is_a_false_not_an_error() {
        let dir = tempdir().unwrap();
        let store = ApiKeyStore::open(dir.path().join("keys.log")).await.unwrap();
        assert!(!store.revoke("qaas_never_existed").await.unwrap());
    }

    #[tokio::test]
    async fn valid_keys_survive_reopening_the_same_wal_but_revoked_ones_do_not() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("keys.log");
        let tenant = TenantId::new("acme").unwrap();
        let (kept, revoked) = {
            let store = ApiKeyStore::open(&path).await.unwrap();
            let kept = store.mint(tenant.clone()).await.unwrap();
            let revoked = store.mint(tenant.clone()).await.unwrap();
            store.revoke(&revoked).await.unwrap();
            (kept, revoked)
        };

        let reopened = ApiKeyStore::open(&path).await.unwrap();
        assert_eq!(reopened.authenticate(&kept).await, Some(tenant));
        assert_eq!(reopened.authenticate(&revoked).await, None);
    }

    #[test]
    fn raw_keys_are_never_the_same_as_their_stored_hash() {
        // A cheap sanity check that mint() isn't accidentally storing
        // (or returning) the hash instead of a real random secret.
        let raw = super::generate_raw_key();
        assert_ne!(raw, super::hash_key(&raw));
        assert!(raw.starts_with("qaas_"));
    }
}
