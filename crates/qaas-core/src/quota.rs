//! Per-tenant resource quotas: how many MCP tool calls a tenant may make
//! per unit time, denominated in plain request counts rather than the
//! LLM tokens/dollars [`admission`](crate::admission) checks.
//!
//! `feature/token-cost-aware-admission` generalized global-rate-limiter's
//! original `SlidingWindowLimiter` (see `CLAUDE.md`'s reference
//! architecture section) from counting requests to summing tokens and
//! dollars instead — see [`admission`](crate::admission)'s own docs.
//! This module goes back the other way: a runaway agent loop hammering
//! `claim` in a tight retry storm is a request-*count* problem, not a
//! token-cost one, and nothing here needs to know what an LLM call costs
//! to be a real problem for the rest of the cluster. [`TenantQuota`]
//! reuses the exact same trailing-window mechanism — every acquired
//! request is timestamped; a check sums everything still inside the
//! window and compares against a configured ceiling; entries age out on
//! their own — just with `count` as its only unit, the dimension that
//! algorithm started with before `admission` generalized it.
//!
//! # Two shapes of limit, only one enforced here
//!
//! [`QuotaConfig`] carries three ceilings, but [`TenantQuota`] itself
//! only *enforces* one:
//!
//! - `requests_per_window` is checked and recorded by
//!   [`TenantQuota::try_acquire`], the sliding-window mechanism this
//!   module actually implements.
//! - `max_queues` and `max_pending_messages` are plain ceilings against a
//!   live count only `qaas-server`'s registry can ever know — how many
//!   queues a tenant currently has open, how many messages across all of
//!   them are still unacknowledged. This module has no visibility into
//!   either (a domain-agnostic quota primitive has no business knowing
//!   what a "queue" is, the same reason [`crate::admission::AdmissionController`]
//!   doesn't know what an LLM token is), so it only stores the
//!   configured numbers; comparing them against a live count is
//!   `qaas-server`'s job, at the layer that actually has one.
//!
//! # Why its own primitive, not a generalized `AdmissionController`
//!
//! Bolting a third, unrelated unit onto [`crate::admission::AdmissionController`]
//! (already shipped, already exercised by
//! `feature/token-cost-aware-admission`'s own tests) would mean either an
//! awkward third field nobody's ceiling actually shares logic with, or a
//! genuine unit-generalizing refactor of a type another branch owns —
//! both larger and riskier than this branch's actual scope. A second,
//! narrower sibling primitive costs one more module for one clearly
//! separate concern.
//!
//! Not wired into [`ConsumerGroup`](crate::ConsumerGroup), for the same
//! reason every other primitive in this crate isn't: a domain-agnostic
//! queue engine has no idea what a tenant is. `qaas-server`'s MCP layer
//! (`feature/multi-tenant-quotas`) is where a per-tenant [`TenantQuota`]
//! actually gets consulted — on every tool call for the rate quota, and
//! at `enqueue`/queue-open time for the other two ceilings.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

/// Tunables for one tenant's resource quota. Every ceiling is
/// independently optional: a deployment that only cares about the queue
/// count can leave the others as `None`, and a tenant with nothing
/// configured is never denied anything — quotas are opt-in per tenant,
/// not a surprise default every existing tenant suddenly has to satisfy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct QuotaConfig {
    /// Maximum number of distinct queues this tenant may have open at
    /// once. `None` means no limit. Enforced by `qaas-server`'s registry
    /// when a *new* queue would be opened — an already-open queue never
    /// becomes invalid because a ceiling was lowered since.
    pub max_queues: Option<usize>,
    /// Maximum total unacknowledged messages this tenant may have
    /// outstanding across all of its queues at once. `None` means no
    /// limit. Enforced by `qaas-server` at `enqueue` — the one operation
    /// that grows this count.
    pub max_pending_messages: Option<u64>,
    /// Maximum MCP tool calls this tenant may make within any trailing
    /// `window`. `None` means no limit. The one ceiling this module
    /// enforces itself, via [`TenantQuota::try_acquire`].
    pub requests_per_window: Option<u64>,
    /// How far back "within the window" looks, for `requests_per_window`.
    /// Irrelevant while that ceiling is `None`, but a sane default the
    /// moment it's set without also overriding `window`.
    pub window: Duration,
}

impl QuotaConfig {
    /// No ceilings on any dimension, one-minute window (irrelevant while
    /// `requests_per_window` is `None`, but a sane default the moment
    /// it's set without also overriding `window`) — a tenant no operator
    /// has configured a quota for is never denied anything, not throttled
    /// by some undocumented default. A minute, not `AdmissionConfig`'s
    /// hour: a runaway agent loop worth catching is a tight retry storm,
    /// and a window long enough to smooth out a token/cost budget's
    /// legitimate hour-scale usage would let a genuine request storm run
    /// for a very long time before the ceiling ever engages.
    pub const UNLIMITED: Self = Self {
        max_queues: None,
        max_pending_messages: None,
        requests_per_window: None,
        window: Duration::from_secs(60),
    };
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self::UNLIMITED
    }
}

/// The outcome of [`TenantQuota::try_acquire`].
#[derive(Debug, Clone, PartialEq)]
pub enum QuotaDecision {
    /// The request fit under the configured rate ceiling (or none is
    /// configured) and has been recorded.
    Allowed {
        /// Requests still available in the current window after this
        /// one, if a ceiling is configured. `None` against an unbounded
        /// budget, where "remaining" is meaningless.
        requests_remaining: Option<u64>,
    },
    /// The request was refused; nothing was recorded.
    Denied {
        /// Meant to be surfaced directly to whatever's calling this (an
        /// agent, an operator), not just logged.
        reason: String,
        /// How long until *some* capacity frees up (when the oldest
        /// request currently in the window ages out) — a best-effort
        /// hint, not a guarantee, since other requests can be admitted in
        /// the meantime. `None` only when the configured ceiling is `0`:
        /// no amount of waiting ever produces a window with room for
        /// even one request.
        retry_after: Option<Duration>,
    },
}

struct Inner {
    config: QuotaConfig,
    requests: VecDeque<Instant>,
}

/// A sliding-window request-rate budget for one tenant. See the module
/// docs for the algorithm, and for why `max_queues` /
/// `max_pending_messages` live in [`QuotaConfig`] but aren't enforced
/// here.
pub struct TenantQuota {
    inner: Mutex<Inner>,
}

impl TenantQuota {
    /// Creates a quota starting with `config` and no requests recorded
    /// yet.
    #[must_use]
    pub fn new(config: QuotaConfig) -> Self {
        Self { inner: Mutex::new(Inner { config, requests: VecDeque::new() }) }
    }

    /// Replaces this tenant's configuration. Takes effect immediately —
    /// already-recorded requests aren't cleared, they're simply measured
    /// against the new ceiling (and the new window) starting with the
    /// very next call, the same "operator tightened the budget live"
    /// semantics [`crate::admission::AdmissionController::set_config`] already
    /// documents.
    pub async fn set_config(&self, config: QuotaConfig) {
        self.inner.lock().await.config = config;
    }

    /// This tenant's current configuration.
    pub async fn config(&self) -> QuotaConfig {
        self.inner.lock().await.config
    }

    /// Drops every request older than `window` relative to `now`.
    fn evict_expired(requests: &mut VecDeque<Instant>, window: Duration, now: Instant) {
        while let Some(&front) = requests.front() {
            if now.duration_since(front) >= window {
                requests.pop_front();
            } else {
                break;
            }
        }
    }

    /// How long until the oldest request currently in the window ages
    /// out.
    fn retry_after(requests: &VecDeque<Instant>, window: Duration, now: Instant) -> Duration {
        requests
            .front()
            .map_or(Duration::ZERO, |oldest| window.saturating_sub(now.duration_since(*oldest)))
    }

    /// Current request count within the trailing window, without
    /// recording a new one — a read-only status check, the same role
    /// [`crate::admission::AdmissionController::usage`] plays for token/cost
    /// budgets.
    pub async fn requests_in_window(&self) -> u64 {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let window = inner.config.window;
        Self::evict_expired(&mut inner.requests, window, now);
        u64::try_from(inner.requests.len()).unwrap_or(u64::MAX)
    }

    /// Checks one request against the configured rate ceiling and, if it
    /// fits, records it as happening now. A tenant with no
    /// `requests_per_window` configured is never denied here.
    pub async fn try_acquire(&self) -> QuotaDecision {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let window = inner.config.window;
        Self::evict_expired(&mut inner.requests, window, now);

        let Some(limit) = inner.config.requests_per_window else {
            return QuotaDecision::Allowed { requests_remaining: None };
        };

        let current = u64::try_from(inner.requests.len()).unwrap_or(u64::MAX);
        if current >= limit {
            // limit == 0 is the one case waiting can never fix - there is
            // no window, empty or not, with room for even one request.
            let retry_after = (limit > 0).then(|| Self::retry_after(&inner.requests, window, now));
            return QuotaDecision::Denied {
                reason: format!(
                    "would make {} requests this window, exceeding the {limit}-request budget",
                    current + 1
                ),
                retry_after,
            };
        }

        inner.requests.push_back(now);
        QuotaDecision::Allowed { requests_remaining: Some(limit - (current + 1)) }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{QuotaConfig, QuotaDecision, TenantQuota};

    fn rate_limited(limit: u64, window: Duration) -> QuotaConfig {
        QuotaConfig { requests_per_window: Some(limit), window, ..QuotaConfig::UNLIMITED }
    }

    #[tokio::test]
    async fn an_unconfigured_quota_never_denies_anything() {
        let quota = TenantQuota::new(QuotaConfig::UNLIMITED);
        for _ in 0..10 {
            assert!(matches!(
                quota.try_acquire().await,
                QuotaDecision::Allowed { requests_remaining: None }
            ));
        }
    }

    #[tokio::test]
    async fn admits_up_to_and_including_the_exact_limit() {
        let quota = TenantQuota::new(rate_limited(3, Duration::from_secs(60)));
        assert_eq!(
            quota.try_acquire().await,
            QuotaDecision::Allowed { requests_remaining: Some(2) }
        );
        assert_eq!(
            quota.try_acquire().await,
            QuotaDecision::Allowed { requests_remaining: Some(1) }
        );
        assert_eq!(
            quota.try_acquire().await,
            QuotaDecision::Allowed { requests_remaining: Some(0) }
        );
        assert_eq!(quota.requests_in_window().await, 3);
    }

    #[tokio::test]
    async fn denies_once_the_request_ceiling_would_be_exceeded() {
        let quota = TenantQuota::new(rate_limited(2, Duration::from_secs(60)));
        quota.try_acquire().await;
        quota.try_acquire().await;

        let decision = quota.try_acquire().await;
        let QuotaDecision::Denied { reason, retry_after } = decision else {
            panic!("expected denial");
        };
        assert!(reason.contains("3 requests"), "{reason}");
        assert!(reason.contains("2-request budget"), "{reason}");
        assert!(retry_after.is_some(), "waiting for the oldest request to age out should help");

        // Denied, so nothing beyond the first two was recorded.
        assert_eq!(quota.requests_in_window().await, 2);
    }

    #[tokio::test]
    async fn a_zero_request_limit_never_has_a_useful_retry_after() {
        let quota = TenantQuota::new(rate_limited(0, Duration::from_secs(60)));
        let decision = quota.try_acquire().await;
        let QuotaDecision::Denied { retry_after, .. } = decision else {
            panic!("expected denial");
        };
        assert_eq!(
            retry_after, None,
            "a 0-request budget never has room, no matter how long you wait"
        );
    }

    #[tokio::test]
    async fn usage_ages_out_of_the_window_and_frees_capacity() {
        let quota = TenantQuota::new(rate_limited(1, Duration::from_millis(50)));
        assert!(matches!(quota.try_acquire().await, QuotaDecision::Allowed { .. }));
        assert!(matches!(quota.try_acquire().await, QuotaDecision::Denied { .. }));

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(quota.requests_in_window().await, 0);
        assert!(matches!(quota.try_acquire().await, QuotaDecision::Allowed { .. }));
    }

    #[tokio::test]
    async fn set_config_takes_effect_on_the_very_next_call() {
        let quota = TenantQuota::new(QuotaConfig::UNLIMITED);
        assert!(matches!(quota.try_acquire().await, QuotaDecision::Allowed { .. }));

        quota.set_config(rate_limited(0, Duration::from_secs(60))).await;
        assert!(matches!(quota.try_acquire().await, QuotaDecision::Denied { .. }));
    }

    #[tokio::test]
    async fn config_round_trips_including_the_other_two_ceilings() {
        let config = QuotaConfig {
            max_queues: Some(5),
            max_pending_messages: Some(1000),
            requests_per_window: Some(50),
            window: Duration::from_secs(30),
        };
        let quota = TenantQuota::new(config);
        assert_eq!(quota.config().await, config);
    }
}
