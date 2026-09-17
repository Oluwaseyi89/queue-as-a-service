//! Token/cost-aware admission control: a sliding-window budget, denominated
//! in LLM tokens and dollars instead of raw request counts.
//!
//! Ported from global-rate-limiter's `SlidingWindowLimiter` — verified
//! against its actual Go source (see `CLAUDE.md`'s reference architecture
//! section) rather than worked from memory. The mechanism is the same:
//! every admitted request is recorded with a timestamp; a check sums
//! everything still inside the trailing `window` and compares against a
//! configured ceiling; entries older than `window` age out on their own,
//! so the budget continuously refills as time passes rather than needing
//! an explicit reset. What's different is the unit — Go's version counts
//! *requests*; [`AdmissionController`] sums *tokens* and *dollars*
//! instead, and enforces both ceilings independently rather than a single
//! count.
//!
//! This is deliberately not wired into [`ConsumerGroup`](crate::ConsumerGroup)
//! itself, the same "generic primitive, integrated at the layer that
//! actually knows about the domain" split `circuit_breaker` already
//! established for this crate: `ConsumerGroup` is a domain-agnostic queue
//! engine that has no idea what an LLM token is, and forcing it to would
//! be exactly the kind of leak `qaas-core`'s crate docs warn against.
//! `qaas-server`'s MCP layer (`feature/token-cost-aware-admission`) is
//! where a per-queue [`AdmissionController`] actually gets consulted: once
//! at `enqueue`, admitting (and durably contributing to) an estimated
//! cost the producer declares up front, and again — read-only, no new
//! consumption — at `claim`, refusing to hand out work while the queue is
//! currently over budget. See that module's own docs for why those are
//! different operations on the same underlying window rather than two
//! independent deductions that would double-count the same task's cost.
//!
//! # Why an estimate, never reconciled against actual usage
//!
//! A caller declares what a task is expected to cost at admission time;
//! this module has no way to learn what it actually cost afterward, and
//! deliberately doesn't try to. A real reconciliation ledger — capture an
//! estimate, let a consumer report the true number once the LLM call
//! finishes, adjust the window's accounting to match — is a genuinely
//! larger feature (a new tool, a reservation-adjustment path, decisions
//! about what happens to work already admitted under a since-corrected
//! estimate) than this branch's own scope. Single-phase "check and
//! record the estimate" mirrors exactly how the Go version it's ported
//! from works, too: `Allow()` has no corresponding "actually, undo that"
//! call either.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

/// Tunables for one queue's [`AdmissionController`].
///
/// Both ceilings are independently optional: a queue that only cares
/// about dollar spend can leave `tokens_per_window` as `None` (and vice
/// versa), and a queue with neither configured never denies anything —
/// admission control is opt-in per queue, not a surprise default limit
/// every existing queue suddenly has to satisfy.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdmissionConfig {
    /// Maximum total tokens [`AdmissionController::try_admit`] may admit
    /// within any trailing `window`. `None` means no token ceiling.
    pub tokens_per_window: Option<u64>,
    /// Maximum total dollars [`AdmissionController::try_admit`] may
    /// admit within any trailing `window`. `None` means no cost ceiling.
    pub cost_ceiling_per_window: Option<f64>,
    /// How far back "within the window" looks. Shared by both ceilings —
    /// a queue wanting a different rolling period per dimension would
    /// need two `AdmissionController`s, which this branch doesn't build
    /// since nothing in `Plan.md`'s line for it asks for that.
    pub window: Duration,
}

impl AdmissionConfig {
    /// No ceilings on either dimension, one-hour window (irrelevant while
    /// both ceilings are `None`, but a sane default the moment either one
    /// is set without also overriding `window`). This — not some
    /// artificially low limit — is what a queue no one has configured
    /// gets: admission control that never denies anything until an
    /// operator opts a queue into it.
    pub const UNLIMITED: Self = Self {
        tokens_per_window: None,
        cost_ceiling_per_window: None,
        window: Duration::from_secs(3600),
    };
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self::UNLIMITED
    }
}

/// A single admitted request's contribution to the window, timestamped
/// so it can age out once `window` has passed.
#[derive(Debug, Clone, Copy)]
struct UsageEntry {
    at: Instant,
    tokens: u64,
    cost: f64,
}

/// Total usage currently inside the trailing window, as of the moment it
/// was read — see [`AdmissionController::usage`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct UsageSnapshot {
    /// Tokens admitted within the current window.
    pub tokens_used: u64,
    /// Dollars admitted within the current window.
    pub cost_used: f64,
}

/// The outcome of [`AdmissionController::try_admit`].
#[derive(Debug, Clone, PartialEq)]
pub enum AdmissionDecision {
    /// The request fit under both configured ceilings and has been
    /// recorded. `_remaining` fields are `None` for whichever dimension
    /// has no configured ceiling — "remaining" is meaningless against an
    /// unbounded budget.
    Admitted {
        /// Tokens still available in the current window after this
        /// admission, if a token ceiling is configured.
        tokens_remaining: Option<u64>,
        /// Dollars still available in the current window after this
        /// admission, if a cost ceiling is configured.
        cost_remaining: Option<f64>,
    },
    /// The request was refused; nothing was recorded.
    Denied {
        /// Which ceiling was hit and by how much, in plain language —
        /// meant to be surfaced directly to whatever's calling this (an
        /// agent, an operator), not just logged.
        reason: String,
        /// How long until *some* capacity frees up (when the oldest
        /// entry currently in the window ages out) — a best-effort hint,
        /// not a guarantee the request will fit even then, since other
        /// admissions can happen in the meantime. `None` means waiting
        /// can never help: this single request's own tokens or cost
        /// exceed the configured ceiling outright, so no amount of
        /// aging-out changes the outcome.
        retry_after: Option<Duration>,
    },
}

struct Inner {
    config: AdmissionConfig,
    entries: VecDeque<UsageEntry>,
}

/// A sliding-window token/cost budget for one queue. See the module docs
/// for the algorithm and why it lives here rather than inside
/// [`ConsumerGroup`](crate::ConsumerGroup).
pub struct AdmissionController {
    inner: Mutex<Inner>,
}

impl AdmissionController {
    /// Creates a controller starting with `config` and no usage recorded
    /// yet.
    #[must_use]
    pub fn new(config: AdmissionConfig) -> Self {
        Self { inner: Mutex::new(Inner { config, entries: VecDeque::new() }) }
    }

    /// Replaces this controller's configuration. Takes effect
    /// immediately — already-recorded usage isn't cleared or rescaled,
    /// it's simply measured against the new ceilings (and the new
    /// window) starting with the very next call. Lowering a ceiling
    /// below what's already been admitted this window doesn't retroactively
    /// deny anything already admitted; it does mean
    /// [`try_admit`](Self::try_admit) starts refusing sooner than it
    /// otherwise would have, and [`has_headroom`](Self::has_headroom)
    /// reports `false` immediately if usage already exceeds the new,
    /// lower ceiling — this is precisely the "operator tightened the
    /// budget live" case dequeue-side throttling exists to catch.
    pub async fn set_config(&self, config: AdmissionConfig) {
        self.inner.lock().await.config = config;
    }

    /// This controller's current configuration.
    pub async fn config(&self) -> AdmissionConfig {
        self.inner.lock().await.config
    }

    /// Drops every entry older than `window` relative to `now` — the
    /// "old entries age out" half of the sliding-window algorithm.
    /// Called at the start of every other method here rather than on a
    /// timer: a controller nobody is calling doesn't need to tick, the
    /// same reasoning [`circuit_breaker`](crate::circuit_breaker)'s
    /// lazy `Open` -> `HalfOpen` check already uses.
    fn evict_expired(entries: &mut VecDeque<UsageEntry>, window: Duration, now: Instant) {
        while let Some(front) = entries.front() {
            if now.duration_since(front.at) >= window {
                entries.pop_front();
            } else {
                break;
            }
        }
    }

    /// Sums every entry currently in the (already-evicted) window.
    fn sum(entries: &VecDeque<UsageEntry>) -> (u64, f64) {
        entries.iter().fold((0, 0.0), |(tokens, cost), entry| {
            (tokens.saturating_add(entry.tokens), cost + entry.cost)
        })
    }

    /// How long until the oldest entry currently in the window ages out
    /// — see [`AdmissionDecision::Denied`]'s `retry_after` for what this
    /// promises and doesn't.
    fn retry_after(entries: &VecDeque<UsageEntry>, window: Duration, now: Instant) -> Duration {
        entries
            .front()
            .map_or(Duration::ZERO, |oldest| window.saturating_sub(now.duration_since(oldest.at)))
    }

    /// Current total usage within the trailing window, as of now.
    pub async fn usage(&self) -> UsageSnapshot {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let window = inner.config.window;
        Self::evict_expired(&mut inner.entries, window, now);
        let (tokens_used, cost_used) = Self::sum(&inner.entries);
        UsageSnapshot { tokens_used, cost_used }
    }

    /// Whether the window is currently within *both* configured
    /// ceilings, without admitting or recording anything — the read-only
    /// operation `claim`'s dequeue-side throttling uses. A dimension
    /// with no configured ceiling never counts against headroom.
    ///
    /// This can only ever return `false` because usage that was valid
    /// when admitted is no longer valid under a ceiling lowered since —
    /// [`try_admit`](Self::try_admit) never records more than a ceiling allows in the
    /// first place, so headroom can't be lost purely by the window
    /// filling up under an unchanged config without `try_admit` having
    /// already started denying first.
    pub async fn has_headroom(&self) -> bool {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let window = inner.config.window;
        Self::evict_expired(&mut inner.entries, window, now);
        let (tokens_used, cost_used) = Self::sum(&inner.entries);
        let tokens_ok = inner.config.tokens_per_window.is_none_or(|limit| tokens_used < limit);
        let cost_ok =
            inner.config.cost_ceiling_per_window.is_none_or(|ceiling| cost_used < ceiling);
        tokens_ok && cost_ok
    }

    /// Checks `tokens`/`cost` against both configured ceilings and, if
    /// both fit, records them as used starting now. Denies (recording
    /// nothing) if either ceiling would be exceeded — a request that
    /// fits the token budget but blows the cost ceiling is refused just
    /// as firmly as one that fits neither.
    pub async fn try_admit(&self, tokens: u64, cost: f64) -> AdmissionDecision {
        let mut inner = self.inner.lock().await;
        let now = Instant::now();
        let window = inner.config.window;
        Self::evict_expired(&mut inner.entries, window, now);
        let (tokens_used, cost_used) = Self::sum(&inner.entries);

        let projected_tokens = tokens_used.saturating_add(tokens);
        if let Some(limit) = inner.config.tokens_per_window
            && projected_tokens > limit
        {
            let retry_after =
                (tokens <= limit).then(|| Self::retry_after(&inner.entries, window, now));
            return AdmissionDecision::Denied {
                reason: format!(
                    "would use {projected_tokens} tokens this window, exceeding the \
                     {limit}-token budget"
                ),
                retry_after,
            };
        }

        let projected_cost = cost_used + cost;
        if let Some(ceiling) = inner.config.cost_ceiling_per_window
            && projected_cost > ceiling
        {
            let retry_after =
                (cost <= ceiling).then(|| Self::retry_after(&inner.entries, window, now));
            return AdmissionDecision::Denied {
                reason: format!(
                    "would spend ${projected_cost:.4} this window, exceeding the \
                     ${ceiling:.4} cost ceiling"
                ),
                retry_after,
            };
        }

        inner.entries.push_back(UsageEntry { at: now, tokens, cost });
        AdmissionDecision::Admitted {
            tokens_remaining: inner.config.tokens_per_window.map(|limit| limit - projected_tokens),
            cost_remaining: inner
                .config
                .cost_ceiling_per_window
                .map(|ceiling| ceiling - projected_cost),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{AdmissionConfig, AdmissionController, AdmissionDecision, UsageSnapshot};

    fn tokens_only(limit: u64, window: Duration) -> AdmissionConfig {
        AdmissionConfig { tokens_per_window: Some(limit), cost_ceiling_per_window: None, window }
    }

    #[tokio::test]
    async fn an_unconfigured_controller_never_denies_anything() {
        let controller = AdmissionController::new(AdmissionConfig::UNLIMITED);
        for _ in 0..5 {
            let decision = controller.try_admit(1_000_000, 1_000_000.0).await;
            assert!(matches!(decision, AdmissionDecision::Admitted { .. }));
        }
        assert!(controller.has_headroom().await);
    }

    #[tokio::test]
    async fn admits_up_to_and_including_the_exact_limit() {
        let controller = AdmissionController::new(tokens_only(100, Duration::from_secs(60)));
        let first = controller.try_admit(60, 0.0).await;
        assert_eq!(
            first,
            AdmissionDecision::Admitted { tokens_remaining: Some(40), cost_remaining: None }
        );
        let second = controller.try_admit(40, 0.0).await;
        assert_eq!(
            second,
            AdmissionDecision::Admitted { tokens_remaining: Some(0), cost_remaining: None }
        );
        assert_eq!(controller.usage().await, UsageSnapshot { tokens_used: 100, cost_used: 0.0 });
    }

    #[tokio::test]
    async fn denies_once_the_token_ceiling_would_be_exceeded() {
        let controller = AdmissionController::new(tokens_only(100, Duration::from_secs(60)));
        controller.try_admit(90, 0.0).await;

        let decision = controller.try_admit(20, 0.0).await;
        let AdmissionDecision::Denied { reason, retry_after } = decision else {
            panic!("expected denial");
        };
        assert!(reason.contains("110 tokens"), "{reason}");
        assert!(reason.contains("100-token budget"), "{reason}");
        assert!(retry_after.is_some(), "20 tokens alone fits under 100, so waiting should help");

        // Denied, so nothing was recorded beyond the first admission.
        assert_eq!(controller.usage().await, UsageSnapshot { tokens_used: 90, cost_used: 0.0 });
    }

    #[tokio::test]
    async fn a_single_request_exceeding_the_ceiling_outright_has_no_retry_after() {
        let controller = AdmissionController::new(tokens_only(100, Duration::from_secs(60)));
        let decision = controller.try_admit(500, 0.0).await;
        let AdmissionDecision::Denied { retry_after, .. } = decision else {
            panic!("expected denial");
        };
        assert_eq!(retry_after, None, "500 tokens can never fit an 100-token budget, ever");
    }

    #[tokio::test]
    async fn a_cost_ceiling_denies_independently_of_the_token_budget() {
        let controller = AdmissionController::new(AdmissionConfig {
            tokens_per_window: Some(1_000_000),
            cost_ceiling_per_window: Some(1.0),
            window: Duration::from_secs(60),
        });

        // Comfortably fits the token budget, blows the cost ceiling.
        let decision = controller.try_admit(10, 5.0).await;
        let AdmissionDecision::Denied { reason, .. } = decision else {
            panic!("expected denial on cost alone");
        };
        assert!(reason.contains("cost ceiling"), "{reason}");
    }

    #[tokio::test]
    async fn usage_ages_out_of_the_window_and_frees_capacity() {
        let controller = AdmissionController::new(tokens_only(100, Duration::from_millis(50)));
        assert!(matches!(controller.try_admit(100, 0.0).await, AdmissionDecision::Admitted { .. }));
        assert!(matches!(controller.try_admit(1, 0.0).await, AdmissionDecision::Denied { .. }));

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert_eq!(controller.usage().await, UsageSnapshot { tokens_used: 0, cost_used: 0.0 });
        assert!(matches!(controller.try_admit(100, 0.0).await, AdmissionDecision::Admitted { .. }));
    }

    #[tokio::test]
    async fn has_headroom_reflects_a_ceiling_lowered_below_existing_usage() {
        let controller = AdmissionController::new(tokens_only(100, Duration::from_secs(60)));
        controller.try_admit(80, 0.0).await;
        assert!(controller.has_headroom().await, "80 used, 100 allowed");

        // The operator tightens the budget live, below what's already
        // been admitted this window — exactly the scenario dequeue-side
        // throttling exists to catch.
        controller.set_config(tokens_only(50, Duration::from_secs(60))).await;
        assert!(!controller.has_headroom().await, "80 used now exceeds the new 50-token ceiling");
    }

    #[tokio::test]
    async fn set_config_takes_effect_on_the_very_next_call() {
        let controller = AdmissionController::new(AdmissionConfig::UNLIMITED);
        assert!(matches!(
            controller.try_admit(1_000_000, 0.0).await,
            AdmissionDecision::Admitted { .. }
        ));

        controller.set_config(tokens_only(10, Duration::from_secs(60))).await;
        assert!(matches!(controller.try_admit(5, 0.0).await, AdmissionDecision::Denied { .. }));
    }
}
