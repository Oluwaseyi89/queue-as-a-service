//! 3-state circuit breaker + local-cache hybrid fallback.
//!
//! Ported from global-rate-limiter's Go `internal/pkg/circuit_breaker`
//! (the standalone breaker) and `internal/core/limiter.HybridLimiter` (the
//! breaker paired with a local cache) — see `CLAUDE.md`'s "Reference
//! Architecture" section for why that project sets the production bar
//! here rather than a generic tutorial implementation. The shape is the
//! same: [`CircuitBreaker`] tracks CLOSED / OPEN / `HALF_OPEN` exactly like
//! the Go version (`StateClosed` / `StateOpen` / `StateHalfOpen`,
//! `RecordSuccess`/`RecordFailure`, a failure threshold that opens the
//! circuit and a timeout that lets it try recovering), and
//! [`FallbackCache`] + [`HybridGuard`] play the role Go's `LocalCache` +
//! `HybridLimiter` played there: when the thing a call depends on is
//! unhealthy, serve the last known-good result instead of failing the
//! caller outright.
//!
//! What's different is the shape of the port, not the pattern: Go's
//! `HybridLimiter` is one concrete type wired directly to Redis and rate
//! limiting. Rust's ownership rules make a callback-heavy, mutex-guarded
//! struct like that awkward to write generically, and this crate has no
//! single "the downstream dependency" yet to hard-code against — no
//! network layer exists (`qaas-server` doesn't bind a socket until Phase
//! 4/8), so there's no HTTP or Redis client sitting here to wrap the way
//! Go wrapped its Redis client. What *does* exist, and is exactly the
//! kind of "replica or downstream dependency" this branch's `Plan.md`
//! entry names, is [`raft`](super::raft): a real, multi-node consensus
//! cluster whose leader can be unreachable or mid-election. So this
//! module ships the pattern as a generic, reusable primitive —
//! [`CircuitBreaker::call`] guards any fallible async operation,
//! [`HybridGuard::call`] pairs that with a cache keyed however the caller
//! wants — and [`raft::circuit_breaker_tests`](super::raft) proves it
//! against a real cluster: a producer's writes keep succeeding, served
//! from cache, while the replica it's talking to is down, and resume
//! coming through fresh once it recovers. It is deliberately *not* wired
//! permanently into [`ConsumerGroup`](crate::ConsumerGroup) or the raft
//! module's own public API — same "build the primitive, integrate it
//! later" split `raft`'s own docs describe for itself, since forcing a
//! permanent integration point today would mean guessing at a producer
//! API this crate doesn't have yet (no network layer to receive producer
//! calls from at all).

use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

/// Which of the three states a [`CircuitBreaker`] is currently in.
///
/// Transitions: `Closed` -> `Open` after `failure_threshold` consecutive
/// failures; `Open` -> `HalfOpen` once `open_duration` has elapsed since
/// opening; `HalfOpen` -> `Closed` after `half_open_success_threshold`
/// consecutive trial successes, or `HalfOpen` -> `Open` immediately on
/// the first trial failure. There is no direct `Closed` -> `HalfOpen` or
/// `Open` -> `Closed` edge — recovery always passes through a trial.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    /// Calls are attempted normally; consecutive failures count toward
    /// `failure_threshold`, reset to zero by any success.
    Closed,
    /// Calls are rejected without attempting the guarded operation at
    /// all — this is the whole point of a circuit breaker over a plain
    /// retry loop: stop hammering a dependency that's already down.
    Open,
    /// A limited trial period after `open_duration` has elapsed: calls
    /// are attempted again, but a single failure reopens the circuit
    /// immediately rather than accumulating toward the full
    /// `failure_threshold` again.
    HalfOpen,
}

/// Tunables for a [`CircuitBreaker`]. Field-for-field the same knobs as
/// Go's `circuit_breaker.Config` (`MaxFailures`, `Timeout`,
/// `MaxRequests`), renamed to read clearly at the call site rather than
/// staying abbreviated.
#[derive(Debug, Clone, Copy)]
pub struct CircuitBreakerConfig {
    /// Consecutive failures while `Closed` before the circuit opens.
    pub failure_threshold: u32,
    /// How long the circuit stays `Open` before allowing a `HalfOpen`
    /// trial.
    pub open_duration: Duration,
    /// Consecutive trial successes while `HalfOpen` before the circuit
    /// closes again.
    pub half_open_success_threshold: u32,
}

impl CircuitBreakerConfig {
    /// Matches Go's `DefaultConfig()`: five failures to open, a
    /// thirty-second cooldown, three trial successes to close again.
    pub const DEFAULT: Self = Self {
        failure_threshold: 5,
        open_duration: Duration::from_secs(30),
        half_open_success_threshold: 3,
    };
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Mutable state behind the [`Mutex`] — split out from
/// [`CircuitBreaker`] itself so `config` (immutable after construction)
/// doesn't need to sit behind the same lock as the fields that actually
/// change on every call.
#[derive(Debug)]
struct Inner {
    state: CircuitState,
    consecutive_failures: u32,
    consecutive_successes: u32,
    opened_at: Option<Instant>,
}

/// A 3-state circuit breaker guarding a fallible async operation.
///
/// Unlike the Go version, there's no `OnOpen`/`OnClose`/`OnHalfOpen`
/// callback API — this crate already has a structured way to observe
/// state changes ([`tracing`], used the same way [`super::raft::membership`]
/// logs reconciliation failures) rather than needing callers to register
/// closures for it. Call [`state`](Self::state) directly if a caller
/// needs to branch on the current state explicitly, or use
/// [`call`](Self::call) to have failures and successes recorded
/// automatically around an operation.
#[derive(Debug)]
pub struct CircuitBreaker {
    config: CircuitBreakerConfig,
    inner: Mutex<Inner>,
}

impl CircuitBreaker {
    /// Creates a breaker starting `Closed`, as every breaker does — there
    /// is no such thing as a circuit that's already tripped before its
    /// first failure.
    #[must_use]
    pub fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            inner: Mutex::new(Inner {
                state: CircuitState::Closed,
                consecutive_failures: 0,
                consecutive_successes: 0,
                opened_at: None,
            }),
        }
    }

    /// The current state, first resolving an elapsed `Open` ->
    /// `HalfOpen` transition if `open_duration` has passed. This mirrors
    /// Go's `IsOpen()`, which does the same lazy check-and-flip rather
    /// than running a background timer — a breaker nobody is calling
    /// doesn't need to tick.
    pub async fn state(&self) -> CircuitState {
        let mut inner = self.inner.lock().await;
        Self::resolve_open_timeout(&self.config, &mut inner);
        inner.state
    }

    /// If `inner` is `Open` and `open_duration` has elapsed since it
    /// opened, flips it to `HalfOpen` and resets the trial counters.
    /// Split out from [`state`](Self::state) so [`call`](Self::call) can
    /// reuse it under the same lock acquisition instead of taking the
    /// lock twice (once to check, once to act) and racing another
    /// caller's transition in between.
    fn resolve_open_timeout(config: &CircuitBreakerConfig, inner: &mut Inner) {
        if inner.state == CircuitState::Open {
            let elapsed = inner.opened_at.is_some_and(|at| at.elapsed() >= config.open_duration);
            if elapsed {
                inner.state = CircuitState::HalfOpen;
                inner.consecutive_failures = 0;
                inner.consecutive_successes = 0;
                tracing::info!("circuit breaker: OPEN -> HALF_OPEN (cooldown elapsed)");
            }
        }
    }

    /// Records a successful call. In `Closed`, resets the failure streak
    /// — a breaker only trips on *consecutive* failures, so one success
    /// among many failures means the dependency isn't reliably down. In
    /// `HalfOpen`, counts toward `half_open_success_threshold`; enough of
    /// them closes the circuit. A no-op in `Open`, since nothing calling
    /// this method should have run the guarded operation while open in
    /// the first place — see [`call`](Self::call).
    pub async fn record_success(&self) {
        let mut inner = self.inner.lock().await;
        match inner.state {
            CircuitState::Closed => inner.consecutive_failures = 0,
            CircuitState::HalfOpen => {
                inner.consecutive_successes += 1;
                if inner.consecutive_successes >= self.config.half_open_success_threshold {
                    inner.state = CircuitState::Closed;
                    inner.consecutive_failures = 0;
                    inner.consecutive_successes = 0;
                    tracing::info!("circuit breaker: HALF_OPEN -> CLOSED (trial succeeded)");
                }
            }
            CircuitState::Open => {}
        }
    }

    /// Records a failed call. In `Closed`, counts toward
    /// `failure_threshold`; enough of them opens the circuit. In
    /// `HalfOpen`, a *single* failure reopens it immediately — matching
    /// Go's `RecordFailure`, which treats any half-open failure as proof
    /// the dependency isn't actually recovered yet, rather than giving it
    /// another `failure_threshold`-sized budget to fail into.
    pub async fn record_failure(&self) {
        let mut inner = self.inner.lock().await;
        match inner.state {
            CircuitState::Closed => {
                inner.consecutive_failures += 1;
                if inner.consecutive_failures >= self.config.failure_threshold {
                    Self::open(&mut inner);
                }
            }
            CircuitState::HalfOpen => Self::open(&mut inner),
            CircuitState::Open => {}
        }
    }

    /// Transitions `inner` to `Open`, resetting both trial counters and
    /// stamping `opened_at` so [`resolve_open_timeout`](Self::resolve_open_timeout)
    /// knows when the cooldown started.
    fn open(inner: &mut Inner) {
        inner.state = CircuitState::Open;
        inner.opened_at = Some(Instant::now());
        inner.consecutive_failures = 0;
        inner.consecutive_successes = 0;
        tracing::warn!("circuit breaker: -> OPEN");
    }

    /// Runs `operation`, guarded by this breaker: skipped entirely (with
    /// [`CircuitBreakerError::Open`]) while the circuit is `Open`,
    /// otherwise attempted with the result fed back into
    /// [`record_success`](Self::record_success) /
    /// [`record_failure`](Self::record_failure) automatically. This is
    /// the version most callers want — [`HybridGuard::call`] is built on
    /// top of exactly this method rather than duplicating the
    /// check-then-record dance.
    ///
    /// # Errors
    ///
    /// [`CircuitBreakerError::Open`] if the circuit was open (`operation`
    /// was never called); [`CircuitBreakerError::Failed`] wrapping
    /// `operation`'s own error if it was called and failed.
    pub async fn call<F, Fut, T, E>(&self, operation: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        if self.state().await == CircuitState::Open {
            return Err(CircuitBreakerError::Open);
        }

        match operation().await {
            Ok(value) => {
                self.record_success().await;
                Ok(value)
            }
            Err(error) => {
                self.record_failure().await;
                Err(CircuitBreakerError::Failed(error))
            }
        }
    }
}

/// Why a [`CircuitBreaker::call`] didn't return a value.
#[derive(Debug, thiserror::Error)]
pub enum CircuitBreakerError<E> {
    /// The circuit was open; `operation` was never attempted.
    #[error("circuit breaker is open; call rejected without attempting the operation")]
    Open,
    /// `operation` was attempted and failed.
    #[error(transparent)]
    Failed(E),
}

/// A single cached value plus when it was written, so
/// [`FallbackCache::get`] can decide whether it's still within its TTL.
#[derive(Debug, Clone)]
struct CacheEntry<V> {
    value: V,
    inserted_at: Instant,
}

/// A generic, TTL'd in-memory cache — the "local cache" half of Go's
/// hybrid pattern, minus the rate-limiting-specific fields
/// (`LocalCacheEntry`'s `Count`/`Limit`/window-reset logic) that were
/// really `HybridLimiter`'s domain, not the cache's. What's kept is the
/// part that generalizes: remember the last value seen for a key, expire
/// it after a TTL so a fallback never serves data that's arbitrarily
/// stale, and forget it once expired rather than serving it anyway.
#[derive(Debug)]
pub struct FallbackCache<K, V> {
    ttl: Duration,
    entries: Mutex<HashMap<K, CacheEntry<V>>>,
}

impl<K: Eq + Hash, V: Clone> FallbackCache<K, V> {
    /// Creates an empty cache; entries written to it are served back for
    /// up to `ttl` before being treated as gone.
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self { ttl, entries: Mutex::new(HashMap::new()) }
    }

    /// The cached value for `key`, if one exists and is still within its
    /// TTL. An expired entry is removed as a side effect of being read —
    /// there's no separate sweep task, so a stale entry that's never
    /// looked up again would otherwise sit in the map forever; this
    /// keeps memory bounded by "keys someone has actually asked about"
    /// instead.
    pub async fn get(&self, key: &K) -> Option<V> {
        let mut entries = self.entries.lock().await;
        match entries.get(key) {
            Some(entry) if entry.inserted_at.elapsed() < self.ttl => Some(entry.value.clone()),
            Some(_) => {
                entries.remove(key);
                None
            }
            None => None,
        }
    }

    /// Stores `value` for `key`, overwriting whatever was there and
    /// resetting its TTL clock — a fresh successful call always
    /// supersedes an older cached one, never the other way around.
    pub async fn set(&self, key: K, value: V) {
        let mut entries = self.entries.lock().await;
        entries.insert(key, CacheEntry { value, inserted_at: Instant::now() });
    }
}

/// The result of a [`HybridGuard::call`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HybridOutcome<T, E> {
    /// The guarded operation ran and succeeded; `T` came straight from
    /// it, not the cache.
    Fresh(T),
    /// The operation was skipped (circuit open) or failed, but a
    /// previously cached value for the same key was still within its
    /// TTL and is served instead — this is the "producer degrades
    /// gracefully" outcome the branch is named for: the caller gets a
    /// usable value, just not necessarily an up-to-the-moment one.
    Cached(T),
    /// No value at all: nothing was cached for this key, and either the
    /// circuit was open (`None`) so the operation was never attempted,
    /// or it was attempted and failed (`Some`).
    Unavailable(Option<E>),
}

impl<T, E> HybridOutcome<T, E> {
    /// Collapses `Fresh` and `Cached` into one value — a caller that just
    /// wants "the best available answer, freshness aside" doesn't need
    /// to match both arms separately.
    #[must_use]
    pub fn into_value(self) -> Option<T> {
        match self {
            Self::Fresh(value) | Self::Cached(value) => Some(value),
            Self::Unavailable(_) => None,
        }
    }
}

/// Pairs a [`CircuitBreaker`] with a [`FallbackCache`]: the combination
/// Go's `HybridLimiter` hard-coded around Redis, generalized to any
/// keyed, fallible async operation. `K` is whatever a caller uses to
/// identify "the same logical thing" across calls (a queue name, a
/// producer id — [`raft::circuit_breaker_tests`](super::raft) uses the
/// Raft cluster's own key from its `Request::Set` writes); `V` is the
/// value produced.
pub struct HybridGuard<K, V> {
    breaker: CircuitBreaker,
    cache: FallbackCache<K, V>,
}

impl<K: Eq + Hash + Clone, V: Clone> HybridGuard<K, V> {
    /// Creates a guard with its own breaker (configured by `breaker_config`)
    /// and its own cache (entries expire after `cache_ttl`) — the two
    /// are independent knobs: how quickly to give up on the primary
    /// operation versus how long a fallback answer stays trustworthy
    /// once given up on.
    #[must_use]
    pub fn new(breaker_config: CircuitBreakerConfig, cache_ttl: Duration) -> Self {
        Self { breaker: CircuitBreaker::new(breaker_config), cache: FallbackCache::new(cache_ttl) }
    }

    /// The guarded breaker's current state, for a caller that wants to
    /// report or log it (a health endpoint, once one exists) without
    /// going through a full [`call`](Self::call).
    pub async fn breaker_state(&self) -> CircuitState {
        self.breaker.state().await
    }

    /// Attempts `primary`, guarded by this breaker, keyed by `key`.
    ///
    /// - Circuit open: `primary` is never called; serves
    ///   [`HybridOutcome::Cached`] if `key` has a live cache entry, else
    ///   [`HybridOutcome::Unavailable`] with `None`.
    /// - `primary` succeeds: records the success, caches the value under
    ///   `key`, returns [`HybridOutcome::Fresh`].
    /// - `primary` fails: records the failure (which may open the
    ///   circuit for subsequent calls), then falls back the same way the
    ///   open-circuit path does — cached value if one exists, otherwise
    ///   [`HybridOutcome::Unavailable`] with the error.
    pub async fn call<F, Fut, E>(&self, key: K, primary: F) -> HybridOutcome<V, E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<V, E>>,
    {
        if self.breaker.state().await == CircuitState::Open {
            return self.fall_back(key, None).await;
        }

        match primary().await {
            Ok(value) => {
                self.breaker.record_success().await;
                self.cache.set(key, value.clone()).await;
                HybridOutcome::Fresh(value)
            }
            Err(error) => {
                self.breaker.record_failure().await;
                self.fall_back(key, Some(error)).await
            }
        }
    }

    /// Shared tail of [`call`](Self::call)'s open-circuit and
    /// operation-failed paths: both want "serve the cache if there's
    /// something there, otherwise report unavailable with whatever error
    /// (if any) is on hand."
    async fn fall_back<E>(&self, key: K, error: Option<E>) -> HybridOutcome<V, E> {
        match self.cache.get(&key).await {
            Some(value) => HybridOutcome::Cached(value),
            None => HybridOutcome::Unavailable(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;

    use super::{
        CircuitBreaker, CircuitBreakerConfig, CircuitBreakerError, CircuitState, FallbackCache,
        HybridGuard, HybridOutcome,
    };

    fn fast_config() -> CircuitBreakerConfig {
        // Real-millisecond durations, not `Duration::ZERO` — a zero
        // cooldown would make every `Open` call immediately eligible for
        // `resolve_open_timeout`, which defeats the point of the test
        // cases that specifically want to observe `Open` persisting
        // until an explicit sleep past the cooldown.
        CircuitBreakerConfig {
            failure_threshold: 3,
            open_duration: Duration::from_millis(50),
            half_open_success_threshold: 2,
        }
    }

    #[tokio::test]
    async fn starts_closed() {
        let breaker = CircuitBreaker::new(fast_config());
        assert_eq!(breaker.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn opens_after_reaching_the_failure_threshold_not_before() {
        let breaker = CircuitBreaker::new(fast_config());
        breaker.record_failure().await;
        breaker.record_failure().await;
        assert_eq!(breaker.state().await, CircuitState::Closed, "two failures, threshold is three");

        breaker.record_failure().await;
        assert_eq!(breaker.state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn a_success_amid_failures_resets_the_streak() {
        let breaker = CircuitBreaker::new(fast_config());
        breaker.record_failure().await;
        breaker.record_failure().await;
        breaker.record_success().await;
        // Streak reset: two more failures should not be enough to open,
        // since the count started over.
        breaker.record_failure().await;
        breaker.record_failure().await;
        assert_eq!(breaker.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn open_transitions_to_half_open_only_after_the_cooldown_elapses() {
        let breaker = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            breaker.record_failure().await;
        }
        assert_eq!(breaker.state().await, CircuitState::Open);

        // Well before the 50ms cooldown: still open.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(breaker.state().await, CircuitState::Open);

        // Comfortably past it: now half-open. The margin above 50ms is
        // generous on purpose — this crate has already hit flaky timing
        // tests from cutting it too close under parallel test-suite
        // load, see `dead_letter`'s tests for the precedent.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(breaker.state().await, CircuitState::HalfOpen);
    }

    #[tokio::test]
    async fn half_open_closes_after_enough_consecutive_trial_successes() {
        let breaker = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            breaker.record_failure().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(breaker.state().await, CircuitState::HalfOpen);

        breaker.record_success().await;
        assert_eq!(breaker.state().await, CircuitState::HalfOpen, "one success, threshold is two");
        breaker.record_success().await;
        assert_eq!(breaker.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn half_open_reopens_immediately_on_a_single_failure() {
        let breaker = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            breaker.record_failure().await;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(breaker.state().await, CircuitState::HalfOpen);

        breaker.record_success().await;
        breaker.record_failure().await;
        // Reopened despite the one prior trial success not being anywhere
        // near the (unrelated, closed-state) failure threshold — half-open
        // has zero tolerance, by design.
        assert_eq!(breaker.state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn call_skips_the_operation_entirely_while_open() {
        let breaker = CircuitBreaker::new(fast_config());
        for _ in 0..3 {
            breaker.record_failure().await;
        }
        assert_eq!(breaker.state().await, CircuitState::Open);

        let attempts = AtomicU32::new(0);
        let result = breaker
            .call(|| async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>("unused")
            })
            .await;

        assert!(matches!(result, Err(CircuitBreakerError::Open)));
        assert_eq!(attempts.load(Ordering::SeqCst), 0, "operation must not run while open");
    }

    #[tokio::test]
    async fn call_records_success_and_failure_automatically() {
        let breaker = CircuitBreaker::new(fast_config());

        let ok: Result<_, CircuitBreakerError<&str>> = breaker.call(|| async { Ok("value") }).await;
        assert_eq!(ok.unwrap(), "value");

        for _ in 0..3 {
            let failed = breaker.call(|| async { Err::<&str, _>("boom") }).await;
            assert!(matches!(failed, Err(CircuitBreakerError::Failed("boom"))));
        }
        assert_eq!(breaker.state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn fallback_cache_serves_a_value_within_its_ttl() {
        let cache = FallbackCache::new(Duration::from_secs(10));
        assert_eq!(cache.get(&"key").await, None);

        cache.set("key", "value").await;
        assert_eq!(cache.get(&"key").await, Some("value"));
    }

    #[tokio::test]
    async fn fallback_cache_forgets_an_expired_entry() {
        let cache = FallbackCache::new(Duration::from_millis(20));
        cache.set("key", "value").await;
        assert_eq!(cache.get(&"key").await, Some("value"));

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(cache.get(&"key").await, None);
    }

    #[tokio::test]
    async fn hybrid_guard_returns_fresh_on_a_successful_primary_call() {
        let guard: HybridGuard<&str, &str> =
            HybridGuard::new(fast_config(), Duration::from_secs(10));
        let outcome = guard.call("key", || async { Ok::<_, &str>("value") }).await;
        assert_eq!(outcome, HybridOutcome::Fresh("value"));
    }

    #[tokio::test]
    async fn hybrid_guard_falls_back_to_the_cache_once_the_circuit_opens() {
        let guard: HybridGuard<&str, &str> =
            HybridGuard::new(fast_config(), Duration::from_secs(10));

        // Seed the cache with one real success before the dependency
        // "goes down" — this is the value a degraded producer should
        // keep being served.
        let seeded = guard.call("key", || async { Ok::<_, &str>("last-good") }).await;
        assert_eq!(seeded, HybridOutcome::Fresh("last-good"));

        // Three consecutive failures reaches `fast_config`'s threshold
        // and opens the circuit.
        for _ in 0..3 {
            let outcome = guard.call("key", || async { Err::<&str, _>("unhealthy") }).await;
            // Cache already has a value, so even the calls that trip the
            // breaker degrade to `Cached` rather than surfacing the raw
            // error straight to the caller.
            assert_eq!(outcome, HybridOutcome::Cached("last-good"));
        }
        assert_eq!(guard.breaker_state().await, CircuitState::Open);

        // Now open: the primary closure must not run at all, and the
        // cached value keeps being served.
        let attempts = AtomicU32::new(0);
        let outcome = guard
            .call("key", || async {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok::<_, &str>("should not run")
            })
            .await;
        assert_eq!(outcome, HybridOutcome::Cached("last-good"));
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn hybrid_guard_reports_unavailable_with_no_cache_and_no_success_yet() {
        let guard: HybridGuard<&str, &str> =
            HybridGuard::new(fast_config(), Duration::from_secs(10));

        // Nothing has ever succeeded for this key — a fresh producer
        // whose very first calls are the ones failing has no last-good
        // value to degrade to, and should be told that plainly rather
        // than getting back some invented placeholder.
        for _ in 0..2 {
            let outcome = guard.call("key", || async { Err::<&str, _>("down") }).await;
            assert_eq!(outcome, HybridOutcome::Unavailable(Some("down")));
        }
        let outcome = guard.call("key", || async { Err::<&str, _>("down") }).await;
        assert_eq!(outcome, HybridOutcome::Unavailable(Some("down")));
        assert_eq!(guard.breaker_state().await, CircuitState::Open);

        let outcome = guard.call("key", || async { Ok::<_, &str>("too late") }).await;
        assert_eq!(
            outcome,
            HybridOutcome::Unavailable(None),
            "circuit open: primary not attempted"
        );
    }

    #[tokio::test]
    async fn hybrid_guard_serves_fresh_again_once_the_circuit_recovers() {
        let guard: HybridGuard<&str, &str> =
            HybridGuard::new(fast_config(), Duration::from_secs(10));
        guard.call("key", || async { Ok::<_, &str>("v1") }).await;
        for _ in 0..3 {
            guard.call("key", || async { Err::<&str, _>("down") }).await;
        }
        assert_eq!(guard.breaker_state().await, CircuitState::Open);

        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(guard.breaker_state().await, CircuitState::HalfOpen);

        // Two trial successes (this config's `half_open_success_threshold`)
        // closes the circuit, and each returns `Fresh` — a recovering
        // dependency should stop looking degraded the moment it's
        // actually answering again, not stay pinned to the cache.
        for _ in 0..2 {
            let outcome = guard.call("key", || async { Ok::<_, &str>("v2") }).await;
            assert_eq!(outcome, HybridOutcome::Fresh("v2"));
        }
        assert_eq!(guard.breaker_state().await, CircuitState::Closed);
    }
}
