//! Exponential backoff with jitter for redelivery.
//!
//! Without this, [`ConsumerGroup`](crate::ConsumerGroup) would make a
//! failed or timed-out message claimable again *immediately* — which,
//! under a downstream outage (a struggling LLM provider, a rate-limited
//! API), means every consumer in the group re-claims and re-fails the
//! same messages in a tight loop, hammering the exact thing that's
//! already struggling. [`RetryPolicy`] fixes that by making redelivery
//! wait, longer after each successive failure, with randomized jitter so
//! many messages failing at once don't all become claimable again at
//! the same synchronized instant.
//!
//! Deliberately out of scope here: a maximum-attempts cutoff or routing
//! to a dead-letter destination once retries are exhausted. This policy
//! backs off forever — `feature/dead-letter-queue` is where "and after N
//! failures, give up" gets decided.

use std::time::Duration;

/// Exponential backoff with jitter, applied before a failed or expired
/// delivery becomes claimable again.
///
/// The delay before retry `n` (the `n`-th failure, 1-indexed) is
/// `base_delay * multiplier^(n-1)`, capped at `max_delay`, then
/// randomized down by up to `jitter` (a fraction of the capped value —
/// `0.0` leaves the delay untouched, `1.0` allows anything from zero up
/// to the full capped delay).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    /// The delay before the first retry.
    pub base_delay: Duration,
    /// The delay before any retry never exceeds this, however many
    /// times a message has failed.
    pub max_delay: Duration,
    /// How much longer each successive retry's delay is than the last,
    /// before capping. `2.0` doubles it every time.
    pub multiplier: f64,
    /// Fraction of the capped delay to randomize away, in `0.0..=1.0`.
    /// See this type's docs for the exact interpretation.
    pub jitter: f64,
}

/// Exponents beyond this are never reached in practice — any reasonable
/// `multiplier` and `base_delay` combination will already have hit
/// `max_delay` long before `delivery_count` gets anywhere near this
/// high. Capping the exponent here avoids computing (or overflowing
/// toward) an astronomically large float for no reason on a message
/// that has failed an unreasonable number of times.
const MAX_BACKOFF_EXPONENT: u32 = 32;

impl RetryPolicy {
    /// A reasonable default: 500ms base delay, doubling each time, capped
    /// at 60 seconds, with 20% jitter.
    pub const DEFAULT: Self = Self {
        base_delay: Duration::from_millis(500),
        max_delay: Duration::from_secs(60),
        multiplier: 2.0,
        jitter: 0.2,
    };

    /// The delay to wait before a message that has now failed
    /// `delivery_count` times (its delivery count *at the point of
    /// failure*, i.e. what [`Claim::delivery_count`](crate::Claim) was
    /// for the attempt that just failed) becomes claimable again.
    ///
    /// `delivery_count == 0` is treated the same as `1` — there's no
    /// such thing as backing off before the first delivery, which
    /// hasn't failed yet.
    #[must_use]
    pub fn delay_for(&self, delivery_count: u32) -> Duration {
        let exponent = delivery_count.saturating_sub(1).min(MAX_BACKOFF_EXPONENT);
        // `MAX_BACKOFF_EXPONENT` fits comfortably in an `i32`, so this
        // conversion never actually saturates in practice — `unwrap_or`
        // is here to make that assumption explicit, not a real fallback.
        let exponent = i32::try_from(exponent).unwrap_or(i32::MAX);

        let raw_secs = self.base_delay.as_secs_f64() * self.multiplier.powi(exponent);
        let capped_secs = raw_secs.min(self.max_delay.as_secs_f64());
        let jittered_secs = apply_jitter(capped_secs, self.jitter);

        Duration::from_secs_f64(jittered_secs.max(0.0))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Randomizes `capped_secs` down by up to `jitter` (clamped to
/// `0.0..=1.0`): the result is uniformly distributed in
/// `[capped_secs * (1 - jitter), capped_secs]`.
fn apply_jitter(capped_secs: f64, jitter: f64) -> f64 {
    let jitter = jitter.clamp(0.0, 1.0);
    let floor = capped_secs * (1.0 - jitter);
    floor + fastrand::f64() * (capped_secs - floor)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::RetryPolicy;

    #[test]
    fn first_retry_is_close_to_the_base_delay() {
        let policy = RetryPolicy { jitter: 0.0, ..RetryPolicy::DEFAULT };
        assert_eq!(policy.delay_for(1), policy.base_delay);
    }

    #[test]
    fn delay_grows_by_the_multiplier_each_time_before_capping() {
        let policy = RetryPolicy {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(3600),
            multiplier: 2.0,
            jitter: 0.0,
        };
        assert_eq!(policy.delay_for(1), Duration::from_millis(100));
        assert_eq!(policy.delay_for(2), Duration::from_millis(200));
        assert_eq!(policy.delay_for(3), Duration::from_millis(400));
        assert_eq!(policy.delay_for(4), Duration::from_millis(800));
    }

    #[test]
    fn delay_never_exceeds_max_delay_however_many_failures() {
        let policy = RetryPolicy {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
            jitter: 0.0,
        };
        assert_eq!(policy.delay_for(10), Duration::from_secs(5));
        assert_eq!(policy.delay_for(1000), Duration::from_secs(5));
        assert_eq!(policy.delay_for(u32::MAX), Duration::from_secs(5));
    }

    #[test]
    fn zero_delivery_count_behaves_like_one() {
        let policy = RetryPolicy { jitter: 0.0, ..RetryPolicy::DEFAULT };
        assert_eq!(policy.delay_for(0), policy.delay_for(1));
    }

    #[test]
    fn jitter_only_ever_shortens_the_delay_never_lengthens_it() {
        let policy = RetryPolicy {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(3600),
            multiplier: 1.0,
            jitter: 0.5,
        };
        for _ in 0..200 {
            let delay = policy.delay_for(1);
            assert!(delay <= Duration::from_secs(1));
            assert!(delay >= Duration::from_millis(500));
        }
    }

    #[test]
    fn full_jitter_can_reach_arbitrarily_close_to_zero() {
        let policy = RetryPolicy {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(3600),
            multiplier: 1.0,
            jitter: 1.0,
        };
        // Not a proof, but 500 samples landing under 100ms with a
        // uniform draw over [0, 1000ms) is astronomically likely to
        // happen if the implementation is correct, and astronomically
        // unlikely to happen by chance if jitter were secretly clamped
        // away or inverted.
        let saw_a_short_delay = (0..500).any(|_| policy.delay_for(1) < Duration::from_millis(100));
        assert!(saw_a_short_delay);
    }

    #[test]
    fn no_jitter_is_deterministic() {
        let policy = RetryPolicy { jitter: 0.0, ..RetryPolicy::DEFAULT };
        let first = policy.delay_for(3);
        for _ in 0..20 {
            assert_eq!(policy.delay_for(3), first);
        }
    }
}
