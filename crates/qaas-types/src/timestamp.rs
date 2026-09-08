use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

/// Milliseconds since the Unix epoch.
///
/// A plain integer rather than a pulled-in datetime crate: nothing in
/// this crate needs calendar arithmetic, time zones, or formatting —
/// just an orderable, unambiguous, wire-stable instant. If a consumer
/// needs a calendar date out of one, that conversion belongs in the
/// consumer, not forced on every crate that depends on `qaas-types`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Timestamp(pub u64);

impl Timestamp {
    /// The current wall-clock time.
    ///
    /// # Panics
    ///
    /// Panics if the system clock reports a time before the Unix epoch —
    /// a misconfigured clock, not a condition any caller can meaningfully
    /// recover from.
    #[must_use]
    pub fn now() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock is set before the Unix epoch")
            .as_millis();
        // `u64` overflows at roughly the year 584 million — `unwrap_or`
        // here is unreachable in practice, not a real fallback.
        Self(u64::try_from(millis).unwrap_or(u64::MAX))
    }
}

#[cfg(test)]
mod tests {
    use super::Timestamp;

    #[test]
    fn now_is_strictly_after_the_epoch() {
        assert!(Timestamp::now().0 > 0);
    }

    #[test]
    fn now_does_not_go_backwards() {
        let first = Timestamp::now();
        let second = Timestamp::now();
        assert!(second >= first);
    }
}
