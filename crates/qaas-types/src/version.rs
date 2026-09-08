use serde::{Deserialize, Serialize};

/// The version an [`Envelope`](crate::Envelope) was encoded with.
///
/// Follows semver-style discipline, but only the `major` field actually
/// gates compatibility: a `minor` bump is a promise that the change is
/// additive (a new optional field, a new header) and safe for an older
/// reader to ignore — `serde_json` already drops unrecognized fields by
/// default, so an older reader genuinely can decode a newer-minor
/// message correctly without this type doing anything extra. A `major`
/// bump is the only thing that means "an old reader would misunderstand
/// this," and is the only thing [`is_compatible_with`](Self::is_compatible_with)
/// checks. `minor` is kept and carried on the wire anyway — as
/// information for logging, metrics, and future finer-grained decisions,
/// not as a compatibility gate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SchemaVersion {
    /// Bumped for a breaking change: a reader on a different major
    /// version cannot safely interpret this envelope.
    pub major: u16,
    /// Bumped for an additive, backward-compatible change.
    pub minor: u16,
}

impl SchemaVersion {
    /// The envelope schema version this build of QaaS writes and expects
    /// to read.
    pub const CURRENT: Self = Self { major: 1, minor: 0 };

    /// Constructs a version directly. Prefer [`CURRENT`](Self::CURRENT)
    /// for anything this build produces — this exists for tests and for
    /// constructing versions read off the wire.
    #[must_use]
    pub const fn new(major: u16, minor: u16) -> Self {
        Self { major, minor }
    }

    /// Whether a reader on `self`'s version can safely interpret an
    /// envelope written at `other`'s version. See this type's own docs
    /// for why that's a major-version-only check.
    #[must_use]
    pub const fn is_compatible_with(self, other: Self) -> bool {
        self.major == other.major
    }
}

#[cfg(test)]
mod tests {
    use super::SchemaVersion;

    #[test]
    fn same_major_is_compatible_regardless_of_minor() {
        let reader = SchemaVersion::new(1, 0);
        let older_minor = SchemaVersion::new(1, 0);
        let newer_minor = SchemaVersion::new(1, 7);
        assert!(reader.is_compatible_with(older_minor));
        assert!(reader.is_compatible_with(newer_minor));
    }

    #[test]
    fn different_major_is_never_compatible() {
        let reader = SchemaVersion::new(2, 0);
        let writer = SchemaVersion::new(1, 9);
        assert!(!reader.is_compatible_with(writer));
    }

    #[test]
    fn ordering_is_major_then_minor() {
        assert!(SchemaVersion::new(1, 9) < SchemaVersion::new(2, 0));
        assert!(SchemaVersion::new(1, 0) < SchemaVersion::new(1, 1));
    }
}
