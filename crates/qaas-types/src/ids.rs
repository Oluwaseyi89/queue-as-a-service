use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A message's unique identifier.
///
/// Backed by a `UUIDv7`, not v4: v7 embeds a millisecond timestamp in its
/// high bits, so IDs generated in order sort in the same order — useful
/// for anything that indexes or displays messages by ID (logs, a future
/// dashboard, a DLQ listing) without needing a separate sequence number
/// or a join against [`Timestamp`](crate::Timestamp) to order them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct MessageId(Uuid);

impl MessageId {
    /// Generates a new, time-ordered message ID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for MessageId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for MessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// A distributed-tracing correlation ID.
///
/// Opaque and unstructured for now — just a UUID a caller can thread
/// through logs and (once `feature/tracing-metrics` wires up
/// `OpenTelemetry`) spans. Deliberately not doing any W3C `traceparent`
/// formatting or validation here: that's real work belonging to the
/// branch that actually integrates a tracing backend, not this one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TraceId(Uuid);

impl TraceId {
    /// Generates a new trace ID.
    #[must_use]
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

/// A caller-supplied deduplication key.
///
/// Two enqueues carrying the same idempotency key represent the same
/// logical operation — critical for agent workloads, where a retried
/// tool-call must not be billed or executed twice. This type only
/// carries and validates the key; actually deduplicating on it is
/// `feature/idempotent-delivery`'s job, not this one's. Validation here
/// is deliberately minimal (non-empty) rather than opinionated about
/// format, since the right key shape is caller-defined (a request ID, a
/// content hash, whatever the caller's own idempotency boundary is).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IdempotencyKey(String);

impl IdempotencyKey {
    /// Wraps `key` as an idempotency key.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidIdempotencyKey`] if `key` is empty — an empty
    /// key would deduplicate every message with no explicit key against
    /// every other one, silently, which is never what a caller wants.
    pub fn new(key: impl Into<String>) -> Result<Self, InvalidIdempotencyKey> {
        let key = key.into();
        if key.is_empty() {
            return Err(InvalidIdempotencyKey);
        }
        Ok(Self(key))
    }

    /// The key as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// An idempotency key was empty.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("idempotency key must not be empty")]
pub struct InvalidIdempotencyKey;

#[cfg(test)]
mod tests {
    use super::{IdempotencyKey, MessageId, TraceId};

    #[test]
    fn message_ids_generated_in_order_sort_in_order() {
        let first = MessageId::new();
        let second = MessageId::new();
        assert!(second > first);
    }

    #[test]
    fn message_ids_are_unique() {
        assert_ne!(MessageId::new(), MessageId::new());
    }

    #[test]
    fn trace_ids_are_unique() {
        assert_ne!(TraceId::new(), TraceId::new());
    }

    #[test]
    fn empty_idempotency_key_is_rejected() {
        assert!(IdempotencyKey::new("").is_err());
    }

    #[test]
    fn non_empty_idempotency_key_round_trips() {
        let key = IdempotencyKey::new("order-42").unwrap();
        assert_eq!(key.as_str(), "order-42");
    }
}
