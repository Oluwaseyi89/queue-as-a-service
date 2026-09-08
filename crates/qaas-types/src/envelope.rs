use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::{IdempotencyKey, MessageId, SchemaVersion, Timestamp, TraceId};

/// The on-wire envelope every message carries, wrapped around an
/// arbitrary caller-supplied payload `T`.
///
/// This is the stable contract this branch exists to lock in: the queue
/// engine, `qaas-server`, every client SDK, and (later) a monitoring
/// dashboard all need to agree on this shape without depending on each
/// other directly. `qaas-core`'s `FifoQueue`/`PriorityQueue` (and their
/// WAL-backed wrappers) are generic over an arbitrary payload and don't
/// require this type — a caller is free to enqueue raw data instead.
/// Using `Envelope<T>` as that payload is how idempotency, tracing, and
/// versioned headers actually reach the queue engine and (eventually)
/// the network layer.
///
/// `BTreeMap` for `headers`, not `HashMap`: this is a wire format, and a
/// deterministic field order makes encoded output stable across runs —
/// useful for tests, logs, and anything that hashes or diffs an encoded
/// envelope later, at no real cost for the small header counts a message
/// envelope actually carries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope<T> {
    /// The schema version this envelope was encoded with. Checked by
    /// [`Envelope::from_json`] against [`SchemaVersion::CURRENT`] on
    /// decode — see [`SchemaVersion`] for the compatibility rule.
    pub version: SchemaVersion,
    /// This message's unique, time-ordered identifier.
    pub id: MessageId,
    /// When this envelope was created.
    pub created_at: Timestamp,
    /// Caller-supplied deduplication key, if any. See [`IdempotencyKey`].
    pub idempotency_key: Option<IdempotencyKey>,
    /// Distributed-tracing correlation ID, if any. See [`TraceId`].
    pub trace_id: Option<TraceId>,
    /// Free-form caller/consumer metadata — content type, tenant ID,
    /// originating agent ID, anything that isn't the payload itself but
    /// needs to travel with it. Extensible without a schema change: a
    /// new header is exactly the kind of additive change a minor version
    /// bump is for.
    pub headers: BTreeMap<String, String>,
    /// The message payload.
    pub payload: T,
}

impl<T> Envelope<T> {
    /// Wraps `payload` in a new envelope: the current schema version, a
    /// fresh [`MessageId`], the current time, no idempotency key, no
    /// trace ID, and no headers. Use the `with_*` methods to fill in the
    /// optional fields.
    #[must_use]
    pub fn new(payload: T) -> Self {
        Self {
            version: SchemaVersion::CURRENT,
            id: MessageId::new(),
            created_at: Timestamp::now(),
            idempotency_key: None,
            trace_id: None,
            headers: BTreeMap::new(),
            payload,
        }
    }

    /// Sets this envelope's idempotency key.
    #[must_use]
    pub fn with_idempotency_key(mut self, key: IdempotencyKey) -> Self {
        self.idempotency_key = Some(key);
        self
    }

    /// Sets this envelope's trace ID.
    #[must_use]
    pub fn with_trace_id(mut self, trace_id: TraceId) -> Self {
        self.trace_id = Some(trace_id);
        self
    }

    /// Adds a single header, overwriting any existing value for the same
    /// key.
    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }
}

impl<T: Serialize> Envelope<T> {
    /// Encodes this envelope as JSON.
    ///
    /// # Errors
    ///
    /// Returns an error if `T`'s `Serialize` implementation fails.
    pub fn to_json(&self) -> Result<Vec<u8>, EnvelopeError> {
        serde_json::to_vec(self).map_err(EnvelopeError::Serialize)
    }
}

impl<T: DeserializeOwned> Envelope<T> {
    /// Decodes an envelope from JSON, rejecting it if its schema version
    /// isn't compatible with [`SchemaVersion::CURRENT`].
    ///
    /// This is the actual version-negotiation check: without it,
    /// decoding would happily accept bytes written under a future,
    /// incompatible major version and hand the caller a `T` built from
    /// fields that mean something different than this build assumes.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeError::Deserialize`] if `bytes` isn't valid JSON
    /// or doesn't match this shape, or
    /// [`EnvelopeError::IncompatibleVersion`] if it parses fine but was
    /// written under an incompatible major version.
    pub fn from_json(bytes: &[u8]) -> Result<Self, EnvelopeError> {
        let envelope: Self = serde_json::from_slice(bytes).map_err(EnvelopeError::Deserialize)?;
        if SchemaVersion::CURRENT.is_compatible_with(envelope.version) {
            Ok(envelope)
        } else {
            Err(EnvelopeError::IncompatibleVersion {
                found: envelope.version,
                current: SchemaVersion::CURRENT,
            })
        }
    }
}

/// An error encoding or decoding an [`Envelope`].
#[derive(Debug, thiserror::Error)]
pub enum EnvelopeError {
    /// The envelope's schema version is incompatible with this build's
    /// [`SchemaVersion::CURRENT`] — see [`SchemaVersion`] for the rule.
    #[error(
        "envelope schema version {found:?} is incompatible with this build's version {current:?}"
    )]
    IncompatibleVersion {
        /// The version found in the decoded envelope.
        found: SchemaVersion,
        /// The version this build understands.
        current: SchemaVersion,
    },
    /// The payload could not be serialized to JSON.
    #[error("failed to serialize envelope: {0}")]
    Serialize(#[source] serde_json::Error),
    /// The bytes could not be deserialized as an envelope.
    #[error("failed to deserialize envelope: {0}")]
    Deserialize(#[source] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::{Envelope, EnvelopeError};
    use crate::{IdempotencyKey, SchemaVersion, TraceId};

    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct Payload {
        text: String,
    }

    fn sample_payload() -> Payload {
        Payload { text: "hello".to_string() }
    }

    #[test]
    fn new_envelope_uses_the_current_schema_version() {
        let envelope = Envelope::new(sample_payload());
        assert_eq!(envelope.version, SchemaVersion::CURRENT);
    }

    #[test]
    fn round_trips_through_json() {
        let original = Envelope::new(sample_payload())
            .with_idempotency_key(IdempotencyKey::new("dedup-1").unwrap())
            .with_trace_id(TraceId::new())
            .with_header("tenant", "acme");

        let bytes = original.to_json().unwrap();
        let decoded = Envelope::<Payload>::from_json(&bytes).unwrap();

        assert_eq!(decoded.id, original.id);
        assert_eq!(decoded.payload, original.payload);
        assert_eq!(decoded.idempotency_key, original.idempotency_key);
        assert_eq!(decoded.trace_id, original.trace_id);
        assert_eq!(decoded.headers, original.headers);
    }

    #[test]
    fn decoding_an_incompatible_major_version_is_rejected() {
        let mut envelope = Envelope::new(sample_payload());
        envelope.version = SchemaVersion::new(SchemaVersion::CURRENT.major + 1, 0);
        let bytes = envelope.to_json().unwrap();

        let error = Envelope::<Payload>::from_json(&bytes).unwrap_err();
        assert!(matches!(error, EnvelopeError::IncompatibleVersion { .. }));
    }

    #[test]
    fn decoding_a_newer_compatible_minor_version_succeeds() {
        let mut envelope = Envelope::new(sample_payload());
        envelope.version = SchemaVersion::new(SchemaVersion::CURRENT.major, u16::MAX);
        let bytes = envelope.to_json().unwrap();

        assert!(Envelope::<Payload>::from_json(&bytes).is_ok());
    }

    #[test]
    fn an_unknown_field_in_the_wire_json_is_ignored_not_rejected() {
        // Simulates a message written by a future minor version that
        // added a field this build doesn't know about — the whole point
        // of minor versions being additive-only is that this must still
        // decode successfully.
        let mut envelope = Envelope::new(sample_payload());
        envelope.version = SchemaVersion::new(SchemaVersion::CURRENT.major, 1);
        let mut value = serde_json::to_value(&envelope).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("future_field".to_string(), serde_json::json!("surprise"));
        let bytes = serde_json::to_vec(&value).unwrap();

        let decoded = Envelope::<Payload>::from_json(&bytes).unwrap();
        assert_eq!(decoded.payload, sample_payload());
    }

    #[test]
    fn malformed_json_is_a_deserialize_error_not_a_panic() {
        let error = Envelope::<Payload>::from_json(b"not json").unwrap_err();
        assert!(matches!(error, EnvelopeError::Deserialize(_)));
    }
}
