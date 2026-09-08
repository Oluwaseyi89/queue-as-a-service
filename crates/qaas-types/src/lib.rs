//! Shared wire types for the QaaS workspace.
//!
//! `qaas-core`, `qaas-server`, and `qaas-client` all need to agree on the
//! same message envelope, identifiers, and error types — otherwise the
//! server and the SDK drift apart the moment either one changes its own
//! copy. Putting those types in their own crate, with no dependency on
//! `qaas-core` or `qaas-server`, means every other crate can depend on
//! `qaas-types` without pulling in the queue engine or networking code.
//!
//! [`Envelope`] (`feature/message-schema-versioning`) is the actual
//! contract: every message that crosses a boundary between crates —
//! or, once there's a network layer, between processes — is expected to
//! be wrapped in one. [`SchemaVersion`] is what makes that contract
//! survive changing over time: [`Envelope::from_json`] checks it on
//! decode and rejects anything encoded under an incompatible major
//! version, rather than silently misinterpreting it.

mod envelope;
mod ids;
mod timestamp;
mod version;

pub use envelope::{Envelope, EnvelopeError};
pub use ids::{IdempotencyKey, InvalidIdempotencyKey, MessageId, TraceId};
pub use timestamp::Timestamp;
pub use version::SchemaVersion;
