//! Shared wire types for the QaaS workspace.
//!
//! `qaas-core`, `qaas-server`, and `qaas-client` all need to agree on the
//! same message envelope, identifiers, and error types — otherwise the
//! server and the SDK drift apart the moment either one changes its own
//! copy. Putting those types in their own crate, with no dependency on
//! `qaas-core` or `qaas-server`, means every other crate can depend on
//! `qaas-types` without pulling in the queue engine or networking code.
//!
//! This crate is intentionally empty right now. The actual message
//! envelope and its version-negotiation scheme are substantial enough to
//! deserve their own branch (`feature/message-schema-versioning`) rather
//! than being sketched out here and reworked later. Scaffolding this crate
//! first means that work has a stable home to land in without a
//! workspace-wide refactor.
