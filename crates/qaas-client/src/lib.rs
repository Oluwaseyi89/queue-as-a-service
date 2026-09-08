//! Rust client SDK for QaaS.
//!
//! This is the crate an external Rust application (or another crate in
//! this workspace, for integration tests) depends on to produce and
//! consume messages against a running `qaas-server`, without needing to
//! know the wire protocol directly. It depends only on `qaas-types`, never
//! on `qaas-core` or `qaas-server` — a client SDK that could accidentally
//! pull in the queue engine or the broker binary would be a sign the
//! crate boundaries are wrong.
//!
//! Empty for now: the actual producer/consumer API is built alongside the
//! server's network interface in `feature/mcp-server-interface` and
//! `feature/client-sdks`, once there's a real protocol to speak.
