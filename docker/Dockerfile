# Dev/test image for the qaas-server broker.
#
# This intentionally does the simple thing: copy the whole workspace and
# `cargo build` in one layer, so every source change invalidates the
# build cache instead of only dependency changes doing so. A real
# dependency-caching build (e.g. via cargo-chef) is explicitly
# feature/docker-ha-cluster's job (Phase 8) — that branch owns the
# production image's build time and size budget. Here, for local dev,
# correctness and a short Dockerfile win over build-time optimization.

# --- Build stage ---------------------------------------------------------
FROM rust:1.95-slim-bookworm AS builder
WORKDIR /workspace

# Matches this workspace's rust-toolchain.toml channel (stable). Bump
# alongside that file if it ever pins something other than "stable".
COPY . .
RUN cargo build --release --package qaas-server

# --- Runtime stage --------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Run as a non-root user — basic container hygiene, not a Phase 8
# hardening concern.
RUN useradd --system --create-home --uid 10001 qaas

COPY --from=builder --chown=qaas:qaas \
    /workspace/target/release/qaas-server /usr/local/bin/qaas-server

USER qaas
ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/qaas-server"]
