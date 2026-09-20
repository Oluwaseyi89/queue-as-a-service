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
# QAAS_DATA_DIR defaults to the relative "./data/queues" (see main.rs) -
# with no WORKDIR set, that resolves against the image's inherited
# default ("/"), which qaas can't write to, so every listener this
# process starts would crash-loop before ever binding a socket. Caught
# by actually running this image via `docker compose up`, not just
# reading the Dockerfile: `docker logs qaas-queue-node` showed exactly
# "failed to create queue data directory: Permission denied", restarting
# on Compose's own backoff forever. Reproduced (and the fix confirmed)
# against the already-built qaas-server binary directly, outside Docker,
# once a from-scratch container rebuild became infeasible in a
# memory-constrained sandbox: running it with a read-only directory as
# its own cwd reproduces the identical error; a writable one resolves it
# completely. --create-home above already gives qaas a writable home to
# put it in.
WORKDIR /home/qaas
ENV RUST_LOG=info

ENTRYPOINT ["/usr/local/bin/qaas-server"]
