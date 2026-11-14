# One command instead of remembering Docker Compose flags/paths and the
# right cargo incantations. Requires Docker Compose v2 (`docker compose`,
# not the standalone v1 `docker-compose` binary) for the stack recipes,
# and the toolchain pinned in rust-toolchain.toml for the cargo recipes.
#
# `check` mirrors CI (.github/workflows/ci.yml), which in turn mirrors
# CLAUDE.md's verification checklist — CLAUDE.md is the actual source of
# truth for what "passing" means; this recipe just saves retyping it.
#
# `loom` and `miri` are deliberately NOT part of `check` or CI: both need
# a non-default toolchain setup (a nightly + the miri component, or a
# `--cfg` flag plus `--release` for loom's exhaustive search to finish in
# a reasonable time) and both are slow — minutes, not seconds, for a
# workspace this size. Run them deliberately, not on every push; see
# `crates/qaas-core/tests/loom_*.rs`'s own module docs for what each
# actually checks and why it's scoped to qaas-core specifically.
#
# Note: `just --list` only shows the single comment line directly above
# each recipe, so keep those one-liners — put anything longer here in
# the file header instead of over a recipe.

compose := "docker compose -f docker/docker-compose.yml"

# List available recipes (what runs when you type `just` with no args)
default:
    @just --list

# Bring up the dev stack (queue node + Prometheus + Grafana), rebuilding on source changes
up:
    {{ compose }} up --build

# Same as `up`, but detached
up-detached:
    {{ compose }} up --build -d

# Stop and remove the dev stack's containers and network (volumes survive)
down:
    {{ compose }} down

# Stop the dev stack and delete its volumes too — a clean slate
clean:
    {{ compose }} down -v

# Tail logs from every service in the dev stack
logs:
    {{ compose }} logs -f

# Run the exact checks CI runs, in the same order
check:
    cargo fmt --all -- --check
    cargo clippy --workspace --all-targets --all-features -- -D warnings
    cargo build --workspace
    cargo test --workspace

# Auto-fix formatting
fmt:
    cargo fmt --all

# Exhaustive interleaving check for qaas-core's locking models (slow, not part of `check` — see tests/loom_*.rs)
loom:
    RUSTFLAGS="--cfg qaas_loom" cargo test --release -p qaas-core --test loom_fifo --test loom_consumer_group

# Run qaas-core's tests under Miri (slow, not part of `check`; needs `rustup component add --toolchain nightly miri`)
miri:
    MIRIFLAGS="-Zmiri-disable-isolation" cargo +nightly miri test -p qaas-core
