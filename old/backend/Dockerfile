# Multi-stage Rust build for the delphi backend binary.
# Production deploy will additionally bake the frontend's `dist/` into
# /app/static — handled by the root Dockerfile / CI, not here.
#
# Requires BuildKit (default in modern Docker / Compose v2) for the
# `--mount=type=cache` directives below.

# syntax=docker/dockerfile:1.7
FROM rust:1.95-slim-bookworm AS build
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

# `BACKEND_FEATURES` selects cargo features at build time. Default is empty
# (production-shaped build with NO dev-auth). The dev compose passes
# `dev-auth` so local dev gets the dev-mode auth bypass.
ARG BACKEND_FEATURES=""

# Persistent caches shared across builds (including T1 with `dev-auth` and
# T2 without): cargo's registry/git stores, and the `target/` dir. Cargo
# fingerprints by feature set, so artifacts for different feature combos
# coexist in one target dir without conflict. `sharing=locked` serializes
# concurrent builds on the same cache so the two stacks don't race; the
# second build then reuses everything the first compiled.
#
# The deps-stub layer below still earns its keep on cold cache: Docker
# layer caching keys on (Cargo.toml, Cargo.lock, BACKEND_FEATURES), so a
# source-only edit skips the deps step entirely. The cache mounts make
# Cargo.toml edits incremental on top of that.
COPY Cargo.toml Cargo.lock ./
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=delphi-cargo-registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,id=delphi-cargo-git,sharing=locked \
    --mount=type=cache,target=/app/target,id=delphi-target,sharing=locked \
    mkdir src \
 && echo "fn main() {}" > src/main.rs \
 && echo ""              > src/lib.rs \
 && cargo build --release --bin delphi ${BACKEND_FEATURES:+--features ${BACKEND_FEATURES}} \
 && rm -rf src \
            target/release/deps/delphi-* target/release/deps/libdelphi-* \
            target/release/delphi target/release/delphi.d

COPY src ./src
COPY schema.surql ./schema.surql

# The `target/` cache mount is empty in the resulting image layer, so the
# binary is copied out to a stable path within the same RUN step.
RUN --mount=type=cache,target=/usr/local/cargo/registry,id=delphi-cargo-registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,id=delphi-cargo-git,sharing=locked \
    --mount=type=cache,target=/app/target,id=delphi-target,sharing=locked \
    cargo build --release --bin delphi ${BACKEND_FEATURES:+--features ${BACKEND_FEATURES}} \
 && cp target/release/delphi /delphi

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    poppler-utils \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /delphi /usr/local/bin/delphi

EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/delphi"]
CMD ["serve"]
