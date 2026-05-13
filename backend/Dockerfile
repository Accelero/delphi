# Multi-stage Rust build for the delphi backend binary.
# Production deploy will additionally bake the frontend's `dist/` into
# /app/static — handled by the root Dockerfile / CI, not here.

FROM rust:1.89-slim-bookworm AS build
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

# `BACKEND_FEATURES` selects cargo features at build time. Default is empty
# (production-shaped build with NO dev-auth). The dev compose passes
# `dev-auth` so local dev gets the dev-mode auth bypass.
ARG BACKEND_FEATURES=""

# Pre-build dependencies against stub sources so source-only edits don't
# trigger a full recompile of every crate. Cache key is (Cargo.toml,
# Cargo.lock, BACKEND_FEATURES); the real `src/` change below only
# recompiles the leaf `delphi` crate.
#
# Cargo has no native "deps-only build" command. The stub trick — feed it
# an empty lib + bin so it resolves and compiles the full dep graph, then
# delete the stub artifacts so cargo recompiles the leaf against real src
# — is the standard workaround. cargo-chef would also work; it's not
# worth the extra base image for a single-crate project.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
 && echo "fn main() {}" > src/main.rs \
 && echo ""              > src/lib.rs \
 && cargo build --release --bin delphi ${BACKEND_FEATURES:+--features ${BACKEND_FEATURES}} \
 && rm -rf src \
            target/release/deps/delphi-* target/release/deps/libdelphi-* \
            target/release/delphi target/release/delphi.d

COPY src ./src
COPY schema.surql ./schema.surql

RUN cargo build --release --bin delphi ${BACKEND_FEATURES:+--features ${BACKEND_FEATURES}}

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    poppler-utils \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/delphi /usr/local/bin/delphi

EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/delphi"]
CMD ["serve"]
