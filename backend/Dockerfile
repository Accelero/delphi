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

COPY Cargo.toml ./
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
