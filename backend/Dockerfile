# Multi-stage Rust build for the delphi backend binary.
# Production deploy will additionally bake the frontend's `dist/` into
# /app/static — handled by the root Dockerfile / CI, not here.

FROM rust:1.89-slim-bookworm AS build
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
 && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml ./
COPY src ./src
COPY schema.surql ./schema.surql

RUN cargo build --release --bin delphi

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /app/target/release/delphi /usr/local/bin/delphi

EXPOSE 8081
ENTRYPOINT ["/usr/local/bin/delphi"]
CMD ["serve"]
