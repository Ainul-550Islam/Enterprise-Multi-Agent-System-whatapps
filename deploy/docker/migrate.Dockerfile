# Dockerfile — mas-migrate (one-shot migration runner)
FROM rust:1.98.1-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq-dev pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY enterprise-mas/rust-core/Cargo.toml enterprise-mas/rust-core/Cargo.lock ./
COPY enterprise-mas/rust-core/crates/persistence ./crates/persistence
COPY enterprise-mas/rust-core/crates/common ./crates/common
COPY enterprise-mas/rust-core/crates/domain ./crates/domain
COPY enterprise-mas/rust-core/crates/contracts ./crates/contracts
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build -p mas-persistence --locked --release --example mas_migrate

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r mas && useradd -r -g mas -s /sbin/nologin mas
WORKDIR /app
COPY --from=builder /build/target/release/examples/mas_migrate /usr/local/bin/mas_migrate
COPY enterprise-mas/rust-core/migrations ./migrations
COPY enterprise-mas/rust-core/scripts/provision-plane-roles.sh ./scripts/provision-plane-roles.sh
USER mas
ENV DATABASE_URL=""
ENTRYPOINT ["/usr/local/bin/mas_migrate"]
CMD ["up"]
