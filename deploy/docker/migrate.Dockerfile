# Dockerfile — mas-migrate (one-shot migration runner)
#
# Builds the `mas_migrate` example from `mas-persistence` (which is why the
# builder copies the whole workspace below: `mas-persistence` is a member of the
# workspace, so Cargo must be able to load every member manifest even when only
# one package is compiled).
FROM rust:1.98.1-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq-dev pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY enterprise-mas/rust-core/Cargo.toml enterprise-mas/rust-core/Cargo.lock ./
COPY enterprise-mas/rust-core/crates ./crates
COPY enterprise-mas/rust-core/benches ./benches
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
