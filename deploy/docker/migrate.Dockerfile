# Dockerfile — mas-migrate (one-shot migration runner)
FROM rust:1.98.1-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq-dev pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
# The workspace manifest lists every member, so Cargo resolves the whole graph
# before it builds one crate: copying only the four crates this binary needs
# fails with "failed to load manifest for workspace member
# /build/crates/orchestration". Copy the workspace manifest set and the crates
# that participate in the build.
COPY enterprise-mas/rust-core/Cargo.toml enterprise-mas/rust-core/Cargo.lock ./
COPY enterprise-mas/rust-core/crates ./crates
# `mas-persistence` embeds every migration with
# `include_str!("../../../migrations/<id>/up.sql")`, which resolves from
# crates/persistence/src to the crate root's `migrations/`. That directory is
# part of the *build*, not only of the runtime image, so it has to be present
# before the library compiles — omitting it fails the build with one
# "couldn't read .../up.sql" error per migration.
COPY enterprise-mas/rust-core/migrations ./migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build -p mas-persistence --locked --release --example mas_migrate \
    && cp target/release/examples/mas_migrate /usr/local/bin/mas_migrate

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r mas && useradd -r -g mas -s /sbin/nologin mas
WORKDIR /app
COPY --from=builder /usr/local/bin/mas_migrate /usr/local/bin/mas_migrate
COPY enterprise-mas/rust-core/migrations ./migrations
COPY enterprise-mas/rust-core/scripts/provision-plane-roles.sh ./scripts/provision-plane-roles.sh
USER mas
ENV DATABASE_URL=""
ENTRYPOINT ["/usr/local/bin/mas_migrate"]
CMD ["up"]
