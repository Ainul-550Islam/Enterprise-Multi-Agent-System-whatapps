# Dockerfile — mas-worker (multi-stage, hardened)
FROM rust:1.98.1-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq-dev pkg-config ca-certificates \
      # NOTE: no `protobuf-compiler` package here. crates/api and
      # crates/ai-bridge build.rs resolve `protoc` from the vendored
      # `protoc_bin_vendored` crate, so the image does not depend on a
      # system protoc (and must not: the version would be unpinned).
    && rm -rf /var/lib/apt/lists/*
COPY enterprise-mas/rust-core/Cargo.toml enterprise-mas/rust-core/Cargo.lock ./
COPY enterprise-mas/rust-core/crates ./crates
COPY enterprise-mas/rust-core/benches ./benches
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build -p mas-worker -p mas-worker-service --locked --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      # procps supplies `pgrep`, which the HEALTHCHECK below calls. It is not
      # part of debian:bookworm-slim, so without it the check fails with
      # "executable file not found" and the container never reports healthy.
      libpq5 ca-certificates procps \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r mas && useradd -r -g mas -s /sbin/nologin mas
WORKDIR /app
COPY --from=builder /build/target/release/mas-worker /usr/local/bin/mas-worker
USER mas
ENV MAS_ENV=production
EXPOSE 9100
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD pgrep -x mas-worker >/dev/null || exit 1
ENTRYPOINT ["/usr/local/bin/mas-worker"]
