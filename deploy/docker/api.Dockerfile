# Dockerfile — mas-api (multi-stage, hardened)
# Build stage
FROM rust:1.98.1-bookworm AS builder
WORKDIR /build
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq-dev pkg-config protobuf-compiler ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY enterprise-mas/rust-core/Cargo.toml enterprise-mas/rust-core/Cargo.lock ./
COPY enterprise-mas/rust-core/crates ./crates
# `mas-persistence` embeds all 53 migrations with
# `include_str!("../../../migrations/<id>/up.sql")`, so the directory is
# needed at compile time, not only at runtime.
COPY enterprise-mas/rust-core/migrations ./migrations
COPY enterprise-mas/rust-core/benches ./benches
# /build/target is a build cache: BuildKit detaches it when the RUN ends and
# it never enters the image layer. The binary must be copied to a normal
# path inside this same step, otherwise the runtime stage's
# `COPY --from=builder` fails with "no such file or directory".
#
# `-p mas-api` rather than `--workspace`: only this crate's binary ships, and
# building all 22 members multiplied the release-compile memory peak (thin
# LTO against a 22-crate graph) until rustc was OOM-killed in a 2 GB
# container. Both commands produce a byte-identical mas-api binary; the
# narrower one just does not compile seven other service binaries nobody
# copies out of this image.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build -p mas-api --locked --release \
    && cp target/release/mas-api /usr/local/bin/mas-api

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r mas && useradd -r -g mas -s /sbin/nologin mas
WORKDIR /app
COPY --from=builder /usr/local/bin/mas-api /usr/local/bin/mas-api
USER mas
ENV MAS_ENV=production
EXPOSE 8080 50051
# Liveness is probed over HTTP: `/v1/health/live` answers 200 whenever the
# process is serving, and it is unauthenticated for exactly this purpose.
# The previous probe ran `mas-api --health`, a flag the binary does not
# accept, so the container reported unhealthy forever and was restarted on
# a loop. The `|| exit 1` it carried was exec-form tail text that Docker
# silently discarded, which is why the breakage was invisible in review.
# `/dev/tcp` is a bash feature: the image's /bin/sh is dash, where the
# redirect fails with "Directory nonexistent" and the container is never
# healthy. Probe with curl over the real HTTP endpoint instead.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["curl", "-fsS", "-o", "/dev/null", "http://127.0.0.1:8080/v1/health/live"]
ENTRYPOINT ["/usr/local/bin/mas-api"]
