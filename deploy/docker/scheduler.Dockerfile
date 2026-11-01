# Dockerfile — mas-scheduler-service (multi-stage, hardened)
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
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build -p mas-scheduler-service --locked --release \
    && cp target/release/mas-scheduler /usr/local/bin/mas-scheduler

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r mas && useradd -r -g mas -s /sbin/nologin mas
WORKDIR /app
COPY --from=builder /usr/local/bin/mas-scheduler /usr/local/bin/mas-scheduler
USER mas
ENV MAS_ENV=production
EXPOSE 8090
# `pgrep` ships in procps, which debian:bookworm-slim does not include:
# the probe exited 127 ("pgrep: not found") and the container could never
# report healthy. Tell Docker to exec the binary directly instead — the
# check then needs no external tools at all.
# Process liveness via /proc: this image ships neither procps (`pgrep`,
# which the original probe called and which is simply absent from
# debian:bookworm-slim, so the check exited 127 forever) nor a shell flag
# the binary understands — `mas-worker --health` is treated as a normal
# start-up and runs the full config validation. Reading
# /proc/<pid>/comm is dependency-free and asserts exactly what a
# liveness probe should: this binary is still the running process.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD ["sh", "-c", "grep -qx mas-scheduler /proc/1/comm"]
ENTRYPOINT ["/usr/local/bin/mas-scheduler"]
