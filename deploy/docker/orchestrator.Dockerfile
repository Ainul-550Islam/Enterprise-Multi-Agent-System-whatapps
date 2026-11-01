# Dockerfile — mas-orchestrator (Python, multi-stage, hardened)
FROM python:3.13-slim AS builder
WORKDIR /build
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*
# The package is a `src/` layout, so an editable install needs the sources as
# well as the manifest: copying only `pyproject.toml` fails with
# "error in 'egg_base' option: 'src' does not exist or is not a directory".
# `src` and `README.md` are what setuptools needs to resolve metadata; the
# editable install only uses this tree for that, and the runtime stage ships
# its own copy below.
COPY enterprise-mas/python-orchestrator/pyproject.toml enterprise-mas/python-orchestrator/README.md ./
COPY enterprise-mas/python-orchestrator/src ./src
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --no-cache-dir --upgrade pip \
    && python3 -m pip install --no-cache-dir -e ".[pgvector,ingest]"

FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r mas && useradd -r -g mas -s /sbin/nologin mas
WORKDIR /app
# The package is copied to `/app/enterprise-mas/python-orchestrator`, so its
# import root is that directory's `src`. The previous `PYTHONPATH=/app/src`
# pointed at a directory that does not exist, and the container died at boot
# with `ModuleNotFoundError: No module named 'mas_orchestrator'` — the exact
# failure `ENTRYPOINT ["python3", "-m", "mas_orchestrator.main"]` would hit.
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/enterprise-mas/python-orchestrator/src
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY enterprise-mas/python-orchestrator ./enterprise-mas/python-orchestrator
USER mas
ENV MAS_ENV=production
EXPOSE 19200
# gRPC private port only — never published to the internet.
# The probe completes a TCP handshake against the configured port and
# closes it. It deliberately does not read: with mTLS enabled the server
# waits for a client certificate before it speaks, so a probe that demanded
# bytes would fail on exactly the hardened configuration that matters most.
# The previous probe only checked that `import grpc` succeeded, on port
# 9100 (the Rust worker's port, not this server's 19200), and called
# `grpc.channel(...)`, which is not a grpc-python API (the type is
# `grpc.Channel`) — it raised AttributeError, so the container reported
# unhealthy forever while serving correctly.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python3 -c "import socket,os,sys; s=socket.create_connection(('127.0.0.1',int(os.environ.get('MAS_ORCH_SERVICE__BIND_PORT','19200'))),5); s.settimeout(5); s.close()"
ENTRYPOINT ["python3", "-m", "mas_orchestrator.main"]
