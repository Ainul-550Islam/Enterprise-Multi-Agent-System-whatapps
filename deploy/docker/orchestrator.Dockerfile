# Dockerfile — mas-orchestrator (Python, multi-stage, hardened)
FROM python:3.13-slim AS builder
WORKDIR /build
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*
COPY enterprise-mas/python-orchestrator/pyproject.toml enterprise-mas/python-orchestrator/README.md ./
COPY enterprise-mas/python-orchestrator/src ./src
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --no-cache-dir --upgrade pip \
    && python3 -m pip install --no-cache-dir ".[pgvector,ingest]" \
    && python3 -m pip check

FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r mas && useradd -r -g mas -s /sbin/nologin mas
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
USER mas
ENV MAS_ENV=production
EXPOSE 9100
# TCP liveness only: works with TLS/mTLS without embedding client credentials.
# Dependency readiness still requires the authenticated service health checks.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python3 -c "import socket; from mas_orchestrator.config.settings import load_settings; socket.create_connection(('127.0.0.1', load_settings().service.bind_port), timeout=2).close()"
ENTRYPOINT ["python3", "-m", "mas_orchestrator.main"]
