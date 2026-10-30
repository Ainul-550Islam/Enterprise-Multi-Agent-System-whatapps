# Dockerfile — mas-orchestrator (Python, multi-stage, hardened)
FROM python:3.13-slim AS builder
WORKDIR /build
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*
COPY enterprise-mas/python-orchestrator/pyproject.toml ./
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --no-cache-dir --upgrade pip \
    && python3 -m pip install --no-cache-dir -e ".[pgvector,ingest]"

FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r mas && useradd -r -g mas -s /sbin/nologin mas
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app/src
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY enterprise-mas/python-orchestrator ./enterprise-mas/python-orchestrator
USER mas
ENV MAS_ENV=production
EXPOSE 9100
# gRPC private port only — never published to the internet.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python3 -c "import grpc; grpc.channel.Channel('127.0.0.1:9100')" || exit 1
ENTRYPOINT ["python3", "-m", "mas_orchestrator.main"]
