# Dockerfile — mas-orchestrator (Python, multi-stage, hardened)
#
# The builder runs in the application source root, not one level up: the package
# install (`-e .`) and the `[otel,metrics]` extras are resolved from the real
# `pyproject.toml` in `enterprise-mas/python-orchestrator`. Installing from a
# directory that contains the source tree without a `pyproject.toml` produced an
# image whose `python3 -m mas_orchestrator.main` could not import the package.
FROM python:3.13-slim AS builder
WORKDIR /build
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential libpq-dev \
    && rm -rf /var/lib/apt/lists/*
# Whole source tree: the package is built from `src/` via
# `[tool.setuptools.packages.find] where = ["src"]`.
COPY enterprise-mas/python-orchestrator ./
RUN --mount=type=cache,target=/root/.cache/pip \
    python3 -m pip install --no-cache-dir --upgrade pip \
    && python3 -m pip install --no-cache-dir ".[otel,metrics]"

FROM python:3.13-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
      libpq5 ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -r mas && useradd -r -g mas -s /sbin/nologin mas
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
# The package is installed into site-packages by the builder, so nothing here
# depends on a source path being on PYTHONPATH.
COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin/mas-orchestrator /usr/local/bin/mas-orchestrator
# Kept for operators who want to read the shipped configuration alongside the
# image; not on the import path.
COPY enterprise-mas/python-orchestrator ./enterprise-mas/python-orchestrator
USER mas
ENV MAS_ENV=production
EXPOSE 9100
# gRPC private port only — never published to the internet.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python3 -c "import mas_orchestrator.main" || exit 1
ENTRYPOINT ["python3", "-m", "mas_orchestrator.main"]
