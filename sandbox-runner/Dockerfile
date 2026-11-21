# sandbox-runner — HMAC-gated snippet executor for the code tool.
#
# The image does not load seccomp itself. The runtime MUST apply the profile
# next to this file and keep the container off any network with a default
# route (an internal compose network is enough; `--network=none` would make
# POST /execute unreachable from the orchestrator).
#
#   docker run --rm \
#     --security-opt seccomp=sandbox-runner/seccomp.json \
#     --memory=64m --cpus=1 --pids-limit=64 \
#     -e MAS_SANDBOX_HMAC_KEY \
#     -p 8090:8090 \
#     mas-sandbox
#
# Unsigned /execute is refused (503 without the key, 401 with a bad tag).
FROM python:3.12-slim-bookworm
RUN groupadd -r sandbox && useradd -r -g sandbox -s /sbin/nologin sandbox \
 && mkdir -p /app /work && chown sandbox:sandbox /work
WORKDIR /app
COPY runner.py seccomp.json /app/
USER sandbox
EXPOSE 8090
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/app/runner.py", "--serve", "--bind", "0.0.0.0", "--port", "8090"]
