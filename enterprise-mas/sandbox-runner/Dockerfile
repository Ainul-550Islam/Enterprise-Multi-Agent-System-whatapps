FROM python:3.12-slim AS builder

# Install system dependencies for sandboxing
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# Install FastAPI and httpx
RUN pip install --no-cache-dir fastapi uvicpx httpx python-multipart

# Install resource monitoring tools
RUN pip install --no-cache-dir psutil

# Final stage
FROM python:3.12-slim

# Create non-root user
RUN useradd --no-create-home --uid 1000 --shell /usr/sbin/nologin sandbox

# Copy installed packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /usr/local/lib /usr/local/lib

# Set working directory
WORKDIR /app

# Copy runner script
COPY runner.py .

# Set user to non-root
USER sandbox

# Expose the port
EXPOSE 8080

# Run the server
CMD ["uvicorn", "runner:app", "--host", "0.0.0.0", "--port", "8080"]
