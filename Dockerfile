# 1. Build stage
FROM python:3.11-slim as builder

# Working directory
WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements before the rest of the code to leverage Docker cache
COPY requirements.txt .

# Install /root/.local
RUN pip install --no-cache-dir --user -r requirements.txt

# 2. Run time stage
FROM python:3.11-slim

# Metadata
LABEL maintainer="damodarabarbosa@gmail.com"
LABEL description="Camara dos deputados data extraction"

# Working directory
WORKDIR /app

# Copy only the necessary files from the builder stage
COPY --from=builder /root/.local /root/.local

# Copy source code
COPY src/ src/
COPY bundles/ bundles/

# Copy entry point script
COPY scripts/entry.sh /entry.sh
RUN chmod +x /entry.sh

# Python settings
ENV PATH=/root/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# App variables - Can be overridden at runtime with -e BUNDLE=<name>
ENV BUNDLE=proposicoes \
    RUN_ID=docker-run \
    DEBUG_MODE=false

# Entry point
ENTRYPOINT ["/entry.sh"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1
 