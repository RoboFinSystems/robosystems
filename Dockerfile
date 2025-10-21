# Stage 1: Builder
FROM python:3.12.10-slim AS builder

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_CACHE_DIR=/tmp/uv-cache \
    UV_LINK_MODE=copy

# Install system dependencies and uv using official installer
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv

# Copy the Kuzu httpfs extension from local archive with architecture support
# Extensions are bundled locally to eliminate network dependencies during build
ARG TARGETARCH=arm64
RUN mkdir -p /kuzu-extension/0.11.3/linux_${TARGETARCH}/httpfs
COPY bin/kuzu-extensions/v0.11.3/linux_${TARGETARCH}/httpfs/libhttpfs.kuzu_extension \
    /kuzu-extension/0.11.3/linux_${TARGETARCH}/httpfs/libhttpfs.kuzu_extension

# Verify extension integrity with checksum
RUN if [ "${TARGETARCH}" = "arm64" ]; then \
        echo "ea1b8f35234e57e961e1e0ca540769fc0192ff2e360b825a7e7b0e532f0f696e  /kuzu-extension/0.11.3/linux_arm64/httpfs/libhttpfs.kuzu_extension" | sha256sum -c - || \
        (echo "ERROR: ARM64 httpfs extension checksum verification failed!" && exit 1); \
    elif [ "${TARGETARCH}" = "amd64" ]; then \
        echo "f7ba3e34b801d8d023a5247f797b99f99fa6c4be104f6c9bbf4ae15d4c97d1da  /kuzu-extension/0.11.3/linux_amd64/httpfs/libhttpfs.kuzu_extension" | sha256sum -c - || \
        (echo "ERROR: AMD64 httpfs extension checksum verification failed!" && exit 1); \
    fi

WORKDIR /build

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock ./

# Install git for fetching EDGAR subtree
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies into project .venv (uv handles this automatically)
RUN --mount=type=cache,target=/tmp/uv-cache \
    uv sync --frozen --no-dev --no-install-project

# Copy source code and install project
COPY robosystems/ ./robosystems/
COPY main.py ./

# Copy pre-built cache bundles and cache manager script (required for build)
COPY robosystems/arelle/bundles/ ./robosystems/arelle/bundles/
COPY robosystems/scripts/arelle_cache_manager.py ./robosystems/scripts/

# Validate that required bundles exist before attempting extraction
RUN if [ ! -f "./robosystems/arelle/bundles/arelle-schemas-latest.tar.gz" ]; then \
        echo "ERROR: Schema bundle (arelle-schemas-latest.tar.gz) is missing!" && \
        echo "Run 'just cache-arelle-update' to generate bundles before building" && \
        exit 1; \
    fi

# Extract schemas from bundle and fetch EDGAR plugin from GitHub
RUN python robosystems/scripts/arelle_cache_manager.py extract && \
    python robosystems/scripts/arelle_cache_manager.py fetch-edgar
RUN --mount=type=cache,target=/tmp/uv-cache \
    uv sync --frozen --no-dev

# Stage 2: Runtime
FROM python:3.12.10-slim

# Accept architecture argument in runtime stage
ARG TARGETARCH=arm64

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/build/.venv/bin:$PATH" \
    ARELLE_CACHE_DIR="/app/robosystems/arelle/cache" \
    KUZU_HOME="/app/data/.kuzu"

# Install runtime dependencies and uv
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    postgresql-client \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv

# Copy virtual environment from builder stage
COPY --from=builder /build/.venv /build/.venv

# Set working directory
WORKDIR /app

# Copy application code first (includes arelle/bundles but not EDGAR/cache)
COPY robosystems/ /app/robosystems/
# Remove the incomplete arelle directory and replace with builder's complete version
RUN rm -rf /app/robosystems/arelle
# Copy builder's complete arelle directory (includes EDGAR + cache + bundles)
COPY --from=builder /build/robosystems/arelle/ /app/robosystems/arelle/
COPY main.py ./
COPY bin/ /app/bin/
# Copy static files for serving directly from container
COPY static/ /app/static/
# Copy alembic configuration and migrations
COPY alembic.ini /app/
COPY alembic/ /app/alembic/
# Copy configuration files
COPY .github/configs/graph.yml /app/configs/graph.yml
COPY .github/configs/stacks.yml /app/configs/stacks.yml

# Make entrypoint script executable
RUN chmod +x bin/entrypoint.sh

# Use non-root user for better security
RUN useradd -m appuser
# Ensure uv is accessible by appuser
RUN chown appuser:appuser /usr/local/bin/uv
# Create data directory and Kuzu home directory, set ownership for XBRL processing
RUN mkdir -p /app/data /app/data/.kuzu/extension && chown -R appuser:appuser /app/data
# Also create extension directory in appuser's home (where Kuzu looks for extensions)
RUN mkdir -p /home/appuser/.kuzu/extension && chown -R appuser:appuser /home/appuser/.kuzu
# Give appuser write access to /app for log files
RUN chown -R appuser:appuser /app

# Copy pre-downloaded Kuzu httpfs extension to user home directory
# Kuzu expects extensions at ~/.kuzu/extension/<extension_name>/ without version/arch subdirs
COPY --from=builder --chown=appuser:appuser /kuzu-extension/0.11.3/linux_${TARGETARCH}/httpfs /home/appuser/.kuzu/extension/httpfs

# Also copy to data location for consistency (optional, but keeps structure clean)
COPY --from=builder --chown=appuser:appuser /kuzu-extension/0.11.3/linux_${TARGETARCH}/httpfs /app/data/.kuzu/extension/httpfs

# Switch to non-root user
USER appuser

# Set the entrypoint
ENTRYPOINT ["/app/bin/entrypoint.sh"]
