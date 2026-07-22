# Stage 0: Extension Repository (pull LadybugDB extensions)
FROM ghcr.io/ladybugdb/extension-repo:latest AS extensions

# Stage 1: Builder
# Using Python 3.13 slim (Debian Trixie/13) for GLIBC 2.38+ required by LadybugDB extensions
FROM python:3.13-slim AS builder

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_CACHE_DIR=/tmp/uv-cache \
    UV_LINK_MODE=copy

# Install system dependencies, apply security patches, and install uv
RUN apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    libpq-dev \
    curl \
    unzip \
    file \
    && rm -rf /var/lib/apt/lists/* \
    && curl -LsSf https://astral.sh/uv/install.sh | sh \
    && mv /root/.local/bin/uv /usr/local/bin/uv

# Copy LadybugDB extensions from official extension repository
# Extensions pulled from ghcr.io/ladybugdb/extension-repo:latest
ARG TARGETARCH=arm64
# Extension version: pinned to match the ladybug Python package for ABI compatibility.
# This version is used for both the repo source path and the runtime install path.
ARG LADYBUG_EXT_VERSION=0.18.1

# Create extension directories using internal version (where LadybugDB looks)
# The duckdb extension is deliberately absent: materialization moved from
# DuckDB ATTACH to a parquet handoff (LadybugDB 0.14+ creates persistent
# shadow catalog entries on ATTACH that collide with installed schema).
RUN mkdir -p /ladybug-extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/httpfs \
             /ladybug-extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/vector

# Copy httpfs extension from extension repository (source: repo version, dest: internal version)
COPY --from=extensions \
    /usr/share/nginx/html/v${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/httpfs/libhttpfs.lbug_extension \
    /ladybug-extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/httpfs/libhttpfs.lbug_extension

# Copy vector extension (required for FLOAT[N] column support and vector indexes)
COPY --from=extensions \
    /usr/share/nginx/html/v${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/vector/libvector.lbug_extension \
    /ladybug-extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/vector/libvector.lbug_extension

# Verify LadybugDB extension integrity
# Basic integrity check: verify files exist, are non-empty, and are valid ELF binaries
RUN echo "Verifying LadybugDB extension integrity..." && \
    EXTENSIONS_FOUND=0 && \
    for ext in /ladybug-extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/*/*.lbug_extension; do \
        if [ ! -f "$ext" ]; then \
            echo "ERROR: Extension file not found: $ext" && exit 1; \
        fi; \
        if [ ! -s "$ext" ]; then \
            echo "ERROR: Extension file is empty: $ext" && exit 1; \
        fi; \
        if ! file "$ext" | grep -q "ELF.*shared object"; then \
            echo "ERROR: Extension is not a valid ELF shared library: $ext" && exit 1; \
        fi; \
        echo "✓ Valid extension: $(basename $ext)"; \
        EXTENSIONS_FOUND=$((EXTENSIONS_FOUND + 1)); \
    done && \
    if [ "$EXTENSIONS_FOUND" -lt 2 ]; then \
        echo "ERROR: Expected 2 extension files, found $EXTENSIONS_FOUND" && exit 1; \
    fi && \
    echo "Extension integrity verification complete ($EXTENSIONS_FOUND extensions validated)"

WORKDIR /build

# Copy dependency files first for better layer caching
COPY pyproject.toml uv.lock ./

# Install git for fetching EDGAR subtree
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies into project .venv (uv handles this automatically)
# Note: Cache mount disabled due to intermittent download issues
RUN uv sync --frozen --no-dev --no-install-project

# Copy source code and install project
COPY robosystems/ ./robosystems/
COPY main.py ./

# Copy pre-built cache bundles and cache manager script (required for build)
COPY robosystems/adapters/sec/arelle/bundles/ ./robosystems/adapters/sec/arelle/bundles/
COPY robosystems/scripts/arelle_cache_manager.py ./robosystems/scripts/

# Validate that required bundles exist before attempting extraction
RUN if [ ! -f "./robosystems/adapters/sec/arelle/bundles/arelle-schemas-latest.tar.gz" ]; then \
        echo "ERROR: Schema bundle (arelle-schemas-latest.tar.gz) is missing!" && \
        echo "Run 'just cache-arelle-update' to generate bundles before building" && \
        exit 1; \
    fi

# Extract schemas from bundle and fetch EDGAR plugin from GitHub
# EDGAR is pinned to a specific commit in arelle_cache_manager.py for reproducible builds
RUN python robosystems/scripts/arelle_cache_manager.py extract && \
    python robosystems/scripts/arelle_cache_manager.py fetch-edgar
RUN uv sync --frozen --no-dev

# Pre-cache fastembed model (BAAI/bge-small-en-v1.5) for XBRL semantic enrichment
# Downloads ~130MB model weights at build time so containers start without network dependency
ENV FASTEMBED_CACHE_PATH=/app/fastembed_cache
RUN .venv/bin/python -c "from fastembed import TextEmbedding; TextEmbedding('BAAI/bge-small-en-v1.5')"

# Stage 2: Runtime
# Using Python 3.13 slim (Debian Trixie/13) for GLIBC 2.38+ required by LadybugDB extensions
FROM python:3.13-slim

# Accept architecture argument in runtime stage
ARG TARGETARCH=arm64
# Must match builder stage — used for extension install paths
ARG LADYBUG_EXT_VERSION=0.18.1

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/build/.venv/bin:$PATH" \
    ARELLE_CACHE_DIR="/app/robosystems/adapters/sec/arelle/cache" \
    DAGSTER_HOME="/app/dagster_home" \
    FASTEMBED_CACHE_PATH="/app/fastembed_cache"

# Install runtime dependencies, apply security patches, and install uv.
# CACHE_DATE (set per-build in build.yml) busts this layer so the upgrade
# re-runs despite GHA layer caching — a cached layer keeps stale OS packages.
ARG CACHE_DATE
RUN echo "os-refresh ${CACHE_DATE}" && apt-get update && apt-get upgrade -y && apt-get install -y --no-install-recommends \
    libpq5 \
    libatomic1 \
    postgresql-client \
    curl \
    git \
    zstd \
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
RUN rm -rf /app/robosystems/adapters/sec/arelle
# Copy builder's complete arelle directory (includes EDGAR + cache + bundles)
COPY --from=builder /build/robosystems/adapters/sec/arelle/ /app/robosystems/adapters/sec/arelle/
# Strip vendored Node manifests from the bundled SEC EDGAR iXViewer. node_modules is
# never installed or executed in this Python image — only the compiled viewer assets
# are served — so these lockfiles serve only to trip Inspector's SBOM scanner on inert
# third-party deps (e.g. lodash, flatted). Removing them clears those container-image
# findings with no functional impact.
RUN find /app/robosystems/adapters/sec/arelle -type f \
    \( -name package-lock.json -o -name package.json -o -name yarn.lock \) -delete
# Copy reporting-framework library (top-level, peer to robosystems/;
# loaded by extensions migrations at provision time). Resolved via
# FRAMEWORKS_DIR in robosystems/taxonomy/discovery.py.
COPY frameworks/ /app/frameworks/
COPY main.py ./
COPY bin/ /app/bin/
# Copy static files for serving directly from container
COPY static/ /app/static/
# Copy database migrations (platform + ledger)
COPY migrations/ /app/migrations/
# Copy configuration files
COPY .github/configs/graph.yml /app/configs/graph.yml
COPY .github/configs/stacks.yml /app/configs/stacks.yml
# Copy Dagster configuration (production and development)
COPY dagster_home/ /app/dagster_home/

# Make entrypoint script executable
RUN chmod +x bin/entrypoint.sh

# Copy DuckDB shared library from builder (required by LadybugDB DuckDB extension)
# Use non-root user for better security
RUN useradd -m appuser
# Ensure uv is accessible by appuser
RUN chown appuser:appuser /usr/local/bin/uv
# Create data directory for persistent storage
RUN mkdir -p /app/data && chown -R appuser:appuser /app/data
# Create extension directory in appuser's home (where LadybugDB looks for extensions)
# Extensions are stored at ~/.lbdb/extension/{VERSION}/{PLATFORM}/{EXTENSION_NAME}/
# (the directory moved from ~/.lbug to ~/.lbdb in LadybugDB 0.15+)
# This is in the container filesystem, NOT persistent volume, so extensions refresh with each deploy
RUN mkdir -p /home/appuser/.lbdb/extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH} && chown -R appuser:appuser /home/appuser/.lbdb
# Give appuser write access to /app for log files
RUN chown -R appuser:appuser /app

# Copy pre-cached fastembed model from builder (avoids runtime download from Hugging Face)
COPY --from=builder --chown=appuser:appuser \
    /app/fastembed_cache /app/fastembed_cache

# Copy LadybugDB extensions to user home directory
# LadybugDB expects extensions at ~/.lbdb/extension/{VERSION}/{PLATFORM}/{EXTENSION_NAME}/
COPY --from=builder --chown=appuser:appuser \
    /ladybug-extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/httpfs \
    /home/appuser/.lbdb/extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/httpfs

COPY --from=builder --chown=appuser:appuser \
    /ladybug-extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/vector \
    /home/appuser/.lbdb/extension/${LADYBUG_EXT_VERSION}/linux_${TARGETARCH}/vector

# Switch to non-root user
USER appuser

# Set the entrypoint
ENTRYPOINT ["/app/bin/entrypoint.sh"]
