# Kuzu Extensions Archive

This directory contains pre-downloaded Kuzu extensions that are bundled into the Docker image to eliminate network dependencies during build and deployment.

## Extensions Included

### v0.11.3

#### httpfs
- **Purpose**: S3 and HTTP filesystem access
- **Used for**: SEC pipeline S3 bulk ingestion, remote file access
- **Platforms**: linux_arm64, linux_amd64

#### duckdb
- **Purpose**: DuckDB integration for direct data ingestion
- **Used for**: DuckDB → Kuzu direct ingestion (staging tables feature)
- **Platforms**: linux_arm64, linux_amd64
- **Added**: 2025-01-22

## Extension Source

Extensions are sourced from the official Kuzu extension repository:
```bash
docker pull ghcr.io/kuzudb/extension-repo:latest
```

## Adding New Extensions

To add a new extension:

1. **Pull the extension repository container:**
   ```bash
   docker pull ghcr.io/kuzudb/extension-repo:latest
   ```

2. **List available extensions:**
   ```bash
   docker run --rm ghcr.io/kuzudb/extension-repo:latest \
     ls -la /usr/share/nginx/html/v0.11.3/linux_arm64/
   ```

3. **Extract the extension:**
   ```bash
   # Create directory
   mkdir -p bin/kuzu-extensions/v0.11.3/linux_arm64/<extension_name>
   mkdir -p bin/kuzu-extensions/v0.11.3/linux_amd64/<extension_name>

   # Extract ARM64
   docker run --rm \
     -v $(pwd)/bin/kuzu-extensions/v0.11.3/linux_arm64/<extension_name>:/output \
     ghcr.io/kuzudb/extension-repo:latest \
     sh -c "cp /usr/share/nginx/html/v0.11.3/linux_arm64/<extension_name>/lib<extension_name>.kuzu_extension /output/"

   # Extract AMD64
   docker run --rm \
     -v $(pwd)/bin/kuzu-extensions/v0.11.3/linux_amd64/<extension_name>:/output \
     ghcr.io/kuzudb/extension-repo:latest \
     sh -c "cp /usr/share/nginx/html/v0.11.3/linux_amd64/<extension_name>/lib<extension_name>.kuzu_extension /output/"
   ```

4. **Generate checksums:**
   ```bash
   sha256sum bin/kuzu-extensions/v0.11.3/linux_arm64/<extension_name>/lib<extension_name>.kuzu_extension
   sha256sum bin/kuzu-extensions/v0.11.3/linux_amd64/<extension_name>/lib<extension_name>.kuzu_extension
   ```

5. **Update Dockerfile:**
   - Add mkdir for new extension directory (line ~23)
   - Add COPY command for extension files (after line ~32)
   - Add checksum verification (in RUN block around line ~35)
   - Add COPY to runtime stage (after line ~144)

6. **Test the build:**
   ```bash
   docker build --platform linux/arm64 -t test-image .
   docker build --platform linux/amd64 -t test-image .
   ```

## Checksums (v0.11.3)

### httpfs
- **ARM64**: `ea1b8f35234e57e961e1e0ca540769fc0192ff2e360b825a7e7b0e532f0f696e`
- **AMD64**: `f7ba3e34b801d8d023a5247f797b99f99fa6c4be104f6c9bbf4ae15d4c97d1da`

### duckdb
- **ARM64**: `268150b3c5691febfe2f7ddd5a92270b9946a7053eec29613d385f60c7ee8e56`
- **AMD64**: `f3c118567f1806298ceb05f24c6f3fcd40b3f5b5ef76f2286658c1804b779523`

## Extension Deployment Path

Extensions are deployed to two locations in the container:

1. **User home**: `/home/appuser/.kuzu/extension/<extension_name>/`
   - Primary location where Kuzu looks for extensions

2. **Data directory**: `/app/data/.kuzu/extension/<extension_name>/`
   - Backup location for consistency

## Usage in Application

### httpfs
```python
# Already loaded automatically by KuzuBackend
conn.execute("LOAD EXTENSION '/home/appuser/.kuzu/extension/httpfs/libhttpfs.kuzu_extension'")
```

### duckdb
```python
# For DuckDB → Kuzu direct ingestion
conn.execute("LOAD EXTENSION '/home/appuser/.kuzu/extension/duckdb/libduckdb.kuzu_extension'")
conn.execute("ATTACH '/mnt/kuzu-data/staging/graph_id/staging.duckdb' AS duck (TYPE duckdb)")
conn.execute("COPY Customer FROM duck.Customer (IGNORE_ERRORS=true)")
```

## Why Bundle Extensions?

1. **Offline Builds**: No network dependency during Docker build
2. **Reproducibility**: Exact extension versions pinned
3. **Security**: Extensions verified with checksums
4. **Performance**: No download time during deployment
5. **Reliability**: No external service dependency

## Extension API Status

The official Kuzu extension API (`https://extension.kuzudb.com`) is no longer actively maintained. Extensions must be sourced from the GitHub Container Registry image.

## Related Documentation

- [Kuzu Extensions Official Docs](https://kuzudb.github.io/docs/extensions/)
- [Host Your Own Extension Server](https://kuzudb.github.io/docs/extensions/#host-your-own-extension-server)
- Extension repository: `ghcr.io/kuzudb/extension-repo:latest`
