# Kuzu Extensions Archive

This directory contains pre-downloaded Kuzu extensions bundled with the Docker image to eliminate external network dependencies during builds.

## Current Extensions

### httpfs v0.11.2

**Purpose**: Enables Kuzu to read files from HTTP/HTTPS URLs and S3-compatible storage.

**Architecture**: `linux_arm64`

**Binary**: `v0.11.2/linux_arm64/httpfs/libhttpfs.kuzu_extension`

**Provenance**:
- **Source**: Extracted from Kuzu Docker build prior to repository archival
- **Original Distribution**: Kuzu extension repository (now archived)
- **Version**: v0.11.2
- **SHA256**: `ea1b8f35234e57e961e1e0ca540769fc0192ff2e360b825a7e7b0e532f0f696e`
- **File Size**: 6.1 MB

**Note**: Kuzu archived their GitHub repository in late 2024. This extension was salvaged from working Docker builds before the archival. The checksum is verified during Docker build to ensure integrity.

## Multi-Architecture Support

Currently, only ARM64 is bundled as all production infrastructure runs on AWS Graviton (ARM64):
- ECS Fargate: ARM64
- EC2 Instances: r7g (ARM64)

If AMD64 builds are needed (e.g., for Docker Hub), additional architecture binaries should be added:
- `v0.11.2/linux_amd64/httpfs/libhttpfs.kuzu_extension`

## Update Procedures

### When to Update

Update the httpfs extension when:
1. Upgrading Kuzu to a new minor/major version (e.g., 0.11.x → 0.12.x)
2. Security vulnerabilities are discovered in the extension
3. New httpfs features are needed

### How to Update (If Sources Become Available)

Since the Kuzu repository is archived, future updates depend on:
1. Community forks maintaining extension builds
2. Kuzu project revival or new maintainer
3. Building from source if Kuzu source code remains available

**If official extension builds become available**:

```bash
# 1. Download new version for ARM64
VERSION="0.X.Y"
curl -O "https://extension.kuzudb.com/v${VERSION}/linux_arm64/httpfs/libhttpfs.kuzu_extension"

# 2. Calculate checksum
sha256sum libhttpfs.kuzu_extension

# 3. Move to archive
mkdir -p v${VERSION}/linux_arm64/httpfs
mv libhttpfs.kuzu_extension v${VERSION}/linux_arm64/httpfs/

# 4. For multi-arch support, repeat for AMD64
curl -O "https://extension.kuzudb.com/v${VERSION}/linux_amd64/httpfs/libhttpfs.kuzu_extension"
mkdir -p v${VERSION}/linux_amd64/httpfs
mv libhttpfs.kuzu_extension v${VERSION}/linux_amd64/httpfs/

# 5. Update Dockerfile with new version and checksums
# 6. Update pyproject.toml with new Kuzu version
# 7. Run uv lock to update dependencies
# 8. Update this README with new provenance information
```

### Verification

Always verify the binary integrity:

```bash
# Verify ARM64 checksum
echo "ea1b8f35234e57e961e1e0ca540769fc0192ff2e360b825a7e7b0e532f0f696e  v0.11.2/linux_arm64/httpfs/libhttpfs.kuzu_extension" | sha256sum -c -
```

Expected output: `v0.11.2/linux_arm64/httpfs/libhttpfs.kuzu_extension: OK`

## Related Files

- `/Dockerfile` - Copies extensions into Docker image
- `/pyproject.toml` - Pins Kuzu version (must match extension version)
- `/robosystems/routers/graphs/copy/strategies.py` - Uses httpfs for S3 ingestion
- `/robosystems/kuzu_api/routers/databases/ingest.py` - Uses httpfs for data import

## Testing

To verify httpfs extension loads correctly:

```python
import kuzu

db = kuzu.Database("/tmp/test.kuzu")
conn = db.conn()

# These should not raise errors
conn.execute("INSTALL httpfs")
conn.execute("LOAD EXTENSION httpfs")

# Test S3 read (requires AWS credentials)
conn.execute("LOAD FROM 's3://bucket/file.parquet' RETURN *;")
```

## Troubleshooting

### Extension fails to load

**Error**: `Extension not found` or `Cannot load extension`

**Solutions**:
1. Verify extension is copied to both `/home/appuser/.kuzu/extension` and `/app/data/.kuzu/extension` in Dockerfile
2. Check file permissions (must be readable by appuser)
3. Verify architecture matches (ARM64 extension won't load on AMD64)

### Checksum mismatch

**Error**: Docker build fails checksum verification

**Solutions**:
1. Re-download extension from trusted source
2. Verify download wasn't corrupted
3. Update checksum in Dockerfile if intentionally using different binary

### Architecture mismatch

**Error**: `Exec format error` or `cannot execute binary`

**Solutions**:
1. Ensure building for correct architecture (ARM64 for production)
2. Add AMD64 binary if building multi-arch images
3. Use `TARGETARCH` build arg to select correct binary

## Archive Status

**Repository Status**: Kuzu GitHub repository archived by maintainers (2024)

**Implications**:
- No new official releases expected
- Community forks may continue development
- Extensions frozen at v0.11.2 unless community alternatives emerge
- Consider migration path if Kuzu project remains dormant long-term

**Monitoring**:
- Watch for Kuzu community forks or successor projects
- Monitor GitHub for any repository un-archival
- Track issues in archived repo for community coordination

## Support

For questions about:
- **Extension usage**: See Kuzu documentation (archived but available)
- **Build issues**: Check GitHub Actions logs and Dockerfile
- **Alternative solutions**: Consider DuckDB or other graph databases if Kuzu remains archived
