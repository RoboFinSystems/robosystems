# Storage Configuration

This module provides centralized S3 path helpers for different data domains, ensuring consistent bucket usage and key structure across the platform.

## Architecture

```
storage/
├── __init__.py      # Re-exports from shared and graph
├── shared.py        # Shared/public data sources (SEC, FRED, etc.)
└── graph.py         # Customer graph database storage
```

## Bucket Structure

All storage uses four canonical buckets (defined in `env.py`):

| Bucket | Environment Variable | Purpose |
|--------|---------------------|---------|
| `robosystems-shared-raw-{env}` | `SHARED_RAW_BUCKET` | Raw downloads from external sources |
| `robosystems-shared-processed-{env}` | `SHARED_PROCESSED_BUCKET` | Processed parquet files for ingestion |
| `robosystems-user-{env}` | `USER_DATA_BUCKET` | User uploads, graph backups, staging |
| `robosystems-public-data-{env}` | `PUBLIC_DATA_BUCKET` | CDN-served public content |

## Modules

### shared.py - Shared Data Sources

Manages S3 paths for shared/public data repositories (SEC, FRED, etc.).

```python
from robosystems.config.storage import shared
from robosystems.config.storage.shared import DataSourceType

# Check if a data source is enabled
if shared.is_source_enabled(DataSourceType.SEC):
    # Build S3 keys for raw and processed data
    raw_key = shared.get_raw_key(DataSourceType.SEC, "year=2024", "320193", "filing.zip")
    # → 'sec/year=2024/320193/filing.zip'

    processed_key = shared.get_processed_key(DataSourceType.SEC, "year=2024", "nodes", "Entity.parquet")
    # → 'sec/year=2024/nodes/Entity.parquet'

# Get full S3 URIs
from robosystems.config import env
uri = shared.get_raw_uri(env.SHARED_RAW_BUCKET, DataSourceType.SEC, "year=2024", "file.zip")
# → 's3://robosystems-shared-raw-prod/sec/year=2024/file.zip'
```

#### Data Source Registry

```python
class DataSourceType(Enum):
    SEC = "sec"           # SEC EDGAR filings (enabled)
    FRED = "fred"         # Federal Reserve Economic Data (future)
    BLS = "bls"           # Bureau of Labor Statistics (future)
    CENSUS = "census"     # Census Bureau data (future)
    INDUSTRY = "industry" # Industry benchmarks (future)
```

Each data source has configuration for rate limits, user agents, and enabled status.

### graph.py - Graph Database Storage

Manages S3 paths for customer graph databases with three storage types:

| Storage Type | Prefix | Purpose |
|--------------|--------|---------|
| `USER_STAGING` | `user-staging/` | Pre-ingestion file uploads |
| `BACKUPS` | `graph-backups/` | Application-level backups via API |
| `DATABASES` | `graph-databases/` | Instance-level backups from writer nodes |

```python
from robosystems.config.storage import graph

# User file staging (pre-ingestion uploads)
key = graph.get_staging_key("user123", "kg456", "Entity", "file789", "data.parquet")
# → 'user-staging/user123/kg456/Entity/file789/data.parquet'

# List staged files for a graph
prefix = graph.get_staging_prefix("user123", "kg456")
# → 'user-staging/user123/kg456/'

# Application-level backups
from datetime import datetime, UTC
ts = datetime.now(UTC)
backup_key = graph.get_backup_key("kg456", "full", ts)
# → 'graph-backups/databases/kg456/full/backup-20240115_123045.lbug.gz'

metadata_key = graph.get_backup_metadata_key("kg456", ts)
# → 'graph-backups/metadata/kg456/backup-20240115_123045.json'

# Instance-level backups (from writer nodes)
instance_key = graph.get_instance_backup_key("prod", "kg456", ts)
# → 'graph-databases/prod/kg456/kg456_20240115_123045.tar.gz'

# List instance backups
prefix = graph.get_instance_backup_prefix("prod", "kg456")
# → 'graph-databases/prod/kg456/'
```

## S3 Key Structure

### Shared Raw Bucket
```
s3://robosystems-shared-raw-{env}/
  sec/                           # SEC EDGAR filings
    year=2024/
      320193/                    # CIK
        0000320193-24-000081.zip
  fred/                          # Federal Reserve (future)
    series=GDP/
      2024-Q4.json
```

### Shared Processed Bucket
```
s3://robosystems-shared-processed-{env}/
  sec/
    year=2024/
      nodes/
        Entity/
          320193_0000320193-24-000081.parquet
        Fact/
          320193_0000320193-24-000081.parquet
      edges/
        ...
```

### User Data Bucket
```
s3://robosystems-user-{env}/
  user-staging/                  # Pre-ingestion uploads
    {user_id}/
      {graph_id}/
        {table_name}/
          {file_id}/
            *.parquet

  graph-backups/                 # Application-level backups
    databases/
      {graph_id}/
        full/
          backup-{timestamp}.lbug.gz
        incremental/
          backup-{timestamp}.lbug.gz
    metadata/
      {graph_id}/
        backup-{timestamp}.json

  graph-databases/               # Instance-level backups
    {environment}/
      {graph_id}/
        {graph_id}_{timestamp}.tar.gz
```

## Usage in Code

### Dagster Assets/Sensors
```python
from robosystems.config.storage.shared import DataSourceType, get_raw_key, get_processed_key

# In SEC pipeline
raw_key = get_raw_key(DataSourceType.SEC, f"year={year}", cik, f"{accession}.zip")
processed_key = get_processed_key(DataSourceType.SEC, f"year={year}", "nodes", f"{filename}.parquet")
```

### Backup Services
```python
from robosystems.config.storage import graph

# In LadybugGraphBackupService
s3_prefix = graph.get_instance_backup_prefix(environment)
s3_key = graph.get_instance_backup_key(environment, graph_id, timestamp)
```

### File Upload Routers
```python
from robosystems.config.storage import graph

# Build staging key for uploaded file
s3_key = graph.get_staging_key(user_id, graph_id, table_name, file_id, filename)
```

## Adding New Data Sources

1. Add the source type to `DataSourceType` enum in `shared.py`
2. Add configuration to `DATA_SOURCES` registry
3. Use `get_raw_key()` and `get_processed_key()` with the new type
4. No new buckets needed - data is organized by prefix

```python
# In shared.py
class DataSourceType(Enum):
    SEC = "sec"
    FRED = "fred"
    MY_NEW_SOURCE = "my_source"  # Add new source

DATA_SOURCES = {
    # ... existing sources ...
    DataSourceType.MY_NEW_SOURCE: DataSourceConfig(
        source_type=DataSourceType.MY_NEW_SOURCE,
        raw_prefix="my_source/",
        processed_prefix="my_source/",
        enabled=True,
        rate_limit=10,
    ),
}
```
