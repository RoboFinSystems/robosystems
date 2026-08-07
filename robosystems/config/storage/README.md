# Storage Configuration

S3 key and prefix helpers. Build every S3 path through these functions rather than formatting strings at the call site — the prefixes are the contract between the writers (Dagster assets, backup services, upload routers) and the readers.

## Buckets

Bucket names come from `env.py` and are set from CloudFormation outputs at deploy time.

| Environment variable | Default name pattern | Purpose |
| -------------------- | -------------------- | ------- |
| `SHARED_RAW_BUCKET` | `robosystems-shared-raw-{env}` | Raw downloads from external sources |
| `SHARED_PROCESSED_BUCKET` | `robosystems-shared-processed-{env}` | Processed parquet ready for ingestion |
| `USER_DATA_BUCKET` | `robosystems-user-{env}` | User uploads, graph backups, report bundles |
| `PUBLIC_DATA_BUCKET` | `robosystems-public-data-{env}` | CDN-served public content |

S3 bucket names are globally unique across all AWS accounts, so forks get an account-id namespace injected by the deployment workflows (`robosystems-{account-id}-shared-raw-{env}`). Nothing in application code hardcodes a bucket name.

## `shared.py` — external data sources

```python
from robosystems.config import env
from robosystems.config.storage import shared
from robosystems.config.storage.shared import DataSourceType

if shared.is_source_enabled(DataSourceType.SEC):
    raw_key = shared.get_raw_key(DataSourceType.SEC, "year=2024", "320193", "filing.zip")
    # 'sec/year=2024/320193/filing.zip'

    processed_key = shared.get_processed_key(DataSourceType.SEC, "year=2024", "nodes", "Entity.parquet")
    # 'sec/year=2024/nodes/Entity.parquet'

uri = shared.get_raw_uri(env.SHARED_RAW_BUCKET, DataSourceType.SEC, "year=2024", "file.zip")
# 's3://robosystems-shared-raw-prod/sec/year=2024/file.zip'
```

`DataSourceType` enumerates `SEC`, `FRED`, `BLS`, `CENSUS`, and `INDUSTRY`. The `DATA_SOURCES` registry carries each source's prefixes, rate limit, user agent, and enabled flag; only enabled sources are wired into pipelines.

## `graph.py` — graph database storage

Each storage type owns a top-level prefix, declared once in `GRAPH_STORAGE`:

| `GraphStorageType` | Prefix | Purpose |
| ------------------ | ------ | ------- |
| `USER_STAGING` | `user-staging/` | Pre-ingestion file uploads |
| `BACKUPS` | `graph-backups/` | Application-level backups via the API |
| `DATABASES` | `graph-databases/` | Instance-level backups from writer nodes |
| `REPORT_BUNDLES` | `report-bundles/` | Per-Report serialization bundles |
| `SHARED_REPO_DATABASES` | `shared-repositories/databases/` | Published shared-repository snapshots |
| `SHARED_REPO_BACKUPS` | `shared-repositories/backups/` | Compressed subscriber downloads |
| `R2_DOWNLOADS` | `downloads/` | Uncompressed files on R2 for zero-egress downloads |

```python
from datetime import UTC, datetime
from robosystems.config.storage import graph

ts = datetime.now(UTC)

graph.get_staging_key("user123", "kg456", "Entity", "file789", "data.parquet")
# 'user-staging/user123/kg456/Entity/file789/data.parquet'
graph.get_staging_prefix("user123", "kg456")
# 'user-staging/user123/kg456/'

graph.get_backup_key("kg456", "full", ts)
# 'graph-backups/databases/kg456/full/backup-20240115_123045.lbug.gz'
graph.get_backup_metadata_key("kg456", ts)
# 'graph-backups/metadata/kg456/backup-20240115_123045.json'

graph.get_instance_backup_key("prod", "kg456", ts)
# 'graph-databases/prod/kg456/kg456_20240115_123045.tar.gz'

graph.get_report_bundle_key("kg456", "rpt_01K8", 1)
# 'report-bundles/kg456/rpt_01K8/g1.jsonld'
```

Report bundles are versioned by `Report.generation_count` (the `g` prefix reads as "generation"), so regenerating a report leaves prior generations addressable for restatement audit trails.

## Key structure

### Shared raw bucket

```
s3://robosystems-shared-raw-{env}/
  sec/                           # SEC EDGAR filings
    year=2024/
      320193/                    # CIK
        0000320193-24-000081.zip
  fred/
    series=GDP/
      2024-Q4.json
```

### Shared processed bucket

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

### User data bucket

```
s3://robosystems-user-{env}/
  user-staging/                  # Pre-ingestion uploads
    {user_id}/{graph_id}/{table_name}/{file_id}/*.parquet

  graph-backups/                 # Application-level backups
    databases/{graph_id}/{full|incremental}/backup-{timestamp}.lbug.gz
    metadata/{graph_id}/backup-{timestamp}.json

  graph-databases/               # Instance-level backups
    {environment}/{graph_id}/{graph_id}_{timestamp}.tar.gz

  report-bundles/                # Per-Report serialization bundles
    {graph_id}/{report_id}/g{generation}.jsonld
```

## Adding a data source

1. Add the member to `DataSourceType` in `shared.py`.
2. Add a `DataSourceConfig` entry to the `DATA_SOURCES` registry (prefixes, rate limit, `enabled`).
3. Use `get_raw_key()` / `get_processed_key()` with the new member.

No new bucket is needed — sources are separated by prefix inside the two shared buckets.
