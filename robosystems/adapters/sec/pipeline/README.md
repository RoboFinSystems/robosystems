# SEC Pipeline

This package contains all Dagster orchestration for the SEC adapter: assets, jobs, sensors, and schedules.

## Directory Structure

```
adapters/sec/pipeline/
├── README.md                   # This file
├── __init__.py                 # get_dagster_components() discovery
├── configs.py                  # All configuration classes
├── download.py                 # sec_raw_filings asset
├── process.py                  # sec_processed_filings asset
├── stage.py                    # sec_duckdb_staged, sec_duckdb_incremental_staged
├── materialize.py              # sec_graph_materialized, sec_historical_materialized
├── entity_update.py            # sec_entity_incremental_update asset
├── s3_publish.py               # sec_lbug_s3_published, sec_historical_lbug_s3_published
├── duckdb_s3_publish.py        # sec_duckdb_s3_published, sec_historical_duckdb_s3_published
├── r2_publish.py               # sec_lbug_r2_published
├── artifact.py                 # sec_knowledge_artifacts
├── jobs.py                     # Job definitions
└── sensors.py                  # Sensors + schedule
```

## Discovery Pattern

```python
from robosystems.adapters.sec.pipeline import get_dagster_components

components = get_dagster_components()
# components["assets"] - list of Dagster assets
# components["jobs"] - list of Dagster jobs
# components["sensors"] - list of Dagster sensors
# components["schedules"] - list of Dagster schedules
```

This function is called by `dagster/definitions.py` to collect SEC pipeline components alongside platform operations.

## Pipeline Stages

### 1. Download (`sec_raw_filings`)

Discovers filings via EFTS API and downloads XBRL ZIPs to S3.

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_raw_filings \
  --partition 2024-Q1
```

### 2. Process (`sec_processed_filings`)

Processes raw XBRL files into parquet part files (one per table per batch). Each run handles up to 250 filings (SEC_PROCESS_BATCH_SIZE) then exits for memory release.

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_processed_filings \
  --partition 2024-Q1
```

### 3. Stage (`sec_duckdb_staged` / `sec_duckdb_incremental_staged`)

Stages processed parquet files to persistent DuckDB. Full rebuild uses `CREATE TABLE ... GROUP BY + FIRST()` dedup. Incremental uses `INSERT INTO ... WHERE NOT EXISTS` dedup.

```bash
# Full rebuild (initial load)
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_duckdb_staged

# Incremental (nightly updates)
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_duckdb_incremental_staged
```

### 4. Materialize (`sec_graph_materialized`)

Full LadybugDB rebuild from DuckDB staging. Skips embedding columns (vector search served by DuckDB directly).

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_graph_materialized
```

### 5. Publish

Publish databases to S3 for the replica fleet and vector search:

- `sec_lbug_s3_published` — LadybugDB .lbug file for replica fleet
- `sec_duckdb_s3_published` — DuckDB .duckdb file for embedding/vector search
- `sec_lbug_r2_published` — R2 copy for zero-egress subscriber downloads (manual/weekly)

## Automated Nightly Pipeline

The full automated chain (enable all sensors + schedule in Dagster UI):

```
9pm EST (sec_incremental_download_schedule)
  → download (current quarter + previous at boundary)

sec_incremental_pipeline_sensor:
  → process (250-batch loop, spot-safe via S3 cache)
  → process → process → ...
  → stage (DuckDB INSERT with NOT EXISTS dedup)
  [waits for all partitions to drain at quarter boundaries]

sec_stage_to_materialize_sensor:
  → materialize (full LadybugDB rebuild from DuckDB)

sec_post_materialize_publish_sensor:
  → lbug S3 publish
  → duckdb S3 publish (sequential to avoid overloading instance)
  → replica refresh (rolling, min_healthy=100%, ~15 min warmup)
```

## Sensors and Schedules

### Incremental Pipeline (enable all for automated nightly updates)

| Sensor/Schedule | Triggers | Purpose |
|-----------------|----------|---------|
| `sec_incremental_download_schedule` | `sec_download_job` | 9pm EST weekdays |
| `sec_incremental_pipeline_sensor` | `sec_process_job`, `sec_incremental_stage_job` | Chains download → process (batched loop) → stage |
| `sec_stage_to_materialize_sensor` | `sec_materialize_job` | Chains stage → full graph rebuild |
| `sec_post_materialize_publish_sensor` | `sec_lbug_s3_publish_job`, `sec_duckdb_s3_publish_job`, `shared_replicas_refresh_job` | Chains materialize → lbug publish → duckdb publish → replica refresh |

### Backfill Processing (enable for bulk/manual processing)

| Sensor | Triggers | Purpose |
|--------|----------|---------|
| `sec_processing_sensor` | `sec_process_job` | Discovers pending SourceFiles across all quarters, triggers batch processing. Polls every 5 min. |

All sensors start **STOPPED** by default. Enable in Dagster UI when ready.

## Configuration Classes

All configs are in `configs.py`:

| Config | Asset | Key Options |
|--------|-------|-------------|
| `SECDownloadConfig` | `sec_raw_filings` | `form_types`, `tickers`, `dry_run` |
| `SECProcessConfig` | `sec_processed_filings` | `batch_size`, `continue_on_error`, `form_types` |
| `SECStageConfig` | `sec_duckdb_staged` | `reset_staging`, `year`, `start_year`/`end_year` |
| `SECIncrementalStageConfig` | `sec_duckdb_incremental_staged` | `year`, `quarter` |
| `SECMaterializeConfig` | `sec_graph_materialized` | `rebuild_graph`, `batch_materialization` |
| `SECEntityUpdateConfig` | `sec_entity_incremental_update` | `year`, `quarter` |

## Data Flow

```
                         SEC Pipeline

  Download     Process      Stage         Materialize    Publish
  (S3 ZIP) --> (Parquet) --> (DuckDB) ---> (LadybugDB) --> S3 (.lbug)
                                |                          → Replica refresh
                                +------------------------> S3 (.duckdb)
                                                           → Vector search
```

## Cross-Cutting Imports

This pipeline imports from platform modules (adapter → platform):
- `dagster/assets/shared_repositories/` — Replica refresh asset
- `dagster/jobs/shared_repository` — Replica refresh job (used by publish sensor)

## Related Files

- [`adapters/sec/`](../README.md) — SEC adapter (clients, processors)
- [`dagster/definitions.py`](../../../dagster/definitions.py) — Component collector
