# SEC Pipeline Dagster Assets

This package contains all Dagster assets for the SEC EDGAR XBRL pipeline.

## Directory Structure

```
dagster/
├── assets/sec/              # Asset definitions (this folder)
│   ├── README.md            # This file
│   ├── __init__.py          # Package exports
│   ├── configs.py           # All configuration classes
│   ├── download.py          # sec_raw_filings asset
│   ├── process.py           # sec_processed_filings asset
│   ├── stage.py             # sec_duckdb_staged, sec_duckdb_incremental_staged
│   ├── materialize.py       # sec_graph_materialized, sec_graph_incremental_copy, sec_graph_direct_copy
│   ├── entity_update.py     # sec_entity_incremental_update asset
│   └── backup.py            # sec_backup asset
├── jobs/sec.py              # Job definitions (asset groupings)
└── sensors/sec.py           # Automated triggers and schedules
```

## Pipeline Stages

### 1. Download (`sec_raw_filings`)

Discovers filings via EFTS API and downloads XBRL ZIPs to S3.

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_raw_filings \
  --partition 2024-Q1
```

### 2. Process (`sec_processed_filings`)

Processes raw XBRL files into consolidated parquet files (one per table per quarter).

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_processed_filings \
  --partition 2024-Q1
```

### 3. Stage (`sec_duckdb_staged`)

Stages processed parquet files to persistent DuckDB for querying and materialization.

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_duckdb_staged
```

### 4. Materialize (`sec_graph_materialized`)

Materializes from DuckDB staging to LadybugDB graph database.

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_graph_materialized
```

### Alternative: Direct Copy (`sec_graph_direct_copy`)

Bypasses DuckDB staging, copying directly from S3 to LadybugDB.

```bash
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_graph_direct_copy
```

## Incremental Updates

For daily incremental updates (after initial load):

| Asset | Purpose |
|-------|---------|
| `sec_graph_incremental_copy` | Copy current quarter's new filings to LadybugDB |
| `sec_entity_incremental_update` | Update mutable Entity attributes (name, ticker changes) |
| `sec_duckdb_incremental_staged` | INSERT new files to existing DuckDB tables |

```bash
# Daily incremental pipeline
uv run dagster asset materialize -m robosystems.dagster \
  --select sec_graph_incremental_copy
```

## Configuration Classes

All configs are in `configs.py`:

| Config | Asset | Key Options |
|--------|-------|-------------|
| `SECDownloadConfig` | `sec_raw_filings` | `form_types`, `tickers`, `dry_run` |
| `SECProcessConfig` | `sec_processed_filings` | `batch_limit`, `continue_on_error` |
| `SECStageConfig` | `sec_duckdb_staged` | `reset_staging`, `year` |
| `SECMaterializeConfig` | `sec_graph_materialized` | `rebuild_graph`, `batch_materialization` |
| `SECDirectCopyConfig` | `sec_graph_direct_copy` | `rebuild_graph`, `year` |
| `SECIncrementalCopyConfig` | `sec_graph_incremental_copy` | `year`, `quarter` |
| `SECEntityUpdateConfig` | `sec_entity_incremental_update` | `year`, `quarter` |
| `SECBackupConfig` | `sec_backup` | `retention_days`, `compression` |

## Jobs

Jobs group assets for execution. Defined in [`dagster/jobs/sec.py`](../../jobs/sec.py):

| Job | Assets | Use Case |
|-----|--------|----------|
| `sec_download_job` | `sec_raw_filings` | Download raw filings |
| `sec_process_job` | `sec_processed_filings` | Process to parquet |
| `sec_stage_job` | `sec_duckdb_staged` | Stage to DuckDB |
| `sec_materialize_job` | `sec_graph_materialized` | Materialize to graph |
| `sec_staged_materialize_job` | `sec_duckdb_staged` + `sec_graph_materialized` | Full pipeline |
| `sec_direct_copy_job` | `sec_graph_direct_copy` | Direct S3 → LadybugDB |
| `sec_incremental_copy_job` | `sec_graph_incremental_copy` | Daily incremental |
| `sec_incremental_stage_job` | `sec_duckdb_incremental_staged` | Incremental DuckDB |
| `sec_entity_update_job` | `sec_entity_incremental_update` | Update mutable entities |
| `sec_backup_job` | `sec_backup` | Create downloadable backup |

## Sensors and Schedules

Automated triggers defined in [`dagster/sensors/sec.py`](../../sensors/sec.py):

### Backfill Sensors (enable for bulk processing)

| Sensor | Triggers | Purpose |
|--------|----------|---------|
| `sec_processing_sensor` | `sec_process_job` | Discovers pending SourceFiles, triggers batch processing per quarter |
| `sec_post_materialize_snapshot_sensor` | `shared_repository_snapshot_job` | Creates snapshot after materialization |

### Incremental Pipeline (enable all for automated daily updates)

| Sensor/Schedule | Triggers | Purpose |
|-----------------|----------|---------|
| `sec_incremental_download_schedule` | `sec_download_job` | 9pm EST weekdays |
| `sec_download_to_process_sensor` | `sec_process_job` | Chains download → process |
| `sec_incremental_staging_sensor` | `sec_incremental_stage_job` | Chains process → stage |
| `sec_stage_to_copy_sensor` | `sec_incremental_copy_job` | Chains stage → copy |
| `sec_incremental_post_ingest_snapshot_sensor` | `shared_repository_snapshot_job` | Chains copy → snapshot |

All sensors start **STOPPED** by default. Enable in Dagster UI when ready for automated processing.

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SEC Pipeline                                 │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐      │
│  │ Download │───▶│ Process  │───▶│  Stage   │───▶│Materialize│      │
│  │ (S3 ZIP) │    │(Parquet) │    │ (DuckDB) │    │(LadybugDB)│      │
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘      │
│       │                │                              ▲              │
│       │                │         ┌───────────────────┘              │
│       │                │         │                                   │
│       │                └─────────┼──────────┐                       │
│       │                          │          │                        │
│       │                    ┌─────┴────┐     │                        │
│       │                    │  Direct  │─────┘                        │
│       │                    │   Copy   │                              │
│       │                    └──────────┘                              │
│       │                                                              │
│       ▼                                                              │
│  ┌──────────┐                                                        │
│  │  Backup  │  (post-materialization)                               │
│  └──────────┘                                                        │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

## Related Files

- [`dagster/jobs/sec.py`](../../jobs/sec.py) - Job definitions
- [`dagster/sensors/sec.py`](../../sensors/sec.py) - Sensors and schedules
- [`adapters/sec/`](../../../adapters/sec/README.md) - Adapter implementation
