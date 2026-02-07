# Dagster Orchestration

This directory contains the Dagster-based orchestration system for all scheduled and event-driven tasks.

## Directory Structure

```
dagster/
├── README.md              # This file
├── __init__.py            # Module exports
├── definitions.py         # Main Dagster entry point
├── resources/             # Shared infrastructure resources
│   ├── database.py        # PostgreSQL resource
│   ├── storage.py         # S3 resource
│   └── graph.py           # LadybugDB graph resource
├── jobs/                  # Job definitions
│   ├── billing.py         # Credit allocation, storage billing
│   ├── infrastructure.py  # Auth cleanup, health checks, instance monitoring
│   ├── graph.py           # Graph operations (create, backup, restore)
│   ├── provisioning.py    # Repository and graph provisioning
│   ├── sec.py             # SEC EDGAR pipeline jobs
│   ├── shared_repository.py  # Shared repository S3 sync
│   └── notifications.py   # Email notification jobs
├── sensors/               # Event-driven triggers
│   ├── provisioning.py    # Subscription/repository provisioning sensors
│   └── sec.py             # SEC pipeline sensors and schedules
└── assets/                # Data pipeline assets
    ├── __init__.py        # Asset exports
    ├── graphs.py          # User graph operation assets
    └── sec/               # SEC EDGAR pipeline assets
        ├── README.md      # SEC pipeline documentation
        ├── configs.py     # Configuration classes
        ├── download.py    # sec_raw_filings asset
        ├── process.py     # sec_processed_filings asset
        ├── stage.py       # DuckDB staging assets
        ├── materialize.py # LadybugDB materialization assets
        ├── entity_update.py  # Entity incremental update asset
        └── backup.py      # SEC backup asset
```

## Quick Start

### Local Development

```bash
# Start Dagster development server
uv run dagster dev -m robosystems.dagster

# Access UI at http://localhost:3000
```

### Running Jobs Manually

```bash
# Run a specific job
uv run dagster job execute -m robosystems.dagster -j monthly_credit_allocation_job

# Run with config
uv run dagster job execute -m robosystems.dagster -j daily_storage_billing_job \
  -c '{"ops": {"bill_storage_credits": {"config": {"target_date": "2025-12-15"}}}}'
```

## Jobs Overview

### Billing Jobs

| Job                             | Schedule               | Description                                   |
| ------------------------------- | ---------------------- | --------------------------------------------- |
| `monthly_credit_allocation_job` | 1st of month, midnight | Process overages and allocate monthly credits |
| `daily_storage_billing_job`     | Daily at 2 AM          | Bill storage usage credits                    |
| `hourly_usage_collection_job`   | Every hour at :05      | Collect storage snapshots                     |
| `monthly_usage_report_job`      | 2nd of month, 6 AM     | Generate usage reports                        |

### Infrastructure Jobs

| Job                              | Schedule        | Description                       |
| -------------------------------- | --------------- | --------------------------------- |
| `hourly_auth_cleanup_job`        | Every hour      | Clean up expired API keys         |
| `weekly_health_check_job`        | Mondays at 3 AM | Credit system health checks       |
| `instance_health_check_job`      | Every 5 min     | LadybugDB instance health checks  |
| `instance_metrics_collection_job`| Every 5 min     | Collect instance metrics          |
| `instance_registry_cleanup_job`  | Every hour      | Clean stale registry entries      |
| `volume_registry_cleanup_job`    | Every hour      | Clean orphaned volume entries     |
| `full_instance_maintenance_job`  | Daily at 3 AM   | Full instance maintenance cycle   |

### SEC Pipeline Jobs

| Job                          | Purpose                                      |
| ---------------------------- | -------------------------------------------- |
| `sec_download_job`           | Download raw XBRL filings to S3              |
| `sec_process_job`            | Process filings to consolidated parquet      |
| `sec_stage_job`              | Stage parquet to persistent DuckDB           |
| `sec_materialize_job`        | Materialize from DuckDB to LadybugDB         |
| `sec_staged_materialize_job` | Full pipeline: stage + materialize           |
| `sec_direct_copy_job`        | Direct S3 → LadybugDB (bypasses DuckDB)      |
| `sec_incremental_copy_job`   | Incremental S3 → LadybugDB (daily updates)   |
| `sec_incremental_stage_job`  | Incremental DuckDB staging                   |
| `sec_entity_update_job`      | Update mutable Entity attributes             |
| `sec_backup_job`             | Create downloadable SEC database backup      |

See [`assets/sec/README.md`](assets/sec/README.md) for detailed SEC pipeline documentation.

### Graph Operations Jobs

| Job                    | Purpose                              |
| ---------------------- | ------------------------------------ |
| `create_graph_job`     | Create new user graph                |
| `create_entity_graph_job` | Create entity-scoped graph        |
| `create_subgraph_job`  | Create subgraph workspace            |
| `backup_graph_job`     | Backup graph to S3                   |
| `restore_graph_job`    | Restore graph from backup            |
| `stage_file_job`       | Stage file to graph staging tables   |
| `materialize_file_job` | Materialize staged file to graph     |
| `materialize_graph_job`| Materialize entire graph             |

### Provisioning Jobs

| Job                        | Purpose                              |
| -------------------------- | ------------------------------------ |
| `provision_graph_job`      | Provision graph for subscription     |
| `provision_repository_job` | Provision shared repository access   |

### Shared Repository Jobs

| Job                                   | Purpose                                    |
| ------------------------------------- | ------------------------------------------ |
| `shared_repository_s3_sync_job`       | S3 upload + refresh replicas               |
| `shared_repository_s3_upload_only_job`| S3 upload only (no replica refresh)        |
| `shared_repository_refresh_replicas_job` | Refresh replicas with current S3 database |

## Sensors

Sensors watch for conditions and trigger jobs:

### Provisioning Sensors

| Sensor                       | Triggers                  | Purpose                              |
| ---------------------------- | ------------------------- | ------------------------------------ |
| `pending_subscription_sensor`| `provision_graph_job`     | Provisions graphs for new subscriptions |
| `pending_repository_sensor`  | `provision_repository_job`| Provisions shared repository access  |

### SEC Pipeline Sensors

| Sensor/Schedule                           | Triggers                       | Purpose                              |
| ----------------------------------------- | ------------------------------ | ------------------------------------ |
| `sec_processing_sensor`                   | `sec_process_job`              | Discovers pending files, triggers batch processing |
| `sec_post_materialize_s3_sync_sensor`     | `shared_repository_s3_sync_job`  | Syncs to S3 after materialization      |
| `sec_incremental_download_schedule`       | `sec_download_job`             | 9pm EST weekdays                     |
| `sec_download_to_process_sensor`          | `sec_process_job`              | Chains download → process            |
| `sec_incremental_staging_sensor`          | `sec_incremental_stage_job`    | Chains process → stage               |
| `sec_stage_to_copy_sensor`                | `sec_incremental_copy_job`     | Chains stage → copy                  |
| `sec_incremental_post_ingest_s3_sync_sensor`  | `shared_repository_s3_sync_job`  | Chains copy → S3 sync            |

All sensors start **STOPPED** by default. Enable in Dagster UI when ready for automated processing.

## Resources

Resources provide shared infrastructure to jobs and assets:

```python
from robosystems.dagster.resources import DatabaseResource, S3Resource, GraphResource

@op
def my_op(context, db: DatabaseResource, s3: S3Resource):
    with db.get_session() as session:
        # Database operations
        pass

    s3.upload_file(file_obj, "path/to/file.parquet")
```

## Custom Data Sources (Fork-Friendly)

When forking RoboSystems, add custom data pipelines in the `custom_*` namespace:

1. Create adapter: `adapters/custom_myservice/` (client + processors)
2. Create assets: `dagster/assets/custom_myservice.py`
3. Register in `definitions.py`

The `custom_*` namespace ensures upstream updates never conflict with your additions. See [Adapters README](../adapters/README.md#fork-friendly-custom-adapters) for details.

## Related Documentation

- [Dagster Documentation](https://docs.dagster.io/) - Official Dagster docs
- [Adapters README](../adapters/README.md) - External service integrations
- [SEC Pipeline README](assets/sec/README.md) - SEC EDGAR pipeline details
