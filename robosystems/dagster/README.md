# Dagster Orchestration

This directory contains platform-level Dagster orchestration: billing, infrastructure, provisioning, graph operations, and shared repository management. Adapter-specific pipelines (assets, jobs, sensors, schedules) live inside their adapter packages and expose a `get_dagster_components()` function. `definitions.py` collects adapter pipelines alongside platform components.

## Directory Structure

```
dagster/
├── README.md              # This file
├── __init__.py            # Lazy module exports (PEP 562)
├── definitions.py         # Collector: platform + adapter pipelines
├── resources/             # Shared infrastructure resources
│   ├── database.py        # PostgreSQL resource
│   ├── storage.py         # S3 resource
│   └── graph.py           # LadybugDB graph resource
├── jobs/                  # Platform job definitions
│   ├── billing.py         # Credit allocation, usage reports
│   ├── infrastructure.py  # Auth cleanup, health checks, instance monitoring
│   ├── graph.py           # Graph operations (create, backup, restore)
│   ├── provisioning.py    # Repository and graph provisioning
│   ├── shared_repository.py  # Shared repository S3 sync + replicas
│   └── notifications.py   # Email notification jobs
├── sensors/               # Platform sensors
│   └── provisioning.py    # Subscription/repository provisioning sensors
└── assets/                # Platform assets
    ├── __init__.py        # Asset exports
    ├── graphs.py          # User graph operation assets
    └── shared_repositories/  # S3 publish + replica refresh (all shared repos)
```

Adapter pipelines live in their own packages:
```
adapters/sec/pipeline/     # SEC EDGAR pipeline (12 jobs, 9 assets, 6 sensors, 1 schedule)
adapters/custom_*/pipeline/ # Fork-friendly custom adapter pipelines
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

# Run SEC download with config
uv run dagster job execute -m robosystems.dagster -j sec_download_job \
  -c '{"ops": {"sec_raw_filings": {"config": {"ticker": "NVDA", "year": 2025}}}}'
```

## Jobs Overview

### Billing Jobs

| Job                             | Schedule               | Description                                   |
| ------------------------------- | ---------------------- | --------------------------------------------- |
| `monthly_credit_allocation_job` | 1st of month, midnight | Process overages and allocate monthly credits |
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

### Adapter Pipeline Jobs

Adapter-specific jobs live in their adapter packages. See each adapter's pipeline README for details:

- **SEC**: [`adapters/sec/pipeline/README.md`](../adapters/sec/pipeline/README.md) — 12 jobs (download, process, stage, materialize, incremental, backup)

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

### Adapter Pipeline Sensors

Adapter-specific sensors and schedules live in their adapter packages:

- **SEC**: 6 sensors + 1 schedule — see [`adapters/sec/pipeline/README.md`](../adapters/sec/pipeline/README.md)

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

## Adding Adapter Pipelines

`definitions.py` collects adapter pipelines via the `get_dagster_components()` discovery pattern:

```python
# dagster/definitions.py
from robosystems.adapters.sec.pipeline import get_dagster_components as sec_pipeline

sec = sec_pipeline()
all_assets = [*platform_assets, *sec["assets"]]
all_jobs = [*platform_jobs, *sec["jobs"]]

# === FORK: Add your adapter pipelines here ===
# from robosystems.adapters.custom_erp.pipeline import get_dagster_components as erp_pipeline
```

Each adapter's `pipeline/__init__.py` returns `{"assets": [...], "jobs": [...], "sensors": [...], "schedules": [...]}`. See [Adapters README](../adapters/README.md#adding-new-adapters) for the full pattern.

## Related Documentation

- [Dagster Documentation](https://docs.dagster.io/) - Official Dagster docs
- [Adapters README](../adapters/README.md) - Adapter architecture and extensibility
- [SEC Pipeline README](../adapters/sec/pipeline/README.md) - SEC EDGAR pipeline details
