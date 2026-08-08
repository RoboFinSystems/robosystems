# Dagster Orchestration

Platform-level orchestration: billing, infrastructure, graph operations, graph lifecycle, and shared repository management. Adapter-specific pipelines live inside their adapter packages and expose a `get_dagster_components()` function; `definitions.py` collects them alongside the platform components.

## When to use Dagster

Three places can run work off the request path. Pick by shape:

| Use | For | Because |
| --- | --- | --- |
| **Dagster job** | Scheduled or sensor-triggered platform work; multi-step pipelines; anything that needs run history and retries | It has a scheduler, a run log, and a UI |
| **[Background worker](../worker/README.md)** | Long-running, user-initiated work that streams progress over SSE and can be cancelled | It has a task queue, per-task progress, and tenant-isolated execution |
| **FastAPI `BackgroundTasks`** | Fire-and-forget work measured in milliseconds, where losing it on a restart is fine | It has no durability, no retry, and no visibility |

If a user is waiting on it and wants a progress bar, it is a worker task. If it runs on a clock or reacts to a system condition, it is a Dagster job.

## Layout

| Path | Contents |
| ---- | -------- |
| `definitions.py` | Collector: platform components plus adapter pipelines |
| `resources/` | `DatabaseResource` (PostgreSQL), `S3Resource`, `GraphResource` (LadybugDB) |
| `jobs/` | Platform job definitions and their schedules |
| `sensors/` | Platform sensors |
| `assets/` | External asset specs for graph operations, plus shared-repository publish/replica assets |
| `reporting.py` | Reports `AssetMaterialization` events from outside Dagster jobs (API, provisioning service) so direct operations still show up in the Assets tab |

Adapter pipelines live in their own packages — `adapters/sec/pipeline/` and `adapters/quickbooks/pipeline/`.

## Running locally

```bash
uv run dagster dev -m robosystems.dagster     # UI at http://localhost:3000
```

Run a job by hand:

```bash
uv run dagster job execute -m robosystems.dagster -j monthly_credit_allocation_job

uv run dagster job execute -m robosystems.dagster -j sec_download_job \
  -c '{"ops": {"sec_raw_filings": {"config": {"ticker": "NVDA", "year": 2025}}}}'
```

## Scheduled jobs

| Job | Cron (UTC) | Purpose |
| --- | ---------- | ------- |
| `monthly_credit_allocation_job` | `0 0 1 * *` | Process overages, allocate monthly credits |
| `monthly_usage_report_job` | `0 6 2 * *` | Generate usage reports |
| `hourly_auth_cleanup_job` | `0 * * * *` | Clean up expired API keys |
| `weekly_health_check_job` | `0 3 * * 1` | Credit system health checks |
| `instance_health_check_job` | `0 * * * *` | LadybugDB instance health |
| `instance_metrics_collection_job` | `*/5 * * * *` | Instance metrics — drives graph-tier autoscaling, so the cadence is load-bearing |
| `instance_registry_cleanup_job` | `0 3 * * *` | Clean stale DynamoDB registry entries |
| `volume_registry_cleanup_job` | `0 4 * * *` | Clean orphaned volume entries |
| `full_instance_maintenance_job` | `0 2 * * 0` | Full instance maintenance cycle |
| `daily_backup_cleanup_job` | `0 5 * * *` | Enforce backup retention (tracked backups past `expires_at`, daemon backups over 90 days) |
| `daily_storage_reclaim_job` | `30 5 * * *` | Delete reclaimable instance storage — stranded blue-green build artifacts and orphaned subgraph estates |

The instance-monitoring schedules are auto-enabled in staging and production only, since they need AWS (DynamoDB, EC2, CloudWatch).

## Sensor- and API-triggered jobs

| Job | Trigger | Purpose |
| --- | ------- | ------- |
| `backup_graph_job`, `restore_graph_job` | API | Back up / restore a graph via S3 |
| `stage_file_job`, `materialize_file_job` | API | Stage an uploaded file, then materialize it |
| `materialize_graph_job` | `stale_graph_materialization_sensor` | Rebuild a graph marked stale |
| `suspend_expired_graphs_job` | `expired_graph_subscription_sensor` | Move graphs with expired subscriptions to suspended |
| `deprovision_suspended_graphs_job` | `suspended_graph_deprovisioning_sensor` | Deprovision after the retention window |
| `reap_stalled_provisioning_job` | `stalled_provisioning_sensor` | Write off subscriptions stuck mid-provisioning so their infrastructure is reclaimed |
| `invoice_subscription_renewal_job` | `invoice_subscription_renewal_sensor` | Rotate billing periods and generate invoices for invoice-billed subscriptions |
| `send_email_job` | API | Email notifications |
| `shared_master_wake_job`, `shared_master_sleep_job`, `shared_replicas_refresh_job`, `shared_repository_refresh_replicas_job` | Schedule / sensor / manual | Shared repository master lifecycle and replica refresh |
| `ladybug_migration_export_job`, `ladybug_migration_import_job`, `ladybug_migration_cleanup_job` | Manual | LadybugDB version migration: export pre-deploy, import post-deploy, delete rollback backups post-verify |
| `extensions_materialize_job`, `extensions_promote_obligations_job` | Sensor (extensions builds only) | OLTP→OLAP materialization and period-boundary obligation promotion |

## Sensors

| Sensor | Watches for |
| ------ | ----------- |
| `stale_graph_materialization_sensor` | Graphs marked stale; batches writes within a window to avoid excessive rebuilds |
| `expired_graph_subscription_sensor` | Graphs whose subscription lapsed |
| `suspended_graph_deprovisioning_sensor` | Suspended graphs past their retention window |
| `stalled_provisioning_sensor` | Subscriptions left in `provisioning` past the staleness window — the state no other sensor looks at |
| `invoice_subscription_renewal_sensor` | Invoice-billed subscriptions whose period ended |
| `graph_usage_monitor_sensor` | Storage usage against tier limits (email alerts at 80% and 100%) |
| `worker_inflight_reaper_sensor` | Stale `worker:inflight:*` keys in Valkey DB 6 — runs in the always-on daemon, so no cold start |
| `scheduled_obligation_promotion_sensor` | Matured `schedule_entry_due` events per entity graph |

**Platform sensors default to `RUNNING`;** the obligation promoter is the exception and defaults to `STOPPED`. **Every SEC adapter sensor and its schedule default to `STOPPED`** — enable them in the Dagster UI when you're ready for automated processing.

## Resources

```python
from robosystems.dagster.resources import DatabaseResource, S3Resource, GraphResource

@op
def my_op(context, db: DatabaseResource, s3: S3Resource):
    with db.get_session() as session:
        ...
    s3.upload_file(file_obj, "path/to/file.parquet")
```

Resources fall back to `env.*` when not explicitly configured, so they resolve secrets the same way the rest of the application does.

## Adding an adapter pipeline

`definitions.py` collects adapter pipelines through the `get_dagster_components()` discovery pattern:

```python
# dagster/definitions.py
from robosystems.adapters.sec.pipeline import get_dagster_components as sec_pipeline

sec = sec_pipeline()
all_assets = [*platform_assets, *sec["assets"]]
all_jobs = [*platform_jobs, *sec["jobs"]]

# === FORK: Add your adapter pipelines here ===
# from robosystems.adapters.custom_erp.pipeline import get_dagster_components as erp_pipeline
```

Each adapter's `pipeline/__init__.py` returns `{"assets": [...], "jobs": [...], "sensors": [...], "schedules": [...]}`. See the [Adapters README](../adapters/README.md#adding-new-adapters) for the full pattern and the [SEC pipeline README](../adapters/sec/pipeline/README.md) for a worked example.
