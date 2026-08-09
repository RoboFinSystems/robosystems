"""Dagster definitions entry point for RoboSystems.

This module collects Dagster components from two sources:
1. Platform operations (billing, infrastructure, provisioning, graph ops)
2. Adapter pipelines (SEC, future adapters)

Adapter-specific pipelines live inside their adapter packages
(e.g., adapters/sec/pipeline/) and expose a get_dagster_components() function.
This module collects those components alongside platform operations.

Usage:
    # Local development
    dagster dev -m robosystems.dagster

    # Production (via dagster-webserver)
    dagster-webserver -m robosystems.dagster
"""

from dagster import Definitions

from robosystems.config import env
from robosystems.dagster.assets.graphs import (
  user_graph_backup_source,
  user_graph_creation_source,
  user_graph_extensions_materialized_source,
  user_graph_file_staging_source,
  user_graph_materialized_source,
  user_repository_provisioning_source,
  user_subgraph_creation_source,
)
from robosystems.dagster.assets.shared_repositories import (
  build_shared_replicas_refreshed,
  shared_master_asleep,
  shared_master_awake,
)
from robosystems.dagster.jobs.backup_cleanup import (
  daily_backup_cleanup_job,
  daily_backup_cleanup_schedule,
)
from robosystems.dagster.jobs.backup_schedule import (
  nightly_graph_backup_schedule,
)
from robosystems.dagster.jobs.billing import (
  monthly_credit_allocation_job,
  monthly_credit_allocation_schedule,
  monthly_usage_report_job,
  monthly_usage_report_schedule,
)
from robosystems.dagster.jobs.graph import (
  backup_graph_job,
  materialize_file_job,
  materialize_graph_job,
  restore_graph_job,
  stage_file_job,
)
from robosystems.dagster.jobs.graph_lifecycle import (
  deprovision_suspended_graphs_job,
  reap_stalled_provisioning_job,
  suspend_expired_graphs_job,
)
from robosystems.dagster.jobs.infrastructure import (
  full_instance_maintenance_job,
  full_instance_maintenance_schedule,
  hourly_auth_cleanup_job,
  hourly_auth_cleanup_schedule,
  instance_health_check_job,
  instance_health_check_schedule,
  instance_metrics_collection_job,
  instance_metrics_collection_schedule,
  instance_registry_cleanup_job,
  instance_registry_cleanup_schedule,
  volume_registry_cleanup_job,
  volume_registry_cleanup_schedule,
  weekly_health_check_job,
  weekly_health_check_schedule,
)
from robosystems.dagster.jobs.invoice_billing import (
  invoice_subscription_renewal_job,
)
from robosystems.dagster.jobs.migration import (
  ladybug_migration_cleanup_job,
  ladybug_migration_export_job,
  ladybug_migration_import_job,
)
from robosystems.dagster.jobs.notifications import (
  send_email_job,
)
from robosystems.dagster.jobs.shared_repository import (
  shared_master_sleep_job,
  shared_master_wake_job,
  shared_replicas_refresh_job,
  shared_repository_refresh_replicas_job,
)
from robosystems.dagster.jobs.storage_reclaim import (
  daily_storage_reclaim_job,
  daily_storage_reclaim_schedule,
)
from robosystems.dagster.resources import (
  DatabaseResource,
  GraphResource,
  S3Resource,
)
from robosystems.dagster.sensors.graph_lifecycle import (
  expired_graph_subscription_sensor,
  stalled_provisioning_sensor,
  suspended_graph_deprovisioning_sensor,
)
from robosystems.dagster.sensors.invoice_billing import (
  invoice_subscription_renewal_sensor,
)
from robosystems.dagster.sensors.materialization import (
  stale_graph_materialization_sensor,
)
from robosystems.dagster.sensors.usage_monitor import (
  graph_usage_monitor_sensor,
)
from robosystems.dagster.sensors.worker_reaper import (
  worker_inflight_reaper_sensor,
)

# === FORK: Add your adapter pipelines here ===
# from robosystems.adapters.custom_erp.pipeline import get_dagster_components as erp_pipeline

# ============================================================================
# Adapter Pipeline Components (conditionally loaded via feature flags)
# ============================================================================

_empty_pipeline: dict = {"assets": [], "jobs": [], "schedules": [], "sensors": []}

if env.SEC_PIPELINE_ENABLED:
  from robosystems.adapters.sec.pipeline import (
    get_dagster_components as sec_pipeline,
  )

  sec = sec_pipeline()
else:
  sec = _empty_pipeline

if env.CONNECTION_QUICKBOOKS_ENABLED:
  from robosystems.adapters.quickbooks.pipeline import (
    get_dagster_components as qb_pipeline,
  )

  qb = qb_pipeline()
else:
  qb = _empty_pipeline

if env.EXTENSIONS_ENABLED:
  from robosystems.dagster.jobs.extensions import (
    extensions_materialize_job as _ext_mat_job,
  )
  from robosystems.dagster.jobs.extensions import (
    extensions_promote_obligations_job as _ext_promote_job,
  )
  from robosystems.dagster.sensors.scheduled_obligation_promoter import (
    scheduled_obligation_promotion_sensor as _ext_promote_sensor,
  )

  _extensions_jobs: list = [_ext_mat_job, _ext_promote_job]
  _extensions_sensors: list = [_ext_promote_sensor]
else:
  _extensions_jobs = []
  _extensions_sensors = []
# erp = erp_pipeline()

# Collect shared replica deps from all enabled adapter pipelines
_shared_replica_deps: list[str] = [
  *sec.get("shared_replica_deps", []),
  # *erp.get("shared_replica_deps", []),
]
shared_replicas_refreshed = build_shared_replicas_refreshed(deps=_shared_replica_deps)

# ============================================================================
# Resource Configuration
# ============================================================================

# Resources use internal fallback logic to fetch configuration from
# env.* (which uses secrets_manager for prod/staging). This ensures
# consistency with how the rest of the application fetches secrets.
resources = {
  "db": DatabaseResource(),  # Falls back to env.DATABASE_URL
  "s3": S3Resource(),  # Falls back to env.USER_DATA_BUCKET, env.AWS_REGION
  "graph": GraphResource(),  # Falls back to env.GRAPH_API_URL
}

# ============================================================================
# Collect All Components (Platform + Adapter Pipelines)
# ============================================================================

all_assets = [
  # Platform: User graph operations
  user_graph_file_staging_source,
  user_graph_materialized_source,
  user_graph_extensions_materialized_source,
  user_graph_creation_source,
  user_subgraph_creation_source,
  user_repository_provisioning_source,
  user_graph_backup_source,
  # Platform: Shared repository infrastructure
  shared_master_awake,
  shared_master_asleep,
  shared_replicas_refreshed,
  # Adapter: SEC pipeline (includes sec_lbug_s3_published)
  *sec["assets"],
  # Adapter: QuickBooks pipeline
  *qb["assets"],
]

all_jobs = [
  # Platform: Billing
  monthly_credit_allocation_job,
  monthly_usage_report_job,
  invoice_subscription_renewal_job,
  # Platform: Infrastructure
  daily_backup_cleanup_job,
  daily_storage_reclaim_job,
  hourly_auth_cleanup_job,
  weekly_health_check_job,
  instance_health_check_job,
  instance_metrics_collection_job,
  instance_registry_cleanup_job,
  volume_registry_cleanup_job,
  full_instance_maintenance_job,
  # Platform: Graph operations
  backup_graph_job,
  restore_graph_job,
  stage_file_job,
  materialize_file_job,
  materialize_graph_job,
  # Platform: Graph lifecycle
  suspend_expired_graphs_job,
  deprovision_suspended_graphs_job,
  reap_stalled_provisioning_job,
  # Platform: Shared repository
  shared_master_wake_job,
  shared_master_sleep_job,
  shared_replicas_refresh_job,
  shared_repository_refresh_replicas_job,
  # Platform: Version migration (manually triggered)
  ladybug_migration_export_job,
  ladybug_migration_import_job,
  ladybug_migration_cleanup_job,
  # Platform: Notifications
  send_email_job,
  # Adapter: SEC pipeline
  *sec["jobs"],
  # Adapter: QuickBooks pipeline
  *qb["jobs"],
  # Platform: Extensions materialization (OLTP → graph)
  *_extensions_jobs,
]

all_schedules = [
  # Platform: Billing
  monthly_credit_allocation_schedule,
  monthly_usage_report_schedule,
  # Platform: Infrastructure
  daily_backup_cleanup_schedule,
  nightly_graph_backup_schedule,
  daily_storage_reclaim_schedule,
  hourly_auth_cleanup_schedule,
  weekly_health_check_schedule,
  instance_health_check_schedule,
  instance_metrics_collection_schedule,
  instance_registry_cleanup_schedule,
  volume_registry_cleanup_schedule,
  full_instance_maintenance_schedule,
  # Adapter: SEC pipeline
  *sec["schedules"],
]

all_sensors = [
  # Platform: Billing
  invoice_subscription_renewal_sensor,
  # Platform: Graph lifecycle
  expired_graph_subscription_sensor,
  suspended_graph_deprovisioning_sensor,
  stalled_provisioning_sensor,
  # Platform: Materialization (auto-rematerialize stale graphs)
  stale_graph_materialization_sensor,
  # Platform: Usage monitoring
  graph_usage_monitor_sensor,
  # Platform: Worker reliability
  worker_inflight_reaper_sensor,
  # Extensions: period-boundary obligation promoter
  *_extensions_sensors,
  # Adapter: SEC pipeline
  *sec["sensors"],
]

# ============================================================================
# Definitions Export
# ============================================================================

defs = Definitions(
  assets=all_assets,
  jobs=all_jobs,
  schedules=all_schedules,
  sensors=all_sensors,
  resources=resources,
)
