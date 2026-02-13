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

from robosystems.adapters.sec.pipeline import (
  get_dagster_components as sec_pipeline,
)
from robosystems.dagster.assets.graphs import (
  user_graph_creation_source,
  user_graph_file_staging_source,
  user_graph_materialized_source,
  user_repository_provisioning_source,
  user_subgraph_creation_source,
)
from robosystems.dagster.assets.shared_repositories import (
  shared_replicas_refreshed,
)
from robosystems.dagster.jobs.backup_cleanup import (
  daily_backup_cleanup_job,
  daily_backup_cleanup_schedule,
)
from robosystems.dagster.jobs.billing import (
  monthly_credit_allocation_job,
  monthly_credit_allocation_schedule,
  monthly_usage_report_job,
  monthly_usage_report_schedule,
  process_stripe_webhook_job,
)
from robosystems.dagster.jobs.graph import (
  backup_graph_job,
  materialize_file_job,
  materialize_graph_job,
  restore_graph_job,
  stage_file_job,
  wait_and_create_graph_job,
)
from robosystems.dagster.jobs.graph_lifecycle import (
  deprovision_suspended_graphs_job,
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
from robosystems.dagster.jobs.notifications import (
  send_email_job,
)
from robosystems.dagster.jobs.shared_repository import (
  shared_repository_refresh_replicas_job,
)
from robosystems.dagster.resources import (
  DatabaseResource,
  GraphResource,
  S3Resource,
)
from robosystems.dagster.sensors.graph_lifecycle import (
  expired_graph_subscription_sensor,
  suspended_graph_deprovisioning_sensor,
)
from robosystems.dagster.sensors.graph_queue import (
  graph_creation_queue_sensor,
)

# === FORK: Add your adapter pipelines here ===
# from robosystems.adapters.custom_erp.pipeline import get_dagster_components as erp_pipeline

# ============================================================================
# Adapter Pipeline Components
# ============================================================================

sec = sec_pipeline()
# erp = erp_pipeline()

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
  user_graph_creation_source,
  user_subgraph_creation_source,
  user_repository_provisioning_source,
  # Platform: Shared repository infrastructure
  shared_replicas_refreshed,
  # Adapter: SEC pipeline (includes sec_s3_published)
  *sec["assets"],
]

all_jobs = [
  # Platform: Billing
  monthly_credit_allocation_job,
  monthly_usage_report_job,
  process_stripe_webhook_job,
  # Platform: Infrastructure
  daily_backup_cleanup_job,
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
  wait_and_create_graph_job,
  # Platform: Graph lifecycle
  suspend_expired_graphs_job,
  deprovision_suspended_graphs_job,
  # Platform: Shared repository (standalone refresh only)
  shared_repository_refresh_replicas_job,
  # Platform: Notifications
  send_email_job,
  # Adapter: SEC pipeline
  *sec["jobs"],
]

all_schedules = [
  # Platform: Billing
  monthly_credit_allocation_schedule,
  monthly_usage_report_schedule,
  # Platform: Infrastructure
  daily_backup_cleanup_schedule,
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
  # Platform: Graph lifecycle
  graph_creation_queue_sensor,
  expired_graph_subscription_sensor,
  suspended_graph_deprovisioning_sensor,
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
