"""Dagster definitions entry point for RoboSystems.

This module defines all Dagster components:
- Resources: Database, S3, Graph connections
- Jobs: Billing, infrastructure, provisioning, SEC pipeline jobs
- Schedules: Cron-based job triggers
- Sensors: Event-driven job triggers
- Assets: Data pipeline assets for SEC, QuickBooks, Plaid

Usage:
    # Local development
    dagster dev -m robosystems.dagster

    # Production (via dagster-webserver)
    dagster-webserver -m robosystems.dagster
"""

from dagster import Definitions

# Import assets
from robosystems.dagster.assets import (
  # Plaid pipeline
  plaid_accounts,
  plaid_graph_data,
  plaid_transactions,
  # QuickBooks pipeline
  qb_accounts,
  qb_graph_data,
  qb_transactions,
  # SEC pipeline - download phase
  sec_companies_list,
  # SEC pipeline - staging and materialization
  sec_duckdb_staging,
  sec_graph_materialized,
  # SEC pipeline - dynamic partition processing
  sec_process_filing,
  sec_raw_filings,
  # Direct staging observable source
  staged_files_source,
)

# Import jobs
from robosystems.dagster.jobs.billing import (
  daily_storage_billing_job,
  daily_storage_billing_schedule,
  hourly_usage_collection_job,
  hourly_usage_collection_schedule,
  monthly_credit_allocation_job,
  monthly_credit_allocation_schedule,
  monthly_usage_report_job,
  monthly_usage_report_schedule,
)
from robosystems.dagster.jobs.graph import (
  backup_graph_job,
  create_entity_graph_job,
  create_graph_job,
  create_subgraph_job,
  materialize_file_job,
  materialize_graph_job,
  restore_graph_job,
  stage_file_job,
)
from robosystems.dagster.jobs.infrastructure import (
  full_instance_maintenance_job,
  full_instance_maintenance_schedule,
  hourly_auth_cleanup_job,
  hourly_auth_cleanup_schedule,
  # Instance infrastructure monitoring (from Lambda)
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
from robosystems.dagster.jobs.provisioning import (
  provision_graph_job,
  provision_repository_job,
)
from robosystems.dagster.jobs.sec import (
  sec_daily_download_schedule,
  sec_download_job,
  sec_materialize_job,
  sec_nightly_materialize_schedule,
  sec_process_job,
)
from robosystems.dagster.resources import (
  DatabaseResource,
  GraphResource,
  S3Resource,
)

# Import sensors
from robosystems.dagster.sensors import (
  pending_repository_sensor,
  pending_subscription_sensor,
  sec_processing_sensor,
)

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
# Jobs Registry
# ============================================================================

all_jobs = [
  # Billing jobs
  monthly_credit_allocation_job,
  daily_storage_billing_job,
  hourly_usage_collection_job,
  monthly_usage_report_job,
  # Infrastructure jobs
  hourly_auth_cleanup_job,
  weekly_health_check_job,
  # Instance infrastructure monitoring jobs (from Lambda)
  instance_health_check_job,
  instance_metrics_collection_job,
  instance_registry_cleanup_job,
  volume_registry_cleanup_job,
  full_instance_maintenance_job,
  # Provisioning jobs (triggered by sensors)
  provision_graph_job,
  provision_repository_job,
  # Graph operations jobs (user-triggered via API)
  create_graph_job,
  create_entity_graph_job,
  create_subgraph_job,
  backup_graph_job,
  restore_graph_job,
  stage_file_job,
  materialize_file_job,
  materialize_graph_job,
  # SEC pipeline jobs
  sec_download_job,  # Download raw filings to S3
  sec_process_job,  # Per-filing processing (sensor-triggered)
  sec_materialize_job,  # Staging + materialization to graph
  # Notification jobs
  send_email_job,
]

# ============================================================================
# Schedules Registry
# ============================================================================

all_schedules = [
  # Billing schedules
  monthly_credit_allocation_schedule,
  daily_storage_billing_schedule,
  hourly_usage_collection_schedule,
  monthly_usage_report_schedule,
  # Infrastructure schedules
  hourly_auth_cleanup_schedule,
  weekly_health_check_schedule,
  # Instance infrastructure monitoring schedules (from Lambda - STOPPED by default)
  instance_health_check_schedule,
  instance_metrics_collection_schedule,
  instance_registry_cleanup_schedule,
  volume_registry_cleanup_schedule,
  full_instance_maintenance_schedule,
  # SEC pipeline schedules
  sec_daily_download_schedule,
  sec_nightly_materialize_schedule,
]

# ============================================================================
# Sensors Registry
# ============================================================================

all_sensors = [
  pending_subscription_sensor,
  pending_repository_sensor,
  sec_processing_sensor,
]

# ============================================================================
# Assets Registry
# ============================================================================

all_assets = [
  # Direct staging (observable source for API direct staging)
  staged_files_source,
  # SEC pipeline - download phase
  sec_companies_list,
  sec_raw_filings,
  # SEC pipeline - dynamic partition processing (sensor handles discovery)
  sec_process_filing,
  # SEC pipeline - staging and materialization
  sec_duckdb_staging,
  sec_graph_materialized,
  # QuickBooks pipeline assets
  qb_accounts,
  qb_transactions,
  qb_graph_data,
  # Plaid pipeline assets
  plaid_accounts,
  plaid_transactions,
  plaid_graph_data,
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
