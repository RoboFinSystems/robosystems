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

from dagster import Definitions, EnvVar

from robosystems.dagster.resources import (
  DatabaseResource,
  GraphResource,
  S3Resource,
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
from robosystems.dagster.jobs.infrastructure import (
  hourly_auth_cleanup_job,
  hourly_auth_cleanup_schedule,
  weekly_health_check_job,
  weekly_health_check_schedule,
  # Instance infrastructure monitoring (from Lambda)
  instance_health_check_job,
  instance_health_check_schedule,
  instance_metrics_collection_job,
  instance_metrics_collection_schedule,
  instance_registry_cleanup_job,
  instance_registry_cleanup_schedule,
  volume_registry_cleanup_job,
  volume_registry_cleanup_schedule,
  full_instance_maintenance_job,
  full_instance_maintenance_schedule,
)
from robosystems.dagster.jobs.provisioning import (
  provision_graph_job,
  provision_repository_job,
)
from robosystems.dagster.jobs.graph import (
  create_graph_job,
  create_entity_graph_job,
  create_subgraph_job,
  backup_graph_job,
  restore_graph_job,
  stage_file_job,
  materialize_file_job,
)
from robosystems.dagster.jobs.sec import (
  sec_single_company_job,
  sec_full_rebuild_job,
  sec_materialize_only_job,
  sec_daily_rebuild_schedule,
  sec_weekly_refresh_schedule,
)

# Import sensors
from robosystems.dagster.sensors import (
  pending_subscription_sensor,
  pending_repository_sensor,
)

# Import assets
from robosystems.dagster.assets import (
  # SEC pipeline (new assets)
  sec_companies_list,
  sec_raw_filings,
  sec_processed_filings,
  sec_duckdb_staging,
  sec_graph_materialized,
  # QuickBooks pipeline
  qb_accounts,
  qb_transactions,
  qb_graph_data,
  # Plaid pipeline
  plaid_accounts,
  plaid_transactions,
  plaid_graph_data,
)

# ============================================================================
# Resource Configuration
# ============================================================================

# Resources are configured via environment variables for flexibility
# across dev/staging/prod environments
resources = {
  "db": DatabaseResource(
    database_url=EnvVar("DATABASE_URL"),
  ),
  "s3": S3Resource(
    bucket_name=EnvVar("AWS_S3_BUCKET"),
    region_name=EnvVar("AWS_REGION"),
  ),
  "graph": GraphResource(
    graph_api_url=EnvVar("GRAPH_API_URL"),
  ),
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
  # SEC pipeline jobs
  sec_single_company_job,
  sec_full_rebuild_job,
  sec_materialize_only_job,
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
  sec_daily_rebuild_schedule,
  sec_weekly_refresh_schedule,
]

# ============================================================================
# Sensors Registry
# ============================================================================

all_sensors = [
  pending_subscription_sensor,
  pending_repository_sensor,
]

# ============================================================================
# Assets Registry
# ============================================================================

all_assets = [
  # SEC pipeline assets (new)
  sec_companies_list,
  sec_raw_filings,
  sec_processed_filings,
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
