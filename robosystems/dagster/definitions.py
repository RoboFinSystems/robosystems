"""Dagster definitions entry point for RoboSystems.

This module defines all Dagster components:
- Resources: Database, S3, Graph connections
- Jobs: Billing, infrastructure, provisioning, SEC pipeline jobs
- Schedules: Cron-based job triggers
- Sensors: Event-driven job triggers
- Assets: Data pipeline assets for SEC

Usage:
    # Local development
    dagster dev -m robosystems.dagster

    # Production (via dagster-webserver)
    dagster-webserver -m robosystems.dagster
"""

from dagster import Definitions

# Import assets
from robosystems.dagster.assets import (
  # SEC pipeline - backup
  sec_backup,
  # SEC pipeline - two-stage materialization
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
  sec_entity_incremental_update,
  sec_graph_direct_copy,
  sec_graph_incremental_copy,
  sec_graph_materialized,
  # SEC pipeline - quarterly batch processing with consolidated output
  sec_processed_filings,
  sec_raw_filings,
  # Shared repository infrastructure (S3 publish + replica refresh)
  shared_replicas_refreshed,
  shared_repository_s3_published,
  # User graph operations (external assets for API direct execution)
  user_graph_creation_source,
  user_graph_file_staging_source,
  user_graph_materialized_source,
  # User repository provisioning
  user_repository_provisioning_source,
  user_subgraph_creation_source,
)

# Import jobs
from robosystems.dagster.jobs.billing import (
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
  sec_backup_job,
  sec_direct_copy_job,
  sec_download_job,
  sec_entity_update_job,
  sec_incremental_copy_job,
  sec_incremental_stage_job,
  sec_materialize_job,
  sec_process_job,
  sec_replica_refresh_job,
  sec_s3_publish_job,
  sec_stage_job,
  sec_staged_materialize_job,
)

# Import shared repository jobs
from robosystems.dagster.jobs.shared_repository import (
  shared_repository_refresh_replicas_job,
  shared_repository_s3_sync_job,
  shared_repository_s3_upload_only_job,
)
from robosystems.dagster.resources import (
  DatabaseResource,
  GraphResource,
  S3Resource,
)

# Import sensors and schedules from sensors module
from robosystems.dagster.sensors import (
  pending_repository_sensor,
  pending_subscription_sensor,
  # Incremental pipeline (automated chain, disabled by default)
  sec_download_to_process_sensor,
  sec_incremental_download_schedule,
  sec_incremental_post_ingest_s3_sync_sensor,
  sec_incremental_staging_sensor,
  sec_post_materialize_s3_sync_sensor,
  sec_processing_sensor,
  sec_stage_to_copy_sensor,
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
  sec_process_job,  # Process quarter's filings (standard profile: 2 vCPU, 8 GB)
  sec_stage_job,  # Stage to persistent DuckDB (standard profile: 2 vCPU, 8 GB)
  sec_materialize_job,  # Materialize from DuckDB to LadybugDB (standard profile)
  sec_staged_materialize_job,  # Full pipeline: stage + materialize (standard profile)
  sec_incremental_stage_job,  # Incremental: INSERT new files to DuckDB
  sec_incremental_copy_job,  # Incremental S3 → LadybugDB (direct copy)
  sec_entity_update_job,  # Update existing Entity nodes (mutable attributes)
  sec_direct_copy_job,  # Direct S3 → LadybugDB (bypasses DuckDB staging)
  sec_backup_job,  # Create downloadable backup of SEC database
  sec_s3_publish_job,  # Publish raw .lbug to S3 for replica cluster
  sec_replica_refresh_job,  # Rolling refresh of replica fleet
  # Shared repository jobs (S3 ATTACH mode)
  shared_repository_s3_sync_job,  # Full: checkpoint + S3 upload + refresh replicas
  shared_repository_s3_upload_only_job,  # S3 upload only (no replica refresh)
  shared_repository_refresh_replicas_job,  # Refresh replicas with existing S3 database
  # Notification jobs
  send_email_job,
]

# ============================================================================
# Schedules Registry
# ============================================================================

all_schedules = [
  # Billing schedules
  monthly_credit_allocation_schedule,
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
  # SEC incremental pipeline (automated chain, disabled by default)
  sec_incremental_download_schedule,
]

# ============================================================================
# Sensors Registry
# ============================================================================

all_sensors = [
  pending_subscription_sensor,
  pending_repository_sensor,
  # SEC legacy/manual sensors
  sec_processing_sensor,
  sec_post_materialize_s3_sync_sensor,
  # SEC incremental pipeline chain sensors (disabled by default)
  sec_download_to_process_sensor,
  sec_incremental_staging_sensor,  # process → stage (DuckDB)
  sec_stage_to_copy_sensor,  # stage → copy (S3 → LadybugDB)
  sec_incremental_post_ingest_s3_sync_sensor,  # copy → S3 sync
]

# ============================================================================
# Assets Registry
# ============================================================================

all_assets = [
  # User graph operations (external assets for API direct execution)
  user_graph_file_staging_source,
  user_graph_materialized_source,
  user_graph_creation_source,
  user_subgraph_creation_source,
  # User repository provisioning
  user_repository_provisioning_source,
  # SEC pipeline - download phase (EFTS-based discovery)
  sec_raw_filings,
  # SEC pipeline - quarterly batch processing with consolidated output
  sec_processed_filings,
  # SEC pipeline - two-stage materialization
  sec_duckdb_staged,  # DuckDB staging (full rebuild)
  sec_duckdb_incremental_staged,  # DuckDB incremental staging (INSERT with dedup)
  sec_graph_direct_copy,  # Direct S3 → LadybugDB (bypasses DuckDB staging)
  sec_graph_incremental_copy,  # Incremental S3 → LadybugDB (daily updates)
  sec_entity_incremental_update,  # Update existing Entity nodes (mutable attrs)
  sec_graph_materialized,  # LadybugDB materialization (retry-safe)
  shared_repository_s3_published,  # S3 publish for replica cluster (S3 ATTACH source)
  shared_replicas_refreshed,  # Rolling refresh of replica fleet
  sec_backup,  # Downloadable backup of SEC database
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
