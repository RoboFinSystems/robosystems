"""Dagster jobs for RoboSystems.

Jobs define the execution units that can be scheduled or triggered:
- Billing jobs: Credit allocation, storage billing, usage collection
- Infrastructure jobs: Auth cleanup, health checks
- Provisioning jobs: Graph and repository provisioning
- SEC jobs: XBRL pipeline processing (download, process, materialize)
- Shared Repository jobs: S3 sync for replicas, replica management
"""

from robosystems.dagster.jobs.billing import (
  build_stripe_webhook_job_config,
  monthly_credit_allocation_job,
  monthly_usage_report_job,
  process_stripe_webhook_job,
)
from robosystems.dagster.jobs.infrastructure import (
  hourly_auth_cleanup_job,
  weekly_health_check_job,
)
from robosystems.dagster.jobs.notifications import (
  build_email_job_config,
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
from robosystems.dagster.jobs.shared_repository import (
  shared_repository_refresh_replicas_job,
  shared_repository_s3_sync_job,
  shared_repository_s3_upload_only_job,
)

__all__ = [
  "build_email_job_config",
  "build_stripe_webhook_job_config",
  "hourly_auth_cleanup_job",
  "monthly_credit_allocation_job",
  "monthly_usage_report_job",
  "process_stripe_webhook_job",
  "provision_graph_job",
  "provision_repository_job",
  "sec_backup_job",
  "sec_direct_copy_job",
  "sec_download_job",
  "sec_entity_update_job",
  "sec_incremental_copy_job",
  "sec_incremental_stage_job",
  "sec_materialize_job",
  "sec_process_job",
  "sec_replica_refresh_job",
  "sec_s3_publish_job",
  "sec_stage_job",
  "sec_staged_materialize_job",
  "send_email_job",
  "shared_repository_refresh_replicas_job",
  "shared_repository_s3_sync_job",
  "shared_repository_s3_upload_only_job",
  "weekly_health_check_job",
]
