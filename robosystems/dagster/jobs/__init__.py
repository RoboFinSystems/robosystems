"""Dagster jobs for RoboSystems.

Jobs define the execution units that can be scheduled or triggered:
- Billing jobs: Credit allocation, usage collection, webhook processing
- Infrastructure jobs: Auth cleanup, health checks
- Graph jobs: Backup, restore, staging, materialization
- Shared Repository jobs: S3 sync for replicas, replica management
- Notification jobs: Email sending

Graph/repository provisioning is handled directly via FastAPI BackgroundTasks
(see middleware/sse/direct_monitor.py) and reports AssetMaterializations to Dagster.

SEC pipeline jobs have moved to robosystems.adapters.sec.pipeline.jobs
and are collected via get_dagster_components() in definitions.py.
"""

from robosystems.dagster.jobs.backup_cleanup import (
  daily_backup_cleanup_job,
  daily_backup_cleanup_schedule,
)
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
from robosystems.dagster.jobs.shared_repository import (
  shared_repository_refresh_replicas_job,
  shared_repository_s3_sync_job,
  shared_repository_s3_upload_only_job,
)

__all__ = [
  "build_email_job_config",
  "build_stripe_webhook_job_config",
  "daily_backup_cleanup_job",
  "daily_backup_cleanup_schedule",
  "hourly_auth_cleanup_job",
  "monthly_credit_allocation_job",
  "monthly_usage_report_job",
  "process_stripe_webhook_job",
  "send_email_job",
  "shared_repository_refresh_replicas_job",
  "shared_repository_s3_sync_job",
  "shared_repository_s3_upload_only_job",
  "weekly_health_check_job",
]
