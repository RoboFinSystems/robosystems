"""Dagster jobs for RoboSystems.

Jobs define the execution units that can be scheduled or triggered:
- Billing jobs: Credit allocation, storage billing, usage collection
- Infrastructure jobs: Auth cleanup, health checks
- Provisioning jobs: Graph and repository provisioning
- SEC jobs: XBRL pipeline processing (download, process, materialize)
- Shared Repository jobs: EBS snapshots, replica management
"""

from robosystems.dagster.jobs.billing import (
  monthly_credit_allocation_job,
  daily_storage_billing_job,
  hourly_usage_collection_job,
  monthly_usage_report_job,
  process_stripe_webhook_job,
  build_stripe_webhook_job_config,
)
from robosystems.dagster.jobs.infrastructure import (
  hourly_auth_cleanup_job,
  weekly_health_check_job,
)
from robosystems.dagster.jobs.provisioning import (
  provision_graph_job,
  provision_repository_job,
)
from robosystems.dagster.jobs.sec import (
  sec_download_job,
  sec_process_job,
  sec_materialize_job,
  sec_daily_download_schedule,
  sec_weekly_download_schedule,
)
from robosystems.dagster.jobs.shared_repository import (
  shared_repository_snapshot_job,
  shared_repository_snapshot_only_job,
  shared_repository_refresh_replicas_job,
  weekly_shared_repository_snapshot_schedule,
)
from robosystems.dagster.jobs.notifications import (
  send_email_job,
  build_email_job_config,
)

__all__ = [
  # Billing
  "monthly_credit_allocation_job",
  "daily_storage_billing_job",
  "hourly_usage_collection_job",
  "monthly_usage_report_job",
  "process_stripe_webhook_job",
  "build_stripe_webhook_job_config",
  # Infrastructure
  "hourly_auth_cleanup_job",
  "weekly_health_check_job",
  # Provisioning
  "provision_graph_job",
  "provision_repository_job",
  # SEC Pipeline
  "sec_download_job",
  "sec_process_job",
  "sec_materialize_job",
  "sec_daily_download_schedule",
  "sec_weekly_download_schedule",
  # Shared Repository Management
  "shared_repository_snapshot_job",
  "shared_repository_snapshot_only_job",
  "shared_repository_refresh_replicas_job",
  "weekly_shared_repository_snapshot_schedule",
  # Notifications
  "send_email_job",
  "build_email_job_config",
]
