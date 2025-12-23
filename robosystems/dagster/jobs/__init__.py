"""Dagster jobs for RoboSystems.

Jobs define the execution units that can be scheduled or triggered:
- Billing jobs: Credit allocation, storage billing, usage collection
- Infrastructure jobs: Auth cleanup, health checks
- Provisioning jobs: Graph and repository provisioning
- SEC jobs: XBRL pipeline processing (download, process, materialize)
- Shared Repository jobs: EBS snapshots, replica management
"""

from robosystems.dagster.jobs.billing import (
  build_stripe_webhook_job_config,
  daily_storage_billing_job,
  hourly_usage_collection_job,
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
  sec_daily_download_schedule,
  sec_download_job,
  sec_materialize_job,
  sec_nightly_materialize_schedule,
  sec_process_job,
)
from robosystems.dagster.jobs.shared_repository import (
  shared_repository_refresh_replicas_job,
  shared_repository_snapshot_job,
  shared_repository_snapshot_only_job,
  weekly_shared_repository_snapshot_schedule,
)

__all__ = [
  "build_email_job_config",
  "build_stripe_webhook_job_config",
  "daily_storage_billing_job",
  # Infrastructure
  "hourly_auth_cleanup_job",
  "hourly_usage_collection_job",
  # Billing
  "monthly_credit_allocation_job",
  "monthly_usage_report_job",
  "process_stripe_webhook_job",
  # Provisioning
  "provision_graph_job",
  "provision_repository_job",
  "sec_daily_download_schedule",
  # SEC Pipeline
  "sec_download_job",
  "sec_materialize_job",
  "sec_nightly_materialize_schedule",
  "sec_process_job",
  # Notifications
  "send_email_job",
  "shared_repository_refresh_replicas_job",
  # Shared Repository Management
  "shared_repository_snapshot_job",
  "shared_repository_snapshot_only_job",
  "weekly_health_check_job",
  "weekly_shared_repository_snapshot_schedule",
]
