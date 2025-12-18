"""Dagster jobs for RoboSystems.

Jobs define the execution units that can be scheduled or triggered:
- Billing jobs: Credit allocation, storage billing, usage collection
- Infrastructure jobs: Auth cleanup, health checks
- Provisioning jobs: Graph and repository provisioning
- SEC jobs: XBRL pipeline processing (download, process, materialize)
"""

from robosystems.dagster.jobs.billing import (
  monthly_credit_allocation_job,
  daily_storage_billing_job,
  hourly_usage_collection_job,
  monthly_usage_report_job,
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

__all__ = [
  # Billing
  "monthly_credit_allocation_job",
  "daily_storage_billing_job",
  "hourly_usage_collection_job",
  "monthly_usage_report_job",
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
]
