"""Dagster SEC pipeline jobs and schedules.

Pipeline phases (split due to different partition types):

Phase 1 - Download & Process (year-partitioned):
  sec_download_job: sec_companies_list → sec_raw_filings → sec_batch_process

Phase 2 - Process Individual (dynamic partitions, per-filing - for Dagster UI):
  sec_process_job: sec_filings_to_process → sec_process_filing

Phase 3 - Materialize (unpartitioned):
  sec_materialize_job: sec_duckdb_staging → sec_graph_materialized

The CLI (sec_pipeline.py) uses Phase 1 + Phase 3 for batch workflows.
Phase 2 is for per-filing visibility in Dagster UI.
"""

from dagster import (
  AssetSelection,
  DefaultScheduleStatus,
  RunConfig,
  ScheduleDefinition,
  define_asset_job,
)

from robosystems.config import env
from robosystems.dagster.assets import (
  SECBatchProcessConfig,
  SECCompaniesConfig,
  SECDownloadConfig,
  sec_batch_process,
  sec_companies_list,
  sec_duckdb_staging,
  sec_filing_partitions,
  sec_filings_to_process,
  sec_graph_materialized,
  sec_process_filing,
  sec_raw_filings,
  sec_year_partitions,
)

# ============================================================================
# SEC Pipeline Jobs
# ============================================================================


# Phase 1: Download & Process (year-partitioned)
sec_download_job = define_asset_job(
  name="sec_download_and_process",
  description="Download and process SEC XBRL filings for a specific year.",
  selection=AssetSelection.assets(
    sec_companies_list,
    sec_raw_filings,
    sec_batch_process,
  ),
  tags={"pipeline": "sec", "phase": "download"},
  partitions_def=sec_year_partitions,
)


# Phase 2: Process (dynamic partitions per filing)
sec_process_job = define_asset_job(
  name="sec_process",
  description="Process SEC filings to parquet. One partition per filing.",
  selection=AssetSelection.assets(
    sec_filings_to_process,
    sec_process_filing,
  ),
  tags={"pipeline": "sec", "phase": "process"},
  partitions_def=sec_filing_partitions,
)


# Phase 3: Materialize (unpartitioned)
sec_materialize_job = define_asset_job(
  name="sec_materialize",
  description="Materialize SEC graph from processed parquet files.",
  selection=AssetSelection.assets(
    sec_duckdb_staging,
    sec_graph_materialized,
  ),
  tags={"pipeline": "sec", "phase": "materialize"},
)


# ============================================================================
# SEC Pipeline Schedules
# ============================================================================

# SEC schedules default to STOPPED. Enable via SEC_SCHEDULES_ENABLED=true
# in AWS Secrets Manager or environment variables.
SEC_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.SEC_SCHEDULES_ENABLED
  else DefaultScheduleStatus.STOPPED
)


sec_daily_download_schedule = ScheduleDefinition(
  name="sec_daily_download",
  description="Daily SEC download and process at 2 AM UTC",
  job=sec_download_job,
  cron_schedule="0 2 * * *",
  default_status=SEC_SCHEDULE_STATUS,
  run_config=RunConfig(
    ops={
      "sec_companies_list": SECCompaniesConfig(),
      "sec_raw_filings": SECDownloadConfig(
        skip_existing=True,
        form_types=["10-K", "10-Q"],
      ),
      "sec_batch_process": SECBatchProcessConfig(
        refresh=False,
      ),
    }
  ),
)


sec_weekly_download_schedule = ScheduleDefinition(
  name="sec_weekly_download",
  description="Weekly SEC full download and process at 3 AM UTC on Sundays",
  job=sec_download_job,
  cron_schedule="0 3 * * 0",
  default_status=SEC_SCHEDULE_STATUS,
  run_config=RunConfig(
    ops={
      "sec_companies_list": SECCompaniesConfig(),
      "sec_raw_filings": SECDownloadConfig(
        skip_existing=False,
        form_types=["10-K", "10-Q"],
      ),
      "sec_batch_process": SECBatchProcessConfig(
        refresh=True,
      ),
    }
  ),
)
