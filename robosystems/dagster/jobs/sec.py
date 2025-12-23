"""Dagster SEC pipeline jobs and schedules.

Pipeline Architecture (3 phases, run independently):

  Phase 1 - Download:
    sec_download_only_job: sec_companies_list → sec_raw_filings
    Downloads raw XBRL ZIPs to S3.

  Phase 2 - Process (sensor-triggered or manual):
    sec_process_job: sec_process_filing (dynamic partitions)
    Parallel processing - one partition per filing.

  Phase 3 - Materialize:
    sec_materialize_job: sec_duckdb_staging → sec_graph_materialized
    Ingests all processed data to LadybugDB graph.

Workflow:
  just sec-download 10 2024    # Download top 10 companies
  just sec-process 2024        # Process in parallel
  just sec-materialize         # Ingest to graph

  # Or all-in-one for demos:
  just sec-load NVDA 2024      # Chains all steps
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
  SECCompaniesConfig,
  SECDownloadConfig,
  sec_companies_list,
  sec_duckdb_staging,
  sec_filing_partitions,
  sec_graph_materialized,
  sec_process_filing,
  sec_raw_filings,
  sec_year_partitions,
)

# ============================================================================
# SEC Pipeline Jobs
# ============================================================================


# Phase 1: Download (year-partitioned)
# Downloads raw XBRL ZIPs to S3.
# Use with sec_processing_sensor to trigger parallel processing.
sec_download_only_job = define_asset_job(
  name="sec_download_only",
  description="Download SEC XBRL filings to S3. Use sensor or just sec-process for parallel processing.",
  selection=AssetSelection.assets(
    sec_companies_list,
    sec_raw_filings,
  ),
  tags={"pipeline": "sec", "phase": "download"},
  partitions_def=sec_year_partitions,
)


# Phase 2: Process (dynamic partitions per filing)
# NOTE: This job only includes sec_process_filing. Discovery is done by
# the sec_processing_sensor which registers partitions and triggers runs.
sec_process_job = define_asset_job(
  name="sec_process",
  description="Process SEC filings to parquet. One partition per filing.",
  selection=AssetSelection.assets(
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

# Download schedule: Enable via SEC_DOWNLOAD_SCHEDULE_ENABLED=true
# Fetches new filings daily. Sensor auto-triggers parallel processing.
SEC_DOWNLOAD_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.SEC_DOWNLOAD_SCHEDULE_ENABLED
  else DefaultScheduleStatus.STOPPED
)

# Materialize schedule: Enable via SEC_MATERIALIZE_SCHEDULE_ENABLED=true
# OFF by default - run manually until comfortable with the pipeline.
SEC_MATERIALIZE_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.SEC_MATERIALIZE_SCHEDULE_ENABLED
  else DefaultScheduleStatus.STOPPED
)


sec_daily_download_schedule = ScheduleDefinition(
  name="sec_daily_download",
  description="Daily SEC download at 6 AM UTC. Sensor triggers parallel processing.",
  job=sec_download_only_job,
  cron_schedule="0 6 * * *",
  default_status=SEC_DOWNLOAD_SCHEDULE_STATUS,
  run_config=RunConfig(
    ops={
      "sec_companies_list": SECCompaniesConfig(),
      "sec_raw_filings": SECDownloadConfig(
        skip_existing=True,
        form_types=["10-K", "10-Q"],
      ),
    }
  ),
)


sec_nightly_materialize_schedule = ScheduleDefinition(
  name="sec_nightly_materialize",
  description="Nightly SEC graph materialization at 2 AM UTC. OFF by default.",
  job=sec_materialize_job,
  cron_schedule="0 2 * * *",
  default_status=SEC_MATERIALIZE_SCHEDULE_STATUS,
)
