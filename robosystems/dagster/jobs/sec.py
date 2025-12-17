"""Dagster SEC pipeline jobs and schedules.

These jobs orchestrate the SEC XBRL pipeline:
- sec_single_company_job: Local dev (just sec-load NVDA 2025)
- sec_full_rebuild_job: Production (all companies, all years)
- sec_daily_rebuild_schedule: Daily at 2 AM

Migration Notes:
- Replaces Celery tasks in robosystems/tasks/sec_xbrl/
- Uses Dagster's partitioned assets for year-based processing
- Rate limiting via tag-based concurrency limits
"""

from dagster import (
  AssetSelection,
  DefaultScheduleStatus,
  RunConfig,
  ScheduleDefinition,
  define_asset_job,
)

from robosystems.dagster.assets import (
  sec_companies_list,
  sec_raw_filings,
  sec_processed_filings,
  sec_duckdb_staging,
  sec_graph_materialized,
  sec_year_partitions,
  SECCompaniesConfig,
  SECDownloadConfig,
  SECProcessConfig,
  SECDuckDBConfig,
  SECMaterializeConfig,
)


# ============================================================================
# SEC Pipeline Jobs
# ============================================================================


# Job for single company (local development)
# Usage: just sec-load NVDA 2025
sec_single_company_job = define_asset_job(
  name="sec_single_company",
  description=(
    "Process SEC filings for a single company. "
    "Used for local development and testing with 'just sec-load TICKER YEAR'."
  ),
  selection=AssetSelection.assets(
    sec_companies_list,
    sec_raw_filings,
    sec_processed_filings,
    sec_duckdb_staging,
    sec_graph_materialized,
  ),
  tags={
    "pipeline": "sec",
    "mode": "single_company",
  },
  partitions_def=sec_year_partitions,  # Required for partitioned assets
)


# Job for full rebuild (production)
# Processes all companies across all years
sec_full_rebuild_job = define_asset_job(
  name="sec_full_rebuild",
  description=(
    "Full SEC pipeline rebuild. "
    "Downloads, processes, and materializes all filings for all years. "
    "Rate-limited to 2 concurrent downloads."
  ),
  selection=AssetSelection.assets(
    sec_companies_list,
    sec_raw_filings,
    sec_processed_filings,
    sec_duckdb_staging,
    sec_graph_materialized,
  ),
  tags={
    "pipeline": "sec",
    "mode": "full_rebuild",
  },
  partitions_def=sec_year_partitions,
)


# Job for staging and materialization only (skip download/processing)
# Useful when parquet files already exist in S3
sec_materialize_only_job = define_asset_job(
  name="sec_materialize_only",
  description=(
    "Materialize SEC graph from existing processed files. "
    "Skips download and processing stages."
  ),
  selection=AssetSelection.assets(
    sec_duckdb_staging,
    sec_graph_materialized,
  ),
  tags={
    "pipeline": "sec",
    "mode": "materialize_only",
  },
)


# ============================================================================
# SEC Pipeline Schedules
# ============================================================================


# Daily rebuild at 2 AM
sec_daily_rebuild_schedule = ScheduleDefinition(
  name="sec_daily_rebuild",
  description="Daily SEC pipeline rebuild at 2 AM UTC",
  job=sec_full_rebuild_job,
  cron_schedule="0 2 * * *",  # 2 AM UTC daily
  default_status=DefaultScheduleStatus.STOPPED,  # Manual start in production
  run_config=RunConfig(
    ops={
      "sec_companies_list": SECCompaniesConfig(),
      "sec_raw_filings": SECDownloadConfig(
        skip_existing=True,
        form_types=["10-K", "10-Q"],
      ),
      "sec_processed_filings": SECProcessConfig(refresh=False),
      "sec_duckdb_staging": SECDuckDBConfig(rebuild=True),
      "sec_graph_materialized": SECMaterializeConfig(
        graph_id="sec",
        ignore_errors=True,
        rebuild=True,
      ),
    }
  ),
)


# Weekly full refresh (with reprocessing)
sec_weekly_refresh_schedule = ScheduleDefinition(
  name="sec_weekly_refresh",
  description="Weekly SEC pipeline refresh with reprocessing at 3 AM UTC on Sundays",
  job=sec_full_rebuild_job,
  cron_schedule="0 3 * * 0",  # 3 AM UTC on Sundays
  default_status=DefaultScheduleStatus.STOPPED,
  run_config=RunConfig(
    ops={
      "sec_companies_list": SECCompaniesConfig(),
      "sec_raw_filings": SECDownloadConfig(
        skip_existing=False,  # Re-download all
        form_types=["10-K", "10-Q"],
      ),
      "sec_processed_filings": SECProcessConfig(refresh=True),  # Re-process all
      "sec_duckdb_staging": SECDuckDBConfig(rebuild=True),
      "sec_graph_materialized": SECMaterializeConfig(
        graph_id="sec",
        ignore_errors=True,
        rebuild=True,
      ),
    }
  ),
)
