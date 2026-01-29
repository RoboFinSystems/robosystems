"""Dagster SEC pipeline jobs and schedules.

Pipeline Architecture (3 phases, run independently):

  Phase 1 - Download (EFTS-based, quarterly partitions):
    sec_download_job: sec_raw_filings
    Uses SEC EFTS API to discover and download XBRL ZIPs to S3.
    Quarterly partitions (e.g., 2024-Q1) to stay under EFTS 10k result limit.
    Creates SourceFile records in PostgreSQL for processing tracking.

  Phase 2 - Process (quarterly batch with consolidated output):
    sec_process_job: sec_processed_filings
    Each run processes an entire quarter's worth of filings.
    Outputs consolidated parquet files (one per table per quarter).
    Individual filing failures tracked in SourceFile; job continues processing.
    Parallel across quarters via DAGSTER_MAX_CONCURRENT_RUNS.

  Phase 3 - Materialize (two-stage pipeline):
    sec_stage_job: sec_duckdb_staged (DuckDB staging - full or incremental mode)
    sec_materialize_job: sec_graph_materialized (LadybugDB materialization)

    If LadybugDB materialization fails, just re-run sec_materialize_job.

Workflow:
  just sec-download 10 2024    # Download top 10 companies (all 4 quarters)
  # Enable sec_processing_sensor in Dagster UI to auto-process quarters
  just sec-materialize         # Stage to DuckDB + Materialize to LadybugDB

  # Decoupled (for checkpointing/retry):
  just sec-stage               # Stage 1: Stage to DuckDB only (full mode)
  just sec-materialize-graph   # Stage 2: Materialize to LadybugDB (retry-safe)

  # Manual quarter processing (via Dagster UI):
  # Launch sec_process job with partition_key: "2024-Q1"

  # Or all-in-one for demos:
  just sec-load NVDA 2024      # Chains all steps for single company
"""

from datetime import UTC, datetime

from dagster import (
  AssetSelection,
  DefaultScheduleStatus,
  RunConfig,
  RunRequest,
  ScheduleDefinition,
  define_asset_job,
  schedule,
)

from robosystems.config import env
from robosystems.dagster.assets import (
  SECDownloadConfig,
  sec_duckdb_staged,
  sec_graph_materialized,
  sec_processed_filings,
  sec_quarter_partitions,
  sec_raw_filings,
)

# ============================================================================
# SEC Pipeline Jobs
# ============================================================================


# Phase 1: Download (quarter-partitioned)
# Downloads raw XBRL ZIPs to S3 using EFTS discovery.
# Uses quarterly partitions to stay under EFTS 10k result limit.
# Use with sec_processing_sensor to trigger parallel processing.
sec_download_job = define_asset_job(
  name="sec_download",
  description="Download SEC XBRL filings to S3 via EFTS (quarterly partitions).",
  selection=AssetSelection.assets(
    sec_raw_filings,
  ),
  tags={"pipeline": "sec", "phase": "download"},
  partitions_def=sec_quarter_partitions,
)


# Phase 2: Process (quarterly batch processing)
# Each run processes an entire quarter's worth of filings (up to 10,000),
# outputting consolidated parquet files. Parallel execution across quarters
# is controlled by DAGSTER_MAX_CONCURRENT_RUNS.
#
# Uses Heavy profile (16 vCPU, 64 GB) - sized to handle full quarterly batches.
# EFTS returns max 10k filings per quarterly partition, so batch_limit=10000
# matches the theoretical maximum. Uses On-Demand Fargate for reliability.
sec_process_job = define_asset_job(
  name="sec_process",
  description="Process an entire quarter's SEC filings (up to 10k) with heavy resources.",
  selection=AssetSelection.assets(
    sec_processed_filings,
  ),
  partitions_def=sec_quarter_partitions,
  tags={
    "pipeline": "sec",
    "phase": "process",
    # Heavy profile: 16 vCPU, 64 GB - sized for full quarterly batches (10k filings max)
    "ecs/task_definition": f"robosystems-dagster-run-heavy-{env.ENVIRONMENT}",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 3: Materialize (Decoupled Staging + Materialization)
# ============================================================================
# Decoupled design enables retry of materialization without re-staging:
# - sec_stage_job: Stage to persistent DuckDB (2+ hours for full SEC)
# - sec_materialize_job: Materialize from DuckDB to LadybugDB (retry-safe)
#
# If LadybugDB materialization fails, just re-run sec_materialize_job.

# Stage 1: DuckDB Staging
# Discovers processed files from S3 and stages to persistent DuckDB.
# Uses Standard profile (2 vCPU, 8 GB) - staging is I/O bound, not CPU intensive.
sec_stage_job = define_asset_job(
  name="sec_stage",
  description="Stage SEC files to persistent DuckDB (no graph ingestion).",
  selection=AssetSelection.assets(sec_duckdb_staged),
  tags={
    "pipeline": "sec",
    "phase": "stage",
    # Standard profile: 2 vCPU, 8 GB - staging is mostly I/O bound
    "ecs/task_definition": f"robosystems-dagster-run-standard-{env.ENVIRONMENT}",
    # Long-running job (2+ hours) - use on-demand to avoid Spot interruptions
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)

# Stage 2: LadybugDB Materialization
# Materializes to LadybugDB from existing DuckDB staging.
# Retry-safe: if this fails, just re-run it - DuckDB staging is preserved.
# Uses Standard profile (2 vCPU, 8 GB) - materialization is network/API bound.
sec_materialize_job = define_asset_job(
  name="sec_materialize",
  description="Materialize SEC graph from DuckDB staging (retry-safe).",
  selection=AssetSelection.assets(sec_graph_materialized),
  tags={
    "pipeline": "sec",
    "phase": "materialize",
    # Standard profile: 2 vCPU, 8 GB - materialization is network/API bound
    "ecs/task_definition": f"robosystems-dagster-run-standard-{env.ENVIRONMENT}",
    # Long-running job - use on-demand to avoid Spot interruptions
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)

# Combined: Run both stages in sequence
# Useful for full rebuilds with checkpointing between stages.
# Uses Standard profile (2 vCPU, 8 GB) - combined stage+materialize is I/O bound.
sec_staged_materialize_job = define_asset_job(
  name="sec_staged_materialize",
  description="Full SEC pipeline: stage to DuckDB then materialize to LadybugDB.",
  selection=AssetSelection.assets(sec_duckdb_staged, sec_graph_materialized),
  tags={
    "pipeline": "sec",
    "phase": "full",
    # Standard profile: 2 vCPU, 8 GB - stage+materialize is I/O/network bound
    "ecs/task_definition": f"robosystems-dagster-run-standard-{env.ENVIRONMENT}",
    # Long-running job (2+ hours) - use on-demand to avoid Spot interruptions
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
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


def _get_quarters_to_scan() -> list[str]:
  """Get quarters to scan for daily download.

  Always scans current quarter. Also scans previous quarter during the first
  few days of a new quarter to catch late-indexed filings (filings submitted
  on the last day of a quarter may not appear in EFTS until the next day).

  Returns:
      List of partition keys like ["2025-Q1"] or ["2025-Q1", "2024-Q4"]
  """
  now = datetime.now(UTC)
  current_quarter = (now.month - 1) // 3 + 1
  current_year = now.year

  quarters = [f"{current_year}-Q{current_quarter}"]

  # Quarter start months: Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct
  quarter_start_month = (current_quarter - 1) * 3 + 1

  # Scan previous quarter for first 3 days of new quarter
  # This catches filings submitted late on quarter-end that get indexed next day
  if now.month == quarter_start_month and now.day <= 3:
    if current_quarter == 1:
      quarters.append(f"{current_year - 1}-Q4")
    else:
      quarters.append(f"{current_year}-Q{current_quarter - 1}")

  return quarters


@schedule(
  job=sec_download_job,
  cron_schedule="0 3 * * *",  # 3am UTC = 10pm EST (after last daily filings)
  default_status=SEC_DOWNLOAD_SCHEDULE_STATUS,
)
def sec_daily_download_schedule(context):
  """Daily SEC download at 3 AM UTC (10 PM EST) via EFTS.

  Runs after SEC's typical last filing cutoff (8-9 PM EST).
  Scans current quarter + previous quarter to catch late filings
  at quarter boundaries. Sensor triggers parallel processing.
  """
  quarters = _get_quarters_to_scan()
  context.log.info(f"Scheduling SEC download for quarters: {quarters}")

  for partition_key in quarters:
    yield RunRequest(
      run_key=f"sec-download-{partition_key}-{context.scheduled_execution_time.strftime('%Y%m%d')}",
      partition_key=partition_key,
      run_config=RunConfig(
        ops={
          "sec_raw_filings": SECDownloadConfig(
            skip_existing=True,
            form_types=["10-K", "10-Q"],
          ),
        }
      ),
    )


sec_nightly_materialize_schedule = ScheduleDefinition(
  name="sec_nightly_materialize",
  description="Nightly SEC graph materialization at 6 AM UTC. OFF by default.",
  job=sec_materialize_job,
  cron_schedule="0 6 * * *",  # 6am UTC = 1am EST (after incremental staging)
  default_status=SEC_MATERIALIZE_SCHEDULE_STATUS,
)
