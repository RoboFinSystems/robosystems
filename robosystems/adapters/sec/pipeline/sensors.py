"""SEC pipeline sensors for automated incremental updates.

Sensors trigger jobs based on SourceFile queue state and job completion.
All sensors start STOPPED by default - enable in Dagster UI when ready.

Nightly Pipeline (enable all for automated daily updates):
- Phase 1 (Download): sec_incremental_download_schedule triggers at 9pm EST weekdays
- Phase 2 (Process): sec_download_to_process_sensor chains download → process
- Phase 3 (Stage): sec_incremental_staging_sensor chains process → stage (DuckDB INSERT)
- Phase 4 (Materialize): sec_stage_to_materialize_sensor chains stage → full graph rebuild

Post-materialization S3 publish and replica refresh are handled by asset lineage:
  sec_graph_materialized -> sec_lbug_s3_published -> shared_replicas_refreshed
  sec_historical_materialized -> sec_historical_lbug_s3_published -> shared_replicas_refreshed

Manual Operations (not in automated chain):
- sec_entity_update_job: Update mutable Entity attributes (run manually after materialize)

Backfill Processing (enable for bulk/manual processing):
- sec_processing_sensor: Discovers pending SourceFiles, triggers batch processing per quarter

Nightly flow: New data is added to existing DuckDB tables (INSERT with dedup),
then the LadybugDB graph is fully rebuilt from DuckDB. The sec graph (2024+ only)
is small enough for nightly rebuilds (~75GB vs ~300GB monolith).
"""

from datetime import UTC, datetime

from dagster import (
  DagsterRunStatus,
  DefaultScheduleStatus,
  DefaultSensorStatus,
  RunRequest,
  RunsFilter,
  RunStatusSensorContext,
  SensorEvaluationContext,
  SkipReason,
  run_status_sensor,
  schedule,
  sensor,
)

from robosystems.config import env

from .configs import SEC_HISTORICAL_FORM_TYPES, SEC_PRIMARY_START_YEAR
from .jobs import (
  sec_download_job,
  sec_incremental_stage_job,
  sec_materialize_job,
  sec_process_job,
)


@sensor(
  job=sec_process_job,
  minimum_interval_seconds=300,  # Check every 5 minutes
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  description="Discover quarters with pending SourceFiles and trigger batch processing runs",
)
def sec_processing_sensor(context: SensorEvaluationContext):
  """Discover quarters with pending SEC filings and trigger batch processing.

  Each Dagster run processes up to SEC_PROCESS_BATCH_SIZE filings then exits.
  This sensor continuously triggers new runs while pending files remain,
  enabling natural memory release between batches and crash resilience.

  Batch Processing Model:
  1. Job processes batch, flushes part files to S3, exits
  2. Sensor runs every 5 minutes, detects remaining pending files
  3. Triggers another batch if pending files exist and no active run
  4. Repeats until all files processed

  Parallelism across quarters is controlled by DAGSTER_MAX_CONCURRENT_RUNS.
  Individual filing failures are tracked in SourceFile; jobs continue processing.

  Flow:
  1. Query distinct quarters from pending SourceFiles
  2. Skip quarters that already have in-progress runs
  3. Yield one RunRequest per quarter with pending files
  4. Dagster's run coordinator controls concurrent quarter processing

  Output Structure:
  - All filings output to quarterly partitions (filed=YYYY-QN)
  - Single file per table per quarter with append-based merging
  - Shared tables (Element, Label, etc.) deduplicated on identifier

  Deduplication:
  - No run_key used - allows retries after failures
  - Active run check prevents concurrent runs for same quarter
  - After batch completes, sensor re-triggers if pending files remain
  """
  import re

  from sqlalchemy import func

  from robosystems.database import session as SessionLocal
  from robosystems.models.iam import SourceFile

  # Regex pattern for robust partition key parsing
  # Format: "2024-Q1_cik_accession" -> captures "2024-Q1"
  quarter_pattern = re.compile(r"^(\d{4}-Q[1-4])_")

  # Skip in dev - use manual job triggers for testing
  if env.ENVIRONMENT == "dev":
    yield SkipReason("Skipped in dev - use Dagster UI to trigger sec_process manually")
    return

  # Query for distinct quarters with pending files
  session = None
  try:
    session = SessionLocal()

    # Extract quarter from partition_key (format: "YYYY-QN_cik_accession")
    # Group by quarter prefix to find which quarters have pending files
    pending_files = (
      session.query(SourceFile.partition_key)
      .filter(
        SourceFile.graph_id == "sec",
        SourceFile.status == "pending",
        SourceFile.partition_key.isnot(None),
      )
      .all()
    )

    # Extract unique quarters from partition keys using regex for robust parsing
    quarters_with_pending: set[str] = set()
    for (partition_key,) in pending_files:
      if partition_key:
        match = quarter_pattern.match(partition_key)
        if match:
          quarters_with_pending.add(match.group(1))

    # Get error count for logging
    error_count = (
      session.query(func.count(SourceFile.id))
      .filter(SourceFile.graph_id == "sec", SourceFile.status == "error")
      .scalar()
    )

  except Exception as e:
    context.log.error(f"Database query failed: {e}")
    yield SkipReason(f"Database error: {e}")
    return
  finally:
    if session:
      session.close()

  if not quarters_with_pending:
    if error_count > 0:
      yield SkipReason(f"No pending quarters ({error_count} files in error state)")
    else:
      yield SkipReason("No quarters with pending files")
    return

  context.log.info(
    f"Found {len(quarters_with_pending)} quarters with pending files, "
    f"{error_count} total files in error state. "
    f"Quarters: {sorted(quarters_with_pending)}"
  )

  # Yield RunRequest for each quarter with pending files
  for quarter in sorted(quarters_with_pending):
    # Check for in-progress runs to prevent duplicate processing triggers
    active_runs = context.instance.get_runs(
      filters=RunsFilter(
        job_name="sec_process",
        statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
        tags={"quarter": quarter},
      ),
      limit=1,
    )
    if active_runs:
      context.log.info(f"Skipping {quarter} - already has an active run")
      continue

    # Apply annual-only form type filter for pre-2024 partitions
    # Historical data excludes 10-Q to reduce graph size (~75% fewer filings)
    partition_year = int(quarter.split("-")[0])
    run_config: dict = {}
    if partition_year < SEC_PRIMARY_START_YEAR:
      run_config = {
        "ops": {
          "sec_processed_filings": {
            "config": {"form_types": SEC_HISTORICAL_FORM_TYPES},
          }
        }
      }
      context.log.info(
        f"Triggering {quarter} for processing (annual-only: {SEC_HISTORICAL_FORM_TYPES})"
      )
    else:
      context.log.info(f"Triggering {quarter} for processing")

    # No run_key - rely on active runs check to prevent concurrent runs.
    # This allows re-triggering after failures when pending files remain.
    yield RunRequest(
      partition_key=quarter,  # Use Dagster's partition system
      run_config=run_config,
      tags={
        "quarter": quarter,
        "pipeline": "sec",
        "phase": "process",
      },
    )


# ============================================================================
# SEC Incremental Pipeline (Automated Chain)
# ============================================================================
# When SEC_INCREMENTAL_PIPELINE_ENABLED=true, this enables a fully automated
# pipeline that runs every 3 hours:
#   download → process → stage → materialize → S3 sync
#
# Each step is chained via run_status_sensor, only proceeding on success.
# Keep disabled during backfills; enable for production incremental updates.
# ============================================================================


def _get_quarters_to_scan() -> list[str]:
  """Get quarters to scan for incremental download.

  Always scans current quarter. Also scans previous quarter during the first
  several days of a new quarter to catch late-indexed filings (filings submitted
  near quarter-end may not appear in EFTS for 1-2 days due to SEC indexing delays
  and UTC/EST timing differences).

  Returns:
      List of partition keys like ["2025-Q1"] or ["2025-Q1", "2024-Q4"]
  """
  from robosystems.adapters.sec import get_quarters_to_scan

  return get_quarters_to_scan()


@schedule(
  job=sec_download_job,
  cron_schedule="0 21 * * 1-5",  # 9pm EST, Monday-Friday
  default_status=DefaultScheduleStatus.STOPPED,  # Enable in Dagster UI when ready
  execution_timezone="America/New_York",
)
def sec_incremental_download_schedule(context):
  """Incremental SEC download at 9pm EST on weekdays.

  Part of the automated incremental pipeline. Downloads new filings for
  current quarter (and previous quarter at quarter boundaries).

  Chain: download → process → stage → materialize → S3 sync

  Enable via: SEC_INCREMENTAL_PIPELINE_ENABLED=true
  """
  from robosystems.adapters.sec.pipeline.configs import SECDownloadConfig

  quarters = _get_quarters_to_scan()
  context.log.info(f"Incremental download for quarters: {quarters}")

  # Generate batch_id to track all jobs from this schedule tick
  # Used by downstream sensors to wait for all quarters to complete
  batch_id = context.scheduled_execution_time.strftime("%Y%m%d-%H")

  for partition_key in quarters:
    yield RunRequest(
      run_key=f"sec-incremental-{partition_key}-{batch_id}",
      partition_key=partition_key,
      run_config={
        "ops": {
          "sec_raw_filings": {
            "config": SECDownloadConfig(
              skip_existing=True,
              form_types=["10-K", "10-Q", "20-F", "40-F", "DEF 14A", "S-1"],
            ).model_dump(),
          },
        }
      },
      tags={
        "pipeline": "sec",
        "phase": "download",
        "mode": "incremental",
        "batch_id": batch_id,  # Track jobs from same schedule tick
      },
    )


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_download_job],
  request_job=sec_process_job,
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  minimum_interval_seconds=60,
  description="Chain: download success → trigger processing",
)
def sec_download_to_process_sensor(context: RunStatusSensorContext):
  """Trigger SEC processing after download completes successfully.

  Part of the automated incremental pipeline. When a download job succeeds,
  this triggers processing for the same partition (quarter).

  Enable via: SEC_INCREMENTAL_PIPELINE_ENABLED=true
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping chain sensor in dev environment")
    return

  dagster_run = context.dagster_run

  # Only chain incremental pipeline runs (tagged with mode=incremental)
  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Skipping - not an incremental pipeline run")
    return

  # Get partition from the completed download run
  partition_key = dagster_run.tags.get("dagster/partition")
  if not partition_key:
    context.log.warning("No partition key found on download run")
    return

  # Check for already running process job for this partition
  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name="sec_process",
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
      tags={"dagster/partition": partition_key},
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(f"Process job already running for {partition_key}, skipping")
    return

  # Propagate batch_id for downstream batch tracking
  batch_id = run_tags.get("batch_id")

  context.log.info(
    f"Download completed for {partition_key} (batch={batch_id}), triggering processing"
  )

  yield RunRequest(
    run_key=f"sec-process-chain-{partition_key}-{dagster_run.run_id[:8]}",
    partition_key=partition_key,
    tags={
      "pipeline": "sec",
      "phase": "process",
      "mode": "incremental",
      "quarter": partition_key,
      "batch_id": batch_id,  # Propagate for batch tracking
    },
  )


# ============================================================================
# SEC Incremental Staging Sensor
# ============================================================================
# This sensor triggers DuckDB staging after processing completes:
#   download → process → stage (DuckDB) → copy (LadybugDB) → S3 sync
#
# Controlled by: SEC_INCREMENTAL_PIPELINE_ENABLED=true
#
# DuckDB staging keeps the staging tables in sync for potential full rebuilds.
# LadybugDB updates happen via direct S3 copy (next step in chain).


@sensor(
  job=sec_incremental_stage_job,
  minimum_interval_seconds=300,  # Check every 5 minutes
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  description="Trigger incremental DuckDB staging when all pending SourceFiles are processed",
)
def sec_incremental_staging_sensor(context: SensorEvaluationContext):
  """Trigger incremental DuckDB staging when all pending SourceFiles are processed.

  Part of the automated incremental pipeline. Detects when pending count
  reaches 0 (all files processed), then triggers incremental staging for
  current quarter. Keeps DuckDB in sync for potential full rebuilds.

  Next step: sec_stage_to_materialize_sensor triggers full LadybugDB rebuild after staging.

  Controlled by: SEC_INCREMENTAL_PIPELINE_ENABLED=true
  """
  from datetime import timedelta

  from robosystems.database import session as SessionLocal
  from robosystems.models.iam import SourceFile

  # Skip in dev - use manual job triggers for testing
  if env.ENVIRONMENT == "dev":
    yield SkipReason("Skipped in dev - use Dagster UI to trigger manually")
    return

  session = None
  try:
    session = SessionLocal()

    # Check for pending files
    pending_count = (
      session.query(SourceFile)
      .filter(
        SourceFile.graph_id == "sec",
        SourceFile.status == "pending",
      )
      .count()
    )

    if pending_count > 0:
      yield SkipReason(f"{pending_count} files still pending processing")
      return

    # Check if there are recently processed files (last 24 hours)
    cutoff = datetime.now(UTC) - timedelta(hours=24)

    recent_count = (
      session.query(SourceFile)
      .filter(
        SourceFile.graph_id == "sec",
        SourceFile.status == "success",
        SourceFile.processed_at >= cutoff,
      )
      .count()
    )

    if recent_count == 0:
      yield SkipReason("No recently processed files to stage")
      return

  except Exception as e:
    context.log.error(f"Database query failed: {e}")
    yield SkipReason(f"Database error: {e}")
    return
  finally:
    if session:
      session.close()

  # Check if incremental stage job is already running
  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name="sec_incremental_stage",
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(
      f"Incremental stage already running (run_id={active_runs[0].run_id}), skipping"
    )
    return

  # Create run key based on current date to prevent duplicate daily runs
  today = datetime.now(UTC).strftime("%Y-%m-%d")
  run_key = f"sec-incremental-stage-{today}"

  context.log.info(
    f"All processing complete ({recent_count} files in last 24h), "
    f"triggering incremental DuckDB staging"
  )

  yield RunRequest(
    run_key=run_key,
    run_config={
      "ops": {
        "sec_duckdb_incremental_staged": {
          "config": {
            "graph_id": "sec",
            # year/quarter default to current if not specified
          }
        },
      }
    },
    tags={
      "pipeline": "sec",
      "phase": "incremental_stage",
      "mode": "incremental",
    },
  )


# ============================================================================
# SEC Stage to Materialize Sensor (Chains Stage → Full Graph Rebuild)
# ============================================================================
# After DuckDB incremental staging adds new rows, triggers a full LadybugDB
# rebuild from DuckDB. The sec graph (2024+ only) is small enough for nightly
# rebuilds, and this ensures the graph is always consistent with DuckDB.
#
# Chain: stage (DuckDB INSERT) → materialize (full rebuild) → S3 sync
# S3 sync is handled by sec_post_materialize_s3_sync_sensor (already exists).


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_incremental_stage_job],
  request_job=sec_materialize_job,
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  minimum_interval_seconds=60,
  description="Trigger full graph rebuild after incremental DuckDB staging completes",
)
def sec_stage_to_materialize_sensor(context: RunStatusSensorContext):
  """Trigger full LadybugDB rebuild after incremental DuckDB staging completes.

  Part of the nightly pipeline chain:
    process → stage (DuckDB INSERT) → materialize (full rebuild) → S3 sync

  After new data is added to DuckDB, the LadybugDB graph is fully rebuilt.
  The sec graph (2024+ only, ~75GB) is small enough for nightly rebuilds.
  S3 sync is handled by the existing sec_post_materialize_s3_sync_sensor.
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping chain sensor in dev environment")
    return

  dagster_run = context.dagster_run

  # Only chain incremental pipeline runs
  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Run is not incremental mode, skipping chain")
    return

  # Check if materialize job is already running
  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name="sec_materialize",
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(
      f"Materialize job already running (run_id={active_runs[0].run_id}), skipping"
    )
    return

  context.log.info(
    f"DuckDB staging completed (run_id={dagster_run.run_id}), "
    "triggering full LadybugDB rebuild from DuckDB"
  )

  yield RunRequest(
    run_key=f"sec-materialize-chain-{dagster_run.run_id[:8]}",
    run_config={
      "ops": {
        "sec_graph_materialized": {
          "config": {
            "graph_id": "sec",
            "rebuild_graph": True,
          }
        },
      }
    },
    tags={
      "pipeline": "sec",
      "phase": "materialize",
      "mode": "incremental",
    },
  )
