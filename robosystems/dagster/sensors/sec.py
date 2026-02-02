"""SEC pipeline sensors for automated incremental updates.

Sensors trigger jobs based on SourceFile queue state and job completion.
All sensors start STOPPED by default - enable in Dagster UI when ready.

Incremental Pipeline (enable all for automated daily updates):
- Phase 1 (Download): sec_incremental_download_schedule triggers at 9pm EST weekdays
- Phase 2 (Process): sec_download_to_process_sensor chains download → process
- Phase 3 (Stage): sec_incremental_staging_sensor chains process → stage (DuckDB only)
- Phase 4 (Copy): sec_stage_to_copy_sensor chains stage → copy (S3 → LadybugDB direct)
- Phase 5 (Snapshot): sec_incremental_post_ingest_snapshot_sensor chains copy → snapshot

Backfill Processing (enable for bulk/manual processing):
- sec_processing_sensor: Discovers pending SourceFiles, triggers batch processing per quarter

Incremental staging uses INSERT INTO with dedup (only net new rows added to DuckDB).
LadybugDB updates via direct S3 copy with ignore_errors=true for duplicate handling.
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
from robosystems.dagster.jobs.sec import (
  sec_download_job,
  sec_incremental_copy_job,
  sec_incremental_stage_job,
  sec_materialize_job,
  sec_process_job,
)
from robosystems.dagster.jobs.shared_repository import shared_repository_snapshot_job


@sensor(
  job=sec_process_job,
  minimum_interval_seconds=300,  # Check every 5 minutes
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  description="Discover quarters with pending SourceFiles and trigger batch processing runs",
)
def sec_processing_sensor(context: SensorEvaluationContext):
  """Discover quarters with pending SEC filings and trigger batch processing.

  Each Dagster run processes up to SEC_PROCESS_BATCH_LIMIT filings then exits.
  This sensor continuously triggers new runs while pending files remain,
  enabling natural memory release between batches and crash resilience.

  Batch Processing Model:
  1. Job processes batch, flushes to S3 (merging with existing data), exits
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

    context.log.info(f"Triggering {quarter} for processing")

    # No run_key - rely on active runs check to prevent concurrent runs.
    # This allows re-triggering after failures when pending files remain.
    yield RunRequest(
      partition_key=quarter,  # Use Dagster's partition system
      tags={
        "quarter": quarter,
        "pipeline": "sec",
        "phase": "process",
      },
    )


# ============================================================================
# SEC Post-Materialization Snapshot Sensor
# ============================================================================


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_materialize_job],
  request_job=shared_repository_snapshot_job,
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  minimum_interval_seconds=60,
  description="Trigger shared replica snapshot after SEC materialization completes",
)
def sec_post_materialize_snapshot_sensor(context: RunStatusSensorContext):
  """Trigger shared repository snapshot after SEC materialization succeeds.

  This sensor watches for successful completion of sec_materialize_job
  and triggers shared_repository_snapshot_job to:
  1. Create EBS snapshot of shared master's data volume
  2. Update replica launch template with new snapshot
  3. Trigger rolling refresh of replica fleet

  This ensures replicas are updated with the latest SEC data after each
  nightly materialization run.

  Controlled by: SEC_INCREMENTAL_PIPELINE_ENABLED=true (part of full pipeline)

  Note: Only triggers for jobs on the "sec" graph_id (shared repository).
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping snapshot sensor in dev environment")
    return

  # Get the run that triggered this sensor
  dagster_run = context.dagster_run
  run_config = dagster_run.run_config or {}

  # Extract graph_id from run config to verify it's the SEC shared repository
  try:
    ops_config = run_config.get("ops", {})
    materialize_config = ops_config.get("sec_graph_materialized", {}).get("config", {})
    graph_id = materialize_config.get("graph_id", "sec")
  except (KeyError, TypeError, AttributeError):
    graph_id = "sec"  # Default to sec if not found

  # Only trigger for SEC shared repository materialization
  if graph_id != "sec":
    context.log.info(f"Skipping snapshot - graph_id '{graph_id}' is not 'sec'")
    return

  # Check if a snapshot job is already running to prevent concurrent executions
  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name="shared_repository_snapshot_job",
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(
      f"Snapshot job already running (run_id={active_runs[0].run_id}), skipping"
    )
    return

  context.log.info(
    f"SEC materialization completed (run_id={dagster_run.run_id}). "
    "Triggering shared repository snapshot."
  )

  # Create unique run key to prevent duplicate triggers
  today = datetime.now(UTC).strftime("%Y-%m-%d")
  run_key = f"sec-post-materialize-snapshot-{today}-{dagster_run.run_id[:8]}"

  yield RunRequest(
    run_key=run_key,
    run_config={
      "ops": {
        "get_shared_master_volume": {
          "config": {
            "graph_id": "sec",
            "wait_for_completion": True,
            "description_prefix": "SEC post-materialize",
          }
        },
        "refresh_replica_instances": {
          "config": {
            "min_healthy_percentage": 50,
            "instance_warmup_seconds": 300,
          }
        },
      }
    },
  )


# ============================================================================
# SEC Incremental Pipeline (Automated Chain)
# ============================================================================
# When SEC_INCREMENTAL_PIPELINE_ENABLED=true, this enables a fully automated
# pipeline that runs every 3 hours:
#   download → process → stage → materialize → snapshot
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
  now = datetime.now(UTC)
  current_quarter = (now.month - 1) // 3 + 1
  current_year = now.year

  quarters = [f"{current_year}-Q{current_quarter}"]

  # Quarter start months: Q1=Jan, Q2=Apr, Q3=Jul, Q4=Oct
  quarter_start_month = (current_quarter - 1) * 3 + 1

  # Scan previous quarter for first 5 days of new quarter
  # This catches filings submitted late on quarter-end that get indexed later
  # (SEC indexing delays + UTC/EST timing can push filings 1-2 days)
  if now.month == quarter_start_month and now.day <= 5:
    if current_quarter == 1:
      quarters.append(f"{current_year - 1}-Q4")
    else:
      quarters.append(f"{current_year}-Q{current_quarter - 1}")

  return quarters


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

  Chain: download → process → stage → materialize → snapshot

  Enable via: SEC_INCREMENTAL_PIPELINE_ENABLED=true
  """
  from robosystems.dagster.assets import SECDownloadConfig

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
          "sec_raw_filings": SECDownloadConfig(
            skip_existing=True,
            form_types=["10-K", "10-Q", "20-F", "40-F", "DEF 14A", "S-1"],
          ),
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
#   download → process → stage (DuckDB) → copy (LadybugDB) → snapshot
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

  Next step: sec_stage_to_copy_sensor triggers LadybugDB copy after staging.

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
# SEC Stage to Copy Sensor (Chains Stage → Copy)
# ============================================================================
# This sensor triggers LadybugDB copy after DuckDB staging completes:
#   stage (DuckDB) → copy (S3 → LadybugDB with ignore_errors)
#
# Direct S3 copy uses ignore_errors=true to handle duplicates via constraint
# violations. Much faster than full DuckDB → LadybugDB rebuild.
#
# Controlled by: SEC_INCREMENTAL_PIPELINE_ENABLED=true


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_incremental_stage_job],
  request_job=sec_incremental_copy_job,
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  minimum_interval_seconds=60,
  description="Trigger incremental S3 → LadybugDB copy after DuckDB staging completes",
)
def sec_stage_to_copy_sensor(context: RunStatusSensorContext):
  """Trigger incremental S3 → LadybugDB copy after DuckDB staging completes.

  Part of the automated incremental pipeline chain:
    process → stage (DuckDB) → copy (S3 → LadybugDB) → snapshot

  Uses ignore_errors=true to skip duplicates via constraint violations.
  Much faster than full DuckDB → LadybugDB rebuild.

  Controlled by: SEC_INCREMENTAL_PIPELINE_ENABLED=true
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

  # Check if copy job is already running
  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name="sec_incremental_copy",
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(
      f"Incremental copy already running (run_id={active_runs[0].run_id}), skipping"
    )
    return

  context.log.info(
    f"DuckDB staging completed (run_id={dagster_run.run_id}), triggering S3 → LadybugDB copy"
  )

  yield RunRequest(
    run_key=f"sec-copy-chain-{dagster_run.run_id[:8]}",
    run_config={
      "ops": {
        "sec_graph_incremental_copy": {
          "config": {
            "graph_id": "sec",
            # year/quarter default to current if not specified
          }
        },
      }
    },
    tags={
      "pipeline": "sec",
      "phase": "incremental_copy",
      "mode": "incremental",
    },
  )


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_incremental_copy_job],
  request_job=shared_repository_snapshot_job,
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  minimum_interval_seconds=60,
  description="Trigger shared repository snapshot after incremental copy completes",
)
def sec_incremental_post_ingest_snapshot_sensor(context: RunStatusSensorContext):
  """Trigger shared repository snapshot after incremental copy completes.

  Part of the automated incremental pipeline. After S3 → LadybugDB copy
  succeeds, this triggers:
  1. Create EBS snapshot of shared master's data volume
  2. Update replica launch template with new snapshot
  3. Trigger rolling refresh of replica fleet

  This ensures replicas serve the latest SEC data after daily updates.

  Controlled by: SEC_INCREMENTAL_PIPELINE_ENABLED=true
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping snapshot sensor in dev environment")
    return

  dagster_run = context.dagster_run

  # Check if a snapshot job is already running
  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name="shared_repository_snapshot_job",
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(
      f"Snapshot job already running (run_id={active_runs[0].run_id}), skipping"
    )
    return

  context.log.info(
    f"Incremental ingest completed (run_id={dagster_run.run_id}), "
    "triggering snapshot + replica refresh"
  )

  yield RunRequest(
    run_key=f"snapshot-after-{dagster_run.run_id[:8]}",
    run_config={
      "ops": {
        "get_shared_master_volume": {
          "config": {
            "graph_id": "sec",
            "wait_for_completion": True,
            "description_prefix": "SEC incremental ingest",
          }
        },
        "refresh_replica_instances": {
          "config": {
            "min_healthy_percentage": 50,
            "instance_warmup_seconds": 300,
          }
        },
      }
    },
  )
