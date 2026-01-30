"""SEC pipeline sensors for parallel processing.

Sensors trigger per-filing processing jobs based on SourceFile queue state.

Architecture:
- Phase 1 (Downloads): sec_raw_filings downloads ZIPs to S3, creates SourceFile records
- Phase 2 (Processing): This sensor discovers pending files, triggers per-file runs
- Phase 3 (Materialization): sec_materialize_job ingests to graph (sequential)
- Phase 4 (Snapshot): After materialization, trigger shared replica snapshot

The sensor yields one RunRequest per pending SourceFile. Each run processes
one filing in its own ECS container, enabling parallel execution.
Dagster's run coordinator (DAGSTER_MAX_CONCURRENT_RUNS) controls concurrency.
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
  sec_materialize_job,
  sec_process_job,
  sec_staged_materialize_job,
)
from robosystems.dagster.jobs.shared_repository import shared_repository_snapshot_job

# Sensor status controlled by environment variable
SEC_PROCESSING_SENSOR_STATUS = (
  DefaultSensorStatus.RUNNING
  if env.SEC_PARALLEL_SENSOR_ENABLED
  else DefaultSensorStatus.STOPPED
)


@sensor(
  job=sec_process_job,
  minimum_interval_seconds=300,  # Check every 5 minutes
  default_status=SEC_PROCESSING_SENSOR_STATUS,
  description="Discover quarters with pending SourceFiles and trigger batch processing runs",
)
def sec_processing_sensor(context: SensorEvaluationContext):
  """Discover quarters with pending SEC filings and trigger batch processing.

  Each Dagster run processes up to 500 filings (batch_limit) then exits.
  This sensor continuously triggers new runs while pending files remain,
  enabling natural memory release between batches.

  Batch Processing Model:
  1. Job processes up to 500 filings, exits gracefully
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

  Partition Mode Selection:
  - Current quarter: use_filing_date_partition=True (daily mode)
    Each filing outputs to its actual filing date for incremental staging.
  - Historical quarters: use_filing_date_partition=False (quarterly mode)
    All filings output to quarter-end date for efficient backfill.

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

  # Determine current quarter for partition mode selection
  now = datetime.now(UTC)
  current_quarter = f"{now.year}-Q{(now.month - 1) // 3 + 1}"
  context.log.info(f"Current quarter: {current_quarter} (will use daily partitioning)")

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

    # Use daily partitioning for current quarter (incremental processing)
    # Use quarterly partitioning for historical quarters (backfill)
    use_daily = quarter == current_quarter
    mode = "daily" if use_daily else "quarterly"

    context.log.info(f"Triggering {quarter} with {mode} partitioning")

    # No run_key - rely on active runs check to prevent concurrent runs.
    # This allows re-triggering after failures when pending files remain.
    yield RunRequest(
      partition_key=quarter,  # Use Dagster's partition system
      run_config={
        "ops": {
          "sec_processed_filings": {
            "config": {
              "use_filing_date_partition": use_daily,
            }
          }
        }
      },
      tags={
        "quarter": quarter,
        "pipeline": "sec",
        "phase": "process",
        "partition_mode": mode,
      },
    )


# ============================================================================
# SEC Post-Materialization Snapshot Sensor
# ============================================================================

# Controlled by SEC_INCREMENTAL_PIPELINE_ENABLED - part of the full pipeline chain
# download → process → stage → materialize → snapshot
SEC_SNAPSHOT_SENSOR_STATUS = (
  DefaultSensorStatus.RUNNING
  if env.SEC_INCREMENTAL_PIPELINE_ENABLED
  else DefaultSensorStatus.STOPPED
)


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_materialize_job],
  request_job=shared_repository_snapshot_job,
  default_status=SEC_SNAPSHOT_SENSOR_STATUS,
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

SEC_INCREMENTAL_PIPELINE_STATUS = (
  DefaultSensorStatus.RUNNING
  if env.SEC_INCREMENTAL_PIPELINE_ENABLED
  else DefaultSensorStatus.STOPPED
)

SEC_INCREMENTAL_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.SEC_INCREMENTAL_PIPELINE_ENABLED
  else DefaultScheduleStatus.STOPPED
)


def _get_quarters_to_scan() -> list[str]:
  """Get quarters to scan for incremental download.

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
  cron_schedule="0 */3 * * *",  # Every 3 hours
  default_status=SEC_INCREMENTAL_SCHEDULE_STATUS,
  execution_timezone="UTC",
)
def sec_incremental_download_schedule(context):
  """Incremental SEC download every 3 hours.

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
            form_types=["10-K", "10-Q"],
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
  default_status=SEC_INCREMENTAL_PIPELINE_STATUS,
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

  # Determine partition mode (daily for current quarter, quarterly for historical)
  now = datetime.now(UTC)
  current_quarter = f"{now.year}-Q{(now.month - 1) // 3 + 1}"
  use_daily = partition_key == current_quarter

  yield RunRequest(
    run_key=f"sec-process-chain-{partition_key}-{dagster_run.run_id[:8]}",
    partition_key=partition_key,
    run_config={
      "ops": {
        "sec_processed_filings": {
          "config": {
            "use_filing_date_partition": use_daily,
          }
        }
      }
    },
    tags={
      "pipeline": "sec",
      "phase": "process",
      "mode": "incremental",
      "quarter": partition_key,
      "batch_id": batch_id,  # Propagate for batch tracking
    },
  )


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_process_job],
  request_job=sec_staged_materialize_job,
  default_status=SEC_INCREMENTAL_PIPELINE_STATUS,
  minimum_interval_seconds=60,
  description="Chain: process success → trigger stage + materialize (waits for batch)",
)
def sec_process_to_stage_materialize_sensor(context: RunStatusSensorContext):
  """Trigger full staging and materialization after ALL processing in batch completes.

  Part of the automated incremental pipeline. At quarter boundaries, multiple
  quarters may be downloaded and processed. This sensor waits for ALL process
  jobs in the same batch to complete before triggering stage+materialize.

  Batch tracking:
  - Jobs from the same schedule tick share a batch_id tag
  - Sensor queries all process jobs with same batch_id
  - Only triggers when all are complete (SUCCESS or FAILURE)
  - Uses batch_id in run_key for deduplication (Dagster prevents duplicate runs)

  Note: Uses sec_staged_materialize_job which does both staging and
  materialization in sequence, ensuring atomicity.

  Enable via: SEC_INCREMENTAL_PIPELINE_ENABLED=true
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping chain sensor in dev environment")
    return

  dagster_run = context.dagster_run

  # Only chain incremental pipeline runs
  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Skipping - not an incremental pipeline run")
    return

  # Get batch_id for batch tracking
  batch_id = run_tags.get("batch_id")
  if not batch_id:
    context.log.warning("No batch_id found on process run, skipping batch tracking")
    return

  # Query all process jobs in this batch
  batch_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name="sec_process",
      tags={"batch_id": batch_id, "mode": "incremental"},
    ),
  )

  if not batch_runs:
    context.log.warning(f"No process jobs found for batch {batch_id}")
    return

  # Check if all jobs in batch are complete (SUCCESS or FAILURE)
  terminal_statuses = {DagsterRunStatus.SUCCESS, DagsterRunStatus.FAILURE}
  incomplete = [r for r in batch_runs if r.status not in terminal_statuses]

  if incomplete:
    incomplete_quarters = [r.tags.get("quarter", "?") for r in incomplete]
    context.log.info(
      f"Waiting for {len(incomplete)} more process job(s) in batch {batch_id}: "
      f"{incomplete_quarters}"
    )
    return

  # All jobs complete - check if any succeeded
  succeeded = [r for r in batch_runs if r.status == DagsterRunStatus.SUCCESS]
  if not succeeded:
    context.log.warning(
      f"All {len(batch_runs)} process jobs in batch {batch_id} failed, "
      "skipping stage+materialize"
    )
    return

  succeeded_quarters = [r.tags.get("quarter", "?") for r in succeeded]
  context.log.info(
    f"All process jobs in batch {batch_id} complete. "
    f"Succeeded: {succeeded_quarters}. Triggering stage+materialize."
  )

  # Check for already running stage/materialize job
  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name="sec_staged_materialize",
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(
      f"Stage/materialize job already running (run_id={active_runs[0].run_id}), skipping"
    )
    return

  # Also check for separate stage or materialize jobs
  for job_name in ["sec_stage", "sec_materialize"]:
    active = context.instance.get_runs(
      filters=RunsFilter(
        job_name=job_name,
        statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
      ),
      limit=1,
    )
    if active:
      context.log.info(f"{job_name} job already running, skipping")
      return

  # Use batch_id in run_key for deduplication
  # If sensor fires multiple times (once per completed job in batch),
  # Dagster's run_key deduplication ensures only one stage+materialize runs
  yield RunRequest(
    run_key=f"sec-stage-materialize-batch-{batch_id}",
    run_config={
      "ops": {
        "sec_duckdb_staged": {
          "config": {
            "graph_id": "sec",
            "reset_staging": True,  # Fresh DuckDB file each run
          }
        },
        "sec_graph_materialized": {
          "config": {
            "graph_id": "sec",
            "rebuild_graph": True,  # Delete and rebuild LadybugDB from fresh DuckDB staging
          }
        },
      }
    },
    tags={
      "pipeline": "sec",
      "phase": "stage_materialize",
      "mode": "incremental",
      "batch_id": batch_id,
    },
  )
