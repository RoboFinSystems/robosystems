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

import boto3
from botocore.exceptions import ClientError
from dagster import (
  DagsterRunStatus,
  DefaultScheduleStatus,
  DefaultSensorStatus,
  RunConfig,
  RunRequest,
  RunsFilter,
  RunStatusSensorContext,
  ScheduleEvaluationContext,
  SensorEvaluationContext,
  SkipReason,
  run_status_sensor,
  schedule,
  sensor,
)

from robosystems.config import env
from robosystems.dagster.assets import SECProcessConfig
from robosystems.dagster.jobs.sec import (
  sec_materialize_job,
  sec_process_job,
  sec_stage_job,
)
from robosystems.dagster.jobs.shared_repository import shared_repository_snapshot_job


def _get_s3_client():
  """Create S3 client with LocalStack support for dev."""
  kwargs = {
    "region_name": env.AWS_REGION or "us-east-1",
  }
  if env.AWS_ENDPOINT_URL:
    kwargs["endpoint_url"] = env.AWS_ENDPOINT_URL
  return boto3.client("s3", **kwargs)


# Sensor status controlled by environment variable
SEC_PROCESSING_SENSOR_STATUS = (
  DefaultSensorStatus.RUNNING
  if env.SEC_PARALLEL_SENSOR_ENABLED
  else DefaultSensorStatus.STOPPED
)

# Max files to queue per sensor tick (prevents slow ticks and run queue explosion)
MAX_FILES_PER_TICK = 100


@sensor(
  job=sec_process_job,
  minimum_interval_seconds=30,
  default_status=SEC_PROCESSING_SENSOR_STATUS,
  description="Discover pending SourceFiles and trigger per-filing processing runs",
)
def sec_processing_sensor(context: SensorEvaluationContext):
  """Discover pending SEC filings and trigger processing runs.

  Each pending SourceFile becomes a separate Dagster run, enabling parallel
  processing across multiple ECS containers. Dagster's run coordinator
  controls actual concurrency via DAGSTER_MAX_CONCURRENT_RUNS.

  Flow:
  1. Query SourceFile for pending files (limit per tick to avoid slow ticks)
  2. For each file, yield RunRequest with source_file_id in config
  3. run_key includes attempts count for deduplication + retry support
  4. Dagster queues runs, run coordinator executes up to max_concurrent

  Deduplication:
  - run_key = "sec-{source_file_id}-{attempts}" ensures:
    - Same file won't be queued twice (Dagster skips duplicate run_keys)
    - Retries work because attempts increments during processing
  """
  from robosystems.database import session as SessionLocal
  from robosystems.models.iam import SourceFile

  # Skip in dev - use manual job triggers for testing
  if env.ENVIRONMENT == "dev":
    yield SkipReason("Skipped in dev - use Dagster UI to trigger sec_process manually")
    return

  # Query for pending files
  session = None
  try:
    session = SessionLocal()

    # Get pending files (limited per tick)
    pending_files = (
      session.query(SourceFile)
      .filter(
        SourceFile.graph_id == "sec",
        SourceFile.status == "pending",
      )
      .order_by(SourceFile.discovered_at.asc())
      .limit(MAX_FILES_PER_TICK)
      .all()
    )

    # Extract data while session is open (avoid DetachedInstanceError)
    files_to_process = [
      (sf.id, sf.attempts, sf.partition_key or sf.storage_key) for sf in pending_files
    ]

    # Get counts for logging
    total_pending = (
      session.query(SourceFile)
      .filter(SourceFile.graph_id == "sec", SourceFile.status == "pending")
      .count()
    )
    error_count = (
      session.query(SourceFile)
      .filter(SourceFile.graph_id == "sec", SourceFile.status == "error")
      .count()
    )

  except Exception as e:
    context.log.error(f"Database query failed: {e}")
    yield SkipReason(f"Database error: {e}")
    return
  finally:
    if session:
      session.close()

  if not files_to_process:
    if error_count > 0:
      yield SkipReason(f"No pending files ({error_count} in error state)")
    else:
      yield SkipReason("No pending files to process")
    return

  context.log.info(
    f"Found {total_pending} pending files, {error_count} errors. "
    f"Queuing {len(files_to_process)} runs this tick."
  )

  # Yield RunRequest for each pending file
  for source_file_id, attempts, partition_key in files_to_process:
    # run_key includes attempts for deduplication + retry support
    run_key = f"sec-{source_file_id}-{attempts}"

    yield RunRequest(
      run_key=run_key,
      run_config=RunConfig(
        ops={
          "sec_process_filing": SECProcessConfig(
            source_file_id=source_file_id,
          )
        }
      ),
      tags={
        "source_file_id": source_file_id,
        "partition_key": partition_key,
      },
    )


# ============================================================================
# SEC Incremental Staging Schedule
# ============================================================================


def _list_s3_filed_dates(s3_client, bucket: str, prefix: str) -> list[str]:
  """List all filed= partition dates from S3 processed bucket.

  Args:
      s3_client: boto3 S3 client
      bucket: S3 bucket name
      prefix: Prefix to search (e.g., "sec/processed")

  Returns:
      Sorted list of filing dates (YYYY-MM-DD strings)
  """
  filed_dates: list[str] = []

  paginator = s3_client.get_paginator("list_objects_v2")
  pages = paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/", Delimiter="/")

  for page in pages:
    if "CommonPrefixes" in page:
      for prefix_info in page["CommonPrefixes"]:
        prefix_path = prefix_info["Prefix"]
        if "filed=" in prefix_path:
          try:
            # Extract date from "sec/processed/filed=2026-01-15/"
            filed_part = prefix_path.split("filed=")[1].rstrip("/")
            # Validate date format (YYYY-MM-DD)
            datetime.strptime(filed_part, "%Y-%m-%d")
            filed_dates.append(filed_part)
          except (IndexError, ValueError):
            # Skip malformed partitions
            continue

  filed_dates.sort()
  return filed_dates


async def _get_staged_dates_from_graph_api(graph_id: str) -> list[str]:
  """Query Graph API for already-staged filing dates.

  Args:
      graph_id: Graph database identifier

  Returns:
      List of filing dates that have been staged
  """
  from robosystems.graph_api.client.factory import get_graph_client

  try:
    client = await get_graph_client(graph_id=graph_id, operation_type="read")
    return await client.get_staged_dates(graph_id)
  except Exception as e:
    # If no progress table exists, return empty list
    if "no_progress_table" in str(e).lower() or "does not exist" in str(e).lower():
      return []
    raise


SEC_INCREMENTAL_STAGING_SCHEDULE_STATUS = (
  DefaultScheduleStatus.RUNNING
  if env.SEC_INCREMENTAL_STAGING_SCHEDULE_ENABLED
  else DefaultScheduleStatus.STOPPED
)


@schedule(
  job=sec_stage_job,
  cron_schedule="0 5 * * *",  # 5am UTC = 12am EST (after processing, before materialize)
  default_status=SEC_INCREMENTAL_STAGING_SCHEDULE_STATUS,
  execution_timezone="UTC",
)
def sec_incremental_staging_schedule(context: ScheduleEvaluationContext):
  """Run SEC incremental staging at 5am UTC daily.

  Pipeline timing (all UTC):
  - 3am: Download new filings (sec_daily_download_schedule)
  - 3am-5am: Processing sensor processes downloaded filings
  - 5am: This schedule stages processed parquet to DuckDB (incremental mode)
  - 6am: Materialization (sec_nightly_materialize_schedule)

  Logic:
  1. Lists all filed=YYYY-MM-DD partitions in S3 processed bucket
  2. Queries Graph API for dates already staged (from _sec_staging_progress table)
  3. Triggers sec_stage job with mode="incremental" for each unstaged date

  Enable via: SEC_INCREMENTAL_STAGING_SCHEDULE_ENABLED=true
  Requires initial full staging to create _sec_staging_progress table.
  """
  import asyncio

  processed_bucket = env.SHARED_PROCESSED_BUCKET
  if not processed_bucket:
    context.log.warning("Missing SHARED_PROCESSED_BUCKET configuration")
    return []

  graph_id = "sec"
  s3_prefix = "sec/processed"
  run_requests = []

  try:
    s3_client = _get_s3_client()

    # Step 1: List all filed= dates in S3
    s3_dates = _list_s3_filed_dates(s3_client, processed_bucket, s3_prefix)
    context.log.info(f"Found {len(s3_dates)} filing dates in S3")

    if not s3_dates:
      context.log.info("No filed= partitions found in S3")
      return []

    # Step 2: Get already-staged dates from Graph API
    try:
      staged_dates = asyncio.get_event_loop().run_until_complete(
        _get_staged_dates_from_graph_api(graph_id)
      )
    except RuntimeError:
      staged_dates = asyncio.run(_get_staged_dates_from_graph_api(graph_id))

    staged_dates_set = set(staged_dates)
    context.log.info(f"Found {len(staged_dates_set)} dates already staged")

    if not staged_dates_set:
      context.log.warning(
        "No staging progress found. Run full sec_duckdb_staged job first."
      )
      return []

    # Step 3: Find unstaged dates
    unstaged_dates = sorted([d for d in s3_dates if d not in staged_dates_set])

    if not unstaged_dates:
      context.log.info(
        f"All {len(s3_dates)} S3 filing dates already staged (up to {max(s3_dates)})"
      )
      return []

    context.log.info(f"Found {len(unstaged_dates)} unstaged dates to process")

    # Step 4: Create run request for each unstaged date (chronological order)
    for filing_date in unstaged_dates:
      run_requests.append(
        RunRequest(
          run_key=f"sec-incremental-stage-{filing_date}",
          run_config={
            "ops": {
              "sec_duckdb_staged": {
                "config": {
                  "graph_id": graph_id,
                  "mode": "incremental",
                  "filing_date": filing_date,
                }
              }
            }
          },
        )
      )

    context.log.info(f"Yielding {len(run_requests)} incremental staging runs")
    return run_requests

  except ClientError as e:
    error_code = e.response.get("Error", {}).get("Code", "Unknown")
    context.log.error(f"S3 error ({error_code}): {e}")
    raise
  except Exception as e:
    context.log.error(
      f"Error in SEC incremental staging schedule: {type(e).__name__}: {e}"
    )
    raise


# ============================================================================
# SEC Post-Materialization Snapshot Sensor
# ============================================================================

# Sensor status controlled by environment variable
# Enable via SHARED_REPO_SNAPSHOT_SENSOR_ENABLED=true after verifying manual runs work
SEC_SNAPSHOT_SENSOR_STATUS = (
  DefaultSensorStatus.RUNNING
  if env.SHARED_REPO_SNAPSHOT_SENSOR_ENABLED
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

  Enable via: SHARED_REPO_SNAPSHOT_SENSOR_ENABLED=true

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
