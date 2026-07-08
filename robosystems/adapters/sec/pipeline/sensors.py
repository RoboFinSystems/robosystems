"""SEC pipeline sensors for automated incremental updates.

Sensors trigger jobs based on SourceFile queue state and job completion.
All sensors start STOPPED by default - enable in Dagster UI when ready.

Nightly Pipeline (enable all for automated daily updates):
- Phase 1 (Download): sec_incremental_download_schedule triggers at 9pm EST weekdays
- Phase 2+3 (Process+Stage): sec_incremental_pipeline_sensor chains
  download → process (batched loop) → stage (DuckDB INSERT)
- Phase 4 (Materialize): sec_stage_to_materialize_sensor chains stage → full graph rebuild
- Phase 5 (Publish+Refresh): sec_post_materialize_publish_sensor chains
  materialize → lbug S3 publish → duckdb S3 publish → replica refresh
- Phase 5c (Text Index): sec_post_stage_index_sensor chains
  stage → textblocks index + narratives index (parallel, incremental)

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
from robosystems.dagster.jobs.shared_repository import (
  shared_replicas_refresh_job,
)

from .configs import SEC_HISTORICAL_FORM_TYPES, SEC_PRIMARY_START_YEAR
from .jobs import (
  sec_download_job,
  sec_duckdb_s3_publish_job,
  sec_incremental_stage_job,
  sec_ixbrl_index_job,
  sec_lbug_s3_publish_job,
  sec_master_sleep_job,
  sec_master_wake_job,
  sec_materialize_job,
  sec_narratives_index_job,
  sec_process_job,
  sec_stage_job,
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
  - Part files per table per quarter (part_{uuid}.parquet, additive)
  - Shared tables deduped within batch; DuckDB handles cross-batch dedup

  Deduplication:
  - No run_key used - allows retries after failures
  - Active run check prevents concurrent runs for same quarter
  - After batch completes, sensor re-triggers if pending files remain
  """
  import re

  from sqlalchemy import func

  from robosystems.database import session as SessionLocal
  from robosystems.models.core import SourceFile

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


def _get_quarters_to_scan(now: datetime | None = None) -> list[str]:
  """Get the single quarter to scan for the incremental nightly download.

  Hard cut-over: one quarter per run, keyed off Eastern time. Pass the
  schedule's ``scheduled_execution_time`` so the last-day-of-quarter run
  (21:00 ET, already next-day in UTC) stays on the correct quarter instead of
  rolling to the next quarter by the container's UTC clock.

  Returns:
      Single-element list of partition keys like ["2026-Q2"]
  """
  from robosystems.adapters.sec import get_quarters_to_scan

  return get_quarters_to_scan(now)


@schedule(
  job=sec_download_job,
  cron_schedule="0 21 * * 1-5",  # 9pm EST, Monday-Friday
  default_status=DefaultScheduleStatus.STOPPED,  # Enable in Dagster UI when ready
  execution_timezone="America/New_York",
)
def sec_incremental_download_schedule(context):
  """Incremental SEC download at 9pm ET on weekdays.

  Part of the automated incremental pipeline. Downloads new filings for the
  current quarter only (hard cut-over). The quarter is keyed off the schedule's
  Eastern execution time, so the last-day-of-quarter run stays on the correct
  quarter instead of rolling forward by the container's UTC clock.

  Chain: download → process → stage → materialize → S3 sync

  Enable via: SEC_INCREMENTAL_PIPELINE_ENABLED=true
  """
  from robosystems.adapters.sec.pipeline.configs import SECDownloadConfig

  quarters = _get_quarters_to_scan(context.scheduled_execution_time)
  context.log.info(f"Incremental download for quarter: {quarters[0]}")

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
        "batch_id": batch_id or "",  # Track jobs from same schedule tick
      },
    )


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_download_job, sec_process_job],
  request_jobs=[sec_process_job, sec_master_wake_job],
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  minimum_interval_seconds=60,
  description="Chain: download → process (batched) → stage. Self-contained incremental pipeline.",
)
def sec_incremental_pipeline_sensor(context: RunStatusSensorContext):
  """Orchestrate the full incremental SEC pipeline: download → process → stage.

  Monitors both download and process job completions:
  - On download success: triggers first process batch for the partition
  - On process success: checks for remaining pending files
    - If pending remain in this partition: triggers next process batch
    - If this partition drained but others still pending: waits
    - If all partitions drained: triggers incremental DuckDB staging

  Each process run handles SEC_PROCESS_BATCH_SIZE filings (default 250) then
  exits, enabling natural memory release between batches and spot resilience
  via the S3 filing cache. This sensor re-triggers process runs until the
  queue is drained across all partitions, then hands off to staging.

  The download schedule scans exactly one quarter per run (hard cut-over, keyed
  off Eastern time), so there is normally a single partition to drain before
  staging.

  The stage_to_materialize_sensor handles the next step (stage → materialize).
  """
  import re

  from robosystems.database import session as SessionLocal
  from robosystems.models.core import SourceFile

  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping chain sensor in dev environment")
    return

  dagster_run = context.dagster_run

  # Only chain incremental pipeline runs (tagged with mode=incremental)
  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Skipping - not an incremental pipeline run")
    return

  # Get partition from the completed run
  partition_key = dagster_run.tags.get("dagster/partition")
  if not partition_key:
    context.log.warning("No partition key found on completed run")
    return

  batch_id = run_tags.get("batch_id")

  # When triggered by a process job completion, check if pending files remain
  if dagster_run.job_name == "sec_process":
    quarter_pattern = re.compile(r"^(\d{4}-Q[1-4])_")
    session = None
    try:
      session = SessionLocal()
      pending_files = (
        session.query(SourceFile.partition_key)
        .filter(
          SourceFile.graph_id == "sec",
          SourceFile.status == "pending",
          SourceFile.partition_key.isnot(None),
        )
        .all()
      )

      # Count pending in this partition and total across all partitions
      pending_in_partition = 0
      total_pending = len(pending_files)
      for (pk,) in pending_files:
        if pk and (m := quarter_pattern.match(pk)) and m.group(1) == partition_key:
          pending_in_partition += 1

    except Exception as e:
      context.log.error(f"Database query failed: {e}")
      return
    finally:
      if session:
        session.close()

    if pending_in_partition > 0:
      # Fall through to yield another process RunRequest below
      context.log.info(
        f"Process batch completed for {partition_key}, "
        f"{pending_in_partition} files still pending, triggering next batch"
      )
    elif total_pending > 0:
      # This partition is done but other partitions from the same batch
      # (e.g., quarter boundary scan) still have pending files.
      # Wait for all partitions to drain before triggering staging.
      context.log.info(
        f"Partition {partition_key} fully processed, but {total_pending} files "
        f"pending in other partitions — waiting for batch to complete"
      )
      return
    else:
      # All partitions drained — wake the shared master; staging is triggered
      # once it is healthy (sec_wake_to_stage_sensor).
      context.log.info(
        "All pending files processed across all partitions, waking shared master"
      )

      # Check if a wake is already running (avoids double-wake on races)
      active_wake_runs = context.instance.get_runs(
        filters=RunsFilter(
          job_name="sec_master_wake",
          statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
        ),
        limit=1,
      )
      if active_wake_runs:
        context.log.info(
          f"Master wake already running (run_id={active_wake_runs[0].run_id}), skipping"
        )
        return

      yield RunRequest(
        run_key=f"sec-wake-chain-{batch_id or partition_key}-{dagster_run.run_id[:8]}",
        job_name="sec_master_wake",
        tags={
          "pipeline": "sec",
          "phase": "master_wake",
          "mode": "incremental",
          "batch_id": batch_id or "",
        },
      )
      return
  else:
    context.log.info(f"Download completed for {partition_key}, triggering processing")

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

  yield RunRequest(
    run_key=f"sec-process-chain-{partition_key}-{dagster_run.run_id[:8]}",
    job_name="sec_process",
    partition_key=partition_key,
    tags={
      "pipeline": "sec",
      "phase": "process",
      "mode": "incremental",
      "quarter": partition_key,
      "batch_id": batch_id or "",
    },
  )


# ============================================================================
# SEC Wake to Stage Sensor (Chains Master Wake → DuckDB Staging)
# ============================================================================
# The master is awake and healthy; trigger the first master-dependent step.


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_master_wake_job],
  request_job=sec_incremental_stage_job,
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  minimum_interval_seconds=60,
  description="Trigger incremental DuckDB staging after the shared master is awake",
)
def sec_wake_to_stage_sensor(context: RunStatusSensorContext):
  """Trigger incremental DuckDB staging once the shared master is awake+healthy."""
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping chain sensor in dev environment")
    return

  dagster_run = context.dagster_run
  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Run is not incremental mode, skipping chain")
    return

  batch_id = run_tags.get("batch_id")

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

  context.log.info(
    f"Shared master awake (run_id={dagster_run.run_id}), triggering DuckDB staging"
  )

  yield RunRequest(
    run_key=f"sec-stage-chain-{batch_id or dagster_run.run_id[:8]}",
    run_config={
      "ops": {
        "sec_duckdb_incremental_staged": {
          "config": {
            "graph_id": "sec",
          }
        },
      }
    },
    tags={
      "pipeline": "sec",
      "phase": "incremental_stage",
      "mode": "incremental",
      "batch_id": batch_id or "",
    },
  )


# ============================================================================
# SEC Stage to Materialize Sensor (Chains Stage → Full Graph Rebuild)
# ============================================================================
# After DuckDB incremental staging adds new rows, triggers a full LadybugDB
# rebuild from DuckDB. The sec graph (2024+ only) is small enough for nightly
# rebuilds, and this ensures the graph is always consistent with DuckDB.
#
# Chain: stage (DuckDB INSERT) → materialize (full rebuild) → S3 publish
# S3 publishes are handled by sec_post_materialize_publish_sensor.


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
    process → stage (DuckDB INSERT) → materialize (full rebuild) → S3 publish

  After new data is added to DuckDB, the LadybugDB graph is fully rebuilt.
  The sec graph (2024+ only, ~75GB) is small enough for nightly rebuilds.
  S3 publishes are handled by sec_post_materialize_publish_sensor.
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


# ============================================================================
# SEC Post-Materialize Publish Sensor (Sequential S3 Uploads)
# ============================================================================
# After materialization, publish both databases to S3 sequentially:
#   materialize → lbug S3 publish → duckdb S3 publish
#
# Sequential to avoid overloading the instance with concurrent uploads.
# LadybugDB publish serves the replica fleet (downloaded to local disk on boot).
# DuckDB publish serves the offline knowledge-artifacts build.
# Replica refresh cycles instances to pick up new S3 databases.


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[
    sec_materialize_job,
    sec_lbug_s3_publish_job,
    sec_duckdb_s3_publish_job,
  ],
  request_jobs=[
    sec_lbug_s3_publish_job,
    sec_duckdb_s3_publish_job,
    shared_replicas_refresh_job,
    sec_master_sleep_job,
  ],
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  minimum_interval_seconds=60,
  description=("Chain: materialize → lbug S3 → duckdb S3 → replica refresh"),
)
def sec_post_materialize_publish_sensor(context: RunStatusSensorContext):
  """Publish databases to S3 and refresh replicas after materialization.

  Sequential chain to avoid overloading the instance:
  - On materialize success: triggers lbug S3 publish
  - On lbug publish success: triggers duckdb S3 publish
  - On duckdb publish success: triggers rolling replica refresh

  The replica refresh uses conservative policies (min_healthy=100%,
  max_healthy=200%) so old instances stay alive serving traffic until
  new instances finish downloading (~15 min) and pass health checks.
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping publish sensor in dev environment")
    return

  dagster_run = context.dagster_run

  # Only chain incremental pipeline runs
  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Skipping - not an incremental pipeline run")
    return

  if dagster_run.job_name == "sec_materialize":
    next_job_name = "sec_lbug_s3_publish"
    next_phase = "lbug_s3_publish"
    context.log.info("Materialization complete, triggering LadybugDB S3 publish")

  elif dagster_run.job_name == "sec_lbug_s3_publish":
    next_job_name = "sec_duckdb_s3_publish"
    next_phase = "duckdb_s3_publish"
    context.log.info("LadybugDB S3 publish complete, triggering DuckDB S3 publish")

  elif dagster_run.job_name == "sec_duckdb_s3_publish":
    # Final publish done — the master is now dead weight (replicas refresh from
    # S3, never from the master). Trigger the replica refresh AND sleep the
    # master, in parallel; sleep does not depend on the refresh outcome.
    context.log.info(
      "DuckDB S3 publish complete, triggering replica refresh and master sleep"
    )
    for job_name, phase in (
      ("shared_replicas_refresh", "replica_refresh"),
      ("sec_master_sleep", "master_sleep"),
    ):
      active = context.instance.get_runs(
        filters=RunsFilter(
          job_name=job_name,
          statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
        ),
        limit=1,
      )
      if active:
        context.log.info(
          f"{job_name} already running (run_id={active[0].run_id}), skipping"
        )
        continue
      yield RunRequest(
        run_key=f"sec-{phase}-chain-{dagster_run.run_id[:8]}",
        job_name=job_name,
        tags={"pipeline": "sec", "phase": phase, "mode": "incremental"},
      )
    return

  else:
    return

  # Check if next job is already running
  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name=next_job_name,
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(
      f"{next_job_name} already running (run_id={active_runs[0].run_id}), skipping"
    )
    return

  yield RunRequest(
    run_key=f"sec-{next_phase}-chain-{dagster_run.run_id[:8]}",
    job_name=next_job_name,
    tags={
      "pipeline": "sec",
      "phase": next_phase,
      "mode": "incremental",
    },
  )


# ============================================================================
# SEC Master Sleep-on-Failure Sensor (never strand the master awake)
# ============================================================================
# If any incremental master-dependent job fails, the terminal sleep never runs,
# so scale the master back to 0 here. Sleep is idempotent.


@run_status_sensor(
  run_status=DagsterRunStatus.FAILURE,
  monitored_jobs=[
    sec_master_wake_job,
    sec_incremental_stage_job,
    sec_materialize_job,
    sec_lbug_s3_publish_job,
    sec_duckdb_s3_publish_job,
  ],
  request_job=sec_master_sleep_job,
  default_status=DefaultSensorStatus.STOPPED,  # Enable in Dagster UI when ready
  minimum_interval_seconds=60,
  description="Sleep the shared master if any incremental master-dependent job fails",
)
def sec_master_sleep_on_failure_sensor(context: RunStatusSensorContext):
  """Never strand the master awake: on any incremental master-dependent job
  failure, scale it back to 0."""
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping failure sleep sensor in dev environment")
    return

  dagster_run = context.dagster_run
  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Run is not incremental mode, skipping sleep")
    return

  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name="sec_master_sleep",
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(
      f"Master sleep already running (run_id={active_runs[0].run_id}), skipping"
    )
    return

  context.log.warning(
    f"Incremental job {dagster_run.job_name} failed (run_id={dagster_run.run_id}), "
    "sleeping shared master to avoid stranding it awake"
  )

  yield RunRequest(
    run_key=f"sec-master-sleep-failure-{dagster_run.run_id[:8]}",
    tags={
      "pipeline": "sec",
      "phase": "master_sleep",
      "mode": "incremental",
    },
  )


# ============================================================================
# SEC Post-Stage Text Index Sensor (OpenSearch Indexing)
# ============================================================================
# After staging completes, index text content into OpenSearch in parallel
# with the materialization branch. Both index jobs are independent and
# idempotent (OpenSearch upserts by document_id).
#
# Chain: stage → text index (parallel with materialize branch)


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[
    sec_incremental_stage_job,
    sec_stage_job,
  ],
  request_jobs=[
    sec_narratives_index_job,
    sec_ixbrl_index_job,
  ],
  default_status=DefaultSensorStatus.STOPPED,
  minimum_interval_seconds=60,
  description="Chain: stage → text search indexing (narratives + iXBRL disclosures)",
)
def sec_post_stage_index_sensor(context: RunStatusSensorContext):
  """Trigger text search indexing after staging completes.

  Runs all three indexing jobs in parallel since they're independent.
  Each job is partitioned by quarter. The sensor derives the current
  quarter from the upstream run tags or the current date.
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping text index sensor in dev environment")
    return

  dagster_run = context.dagster_run

  # Only chain incremental pipeline runs
  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Skipping - not an incremental pipeline run")
    return

  graph_id = run_tags.get("graph_id", "sec")

  # Derive the current quarter partition key
  # Try to get from upstream run tags, otherwise compute from current date
  partition_key = run_tags.get("dagster/partition")
  if not partition_key:
    now = datetime.now(UTC)
    quarter = (now.month - 1) // 3 + 1
    partition_key = f"{now.year}-Q{quarter}"

  context.log.info(f"Will index partition {partition_key}")

  # Job name → asset op name mapping
  index_jobs = {
    "sec_narratives_index": "sec_narratives_indexed",
    "sec_ixbrl_index": "sec_ixbrl_disclosures_indexed",
  }

  for job_name, asset_name in index_jobs.items():
    # Skip if already running
    active_runs = context.instance.get_runs(
      filters=RunsFilter(
        job_name=job_name,
        statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
      ),
      limit=1,
    )
    if active_runs:
      context.log.info(
        f"{job_name} already running (run_id={active_runs[0].run_id}), skipping"
      )
      continue
    context.log.info(
      f"Staging complete (run_id={dagster_run.run_id}), triggering {job_name} "
      f"for {partition_key}"
    )

    yield RunRequest(
      run_key=f"sec-{job_name}-chain-{partition_key}-{dagster_run.run_id[:8]}",
      job_name=job_name,
      run_config={
        "ops": {
          asset_name: {
            "config": {
              "graph_id": graph_id,
            }
          }
        }
      },
      partition_key=partition_key,
      tags={
        "pipeline": "sec",
        "phase": "text_index",
        "mode": "incremental",
      },
    )


# Maximum number of automatic retries before giving up
_INDEX_RETRY_MAX = 3


@run_status_sensor(
  run_status=DagsterRunStatus.FAILURE,
  monitored_jobs=[
    sec_narratives_index_job,
    sec_ixbrl_index_job,
  ],
  request_jobs=[
    sec_narratives_index_job,
    sec_ixbrl_index_job,
  ],
  default_status=DefaultSensorStatus.STOPPED,
  minimum_interval_seconds=60,
  description=(
    "Retry failed text search indexing jobs (e.g. Spot interruptions). "
    "Incremental skip ensures completed batches are not re-indexed. "
    f"Caps at {_INDEX_RETRY_MAX} retries per partition to avoid loops."
  ),
)
def sec_index_retry_sensor(context: RunStatusSensorContext):
  """Retry failed indexing jobs with the same partition and config.

  Designed for Spot resilience: when a Fargate Spot task is reclaimed,
  the Dagster run fails. This sensor detects the failure and re-launches
  with the same partition key and run config. The OpenSearch incremental
  skip (_get_indexed_accessions) ensures already-indexed batches are not
  reprocessed, so retries only do the remaining work.
  """
  dagster_run = context.dagster_run
  job_name = dagster_run.job_name
  run_tags = dagster_run.tags or {}

  partition_key = run_tags.get("dagster/partition")
  if not partition_key:
    context.log.warning(
      f"No partition key on failed run {dagster_run.run_id}, skipping retry"
    )
    return

  # Track retry count to cap retries
  retry_count = int(run_tags.get("retry_count", "0"))
  if retry_count >= _INDEX_RETRY_MAX:
    context.log.warning(
      f"{job_name} for {partition_key} has failed {retry_count} times, "
      f"not retrying (max {_INDEX_RETRY_MAX})"
    )
    return

  # Check if the job is already running for this specific partition
  active_runs = context.instance.get_runs(
    filters=RunsFilter(
      job_name=job_name,
      statuses=[DagsterRunStatus.STARTED, DagsterRunStatus.QUEUED],
      tags={"dagster/partition": partition_key},
    ),
    limit=1,
  )
  if active_runs:
    context.log.info(
      f"{job_name} already running (run_id={active_runs[0].run_id}), skipping retry"
    )
    return

  next_retry = retry_count + 1
  context.log.info(
    f"Retrying {job_name} for {partition_key} "
    f"(attempt {next_retry}/{_INDEX_RETRY_MAX}, failed run: {dagster_run.run_id})"
  )

  yield RunRequest(
    run_key=f"sec-{job_name}-retry-{partition_key}-{next_retry}-{dagster_run.run_id[:8]}",
    job_name=job_name,
    run_config=dagster_run.run_config,
    partition_key=partition_key,
    tags={
      **{k: v for k, v in run_tags.items() if not k.startswith("dagster/")},
      "retry_count": str(next_retry),
      "retry_of": dagster_run.run_id,
    },
  )
