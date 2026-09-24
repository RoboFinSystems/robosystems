"""SEC pipeline sensors and the nightly download schedule.

All start STOPPED; enable them in the Dagster UI. Nightly chain (every run
tagged ``mode=incremental``): download → process (batched) → wake master →
incremental DuckDB stage → full LadybugDB rebuild → lbug S3 → duckdb S3 →
replica refresh + master sleep; text indexing and the filer catalog branch off
staging. ``sec_processing_sensor`` is the separate backfill driver.
"""

import re
from datetime import datetime

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
  shared_master_sleep_job,
  shared_master_wake_job,
  shared_replicas_refresh_job,
)

from .configs import SEC_HISTORICAL_FORM_TYPES, SEC_PRIMARY_START_YEAR
from .jobs import (
  sec_download_job,
  sec_duckdb_s3_publish_job,
  sec_filing_catalog_job,
  sec_incremental_stage_job,
  sec_ixbrl_index_job,
  sec_lbug_s3_publish_job,
  sec_materialize_job,
  sec_narratives_index_job,
  sec_process_job,
  sec_stage_job,
)


@sensor(
  job=sec_process_job,
  minimum_interval_seconds=300,
  default_status=DefaultSensorStatus.STOPPED,
  description="Discover quarters with pending SourceFiles and trigger batch processing runs",
)
def sec_processing_sensor(context: SensorEvaluationContext):
  """Trigger one batch run per quarter with pending SourceFiles.

  The single recovery path for sec_process: a quarter is re-triggered every
  tick while it has pending files and no active run. No run_key, so a quarter
  can be re-triggered after a failure; the active-run check is what prevents
  concurrent runs.
  """

  from sqlalchemy import func

  from robosystems.database import session as SessionLocal
  from robosystems.models.core import SourceFile

  # SourceFile.partition_key is "YYYY-QN_cik_accession".
  quarter_pattern = re.compile(r"^(\d{4}-Q[1-4])_")

  if env.ENVIRONMENT == "dev":
    yield SkipReason("Skipped in dev - use Dagster UI to trigger sec_process manually")
    return

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

    quarters_with_pending: set[str] = set()
    for (partition_key,) in pending_files:
      if partition_key:
        match = quarter_pattern.match(partition_key)
        if match:
          quarters_with_pending.add(match.group(1))

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

  for quarter in sorted(quarters_with_pending):
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

    # Pre-2024 (historical) quarters process annual reports only.
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

    yield RunRequest(
      partition_key=quarter,
      run_config=run_config,
      tags={
        "quarter": quarter,
        "pipeline": "sec",
        "phase": "process",
      },
    )


# The nightly chain. Keep it stopped during backfills.


def _get_quarters_to_scan(now: datetime | None = None) -> list[str]:
  """The one quarter for the nightly download, keyed off Eastern time.

  Pass the schedule's ``scheduled_execution_time``: the last-day-of-quarter
  run (21:00 ET) is already the next day in UTC.
  """
  from robosystems.adapters.sec import get_quarters_to_scan

  return get_quarters_to_scan(now)


@schedule(
  job=sec_download_job,
  cron_schedule="0 21 * * 1-5",
  default_status=DefaultScheduleStatus.STOPPED,
  execution_timezone="America/New_York",
)
def sec_incremental_download_schedule(context):
  """Nightly download of the current quarter, weekdays at 9pm ET."""
  from robosystems.adapters.sec.pipeline.configs import SECDownloadConfig

  quarters = _get_quarters_to_scan(context.scheduled_execution_time)
  context.log.info(f"Incremental download for quarter: {quarters[0]}")

  # Ties together every run descended from this tick.
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
        "batch_id": batch_id or "",
      },
    )


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_download_job, sec_process_job],
  request_jobs=[sec_process_job, shared_master_wake_job],
  default_status=DefaultSensorStatus.STOPPED,
  minimum_interval_seconds=60,
  description="Chain: download → process (batched) → stage. Self-contained incremental pipeline.",
)
def sec_incremental_pipeline_sensor(context: RunStatusSensorContext):
  """Chain download → process batches → master wake.

  After a download, or a process batch that leaves files pending in its
  partition, trigger the next batch. Once every partition is drained, wake the
  shared master; sec_wake_to_stage_sensor takes it from there.
  """

  from robosystems.database import session as SessionLocal
  from robosystems.models.core import SourceFile

  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping chain sensor in dev environment")
    return

  dagster_run = context.dagster_run

  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Skipping - not an incremental pipeline run")
    return

  partition_key = dagster_run.tags.get("dagster/partition")
  if not partition_key:
    context.log.warning("No partition key found on completed run")
    return

  batch_id = run_tags.get("batch_id")

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
      context.log.info(
        f"Process batch completed for {partition_key}, "
        f"{pending_in_partition} files still pending, triggering next batch"
      )
    elif total_pending > 0:
      context.log.info(
        f"Partition {partition_key} fully processed, but {total_pending} files "
        f"pending in other partitions — waiting for batch to complete"
      )
      return
    else:
      context.log.info(
        "All pending files processed across all partitions, waking shared master"
      )

      active_wake_runs = context.instance.get_runs(
        filters=RunsFilter(
          job_name="shared_master_wake",
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
        job_name="shared_master_wake",
        tags={
          "pipeline": "sec",
          "phase": "master_wake",
          "mode": "incremental",
          "batch_id": batch_id or "",
          "quarter": partition_key,
        },
      )
      return
  else:
    context.log.info(f"Download completed for {partition_key}, triggering processing")

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


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[shared_master_wake_job],
  request_job=sec_incremental_stage_job,
  default_status=DefaultSensorStatus.STOPPED,
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
  stage_config: dict[str, object] = {"graph_id": "sec"}
  quarter_tag = run_tags.get("quarter", "")
  if match := re.fullmatch(r"(\d{4})-Q([1-4])", quarter_tag):
    stage_config["year"] = int(match.group(1))
    stage_config["quarter"] = int(match.group(2))

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
        "sec_duckdb_incremental_staged": {"config": stage_config},
      }
    },
    tags={
      "pipeline": "sec",
      "phase": "incremental_stage",
      "mode": "incremental",
      "batch_id": batch_id or "",
      "quarter": quarter_tag,
    },
  )


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[sec_incremental_stage_job],
  request_job=sec_materialize_job,
  default_status=DefaultSensorStatus.STOPPED,
  minimum_interval_seconds=60,
  description=(
    "Trigger a full LadybugDB rebuild from DuckDB after incremental staging completes"
  ),
)
def sec_stage_to_materialize_sensor(context: RunStatusSensorContext):
  """Trigger a full LadybugDB rebuild from DuckDB after incremental staging.

  A nightly rebuild erases drift (partial batch failures, stale mutable Entity
  attributes), and the sec graph (2024+) is small enough for it.
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping chain sensor in dev environment")
    return

  dagster_run = context.dagster_run

  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Run is not incremental mode, skipping chain")
    return

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

  # Incremental mode is manual-only: its keyset export + anti-join runs DuckDB
  # and LadybugDB hot at once and OOMs the shared master at current corpus
  # size. Revisit if the full rebuild outgrows the wake window.
  materialize_mode = "full"

  context.log.info(
    f"DuckDB staging completed (run_id={dagster_run.run_id}), "
    f"triggering {materialize_mode} LadybugDB materialization from DuckDB"
  )

  yield RunRequest(
    run_key=f"sec-materialize-chain-{dagster_run.run_id[:8]}",
    run_config={
      "ops": {
        "sec_graph_materialized": {
          "config": {
            "graph_id": "sec",
            "materialize_mode": materialize_mode,
          }
        },
      }
    },
    tags={
      "pipeline": "sec",
      "phase": "materialize",
      # Chain lineage marker, independent of materialize_mode.
      "mode": "incremental",
      "materialize_mode": materialize_mode,
    },
  )


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
    shared_master_sleep_job,
  ],
  default_status=DefaultSensorStatus.STOPPED,
  minimum_interval_seconds=60,
  description=("Chain: materialize → lbug S3 → duckdb S3 → replica refresh"),
)
def sec_post_materialize_publish_sensor(context: RunStatusSensorContext):
  """Chain materialize → lbug S3 → duckdb S3 → replica refresh + master sleep.

  Sequential so concurrent uploads don't overload the instance. The lbug copy
  feeds the replica fleet; the duckdb copy feeds the knowledge-artifacts build.
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping publish sensor in dev environment")
    return

  dagster_run = context.dagster_run

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
    # Replicas refresh from S3, never the master, so the master can sleep in
    # parallel with the refresh.
    context.log.info(
      "DuckDB S3 publish complete, triggering replica refresh and master sleep"
    )
    for job_name, phase in (
      ("shared_replicas_refresh", "replica_refresh"),
      ("shared_master_sleep", "master_sleep"),
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


@run_status_sensor(
  run_status=DagsterRunStatus.FAILURE,
  monitored_jobs=[
    shared_master_wake_job,
    sec_incremental_stage_job,
    sec_materialize_job,
    sec_lbug_s3_publish_job,
    sec_duckdb_s3_publish_job,
  ],
  request_job=shared_master_sleep_job,
  default_status=DefaultSensorStatus.STOPPED,
  minimum_interval_seconds=60,
  description="Sleep the shared master if any incremental master-dependent job fails",
)
def sec_master_sleep_on_failure_sensor(context: RunStatusSensorContext):
  """A failed chain step never reaches the terminal sleep, so sleep here."""
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
      job_name="shared_master_sleep",
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


@run_status_sensor(
  run_status=DagsterRunStatus.SUCCESS,
  monitored_jobs=[
    sec_incremental_stage_job,
    sec_stage_job,
  ],
  request_jobs=[
    sec_narratives_index_job,
    sec_ixbrl_index_job,
    sec_filing_catalog_job,
  ],
  default_status=DefaultSensorStatus.STOPPED,
  minimum_interval_seconds=60,
  description="Chain: stage → text search indexing (narratives + iXBRL disclosures) + filer catalog",
)
def sec_post_stage_index_sensor(context: RunStatusSensorContext):
  """After staging, run the two text indexes and the filer catalog in parallel.

  Runs alongside the materialize branch; the index jobs are idempotent
  (OpenSearch upserts by document_id).
  """
  if env.ENVIRONMENT == "dev":
    context.log.info("Skipping text index sensor in dev environment")
    return

  dagster_run = context.dagster_run

  run_tags = dagster_run.tags or {}
  if run_tags.get("mode") != "incremental":
    context.log.info("Skipping - not an incremental pipeline run")
    return

  graph_id = run_tags.get("graph_id", "sec")

  # The quarter the download ran for, carried down the chain; the Eastern
  # filing calendar only when a run was started by hand without one.
  partition_key = run_tags.get("dagster/partition") or run_tags.get("quarter")
  if not partition_key:
    from robosystems.adapters.sec import get_current_quarter

    year, quarter = get_current_quarter()
    partition_key = f"{year}-Q{quarter}"

  context.log.info(f"Will index partition {partition_key}")

  index_jobs = {
    "sec_narratives_index": "sec_narratives_indexed",
    "sec_ixbrl_index": "sec_ixbrl_disclosures_indexed",
    # Not an index, but the same shape: post-stage, per partition, one op config.
    "sec_catalog": "sec_filing_catalog",
  }

  for job_name, asset_name in index_jobs.items():
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


_INDEX_RETRY_MAX = 3


@run_status_sensor(
  run_status=DagsterRunStatus.FAILURE,
  monitored_jobs=[
    sec_narratives_index_job,
    sec_ixbrl_index_job,
    sec_filing_catalog_job,
  ],
  request_jobs=[
    sec_narratives_index_job,
    sec_ixbrl_index_job,
    sec_filing_catalog_job,
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
  """Relaunch a failed index run (typically a Spot reclaim) with its config.

  The OpenSearch incremental skip (_get_indexed_accessions) means a retry
  only does the remaining work.
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

  retry_count = int(run_tags.get("retry_count", "0"))
  if retry_count >= _INDEX_RETRY_MAX:
    context.log.warning(
      f"{job_name} for {partition_key} has failed {retry_count} times, "
      f"not retrying (max {_INDEX_RETRY_MAX})"
    )
    return

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
