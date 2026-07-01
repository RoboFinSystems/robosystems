"""Tests for SEC pipeline sensors (incremental pipeline chain).

The sec_processing_sensor is covered in test_sec.py.
This file covers the incremental pipeline chain sensors:
- sec_incremental_pipeline_sensor (download → process loop → stage)
- sec_stage_to_materialize_sensor
- sec_incremental_download_schedule
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest
from dagster import (
  DagsterRunStatus,
  RunRequest,
  build_schedule_context,
)

from robosystems.adapters.sec.pipeline.sensors import (
  _get_quarters_to_scan,
  sec_incremental_download_schedule,
  sec_incremental_pipeline_sensor,
  sec_post_materialize_publish_sensor,
  sec_post_stage_index_sensor,
  sec_stage_to_materialize_sensor,
)


def _build_run_status_context(
  sensor_name,
  tags=None,
  run_id="run-123",
  job_name="sec_download",
  instance=None,
  get_runs_return=None,
):
  """Build a RunStatusSensorContext with a DagsterEvent for run_status_sensors."""
  from dagster import DagsterInstance, DagsterRun
  from dagster._core.events import DagsterEvent, DagsterEventType

  dagster_run = DagsterRun(
    job_name=job_name,
    run_id=run_id,
    tags=tags or {},
    status=DagsterRunStatus.SUCCESS,
  )

  dagster_event = DagsterEvent(
    event_type_value=DagsterEventType.RUN_SUCCESS.value,
    job_name=job_name,
  )

  from dagster import build_run_status_sensor_context

  if instance is None:
    instance = DagsterInstance.ephemeral()

  if get_runs_return is not None:
    instance.get_runs = lambda **kwargs: get_runs_return

  context = build_run_status_sensor_context(
    sensor_name=sensor_name,
    dagster_event=dagster_event,
    dagster_run=dagster_run,
    dagster_instance=instance,
  )
  return context


@pytest.mark.unit
class TestSecIncrementalPipelineSensor:
  """Tests for sec_incremental_pipeline_sensor.

  This sensor orchestrates: download → process (batched loop) → stage.
  """

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Test sensor skips in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = _build_run_status_context(
      sensor_name="sec_incremental_pipeline_sensor",
      tags={"mode": "incremental", "dagster/partition": "2025-Q1"},
    )

    result = list(sec_incremental_pipeline_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_non_incremental_runs(self, mock_env):
    """Test sensor skips non-incremental pipeline runs."""
    mock_env.ENVIRONMENT = "prod"

    context = _build_run_status_context(
      sensor_name="sec_incremental_pipeline_sensor",
      tags={"mode": "backfill", "dagster/partition": "2025-Q1"},
    )

    result = list(sec_incremental_pipeline_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_no_partition_key(self, mock_env):
    """Test sensor skips when no partition key in tags."""
    mock_env.ENVIRONMENT = "prod"

    context = _build_run_status_context(
      sensor_name="sec_incremental_pipeline_sensor",
      tags={"mode": "incremental"},
    )

    result = list(sec_incremental_pipeline_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_download_triggers_process(self, mock_env):
    """Test download success triggers first process batch."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_incremental_pipeline_sensor",
        job_name="sec_download",
        run_id="run-abc123",
        tags={
          "mode": "incremental",
          "dagster/partition": "2025-Q1",
          "batch_id": "20250225-21",
        },
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_incremental_pipeline_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].job_name == "sec_process"
    assert result[0].partition_key == "2025-Q1"
    assert result[0].tags["mode"] == "incremental"
    assert result[0].tags["batch_id"] == "20250225-21"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_process_already_running(self, mock_env):
    """Test sensor skips when process job already running for partition."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    active_run = MagicMock()
    active_run.run_id = "active-run"

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_incremental_pipeline_sensor",
        job_name="sec_download",
        run_id="run-abc123",
        tags={
          "mode": "incremental",
          "dagster/partition": "2025-Q1",
        },
        instance=instance,
        get_runs_return=[active_run],
      )

      result = list(sec_incremental_pipeline_sensor(context))

    assert len(result) == 0

  @patch("robosystems.database.session")
  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_process_retriggers_when_pending_remain(self, mock_env, mock_session_factory):
    """Test process success re-triggers another batch when pending files remain."""
    mock_env.ENVIRONMENT = "prod"

    # Mock SourceFile query returning pending files for the partition
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = [
      ("2025-Q1_0001234567_0001234567-25-000001",),
      ("2025-Q1_0001234567_0001234567-25-000002",),
    ]
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_incremental_pipeline_sensor",
        job_name="sec_process",
        run_id="run-proc-001",
        tags={
          "mode": "incremental",
          "dagster/partition": "2025-Q1",
          "quarter": "2025-Q1",
          "batch_id": "20250225-21",
        },
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_incremental_pipeline_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].job_name == "sec_process"
    assert result[0].partition_key == "2025-Q1"

  @patch("robosystems.database.session")
  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_process_triggers_stage_when_queue_drained(
    self, mock_env, mock_session_factory
  ):
    """Test process success triggers staging when no pending files remain."""
    mock_env.ENVIRONMENT = "prod"

    # Mock SourceFile query returning no pending files
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = []  # No pending files
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_incremental_pipeline_sensor",
        job_name="sec_process",
        run_id="run-proc-final",
        tags={
          "mode": "incremental",
          "dagster/partition": "2025-Q1",
          "quarter": "2025-Q1",
          "batch_id": "20250225-21",
        },
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_incremental_pipeline_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].job_name == "sec_incremental_stage"
    assert result[0].tags["phase"] == "incremental_stage"
    assert result[0].tags["mode"] == "incremental"

  @patch("robosystems.database.session")
  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_waits_for_other_partitions_before_staging(
    self, mock_env, mock_session_factory
  ):
    """Test sensor waits when this partition is done but others still have pending files."""
    mock_env.ENVIRONMENT = "prod"

    # This partition (2025-Q1) is drained, but 2024-Q4 still has pending files
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = [
      ("2024-Q4_0001234567_0001234567-24-000001",),
      ("2024-Q4_0001234567_0001234567-24-000002",),
    ]
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = _build_run_status_context(
      sensor_name="sec_incremental_pipeline_sensor",
      job_name="sec_process",
      run_id="run-proc-q1-done",
      tags={
        "mode": "incremental",
        "dagster/partition": "2025-Q1",
        "quarter": "2025-Q1",
        "batch_id": "20250401-21",
      },
    )

    result = list(sec_incremental_pipeline_sensor(context))

    # Should not trigger stage or another process run
    assert len(result) == 0

  @patch("robosystems.database.session")
  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_stage_when_stage_already_running(self, mock_env, mock_session_factory):
    """Test sensor skips staging when stage job is already running."""
    mock_env.ENVIRONMENT = "prod"

    # Mock no pending files
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = []
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    from dagster import DagsterInstance

    active_run = MagicMock()
    active_run.run_id = "active-stage-run"

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_incremental_pipeline_sensor",
        job_name="sec_process",
        run_id="run-proc-final",
        tags={
          "mode": "incremental",
          "dagster/partition": "2025-Q1",
          "quarter": "2025-Q1",
        },
        instance=instance,
        get_runs_return=[active_run],
      )

      result = list(sec_incremental_pipeline_sensor(context))

    assert len(result) == 0

  @patch("robosystems.database.session")
  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_handles_database_error(self, mock_env, mock_session_factory):
    """Test sensor handles database errors gracefully on process completion."""
    mock_env.ENVIRONMENT = "prod"
    mock_session_factory.side_effect = Exception("DB connection refused")

    context = _build_run_status_context(
      sensor_name="sec_incremental_pipeline_sensor",
      job_name="sec_process",
      run_id="run-proc-001",
      tags={
        "mode": "incremental",
        "dagster/partition": "2025-Q1",
        "quarter": "2025-Q1",
      },
    )

    result = list(sec_incremental_pipeline_sensor(context))
    assert len(result) == 0  # Returns nothing on DB error


@pytest.mark.unit
class TestSecStageToMaterializeSensor:
  """Tests for sec_stage_to_materialize_sensor."""

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Test sensor skips in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = _build_run_status_context(
      sensor_name="sec_stage_to_materialize_sensor",
      job_name="sec_incremental_stage",
      tags={"mode": "incremental"},
    )

    result = list(sec_stage_to_materialize_sensor(context))
    assert len(result) == 0  # Info log only

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_non_incremental_runs(self, mock_env):
    """Test sensor skips non-incremental runs."""
    mock_env.ENVIRONMENT = "prod"

    context = _build_run_status_context(
      sensor_name="sec_stage_to_materialize_sensor",
      job_name="sec_incremental_stage",
      tags={"mode": "full"},  # Not incremental
    )

    result = list(sec_stage_to_materialize_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_yields_run_request_after_incremental_stage(self, mock_env):
    """Test sensor yields RunRequest after incremental staging."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_stage_to_materialize_sensor",
        job_name="sec_incremental_stage",
        run_id="run-stage-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_stage_to_materialize_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].tags["mode"] == "incremental"
    assert result[0].tags["phase"] == "materialize"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_materialize_already_running(self, mock_env):
    """Test sensor skips when materialize job is already running."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    active_run = MagicMock()
    active_run.run_id = "active-mat-run"

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_stage_to_materialize_sensor",
        job_name="sec_incremental_stage",
        run_id="run-stage-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[active_run],
      )

      result = list(sec_stage_to_materialize_sensor(context))

    assert len(result) == 0


@pytest.mark.unit
class TestSecPostMaterializePublishSensor:
  """Tests for sec_post_materialize_publish_sensor.

  This sensor orchestrates: materialize → lbug S3 publish → duckdb S3 publish → replica refresh.
  """

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Test sensor skips in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = _build_run_status_context(
      sensor_name="sec_post_materialize_publish_sensor",
      job_name="sec_materialize",
      tags={"mode": "incremental"},
    )

    result = list(sec_post_materialize_publish_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_non_incremental_runs(self, mock_env):
    """Test sensor skips non-incremental runs."""
    mock_env.ENVIRONMENT = "prod"

    context = _build_run_status_context(
      sensor_name="sec_post_materialize_publish_sensor",
      job_name="sec_materialize",
      tags={"mode": "full"},
    )

    result = list(sec_post_materialize_publish_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_materialize_triggers_lbug_publish(self, mock_env):
    """Test materialize success triggers lbug S3 publish."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_post_materialize_publish_sensor",
        job_name="sec_materialize",
        run_id="run-mat-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_post_materialize_publish_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].job_name == "sec_lbug_s3_publish"
    assert result[0].tags["phase"] == "lbug_s3_publish"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_lbug_publish_triggers_duckdb_publish(self, mock_env):
    """Test lbug publish success triggers duckdb S3 publish."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_post_materialize_publish_sensor",
        job_name="sec_lbug_s3_publish",
        run_id="run-lbug-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_post_materialize_publish_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].job_name == "sec_duckdb_s3_publish"
    assert result[0].tags["phase"] == "duckdb_s3_publish"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_duckdb_publish_triggers_replica_refresh(self, mock_env):
    """Test duckdb publish success triggers replica refresh."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_post_materialize_publish_sensor",
        job_name="sec_duckdb_s3_publish",
        run_id="run-duckdb-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_post_materialize_publish_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].job_name == "sec_vector_s3_publish"
    assert result[0].tags["phase"] == "vector_s3_publish"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_next_job_already_running(self, mock_env):
    """Test sensor skips when next job is already running."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    active_run = MagicMock()
    active_run.run_id = "active-publish-run"

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_post_materialize_publish_sensor",
        job_name="sec_materialize",
        run_id="run-mat-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[active_run],
      )

      result = list(sec_post_materialize_publish_sensor(context))

    assert len(result) == 0


@pytest.mark.unit
class TestSecIncrementalDownloadSchedule:
  """Tests for sec_incremental_download_schedule."""

  @patch("robosystems.adapters.sec.pipeline.sensors._get_quarters_to_scan")
  def test_yields_run_requests_per_quarter(self, mock_quarters):
    """Test schedule yields one RunRequest per quarter."""
    mock_quarters.return_value = ["2025-Q1"]

    context = build_schedule_context(
      scheduled_execution_time=datetime(2025, 2, 25, 21, 0, tzinfo=UTC)
    )

    result = list(sec_incremental_download_schedule(context))

    assert len(result) == 1
    assert isinstance(result[0], RunRequest)
    assert result[0].partition_key == "2025-Q1"
    assert result[0].tags["mode"] == "incremental"

  @patch("robosystems.adapters.sec.pipeline.sensors._get_quarters_to_scan")
  def test_yields_single_quarter_keyed_off_et_tick(self, mock_quarters):
    """Hard cut-over: one RunRequest, quarter derived from the ET tick time."""
    mock_quarters.return_value = ["2026-Q2"]

    # 2026-06-30 21:00 ET is already 2026-07-01 in UTC; the schedule must key
    # the quarter off the ET scheduled time, not the container UTC clock.
    sched = datetime(2026, 6, 30, 21, 0, tzinfo=ZoneInfo("America/New_York"))
    context = build_schedule_context(scheduled_execution_time=sched)

    result = list(sec_incremental_download_schedule(context))

    assert len(result) == 1
    assert result[0].partition_key == "2026-Q2"
    mock_quarters.assert_called_once_with(sched)

  @patch("robosystems.adapters.sec.pipeline.sensors._get_quarters_to_scan")
  def test_run_keys_include_batch_id(self, mock_quarters):
    """Test run keys include the batch_id for tracking."""
    mock_quarters.return_value = ["2025-Q1"]

    context = build_schedule_context(
      scheduled_execution_time=datetime(2025, 2, 25, 21, 0, tzinfo=UTC)
    )

    result = list(sec_incremental_download_schedule(context))

    assert result[0].run_key is not None
    assert "sec-incremental" in result[0].run_key
    assert result[0].tags.get("batch_id") is not None

  @patch("robosystems.adapters.sec.pipeline.sensors._get_quarters_to_scan")
  def test_includes_skip_existing_config(self, mock_quarters):
    """Test schedule includes skip_existing=True in config."""
    mock_quarters.return_value = ["2025-Q1"]

    context = build_schedule_context(
      scheduled_execution_time=datetime(2025, 2, 25, 21, 0, tzinfo=UTC)
    )

    result = list(sec_incremental_download_schedule(context))

    run_config = result[0].run_config
    filing_config = run_config["ops"]["sec_raw_filings"]["config"]
    assert filing_config["skip_existing"] is True


@pytest.mark.unit
class TestGetQuartersToScan:
  """Tests for _get_quarters_to_scan helper."""

  @patch("robosystems.adapters.sec.get_quarters_to_scan")
  def test_delegates_to_adapter(self, mock_get_quarters):
    """Test _get_quarters_to_scan delegates to the SEC adapter."""
    mock_get_quarters.return_value = ["2025-Q1"]

    result = _get_quarters_to_scan()

    assert result == ["2025-Q1"]
    mock_get_quarters.assert_called_once()

  @patch("robosystems.adapters.sec.get_quarters_to_scan")
  def test_returns_single_partition_key(self, mock_get_quarters):
    """Hard cut-over: exactly one properly formatted partition key."""
    mock_get_quarters.return_value = ["2026-Q2"]

    result = _get_quarters_to_scan()

    assert isinstance(result, list)
    assert len(result) == 1
    parts = result[0].split("-Q")
    assert len(parts) == 2


@pytest.mark.unit
class TestQuarterSelectionEasternHardCutover:
  """Regression tests for the ET-keyed, single-quarter hard cut-over.

  The bug: quarter was computed from the container's UTC clock, so the 21:00 ET
  run on a quarter's last day (already next-day in UTC) rolled to the next
  quarter and also fired a spurious previous-quarter overlap run.
  """

  def test_last_evening_of_q2_stays_q2(self):
    from robosystems.adapters.sec import get_current_quarter, get_quarters_to_scan

    # 2026-06-30 21:00 ET == 2026-07-01 01:00 UTC — must resolve to Q2, not Q3.
    now = datetime(2026, 6, 30, 21, 0, tzinfo=ZoneInfo("America/New_York"))
    assert get_current_quarter(now) == (2026, 2)
    assert get_quarters_to_scan(now) == ["2026-Q2"]

  def test_first_evening_of_q3_is_q3_only(self):
    from robosystems.adapters.sec import get_quarters_to_scan

    # First day of Q3 — no previous-quarter (Q2) overlap under hard cut-over.
    now = datetime(2026, 7, 1, 21, 0, tzinfo=ZoneInfo("America/New_York"))
    assert get_quarters_to_scan(now) == ["2026-Q3"]

  def test_always_single_quarter(self):
    from robosystems.adapters.sec import get_quarters_to_scan

    # A few days into a new quarter — still exactly one quarter (no overlap).
    now = datetime(2026, 7, 3, 21, 0, tzinfo=ZoneInfo("America/New_York"))
    assert get_quarters_to_scan(now) == ["2026-Q3"]


@pytest.mark.unit
class TestSecPostStageIndexSensor:
  """Tests for sec_post_stage_index_sensor.

  This sensor orchestrates: stage → text search indexing (textblocks + narratives).
  Both index jobs fire in parallel after staging completes.
  """

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Test sensor skips in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = _build_run_status_context(
      sensor_name="sec_post_stage_index_sensor",
      job_name="sec_incremental_stage",
      tags={"mode": "incremental"},
    )

    result = list(sec_post_stage_index_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_non_incremental_runs(self, mock_env):
    """Test sensor skips non-incremental runs."""
    mock_env.ENVIRONMENT = "prod"

    context = _build_run_status_context(
      sensor_name="sec_post_stage_index_sensor",
      job_name="sec_incremental_stage",
      tags={"mode": "full"},
    )

    result = list(sec_post_stage_index_sensor(context))
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_yields_run_requests_for_all_index_jobs(self, mock_env):
    """Test sensor yields RunRequests for textblocks, narratives, and iXBRL."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_post_stage_index_sensor",
        job_name="sec_incremental_stage",
        run_id="run-stage-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_post_stage_index_sensor(context))

    assert len(result) == 2
    job_names = {r.job_name for r in result}
    assert job_names == {
      "sec_narratives_index",
      "sec_ixbrl_index",
    }

    for r in result:
      assert isinstance(r, RunRequest)
      assert r.tags["phase"] == "text_index"
      assert r.tags["mode"] == "incremental"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_passes_graph_id_in_config(self, mock_env):
    """Test sensor passes graph_id through run config."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_post_stage_index_sensor",
        job_name="sec_incremental_stage",
        run_id="run-stage-001",
        tags={"mode": "incremental", "graph_id": "sec"},
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_post_stage_index_sensor(context))

    # Verify config includes graph_id for all jobs
    for r in result:
      # Find the asset op name in the run config
      ops = r.run_config["ops"]
      assert len(ops) == 1
      config = next(iter(ops.values()))["config"]
      assert config["graph_id"] == "sec"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_defaults_graph_id_to_sec(self, mock_env):
    """Test sensor defaults graph_id to 'sec' when not in tags."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_post_stage_index_sensor",
        job_name="sec_incremental_stage",
        run_id="run-stage-001",
        tags={"mode": "incremental"},  # No graph_id tag
        instance=instance,
        get_runs_return=[],
      )

      result = list(sec_post_stage_index_sensor(context))

    config = result[0].run_config["ops"]["sec_narratives_indexed"]["config"]
    assert config["graph_id"] == "sec"

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_job_when_already_running(self, mock_env):
    """Test sensor skips a job when it's already running."""
    mock_env.ENVIRONMENT = "prod"

    from dagster import DagsterInstance

    active_run = MagicMock()
    active_run.run_id = "active-index-run"

    with DagsterInstance.ephemeral() as instance:
      context = _build_run_status_context(
        sensor_name="sec_post_stage_index_sensor",
        job_name="sec_incremental_stage",
        run_id="run-stage-001",
        tags={"mode": "incremental"},
        instance=instance,
        get_runs_return=[active_run],  # Both jobs appear running
      )

      result = list(sec_post_stage_index_sensor(context))

    # Both jobs skipped because get_runs returns active run for each check
    assert len(result) == 0

  @patch("robosystems.adapters.sec.pipeline.sensors.env")
  def test_skips_when_no_mode_tag(self, mock_env):
    """Test sensor skips when mode tag is missing entirely."""
    mock_env.ENVIRONMENT = "prod"

    context = _build_run_status_context(
      sensor_name="sec_post_stage_index_sensor",
      job_name="sec_incremental_stage",
      tags={},  # No mode tag
    )

    result = list(sec_post_stage_index_sensor(context))
    assert len(result) == 0
