"""Tests for SEC pipeline sensors (incremental pipeline chain).

The sec_processing_sensor is covered in test_sec.py.
This file covers the incremental pipeline chain sensors:
- sec_incremental_pipeline_sensor (download → process loop → stage)
- sec_stage_to_materialize_sensor
- sec_incremental_download_schedule
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

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
  def test_yields_multiple_quarters_at_boundary(self, mock_quarters):
    """Test schedule yields two RunRequests at quarter boundary."""
    mock_quarters.return_value = ["2025-Q1", "2024-Q4"]

    context = build_schedule_context(
      scheduled_execution_time=datetime(2025, 1, 2, 21, 0, tzinfo=UTC)
    )

    result = list(sec_incremental_download_schedule(context))

    assert len(result) == 2
    partition_keys = {r.partition_key for r in result}
    assert partition_keys == {"2025-Q1", "2024-Q4"}

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
  def test_returns_list_of_partition_keys(self, mock_get_quarters):
    """Test returns properly formatted partition keys."""
    mock_get_quarters.return_value = ["2025-Q1", "2024-Q4"]

    result = _get_quarters_to_scan()

    assert isinstance(result, list)
    assert len(result) == 2
    for key in result:
      parts = key.split("-Q")
      assert len(parts) == 2
