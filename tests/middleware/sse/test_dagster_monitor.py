"""Unit tests for the Dagster run monitor module."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.config.constants import DAGSTER_CLIENT_TIMEOUT_SECONDS
from robosystems.middleware.sse.dagster_monitor import (
  DAGSTER_STATUS_MAP,
  DagsterRunMonitor,
  build_graph_job_config,
  run_and_monitor_dagster_job,
  submit_dagster_job_sync,
)
from robosystems.middleware.sse.event_storage import EventType

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_event_storage():
  """Create a mock SSEEventStorage for the monitor."""
  storage = MagicMock()
  storage.store_event_sync = MagicMock()
  storage.get_operation_result_sync = MagicMock(return_value=None)
  return storage


@pytest.fixture
def mock_dagster_client():
  """Create a mock DagsterGraphQLClient."""
  client = MagicMock()
  client.submit_job_execution = MagicMock(return_value="run-abc-123")
  return client


@pytest.fixture
def monitor(mock_event_storage, mock_dagster_client):
  """Create a DagsterRunMonitor with mocked dependencies."""
  with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
    mock_env.DAGSTER_HOST = "localhost"
    mock_env.DAGSTER_PORT = 3000
    mock_env.ENVIRONMENT = "dev"
    m = DagsterRunMonitor(
      dagster_host="localhost",
      dagster_port=3000,
      poll_interval=0.01,
      max_poll_time=1.0,
    )
  m.event_storage = mock_event_storage
  m._client = mock_dagster_client
  return m


# ---------------------------------------------------------------------------
# DAGSTER_STATUS_MAP
# ---------------------------------------------------------------------------


class TestDagsterStatusMap:
  @pytest.mark.unit
  def test_all_expected_statuses_mapped(self):
    expected_keys = {
      "QUEUED",
      "NOT_STARTED",
      "STARTING",
      "STARTED",
      "SUCCESS",
      "FAILURE",
      "CANCELED",
    }
    assert set(DAGSTER_STATUS_MAP.keys()) == expected_keys

  @pytest.mark.unit
  def test_terminal_statuses_at_100_percent(self):
    for status in ("SUCCESS", "FAILURE", "CANCELED"):
      _, progress = DAGSTER_STATUS_MAP[status]
      assert progress == 100, f"{status} should be at 100%"

  @pytest.mark.unit
  def test_pending_statuses_at_zero_percent(self):
    for status in ("QUEUED", "NOT_STARTED"):
      mapped, progress = DAGSTER_STATUS_MAP[status]
      assert mapped == "pending"
      assert progress == 0

  @pytest.mark.unit
  def test_running_statuses(self):
    for status in ("STARTING", "STARTED"):
      mapped, _ = DAGSTER_STATUS_MAP[status]
      assert mapped == "running"

  @pytest.mark.unit
  def test_success_maps_to_completed(self):
    mapped, _ = DAGSTER_STATUS_MAP["SUCCESS"]
    assert mapped == "completed"

  @pytest.mark.unit
  def test_failure_maps_to_failed(self):
    mapped, _ = DAGSTER_STATUS_MAP["FAILURE"]
    assert mapped == "failed"

  @pytest.mark.unit
  def test_canceled_maps_to_cancelled(self):
    mapped, _ = DAGSTER_STATUS_MAP["CANCELED"]
    assert mapped == "cancelled"


# ---------------------------------------------------------------------------
# DagsterRunMonitor.__init__
# ---------------------------------------------------------------------------


class TestDagsterRunMonitorInit:
  @pytest.mark.unit
  def test_defaults_from_env(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.DAGSTER_HOST = "dagster-host"
      mock_env.DAGSTER_PORT = 4000
      m = DagsterRunMonitor()
      assert m.dagster_host == "dagster-host"
      assert m.dagster_port == 4000
      assert m.poll_interval == 2.0
      assert m.max_poll_time == 3600.0

  @pytest.mark.unit
  def test_custom_parameters(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.DAGSTER_HOST = "default-host"
      mock_env.DAGSTER_PORT = 3000
      m = DagsterRunMonitor(
        dagster_host="custom-host",
        dagster_port=9999,
        poll_interval=5.0,
        max_poll_time=120.0,
      )
      assert m.dagster_host == "custom-host"
      assert m.dagster_port == 9999
      assert m.poll_interval == 5.0
      assert m.max_poll_time == 120.0

  @pytest.mark.unit
  def test_client_initially_none(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.DAGSTER_HOST = "localhost"
      mock_env.DAGSTER_PORT = 3000
      m = DagsterRunMonitor()
      assert m._client is None


# ---------------------------------------------------------------------------
# DagsterRunMonitor._get_client
# ---------------------------------------------------------------------------


class TestGetClient:
  @pytest.mark.unit
  def test_creates_client_on_first_call(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.DAGSTER_HOST = "localhost"
      mock_env.DAGSTER_PORT = 3000
      m = DagsterRunMonitor()

      mock_client_cls = MagicMock()
      mock_client_instance = MagicMock()
      mock_client_cls.return_value = mock_client_instance

      with patch.dict(
        "sys.modules",
        {"dagster_graphql": MagicMock(DagsterGraphQLClient=mock_client_cls)},
      ):
        client = m._get_client()
        assert client is mock_client_instance
        # An explicit, short timeout: the library default is 300 s, and the
        # call runs on the API event loop. A hung webserver must not hold
        # every tenant on the task for five minutes per poll.
        mock_client_cls.assert_called_once_with(
          hostname="localhost",
          port_number=3000,
          timeout=DAGSTER_CLIENT_TIMEOUT_SECONDS,
        )
        assert DAGSTER_CLIENT_TIMEOUT_SECONDS < 60

  @pytest.mark.unit
  def test_reuses_client_on_subsequent_calls(self, monitor, mock_dagster_client):
    client1 = monitor._get_client()
    client2 = monitor._get_client()
    assert client1 is client2
    assert client1 is mock_dagster_client

  @pytest.mark.unit
  def test_raises_when_dagster_graphql_not_installed(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.DAGSTER_HOST = "localhost"
      mock_env.DAGSTER_PORT = 3000
      m = DagsterRunMonitor()

      with patch("builtins.__import__", side_effect=ImportError("no module")):
        with pytest.raises(ImportError):
          m._get_client()


# ---------------------------------------------------------------------------
# DagsterRunMonitor.submit_job
# ---------------------------------------------------------------------------


class TestSubmitJob:
  @pytest.mark.unit
  def test_submit_with_config_and_tags(self, monitor, mock_dagster_client):
    run_config = {"ops": {"my_op": {"config": {"key": "value"}}}}
    tags = {"env": "test"}
    run_id = monitor.submit_job("test_job", run_config=run_config, tags=tags)
    assert run_id == "run-abc-123"
    mock_dagster_client.submit_job_execution.assert_called_once_with(
      job_name="test_job",
      run_config=run_config,
      tags=tags,
    )

  @pytest.mark.unit
  def test_submit_with_defaults(self, monitor, mock_dagster_client):
    run_id = monitor.submit_job("simple_job")
    assert run_id == "run-abc-123"
    mock_dagster_client.submit_job_execution.assert_called_once_with(
      job_name="simple_job",
      run_config={},
      tags=None,
    )

  @pytest.mark.unit
  def test_submit_propagates_exception(self, monitor, mock_dagster_client):
    mock_dagster_client.submit_job_execution.side_effect = RuntimeError(
      "connection refused"
    )
    with pytest.raises(RuntimeError, match="connection refused"):
      monitor.submit_job("failing_job")


# ---------------------------------------------------------------------------
# DagsterRunMonitor.get_run_status
# ---------------------------------------------------------------------------


class TestGetRunStatus:
  @pytest.mark.unit
  def test_maps_known_status(self, monitor, mock_dagster_client):
    mock_status = MagicMock()
    mock_status.name = "STARTED"
    mock_dagster_client.get_run_status.return_value = mock_status

    result = monitor.get_run_status("run-123")
    assert result["dagster_status"] == "STARTED"
    assert result["status"] == "running"
    assert result["progress_percent"] == 10
    assert result["run_id"] == "run-123"

  @pytest.mark.unit
  def test_maps_success_status(self, monitor, mock_dagster_client):
    mock_status = MagicMock()
    mock_status.name = "SUCCESS"
    mock_dagster_client.get_run_status.return_value = mock_status

    result = monitor.get_run_status("run-456")
    assert result["status"] == "completed"
    assert result["progress_percent"] == 100

  @pytest.mark.unit
  def test_maps_failure_status(self, monitor, mock_dagster_client):
    mock_status = MagicMock()
    mock_status.name = "FAILURE"
    mock_dagster_client.get_run_status.return_value = mock_status

    result = monitor.get_run_status("run-789")
    assert result["status"] == "failed"
    assert result["progress_percent"] == 100

  @pytest.mark.unit
  def test_unknown_status_defaults(self, monitor, mock_dagster_client):
    mock_status = MagicMock()
    mock_status.name = "SOME_NEW_STATUS"
    mock_dagster_client.get_run_status.return_value = mock_status

    result = monitor.get_run_status("run-unknown")
    assert result["status"] == "running"
    assert result["progress_percent"] == 50

  @pytest.mark.unit
  def test_status_without_name_attribute(self, monitor, mock_dagster_client):
    """Test when status object doesn't have a .name attribute (uses str())."""
    mock_dagster_client.get_run_status.return_value = "SUCCESS"

    result = monitor.get_run_status("run-str")
    assert result["dagster_status"] == "SUCCESS"
    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# DagsterRunMonitor.emit_started
# ---------------------------------------------------------------------------


class TestEmitStarted:
  @pytest.mark.unit
  async def test_emits_started_event(self, monitor, mock_event_storage):
    await monitor.emit_started("op-123", "create_graph_job", "run-abc")
    mock_event_storage.store_event_sync.assert_called_once_with(
      "op-123",
      EventType.OPERATION_STARTED,
      {
        "message": "Job 'create_graph_job' started",
        "job_name": "create_graph_job",
        "run_id": "run-abc",
        "progress_percent": 0,
      },
    )

  @pytest.mark.unit
  async def test_emits_started_handles_storage_error(self, monitor, mock_event_storage):
    mock_event_storage.store_event_sync.side_effect = RuntimeError("redis down")
    # Should not raise
    await monitor.emit_started("op-123", "test_job", "run-xyz")


# ---------------------------------------------------------------------------
# DagsterRunMonitor.emit_progress
# ---------------------------------------------------------------------------


class TestEmitProgress:
  @pytest.mark.unit
  async def test_emits_progress_event(self, monitor, mock_event_storage):
    await monitor.emit_progress("op-123", "Working...", progress_percent=42.5)
    mock_event_storage.store_event_sync.assert_called_once_with(
      "op-123",
      EventType.OPERATION_PROGRESS,
      {"message": "Working...", "progress_percent": 42.5},
    )

  @pytest.mark.unit
  async def test_emits_progress_with_details(self, monitor, mock_event_storage):
    await monitor.emit_progress(
      "op-456",
      "Step 2 of 5",
      progress_percent=40.0,
      details={"step": 2, "total_steps": 5},
    )
    call_data = mock_event_storage.store_event_sync.call_args[0][2]
    assert call_data["message"] == "Step 2 of 5"
    assert call_data["progress_percent"] == 40.0
    assert call_data["step"] == 2
    assert call_data["total_steps"] == 5

  @pytest.mark.unit
  async def test_emits_progress_handles_storage_error(
    self, monitor, mock_event_storage
  ):
    mock_event_storage.store_event_sync.side_effect = RuntimeError("redis down")
    await monitor.emit_progress("op-123", "msg")


# ---------------------------------------------------------------------------
# DagsterRunMonitor.emit_completion
# ---------------------------------------------------------------------------


class TestEmitCompletion:
  @pytest.mark.unit
  async def test_emits_completed_event(self, monitor, mock_event_storage):
    result = {"run_id": "run-123", "elapsed_time": 45.0}
    await monitor.emit_completion("op-123", result)
    mock_event_storage.store_event_sync.assert_called_once()
    call_args = mock_event_storage.store_event_sync.call_args
    assert call_args[0][0] == "op-123"
    assert call_args[0][1] == EventType.OPERATION_COMPLETED
    data = call_args[0][2]
    assert data["message"] == "Operation completed successfully"
    assert data["result"]["run_id"] == "run-123"

  @pytest.mark.unit
  async def test_merges_stored_result(self, monitor, mock_event_storage):
    mock_event_storage.get_operation_result_sync.return_value = {"graph_id": "kg12345"}
    result = {"run_id": "run-789", "elapsed_time": 10.0}
    await monitor.emit_completion("op-456", result)
    call_data = mock_event_storage.store_event_sync.call_args[0][2]
    merged = call_data["result"]
    assert merged["graph_id"] == "kg12345"
    assert merged["run_id"] == "run-789"

  @pytest.mark.unit
  async def test_stored_result_none(self, monitor, mock_event_storage):
    mock_event_storage.get_operation_result_sync.return_value = None
    result = {"run_id": "run-000"}
    await monitor.emit_completion("op-000", result)
    call_data = mock_event_storage.store_event_sync.call_args[0][2]
    assert call_data["result"]["run_id"] == "run-000"

  @pytest.mark.unit
  async def test_handles_storage_error(self, monitor, mock_event_storage):
    mock_event_storage.store_event_sync.side_effect = RuntimeError("boom")
    await monitor.emit_completion("op-err", {"run_id": "x"})


# ---------------------------------------------------------------------------
# DagsterRunMonitor.emit_error
# ---------------------------------------------------------------------------


class TestEmitError:
  @pytest.mark.unit
  async def test_emits_error_event(self, monitor, mock_event_storage):
    await monitor.emit_error("op-123", "something failed", {"detail": "timeout"})
    call_args = mock_event_storage.store_event_sync.call_args
    assert call_args[0][1] == EventType.OPERATION_ERROR
    data = call_args[0][2]
    assert data["error"] == "something failed"
    assert data["error_details"]["detail"] == "timeout"

  @pytest.mark.unit
  async def test_emits_error_without_details(self, monitor, mock_event_storage):
    await monitor.emit_error("op-123", "fail")
    data = mock_event_storage.store_event_sync.call_args[0][2]
    assert data["error_details"] is None

  @pytest.mark.unit
  async def test_handles_storage_error(self, monitor, mock_event_storage):
    mock_event_storage.store_event_sync.side_effect = RuntimeError("boom")
    await monitor.emit_error("op-err", "fail")


# ---------------------------------------------------------------------------
# DagsterRunMonitor.monitor_run
# ---------------------------------------------------------------------------


class TestMonitorRun:
  @pytest.mark.unit
  async def test_completes_on_success(
    self, monitor, mock_dagster_client, mock_event_storage
  ):
    mock_status = MagicMock()
    mock_status.name = "SUCCESS"
    mock_dagster_client.get_run_status.return_value = mock_status

    result = await monitor.monitor_run("run-123", "op-123")
    assert result["status"] == "completed"
    # Should have emitted a completion event
    assert any(
      call[0][1] == EventType.OPERATION_COMPLETED
      for call in mock_event_storage.store_event_sync.call_args_list
    )

  @pytest.mark.unit
  async def test_reports_failure(
    self, monitor, mock_dagster_client, mock_event_storage
  ):
    mock_status = MagicMock()
    mock_status.name = "FAILURE"
    mock_dagster_client.get_run_status.return_value = mock_status

    result = await monitor.monitor_run("run-fail", "op-fail")
    assert result["status"] == "failed"
    assert any(
      call[0][1] == EventType.OPERATION_ERROR
      for call in mock_event_storage.store_event_sync.call_args_list
    )

  @pytest.mark.unit
  async def test_reports_cancellation(
    self, monitor, mock_dagster_client, mock_event_storage
  ):
    mock_status = MagicMock()
    mock_status.name = "CANCELED"
    mock_dagster_client.get_run_status.return_value = mock_status

    result = await monitor.monitor_run("run-cancel", "op-cancel")
    assert result["status"] == "cancelled"

  @pytest.mark.unit
  async def test_emits_progress_on_status_change(
    self, monitor, mock_dagster_client, mock_event_storage
  ):
    """Test that progress events are emitted when mapped status changes before completion."""
    # QUEUED -> "pending", STARTING -> "running", SUCCESS -> "completed"
    # Each of these changes the mapped status, so each emits an event
    statuses = []
    for name in ("QUEUED", "STARTING", "SUCCESS"):
      s = MagicMock()
      s.name = name
      statuses.append(s)

    mock_dagster_client.get_run_status.side_effect = statuses

    result = await monitor.monitor_run("run-progress", "op-progress")
    assert result["status"] == "completed"
    # 2 progress events (QUEUED=pending, STARTING=running) + 1 completion (SUCCESS)
    assert mock_event_storage.store_event_sync.call_count == 3

  @pytest.mark.unit
  async def test_timeout(self, mock_event_storage, mock_dagster_client):
    """Test that monitoring times out after max_poll_time."""
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.DAGSTER_HOST = "localhost"
      mock_env.DAGSTER_PORT = 3000
      m = DagsterRunMonitor(
        dagster_host="localhost",
        dagster_port=3000,
        poll_interval=0.01,
        max_poll_time=0.02,
      )
    m.event_storage = mock_event_storage
    m._client = mock_dagster_client

    # Always return "running" so it never completes
    mock_status = MagicMock()
    mock_status.name = "STARTED"
    mock_dagster_client.get_run_status.return_value = mock_status

    result = await m.monitor_run("run-timeout", "op-timeout")
    assert result["status"] == "timeout"
    assert "elapsed_time" in result

  @pytest.mark.unit
  async def test_handles_get_status_error(
    self, monitor, mock_dagster_client, mock_event_storage
  ):
    """Test that get_run_status errors are handled gracefully with retry."""
    call_count = 0

    def side_effect(run_id):
      nonlocal call_count
      call_count += 1
      if call_count <= 1:
        raise ConnectionError("connection lost")
      s = MagicMock()
      s.name = "SUCCESS"
      return s

    mock_dagster_client.get_run_status.side_effect = side_effect

    result = await monitor.monitor_run("run-retry", "op-retry")
    assert result["status"] == "completed"

  @pytest.mark.unit
  async def test_no_duplicate_events_for_same_status(
    self, monitor, mock_dagster_client, mock_event_storage
  ):
    """Test that the same status doesn't emit duplicate progress events."""
    statuses = []
    for name in ("STARTED", "STARTED", "STARTED", "SUCCESS"):
      s = MagicMock()
      s.name = name
      statuses.append(s)

    mock_dagster_client.get_run_status.side_effect = statuses

    await monitor.monitor_run("run-dedup", "op-dedup")
    # Only 2 events: 1 progress for STARTED + 1 completion for SUCCESS
    assert mock_event_storage.store_event_sync.call_count == 2


# ---------------------------------------------------------------------------
# run_and_monitor_dagster_job (module-level convenience)
# ---------------------------------------------------------------------------


class TestRunAndMonitorDagsterJob:
  @pytest.mark.unit
  async def test_successful_run(self):
    mock_monitor = MagicMock(spec=DagsterRunMonitor)
    mock_monitor.submit_job.return_value = "run-abc"
    mock_monitor.emit_started = AsyncMock()
    mock_monitor.monitor_run = AsyncMock(
      return_value={"status": "completed", "run_id": "run-abc"}
    )
    mock_monitor.emit_error = AsyncMock()

    with patch(
      "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
      return_value=mock_monitor,
    ):
      result = await run_and_monitor_dagster_job(
        job_name="test_job",
        operation_id="op-123",
        run_config={"key": "val"},
        tags={"tag": "1"},
      )

    assert result["status"] == "completed"
    mock_monitor.submit_job.assert_called_once_with(
      "test_job", {"key": "val"}, {"tag": "1"}
    )
    mock_monitor.emit_started.assert_called_once_with("op-123", "test_job", "run-abc")
    mock_monitor.monitor_run.assert_called_once_with("run-abc", "op-123")

  @pytest.mark.unit
  async def test_submit_failure_emits_error(self):
    mock_monitor = MagicMock(spec=DagsterRunMonitor)
    mock_monitor.submit_job.side_effect = RuntimeError("connection refused")
    mock_monitor.emit_error = AsyncMock()

    with patch(
      "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
      return_value=mock_monitor,
    ):
      with pytest.raises(RuntimeError, match="connection refused"):
        await run_and_monitor_dagster_job(
          job_name="failing_job",
          operation_id="op-fail",
        )

    mock_monitor.emit_error.assert_called_once()
    error_call = mock_monitor.emit_error.call_args
    assert error_call[0][0] == "op-fail"
    assert "connection refused" in error_call[0][1]


# ---------------------------------------------------------------------------
# submit_dagster_job_sync
# ---------------------------------------------------------------------------


class TestSubmitDagsterJobSync:
  @pytest.mark.unit
  def test_submits_job_and_returns_run_id(self):
    mock_monitor = MagicMock(spec=DagsterRunMonitor)
    mock_monitor.submit_job.return_value = "run-sync-123"

    with patch(
      "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
      return_value=mock_monitor,
    ):
      run_id = submit_dagster_job_sync("my_job", {"config": "val"}, {"tag": "x"})

    assert run_id == "run-sync-123"
    mock_monitor.submit_job.assert_called_once_with(
      "my_job", {"config": "val"}, {"tag": "x"}
    )

  @pytest.mark.unit
  def test_submits_job_with_defaults(self):
    mock_monitor = MagicMock(spec=DagsterRunMonitor)
    mock_monitor.submit_job.return_value = "run-default"

    with patch(
      "robosystems.middleware.sse.dagster_monitor.DagsterRunMonitor",
      return_value=mock_monitor,
    ):
      run_id = submit_dagster_job_sync("default_job")

    assert run_id == "run-default"
    mock_monitor.submit_job.assert_called_once_with("default_job", None, None)


# ---------------------------------------------------------------------------
# build_graph_job_config
# ---------------------------------------------------------------------------


class TestBuildGraphJobConfig:
  @pytest.mark.unit
  def test_known_job_creates_config(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.ENVIRONMENT = "dev"
      config = build_graph_job_config(
        "create_graph_job",
        graph_id="kg123",
        user_id="u1",
      )

    assert "ops" in config
    assert "create_graph_database" in config["ops"]
    assert "create_graph_subscription" in config["ops"]
    assert config["ops"]["create_graph_database"]["config"]["graph_id"] == "kg123"
    assert config["ops"]["create_graph_subscription"]["config"]["user_id"] == "u1"

  @pytest.mark.unit
  def test_stage_file_job_config(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.ENVIRONMENT = "dev"
      config = build_graph_job_config(
        "stage_file_job",
        graph_id="kg456",
        file_path="/tmp/data.csv",
      )

    assert "stage_file_in_duckdb" in config["ops"]
    assert "materialize_file_to_graph" in config["ops"]

  @pytest.mark.unit
  def test_unknown_job_raises(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env"):
      with pytest.raises(ValueError, match="Unknown job name"):
        build_graph_job_config("nonexistent_job", key="val")

  @pytest.mark.unit
  def test_dev_environment_uses_in_process_executor(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.ENVIRONMENT = "dev"
      config = build_graph_job_config("backup_graph_job", graph_id="kg789")

    assert "execution" in config
    assert "in_process" in config["execution"]["config"]

  @pytest.mark.unit
  def test_prod_environment_no_in_process_executor(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      config = build_graph_job_config("backup_graph_job", graph_id="kg789")

    assert "execution" not in config

  @pytest.mark.unit
  def test_all_known_jobs_produce_config(self):
    known_jobs = [
      "create_graph_job",
      "create_entity_graph_job",
      "create_subgraph_job",
      "backup_graph_job",
      "restore_graph_job",
      "stage_file_job",
      "materialize_file_job",
      "materialize_graph_job",
    ]
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.ENVIRONMENT = "dev"
      for job_name in known_jobs:
        config = build_graph_job_config(job_name, test_param="val")
        assert "ops" in config, f"Config missing 'ops' for {job_name}"

  @pytest.mark.unit
  def test_multi_op_job_all_ops_get_same_config(self):
    with patch("robosystems.middleware.sse.dagster_monitor.env") as mock_env:
      mock_env.ENVIRONMENT = "prod"
      config = build_graph_job_config(
        "create_subgraph_job",
        graph_id="kg_parent",
        subgraph_name="dev",
      )

    ops = config["ops"]
    assert "create_subgraph_database" in ops
    assert "fork_parent_to_subgraph" in ops
    for op_name in ("create_subgraph_database", "fork_parent_to_subgraph"):
      assert ops[op_name]["config"]["graph_id"] == "kg_parent"
      assert ops[op_name]["config"]["subgraph_name"] == "dev"
