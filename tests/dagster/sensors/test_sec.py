"""Tests for SEC processing sensor.

The sec_processing_sensor discovers pending SourceFile records and yields
one RunRequest per file. Dagster's QueuedRunCoordinator controls concurrency
via DAGSTER_MAX_CONCURRENT_RUNS. Run deduplication is handled by run_key.
"""

from unittest.mock import MagicMock, patch

from dagster import RunRequest, SkipReason, build_sensor_context

from robosystems.dagster.sensors.sec import sec_processing_sensor


class TestSecProcessingSensor:
  """Tests for sec_processing_sensor (per-filing model)."""

  @patch("robosystems.dagster.sensors.sec.env")
  def test_skips_in_dev_environment(self, mock_env):
    """Test sensor skips in dev environment."""
    mock_env.ENVIRONMENT = "dev"

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "Skipped in dev" in str(result[0])

  @patch("robosystems.database.session")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_skips_when_no_pending_files(self, mock_env, mock_session_factory):
    """Test sensor skips when no pending files exist."""
    mock_env.ENVIRONMENT = "prod"

    # Mock session with no pending files
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = []  # No pending files
    mock_query.count.return_value = 0  # Both pending and error counts
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "No pending files" in str(result[0])

  @patch("robosystems.database.session")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_yields_run_request_per_pending_file(self, mock_env, mock_session_factory):
    """Test sensor yields one RunRequest per pending file."""
    mock_env.ENVIRONMENT = "prod"

    # Create mock SourceFile objects
    mock_files = []
    for i in range(3):
      mock_file = MagicMock()
      mock_file.id = f"sf-{i}"
      mock_file.attempts = 0
      mock_file.partition_key = f"2024-Q{i + 1}"
      mock_file.storage_key = f"sec/raw/filed=2024-01-0{i}/0001234567-24-00000{i}.zip"
      mock_files.append(mock_file)

    # Mock session with pending files
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = mock_files
    mock_query.count.side_effect = [3, 0]  # 3 pending, 0 errors
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    # Should yield 3 RunRequests (one per file)
    assert len(result) == 3
    for i, run_request in enumerate(result):
      assert isinstance(run_request, RunRequest)
      assert run_request.run_key == f"sec-sf-{i}-0"
      assert run_request.tags["source_file_id"] == f"sf-{i}"
      assert run_request.tags["partition_key"] == f"2024-Q{i + 1}"

  @patch("robosystems.database.session")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_run_key_includes_attempts_for_retry(self, mock_env, mock_session_factory):
    """Test run_key includes attempts count for retry deduplication."""
    mock_env.ENVIRONMENT = "prod"

    # Create mock SourceFile with attempts > 0 (retry scenario)
    mock_file = MagicMock()
    mock_file.id = "sf-retry"
    mock_file.attempts = 2  # Already tried twice
    mock_file.partition_key = "2024-Q1"
    mock_file.storage_key = "sec/raw/filed=2024-01-01/test.zip"

    # Mock session
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = [mock_file]
    mock_query.count.side_effect = [1, 0]  # 1 pending, 0 errors
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    assert len(result) == 1
    # run_key should include attempts=2 for deduplication
    assert result[0].run_key == "sec-sf-retry-2"

  @patch("robosystems.database.session")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_shows_error_count_when_no_pending(self, mock_env, mock_session_factory):
    """Test sensor shows error count when no pending files but errors exist."""
    mock_env.ENVIRONMENT = "prod"

    # Mock session with no pending but some errors
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = []  # No pending files
    # First count for pending (after .all()), second for total pending, third for errors
    mock_query.count.side_effect = [0, 10]  # 0 pending, 10 errors
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "10 in error state" in str(result[0])

  @patch("robosystems.database.session")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_handles_database_error(self, mock_env, mock_session_factory):
    """Test sensor handles database errors gracefully."""
    mock_env.ENVIRONMENT = "prod"

    # Mock session that raises an error
    mock_session_factory.side_effect = Exception("Database connection failed")

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "Database error" in str(result[0])

  @patch("robosystems.database.session")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_limits_files_per_tick(self, mock_env, mock_session_factory):
    """Test sensor limits files per tick to prevent slow ticks."""
    mock_env.ENVIRONMENT = "prod"

    # Create 150 mock files (more than MAX_FILES_PER_TICK=100)
    mock_files = []
    for i in range(100):  # Only 100 will be returned due to limit
      mock_file = MagicMock()
      mock_file.id = f"sf-{i}"
      mock_file.attempts = 0
      mock_file.partition_key = "2024-Q1"
      mock_file.storage_key = f"sec/raw/filed=2024-01-01/test-{i}.zip"
      mock_files.append(mock_file)

    # Mock session
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.order_by.return_value = mock_query
    mock_query.limit.return_value = mock_query
    mock_query.all.return_value = mock_files  # Returns up to limit
    mock_query.count.side_effect = [150, 0]  # Total pending is 150, 0 errors
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    # Should yield 100 RunRequests (limited per tick)
    assert len(result) == 100
    # Verify limit was applied in query
    mock_query.limit.assert_called()
