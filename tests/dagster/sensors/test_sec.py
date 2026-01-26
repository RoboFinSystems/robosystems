"""Tests for SEC processing sensor.

The sec_processing_sensor discovers quarters with pending SourceFile records
and yields one RunRequest per quarter. Dagster's QueuedRunCoordinator controls
concurrency across quarters via DAGSTER_MAX_CONCURRENT_RUNS.
"""

from unittest.mock import MagicMock, patch

from dagster import RunRequest, SkipReason, build_sensor_context

from robosystems.dagster.sensors.sec import sec_processing_sensor


class TestSecProcessingSensor:
  """Tests for sec_processing_sensor (quarterly batch model)."""

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
    mock_query.all.return_value = []  # No pending files
    mock_query.scalar.return_value = 0  # No errors
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "No quarters with pending files" in str(result[0])

  @patch("robosystems.database.session")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_yields_run_request_per_quarter(self, mock_env, mock_session_factory):
    """Test sensor yields one RunRequest per quarter with pending files."""
    from dagster import DagsterInstance

    mock_env.ENVIRONMENT = "prod"

    # Create mock partition_key results for different quarters
    mock_pending_files = [
      ("2024-Q1_1234567890_0001234567-24-000001",),
      ("2024-Q1_1234567890_0001234567-24-000002",),  # Same quarter
      ("2024-Q2_1234567890_0001234567-24-000003",),  # Different quarter
      ("2024-Q3_1234567890_0001234567-24-000004",),  # Another quarter
    ]

    # Mock session
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = mock_pending_files
    mock_query.scalar.return_value = 0  # No errors
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    # Use ephemeral instance and patch get_runs to return empty list (no active runs)
    with DagsterInstance.ephemeral() as instance:
      with patch.object(instance, "get_runs", return_value=[]):
        context = build_sensor_context(instance=instance)
        result = list(sec_processing_sensor(context))

    # Should yield 3 RunRequests (one per unique quarter: Q1, Q2, Q3)
    assert len(result) == 3
    quarters_found = set()
    for run_request in result:
      assert isinstance(run_request, RunRequest)
      assert run_request.run_key.startswith("sec-quarter-")
      # Extract quarter from run_key
      quarter = run_request.run_key.replace("sec-quarter-", "")
      quarters_found.add(quarter)
      assert run_request.partition_key == quarter
      assert run_request.tags["quarter"] == quarter

    assert quarters_found == {"2024-Q1", "2024-Q2", "2024-Q3"}

  @patch("robosystems.database.session")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_shows_error_count_when_no_pending(self, mock_env, mock_session_factory):
    """Test sensor shows error count when no pending files but errors exist."""
    mock_env.ENVIRONMENT = "prod"

    # Mock session with no pending but some errors
    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = []  # No pending files
    mock_query.scalar.return_value = 10  # 10 errors
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    context = build_sensor_context()
    result = list(sec_processing_sensor(context))

    assert len(result) == 1
    assert isinstance(result[0], SkipReason)
    assert "10 files in error state" in str(result[0])

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
  def test_handles_malformed_partition_keys(self, mock_env, mock_session_factory):
    """Test sensor handles malformed partition keys gracefully."""
    from dagster import DagsterInstance

    mock_env.ENVIRONMENT = "prod"

    # Mix of valid and invalid partition keys
    mock_pending_files = [
      ("2024-Q1_1234567890_0001234567-24-000001",),  # Valid
      ("invalid_key",),  # Invalid - no quarter format
      (None,),  # None value
      ("2024-Q2_1234567890_0001234567-24-000002",),  # Valid
      ("no-quarter-here",),  # Invalid - no -Q in key
    ]

    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = mock_pending_files
    mock_query.scalar.return_value = 0
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    # Use ephemeral instance and patch get_runs to return empty list (no active runs)
    with DagsterInstance.ephemeral() as instance:
      with patch.object(instance, "get_runs", return_value=[]):
        context = build_sensor_context(instance=instance)
        result = list(sec_processing_sensor(context))

    # Should only yield RunRequests for valid quarters (Q1, Q2)
    assert len(result) == 2
    quarters = {r.partition_key for r in result}
    assert quarters == {"2024-Q1", "2024-Q2"}

  @patch("robosystems.database.session")
  @patch("robosystems.dagster.sensors.sec.env")
  def test_deduplicates_quarters(self, mock_env, mock_session_factory):
    """Test sensor deduplicates multiple files from the same quarter."""
    from dagster import DagsterInstance

    mock_env.ENVIRONMENT = "prod"

    # Many files all from the same quarter
    mock_pending_files = [
      (f"2024-Q1_123456789{i}_0001234567-24-00000{i}",) for i in range(100)
    ]

    mock_session = MagicMock()
    mock_query = MagicMock()
    mock_query.filter.return_value = mock_query
    mock_query.all.return_value = mock_pending_files
    mock_query.scalar.return_value = 0
    mock_session.query.return_value = mock_query
    mock_session_factory.return_value = mock_session

    # Use ephemeral instance and patch get_runs to return empty list (no active runs)
    with DagsterInstance.ephemeral() as instance:
      with patch.object(instance, "get_runs", return_value=[]):
        context = build_sensor_context(instance=instance)
        result = list(sec_processing_sensor(context))

    # Should only yield 1 RunRequest (all files are from Q1)
    assert len(result) == 1
    assert result[0].partition_key == "2024-Q1"
