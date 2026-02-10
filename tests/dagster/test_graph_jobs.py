"""Tests for Dagster graph operation jobs.

Tests the wait-for-capacity graph creation op and its helpers.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.dagster.jobs.graph import (
  WaitAndCreateGraphConfig,
  _emit_completion_sync,
  _emit_failure_sync,
  _emit_progress_sync,
  _wait_and_create,
)


class TestSyncSSEHelpers:
  """Test sync SSE emission helpers."""

  def test_emit_progress_sync(self):
    """Test progress emission from sync context."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      mock_instance = MagicMock()
      MockStorage.return_value = mock_instance

      _emit_progress_sync("op123", "Creating graph...", 50.0)

      mock_instance.store_event_sync.assert_called_once()
      call_args = mock_instance.store_event_sync.call_args
      assert call_args.args[0] == "op123"
      assert call_args.args[2]["message"] == "Creating graph..."
      assert call_args.args[2]["progress_percent"] == 50.0

  def test_emit_failure_sync(self):
    """Test failure emission from sync context."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      mock_instance = MagicMock()
      MockStorage.return_value = mock_instance

      _emit_failure_sync("op123", "Timeout exceeded")

      mock_instance.store_event_sync.assert_called_once()
      call_args = mock_instance.store_event_sync.call_args
      assert call_args.args[0] == "op123"
      assert "Timeout exceeded" in call_args.args[2]["error"]

  def test_emit_completion_sync(self):
    """Test completion emission from sync context."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      mock_instance = MagicMock()
      MockStorage.return_value = mock_instance

      _emit_completion_sync("op123", {"graph_id": "kg123"})

      mock_instance.store_event_sync.assert_called_once()
      call_args = mock_instance.store_event_sync.call_args
      assert call_args.args[0] == "op123"
      assert call_args.args[2]["result"]["graph_id"] == "kg123"

  def test_emit_progress_handles_errors(self):
    """Test that SSE emission errors are swallowed."""
    with patch(
      "robosystems.middleware.sse.event_storage.SSEEventStorage"
    ) as MockStorage:
      MockStorage.return_value.store_event_sync.side_effect = Exception("Redis down")

      # Should not raise
      _emit_progress_sync("op123", "test", 50)


class TestWaitAndCreateGraphConfig:
  """Test WaitAndCreateGraphConfig defaults."""

  def test_default_values(self):
    """Test that config has sensible defaults."""
    config = WaitAndCreateGraphConfig(
      operation_id="op123",
      user_id="user456",
      graph_name="Test Graph",
    )
    assert config.tier == "ladybug-standard"
    assert config.schema_extensions == ""
    assert config.description == ""
    assert config.tags == ""
    assert config.custom_schema_json == ""
    assert config.poll_interval_seconds == 30
    assert config.max_wait_seconds == 600


class TestWaitAndCreate:
  """Test _wait_and_create async logic."""

  @pytest.mark.asyncio
  async def test_finds_capacity_and_creates_graph(self):
    """Test happy path: capacity found on first poll, graph created."""
    mock_instance = MagicMock()
    mock_instance.instance_id = "i-abc123"
    mock_instance.available_capacity = 5

    mock_result = {"graph_id": "kg123abc", "status": "created"}

    mock_context = MagicMock()
    mock_context.log = MagicMock()
    mock_context.log_event = MagicMock()

    config = WaitAndCreateGraphConfig(
      operation_id="op123",
      user_id="user456",
      graph_name="Test Graph",
      tier="ladybug-standard",
      schema_extensions="roboledger",
      poll_interval_seconds=1,
      max_wait_seconds=60,
    )

    with patch("robosystems.dagster.jobs.graph._emit_progress_sync") as mock_progress:
      with patch(
        "robosystems.dagster.jobs.graph._emit_graph_result_to_sse"
      ) as mock_result_sse:
        with patch(
          "robosystems.dagster.jobs.graph._emit_completion_sync"
        ) as mock_completion:
          with patch(
            "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
          ) as MockManager:
            mock_mgr = MagicMock()
            mock_mgr._find_best_instance = AsyncMock(return_value=mock_instance)
            MockManager.return_value = mock_mgr

            with patch(
              "robosystems.operations.graph.generic_graph_service.GenericGraphService"
            ) as MockService:
              mock_service = AsyncMock()
              mock_service.create_graph.return_value = mock_result
              MockService.return_value = mock_service

              with patch("robosystems.database.session") as mock_session:
                mock_session.return_value = MagicMock()
                mock_session.remove = MagicMock()

                with patch(
                  "robosystems.operations.graph.subscription_service.GraphSubscriptionService"
                ) as MockSubService:
                  MockSubService.return_value = MagicMock()

                  result = await _wait_and_create(mock_context, config)

            assert result["graph_id"] == "kg123abc"
            mock_progress.assert_called()
            mock_result_sse.assert_called_once()
            mock_completion.assert_called_once()
            mock_context.log_event.assert_called_once()  # AssetMaterialization

  @pytest.mark.asyncio
  async def test_polls_until_capacity_found(self):
    """Test that it polls multiple times before finding capacity."""
    mock_instance = MagicMock()
    mock_instance.instance_id = "i-abc123"
    mock_instance.available_capacity = 5

    mock_result = {"graph_id": "kg456def", "status": "created"}

    mock_context = MagicMock()
    mock_context.log = MagicMock()
    mock_context.log_event = MagicMock()

    config = WaitAndCreateGraphConfig(
      operation_id="op123",
      user_id="user456",
      graph_name="Test Graph",
      tier="ladybug-standard",
      poll_interval_seconds=0,  # No delay in tests
      max_wait_seconds=60,
    )

    # Return None twice, then an instance
    poll_results = [None, None, mock_instance]

    with patch("robosystems.dagster.jobs.graph._emit_progress_sync"):
      with patch("robosystems.dagster.jobs.graph._emit_graph_result_to_sse"):
        with patch("robosystems.dagster.jobs.graph._emit_completion_sync"):
          with patch(
            "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
          ) as MockManager:
            mock_mgr = MagicMock()
            mock_mgr._find_best_instance = AsyncMock(side_effect=poll_results)
            MockManager.return_value = mock_mgr

            with patch(
              "robosystems.operations.graph.generic_graph_service.GenericGraphService"
            ) as MockService:
              mock_service = AsyncMock()
              mock_service.create_graph.return_value = mock_result
              MockService.return_value = mock_service

              with patch("robosystems.database.session") as mock_session:
                mock_session.return_value = MagicMock()
                mock_session.remove = MagicMock()

                with patch(
                  "robosystems.operations.graph.subscription_service.GraphSubscriptionService"
                ) as MockSubService:
                  MockSubService.return_value = MagicMock()

                  result = await _wait_and_create(mock_context, config)

            assert result["graph_id"] == "kg456def"
            # Should have polled 3 times
            assert mock_mgr._find_best_instance.call_count == 3

  @pytest.mark.asyncio
  async def test_retries_on_race_condition(self):
    """Test that creation failure (race loss) loops back to polling."""
    mock_instance = MagicMock()
    mock_instance.instance_id = "i-abc123"

    mock_result = {"graph_id": "kg789ghi", "status": "created"}

    mock_context = MagicMock()
    mock_context.log = MagicMock()
    mock_context.log_event = MagicMock()

    config = WaitAndCreateGraphConfig(
      operation_id="op123",
      user_id="user456",
      graph_name="Test Graph",
      tier="ladybug-standard",
      poll_interval_seconds=0,
      max_wait_seconds=60,
    )

    # Instance found on every poll, but first create_graph fails (race loss)
    with patch("robosystems.dagster.jobs.graph._emit_progress_sync"):
      with patch("robosystems.dagster.jobs.graph._emit_graph_result_to_sse"):
        with patch("robosystems.dagster.jobs.graph._emit_completion_sync"):
          with patch(
            "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
          ) as MockManager:
            mock_mgr = MagicMock()
            mock_mgr._find_best_instance = AsyncMock(return_value=mock_instance)
            MockManager.return_value = mock_mgr

            with patch(
              "robosystems.operations.graph.generic_graph_service.GenericGraphService"
            ) as MockService:
              mock_service = AsyncMock()
              # First attempt fails (race), second succeeds
              mock_service.create_graph.side_effect = [
                Exception("No capacity available"),
                mock_result,
              ]
              MockService.return_value = mock_service

              with patch("robosystems.database.session") as mock_session:
                mock_session.return_value = MagicMock()
                mock_session.remove = MagicMock()

                with patch(
                  "robosystems.operations.graph.subscription_service.GraphSubscriptionService"
                ) as MockSubService:
                  MockSubService.return_value = MagicMock()

                  result = await _wait_and_create(mock_context, config)

            assert result["graph_id"] == "kg789ghi"
            # Polled twice (once per loop iteration that found instance)
            assert mock_mgr._find_best_instance.call_count == 2
            # Created twice (first failed, second succeeded)
            assert mock_service.create_graph.call_count == 2

  @pytest.mark.asyncio
  async def test_timeout_emits_failure(self):
    """Test that exceeding max_wait_seconds emits failure and raises."""
    from dagster import Failure

    mock_context = MagicMock()
    mock_context.log = MagicMock()

    config = WaitAndCreateGraphConfig(
      operation_id="op123",
      user_id="user456",
      graph_name="Test Graph",
      tier="ladybug-standard",
      poll_interval_seconds=0,
      max_wait_seconds=0,  # Immediate timeout
    )

    with patch("robosystems.dagster.jobs.graph._emit_progress_sync"):
      with patch("robosystems.dagster.jobs.graph._emit_failure_sync") as mock_failure:
        with patch(
          "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
        ) as MockManager:
          mock_mgr = MagicMock()
          mock_mgr._find_best_instance = AsyncMock(return_value=None)
          MockManager.return_value = mock_mgr

          with pytest.raises(Failure, match="Timed out"):
            await _wait_and_create(mock_context, config)

          mock_failure.assert_called_once()
          assert "op123" in mock_failure.call_args.args[0]

  @pytest.mark.asyncio
  async def test_unexpected_error_emits_sse_failure(self):
    """Test that any unhandled error emits SSE failure (no dangling ops)."""
    from dagster import Failure

    mock_context = MagicMock()
    mock_context.log = MagicMock()

    config = WaitAndCreateGraphConfig(
      operation_id="op123",
      user_id="user456",
      graph_name="Test Graph",
      tier="ladybug-standard",
      poll_interval_seconds=0,
      max_wait_seconds=60,
    )

    with patch("robosystems.dagster.jobs.graph._emit_progress_sync"):
      with patch("robosystems.dagster.jobs.graph._emit_failure_sync") as mock_failure:
        with patch(
          "robosystems.middleware.graph.allocation_manager.LadybugAllocationManager"
        ) as MockManager:
          # Simulate unexpected error during polling
          MockManager.side_effect = RuntimeError("DynamoDB connection lost")

          with pytest.raises(Failure, match="Graph creation failed unexpectedly"):
            await _wait_and_create(mock_context, config)

          # Must have emitted SSE failure so user isn't left dangling
          mock_failure.assert_called_once()
          assert "DynamoDB connection lost" in mock_failure.call_args.args[1]
