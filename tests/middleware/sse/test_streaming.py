"""Comprehensive unit tests for SSE streaming module."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from robosystems.middleware.sse.event_storage import (
  EventType,
  OperationMetadata,
  OperationStatus,
  SSEEvent,
)
from robosystems.middleware.sse.streaming import (
  SSEConnectionManager,
  create_sse_stream_starlette,
  emit_event_to_operation,
  get_connection_manager,
)


@pytest.mark.unit
class TestSSEConnectionManagerLifecycle:
  """Tests for SSEConnectionManager connection lifecycle."""

  @pytest.fixture(autouse=True)
  def reset_singleton(self):
    """Reset the global connection manager singleton between tests."""
    import robosystems.middleware.sse.streaming as streaming_mod

    streaming_mod._connection_manager = None
    yield
    streaming_mod._connection_manager = None

  @pytest.fixture
  def manager(self):
    """Create a fresh SSEConnectionManager with mocked config."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=5,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      mgr = SSEConnectionManager()
    return mgr

  @pytest.mark.asyncio
  async def test_add_connection_creates_queue(self, manager):
    """Test that adding a connection returns an asyncio.Queue."""
    queue = await manager.add_connection("op1", "conn1", "user1")
    assert isinstance(queue, asyncio.Queue)

  @pytest.mark.asyncio
  async def test_add_connection_tracks_operation(self, manager):
    """Test that adding a connection registers it under the operation."""
    await manager.add_connection("op1", "conn1", "user1")
    assert "conn1" in manager.connections["op1"]

  @pytest.mark.asyncio
  async def test_add_connection_tracks_user(self, manager):
    """Test that adding a connection tracks user connection keys."""
    await manager.add_connection("op1", "conn1", "user1")
    assert "op1:conn1" in manager.user_connections["user1"]

  @pytest.mark.asyncio
  async def test_add_connection_creates_queue_key(self, manager):
    """Test that connection queue is stored with proper key format."""
    await manager.add_connection("op1", "conn1", "user1")
    assert "op1:conn1" in manager.connection_queues

  @pytest.mark.asyncio
  async def test_remove_connection_cleans_up_operation(self, manager):
    """Test that removing the last connection clears the operation entry."""
    await manager.add_connection("op1", "conn1", "user1")
    await manager.remove_connection("op1", "conn1", "user1")
    assert "op1" not in manager.connections

  @pytest.mark.asyncio
  async def test_remove_connection_cleans_up_queue(self, manager):
    """Test that removing a connection removes its queue."""
    await manager.add_connection("op1", "conn1", "user1")
    await manager.remove_connection("op1", "conn1", "user1")
    assert "op1:conn1" not in manager.connection_queues

  @pytest.mark.asyncio
  async def test_remove_connection_cleans_up_user_tracking(self, manager):
    """Test that removing last connection clears user tracking entry."""
    await manager.add_connection("op1", "conn1", "user1")
    await manager.remove_connection("op1", "conn1", "user1")
    assert "user1" not in manager.user_connections

  @pytest.mark.asyncio
  async def test_remove_connection_keeps_other_connections(self, manager):
    """Test that removing one connection does not affect others."""
    await manager.add_connection("op1", "conn1", "user1")
    await manager.add_connection("op1", "conn2", "user1")
    await manager.remove_connection("op1", "conn1", "user1")
    assert "conn2" in manager.connections["op1"]
    assert "op1:conn2" in manager.connection_queues

  @pytest.mark.asyncio
  async def test_remove_connection_nonexistent_does_not_raise(self, manager):
    """Test that removing a non-existent connection is a no-op."""
    await manager.remove_connection("op_nonexistent", "conn_nonexistent", "user1")

  @pytest.mark.asyncio
  async def test_remove_connection_without_user_id(self, manager):
    """Test that remove_connection works when user_id is None."""
    await manager.add_connection("op1", "conn1", "user1")
    await manager.remove_connection("op1", "conn1")
    assert "op1" not in manager.connections


@pytest.mark.unit
class TestSSEConnectionManagerLimits:
  """Tests for per-user connection limits."""

  @pytest.fixture
  def manager_with_low_limit(self):
    """Create a manager with a low connection limit for testing."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=2,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      mgr = SSEConnectionManager()
    return mgr

  @pytest.mark.asyncio
  async def test_connection_limit_enforced(self, manager_with_low_limit):
    """Test that exceeding connection limit raises HTTPException 429."""
    manager = manager_with_low_limit
    await manager.add_connection("op1", "conn1", "user1")
    await manager.add_connection("op2", "conn2", "user1")
    with pytest.raises(HTTPException) as exc_info:
      await manager.add_connection("op3", "conn3", "user1")
    assert exc_info.value.status_code == 429
    assert "Too many concurrent SSE connections" in exc_info.value.detail

  @pytest.mark.asyncio
  async def test_different_users_have_separate_limits(self, manager_with_low_limit):
    """Test that connection limits are per-user."""
    manager = manager_with_low_limit
    await manager.add_connection("op1", "conn1", "user1")
    await manager.add_connection("op2", "conn2", "user1")
    # user2 should not be affected by user1's connections
    queue = await manager.add_connection("op3", "conn3", "user2")
    assert isinstance(queue, asyncio.Queue)


@pytest.mark.unit
class TestSSEConnectionManagerBroadcast:
  """Tests for event broadcasting."""

  @pytest.fixture
  def manager(self):
    """Create a fresh SSEConnectionManager."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      mgr = SSEConnectionManager()
    return mgr

  def _make_event(self, operation_id="op1"):
    return SSEEvent(
      event_type=EventType.OPERATION_PROGRESS,
      operation_id=operation_id,
      timestamp="2025-01-01T00:00:00Z",
      data={"progress": 50},
      sequence_number=1,
    )

  @pytest.mark.asyncio
  async def test_broadcast_sends_to_all_connections(self, manager):
    """Test that broadcast sends event to all connections for an operation."""
    queue1 = await manager.add_connection("op1", "conn1", "user1")
    queue2 = await manager.add_connection("op1", "conn2", "user2")
    event = self._make_event()
    await manager.broadcast_event("op1", event)
    assert not queue1.empty()
    assert not queue2.empty()
    received1 = queue1.get_nowait()
    received2 = queue2.get_nowait()
    assert received1.event_type == EventType.OPERATION_PROGRESS
    assert received2.data["progress"] == 50

  @pytest.mark.asyncio
  async def test_broadcast_no_connections_does_not_raise(self, manager):
    """Test that broadcasting with no connections is a no-op."""
    event = self._make_event("op_nonexistent")
    await manager.broadcast_event("op_nonexistent", event)

  @pytest.mark.asyncio
  async def test_broadcast_queue_overflow_triggers_removal(self, manager):
    """Test that a full queue triggers connection error handling and removal.

    Note: broadcast_event holds self._lock and calls remove_connection which
    also acquires self._lock, causing a deadlock with asyncio.Lock (non-reentrant).
    We mock remove_connection to avoid the deadlock while still verifying the
    overflow detection path.
    """
    # Create a manager with very small queue
    with (
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=1,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      small_mgr = SSEConnectionManager()

    queue = await small_mgr.add_connection("op1", "conn1", "user1")
    event = self._make_event()
    # Fill the queue
    queue.put_nowait(event)

    # Mock remove_connection to avoid deadlock (broadcast_event holds lock,
    # remove_connection tries to re-acquire same non-reentrant asyncio.Lock)
    small_mgr.remove_connection = AsyncMock()
    small_mgr._handle_connection_error = AsyncMock()

    # Broadcast should detect overflow and attempt removal
    await small_mgr.broadcast_event("op1", event)

    # Verify error handling and removal were called
    small_mgr._handle_connection_error.assert_called_once_with(
      "op1", "conn1", "Queue overflow"
    )
    small_mgr.remove_connection.assert_called_once_with("op1", "conn1")


@pytest.mark.unit
class TestSSEConnectionManagerCounts:
  """Tests for connection count queries."""

  @pytest.fixture
  def manager(self):
    with (
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      mgr = SSEConnectionManager()
    return mgr

  @pytest.mark.asyncio
  async def test_get_active_connections(self, manager):
    """Test get_active_connections returns correct count."""
    await manager.add_connection("op1", "conn1", "user1")
    await manager.add_connection("op1", "conn2", "user2")
    count = await manager.get_active_connections("op1")
    assert count == 2

  @pytest.mark.asyncio
  async def test_get_active_connections_empty(self, manager):
    """Test get_active_connections returns 0 for unknown operation."""
    count = await manager.get_active_connections("op_unknown")
    assert count == 0

  @pytest.mark.asyncio
  async def test_get_user_connection_count(self, manager):
    """Test get_user_connection_count returns correct count."""
    await manager.add_connection("op1", "conn1", "user1")
    await manager.add_connection("op2", "conn2", "user1")
    count = await manager.get_user_connection_count("user1")
    assert count == 2

  @pytest.mark.asyncio
  async def test_get_user_connection_count_empty(self, manager):
    """Test get_user_connection_count returns 0 for unknown user."""
    count = await manager.get_user_connection_count("user_unknown")
    assert count == 0


@pytest.mark.unit
class TestSSEConnectionManagerErrorHandling:
  """Tests for _handle_connection_error."""

  @pytest.fixture
  def manager(self):
    with (
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      mgr = SSEConnectionManager()
    return mgr

  @pytest.mark.asyncio
  async def test_handle_connection_error_sends_error_event(self, manager):
    """Test that _handle_connection_error sends an error event to the queue."""
    queue = await manager.add_connection("op1", "conn1", "user1")
    await manager._handle_connection_error("op1", "conn1", "Test error")
    assert not queue.empty()
    error_event = queue.get_nowait()
    assert error_event.event_type == EventType.OPERATION_ERROR
    assert error_event.data["error"] == "Test error"
    assert error_event.data["connection_closed"] is True

  @pytest.mark.asyncio
  async def test_handle_connection_error_nonexistent_queue(self, manager):
    """Test that _handle_connection_error is a no-op for missing queue."""
    # Should not raise
    await manager._handle_connection_error("op_missing", "conn_missing", "Error")


@pytest.mark.unit
class TestGetConnectionManager:
  """Tests for get_connection_manager singleton."""

  @pytest.fixture(autouse=True)
  def reset_singleton(self):
    import robosystems.middleware.sse.streaming as streaming_mod

    streaming_mod._connection_manager = None
    yield
    streaming_mod._connection_manager = None

  def test_get_connection_manager_returns_instance(self):
    """Test that get_connection_manager returns an SSEConnectionManager."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=5,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      mgr = get_connection_manager()
    assert isinstance(mgr, SSEConnectionManager)

  def test_get_connection_manager_returns_same_instance(self):
    """Test that get_connection_manager returns the same singleton."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=5,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      mgr1 = get_connection_manager()
      mgr2 = get_connection_manager()
    assert mgr1 is mgr2


@pytest.mark.unit
class TestCreateSSEStreamStarlette:
  """Tests for create_sse_stream_starlette generator."""

  @pytest.fixture(autouse=True)
  def reset_singleton(self):
    import robosystems.middleware.sse.streaming as streaming_mod

    streaming_mod._connection_manager = None
    yield
    streaming_mod._connection_manager = None

  def _make_metadata(self, user_id="user1", status=OperationStatus.PENDING):
    return OperationMetadata(
      operation_id="op1",
      operation_type="test_op",
      user_id=user_id,
      graph_id="kg01234567890abcdef",
      status=status,
      created_at="2025-01-01T00:00:00Z",
      updated_at="2025-01-01T00:00:00Z",
    )

  @pytest.mark.asyncio
  async def test_stream_operation_not_found(self):
    """Test that stream yields error when operation not found."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.get_event_storage"
      ) as mock_storage_fn,
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      mock_storage = AsyncMock()
      mock_storage.get_operation_metadata.return_value = None
      mock_storage_fn.return_value = mock_storage

      events = []
      async for event in create_sse_stream_starlette("op1", "user1"):
        events.append(event)

      assert len(events) == 1
      assert events[0]["event"] == "error"
      assert "Operation not found" in events[0]["data"]

  @pytest.mark.asyncio
  async def test_stream_access_denied(self):
    """Test that stream yields error for wrong user."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.get_event_storage"
      ) as mock_storage_fn,
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
    ):
      mock_env.SSE_ENABLED = True
      mock_storage = AsyncMock()
      mock_storage.get_operation_metadata.return_value = self._make_metadata(
        user_id="other_user"
      )
      mock_storage_fn.return_value = mock_storage

      events = []
      async for event in create_sse_stream_starlette("op1", "user1"):
        events.append(event)

      assert len(events) == 1
      assert events[0]["event"] == "error"
      assert "Access denied" in events[0]["data"]

  @pytest.mark.asyncio
  async def test_stream_sends_connected_event(self):
    """Test that stream sends initial 'connected' event."""
    metadata = self._make_metadata(status=OperationStatus.COMPLETED)

    with (
      patch(
        "robosystems.middleware.sse.streaming.get_event_storage"
      ) as mock_storage_fn,
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
      patch(
        "robosystems.middleware.sse.streaming.asyncio.sleep", new_callable=AsyncMock
      ),
    ):
      mock_env.SSE_ENABLED = True
      mock_storage = AsyncMock()
      mock_storage.get_operation_metadata.return_value = metadata
      mock_storage.get_events.return_value = []
      mock_storage_fn.return_value = mock_storage

      # Patch the redis subscriber to avoid import issues
      with patch(
        "robosystems.middleware.sse.streaming.get_connection_manager"
      ) as mock_mgr_fn:
        mock_mgr = MagicMock()
        mock_mgr.add_connection = AsyncMock(return_value=asyncio.Queue())
        mock_mgr.remove_connection = AsyncMock()
        mock_mgr_fn.return_value = mock_mgr

        events = []
        async for event in create_sse_stream_starlette("op1", "user1"):
          events.append(event)
          if len(events) > 10:
            break

        # First event should be "connected"
        assert events[0]["event"] == "connected"
        data = json.loads(events[0]["data"])
        assert data["operation_id"] == "op1"

  @pytest.mark.asyncio
  async def test_stream_replays_historical_events(self):
    """Test that stream replays events from a given sequence number."""
    metadata = self._make_metadata(status=OperationStatus.COMPLETED)

    historical_event = SSEEvent(
      event_type=EventType.OPERATION_PROGRESS,
      operation_id="op1",
      timestamp="2025-01-01T00:00:01Z",
      data={"progress": 25},
      sequence_number=1,
    )

    with (
      patch(
        "robosystems.middleware.sse.streaming.get_event_storage"
      ) as mock_storage_fn,
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
      patch(
        "robosystems.middleware.sse.streaming.asyncio.sleep", new_callable=AsyncMock
      ),
    ):
      mock_env.SSE_ENABLED = True
      mock_storage = AsyncMock()
      mock_storage.get_operation_metadata.return_value = metadata
      # First call for replay, second call for completed replay
      mock_storage.get_events.side_effect = [
        [historical_event],  # from_sequence replay
        [],  # completed operation replay
      ]
      mock_storage_fn.return_value = mock_storage

      with patch(
        "robosystems.middleware.sse.streaming.get_connection_manager"
      ) as mock_mgr_fn:
        mock_mgr = MagicMock()
        mock_mgr.add_connection = AsyncMock(return_value=asyncio.Queue())
        mock_mgr.remove_connection = AsyncMock()
        mock_mgr_fn.return_value = mock_mgr

        events = []
        async for event in create_sse_stream_starlette("op1", "user1", from_sequence=1):
          events.append(event)
          if len(events) > 20:
            break

        # Should have connected event, then replayed event
        event_types = [e["event"] for e in events]
        assert "connected" in event_types
        # The replayed event should be present
        progress_events = [
          e for e in events if e["event"] == str(EventType.OPERATION_PROGRESS)
        ]
        assert len(progress_events) >= 1

  @pytest.mark.asyncio
  async def test_stream_completed_operation_sends_stream_end(self):
    """Test that a completed operation sends stream_end event."""
    metadata = self._make_metadata(status=OperationStatus.COMPLETED)

    with (
      patch(
        "robosystems.middleware.sse.streaming.get_event_storage"
      ) as mock_storage_fn,
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
      patch(
        "robosystems.middleware.sse.streaming.asyncio.sleep", new_callable=AsyncMock
      ),
    ):
      mock_env.SSE_ENABLED = True
      mock_storage = AsyncMock()
      mock_storage.get_operation_metadata.return_value = metadata
      mock_storage.get_events.return_value = []
      mock_storage_fn.return_value = mock_storage

      with patch(
        "robosystems.middleware.sse.streaming.get_connection_manager"
      ) as mock_mgr_fn:
        mock_mgr = MagicMock()
        mock_mgr.add_connection = AsyncMock(return_value=asyncio.Queue())
        mock_mgr.remove_connection = AsyncMock()
        mock_mgr_fn.return_value = mock_mgr

        events = []
        async for event in create_sse_stream_starlette("op1", "user1"):
          events.append(event)
          if len(events) > 20:
            break

        event_types = [e["event"] for e in events]
        assert "stream_end" in event_types

  @pytest.mark.asyncio
  async def test_stream_connection_limit_exceeded(self):
    """Test that connection limit produces error event."""
    metadata = self._make_metadata(status=OperationStatus.RUNNING)

    with (
      patch(
        "robosystems.middleware.sse.streaming.get_event_storage"
      ) as mock_storage_fn,
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_max_connections_per_user",
        return_value=10,
      ),
      patch(
        "robosystems.middleware.sse.streaming.TuningConfig.get_sse_queue_size",
        return_value=100,
      ),
      patch("robosystems.middleware.sse.streaming.env") as mock_env,
      patch(
        "robosystems.middleware.sse.streaming.asyncio.sleep", new_callable=AsyncMock
      ),
    ):
      mock_env.SSE_ENABLED = True
      mock_storage = AsyncMock()
      mock_storage.get_operation_metadata.return_value = metadata
      mock_storage.get_events.return_value = []
      mock_storage_fn.return_value = mock_storage

      with patch(
        "robosystems.middleware.sse.streaming.get_connection_manager"
      ) as mock_mgr_fn:
        mock_mgr = MagicMock()
        mock_mgr.add_connection = AsyncMock(
          side_effect=HTTPException(status_code=429, detail="Too many")
        )
        mock_mgr.remove_connection = AsyncMock()
        mock_mgr_fn.return_value = mock_mgr

        events = []
        async for event in create_sse_stream_starlette("op1", "user1"):
          events.append(event)
          if len(events) > 10:
            break

        # Should have connected + error event
        error_events = [e for e in events if e["event"] == "error"]
        assert len(error_events) >= 1
        assert "Too many" in error_events[0]["data"]


@pytest.mark.unit
class TestEmitEventToOperation:
  """Tests for emit_event_to_operation broadcaster."""

  @pytest.fixture(autouse=True)
  def reset_singleton(self):
    import robosystems.middleware.sse.streaming as streaming_mod

    streaming_mod._connection_manager = None
    yield
    streaming_mod._connection_manager = None

  @pytest.mark.asyncio
  async def test_emit_event_broadcasts_and_returns_true(self):
    """Test that emit_event_to_operation broadcasts and returns True when connections exist."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.get_connection_manager"
      ) as mock_mgr_fn,
    ):
      mock_mgr = MagicMock()
      mock_mgr.broadcast_event = AsyncMock()
      mock_mgr.get_active_connections = AsyncMock(return_value=2)
      mock_mgr_fn.return_value = mock_mgr

      result = await emit_event_to_operation(
        "op1",
        EventType.OPERATION_PROGRESS,
        {"progress": 75},
        sequence_number=5,
      )

      assert result is True
      mock_mgr.broadcast_event.assert_called_once()
      event_arg = mock_mgr.broadcast_event.call_args[0][1]
      assert event_arg.event_type == EventType.OPERATION_PROGRESS
      assert event_arg.data["progress"] == 75
      assert event_arg.sequence_number == 5

  @pytest.mark.asyncio
  async def test_emit_event_returns_false_when_no_connections(self):
    """Test that emit_event_to_operation returns False when no connections."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.get_connection_manager"
      ) as mock_mgr_fn,
    ):
      mock_mgr = MagicMock()
      mock_mgr.broadcast_event = AsyncMock()
      mock_mgr.get_active_connections = AsyncMock(return_value=0)
      mock_mgr_fn.return_value = mock_mgr

      result = await emit_event_to_operation(
        "op1",
        EventType.OPERATION_COMPLETED,
        {"result": "done"},
      )

      assert result is False

  @pytest.mark.asyncio
  async def test_emit_event_creates_sse_event_with_timestamp(self):
    """Test that emit_event creates SSEEvent with a proper timestamp."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.get_connection_manager"
      ) as mock_mgr_fn,
    ):
      mock_mgr = MagicMock()
      mock_mgr.broadcast_event = AsyncMock()
      mock_mgr.get_active_connections = AsyncMock(return_value=1)
      mock_mgr_fn.return_value = mock_mgr

      await emit_event_to_operation(
        "op1",
        EventType.OPERATION_STARTED,
        {"message": "starting"},
      )

      event_arg = mock_mgr.broadcast_event.call_args[0][1]
      assert event_arg.timestamp  # Should be a non-empty ISO string
      assert event_arg.operation_id == "op1"

  @pytest.mark.asyncio
  async def test_emit_event_default_sequence_number(self):
    """Test that sequence_number defaults to 0 when not provided."""
    with (
      patch(
        "robosystems.middleware.sse.streaming.get_connection_manager"
      ) as mock_mgr_fn,
    ):
      mock_mgr = MagicMock()
      mock_mgr.broadcast_event = AsyncMock()
      mock_mgr.get_active_connections = AsyncMock(return_value=1)
      mock_mgr_fn.return_value = mock_mgr

      await emit_event_to_operation(
        "op1",
        EventType.OPERATION_STARTED,
        {"message": "test"},
      )

      event_arg = mock_mgr.broadcast_event.call_args[0][1]
      assert event_arg.sequence_number == 0


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sse_enabled_is_a_real_kill_switch():
  """SSE_ENABLED was resolved from SSM, assigned, and never read.

  A kill switch that reads as live and does nothing is worse than no switch:
  the operator who flips it during an incident believes they have acted.
  """
  manager = SSEConnectionManager()
  manager.sse_enabled = False
  with pytest.raises(HTTPException) as exc:
    await manager.add_connection("op1", "conn1", "usr1")
  assert exc.value.status_code == 503

  manager.sse_enabled = True
  queue = await manager.add_connection("op1", "conn1", "usr1")
  assert queue is not None
