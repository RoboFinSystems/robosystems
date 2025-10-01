"""Tests for SSE event storage module."""

import json
import pytest
from unittest.mock import Mock, patch, AsyncMock

from robosystems.middleware.sse.event_storage import (
  EventType,
  OperationStatus,
  SSEEvent,
  OperationMetadata,
  SSEEventStorage,
  get_event_storage,
)


class TestEventType:
  """Test EventType enum."""

  def test_event_type_values(self):
    """Test that all event types have expected values."""
    assert EventType.OPERATION_STARTED == "operation_started"
    assert EventType.OPERATION_PROGRESS == "operation_progress"
    assert EventType.OPERATION_ERROR == "operation_error"
    assert EventType.OPERATION_COMPLETED == "operation_completed"
    assert EventType.OPERATION_CANCELLED == "operation_cancelled"

    # Custom event types
    assert EventType.GRAPH_CREATION_PROGRESS == "graph_creation_progress"
    assert EventType.AGENT_ANALYSIS_PROGRESS == "agent_analysis_progress"
    assert EventType.BACKUP_PROGRESS == "backup_progress"
    assert EventType.SYNC_PROGRESS == "sync_progress"

  def test_event_type_string_inheritance(self):
    """Test that EventType values are strings."""
    for event_type in EventType:
      assert isinstance(event_type.value, str)


class TestOperationStatus:
  """Test OperationStatus enum."""

  def test_operation_status_values(self):
    """Test that all status values are correct."""
    assert OperationStatus.PENDING == "pending"
    assert OperationStatus.RUNNING == "running"
    assert OperationStatus.COMPLETED == "completed"
    assert OperationStatus.FAILED == "failed"
    assert OperationStatus.CANCELLED == "cancelled"

  def test_operation_status_string_inheritance(self):
    """Test that OperationStatus values are strings."""
    for status in OperationStatus:
      assert isinstance(status.value, str)


class TestSSEEvent:
  """Test SSEEvent dataclass."""

  def test_sse_event_creation(self):
    """Test creating an SSE event."""
    data = {"message": "Test progress", "percentage": 50}
    event = SSEEvent(
      event_type=EventType.OPERATION_PROGRESS,
      operation_id="op123",
      timestamp="2023-01-01T12:00:00Z",
      data=data,
      sequence_number=1,
    )

    assert event.event_type == EventType.OPERATION_PROGRESS
    assert event.operation_id == "op123"
    assert event.timestamp == "2023-01-01T12:00:00Z"
    assert event.data == data
    assert event.sequence_number == 1

  def test_sse_event_default_sequence(self):
    """Test that sequence number defaults to 0."""
    event = SSEEvent(
      event_type=EventType.OPERATION_STARTED,
      operation_id="op123",
      timestamp="2023-01-01T12:00:00Z",
      data={},
    )

    assert event.sequence_number == 0

  def test_to_sse_format(self):
    """Test converting event to SSE format."""
    data = {"message": "Test message", "step": 1}
    event = SSEEvent(
      event_type=EventType.OPERATION_PROGRESS,
      operation_id="op123",
      timestamp="2023-01-01T12:00:00Z",
      data=data,
      sequence_number=5,
    )

    sse_format = event.to_sse_format()

    assert "event: operation_progress" in sse_format
    assert "data: " in sse_format
    assert '"operation_id":"op123"' in sse_format
    assert '"timestamp":"2023-01-01T12:00:00Z"' in sse_format
    assert '"sequence_number":5' in sse_format
    assert '"message":"Test message"' in sse_format
    assert '"step":1' in sse_format
    assert sse_format.endswith("\n\n")

  def test_to_sse_format_json_formatting(self):
    """Test that SSE format produces valid JSON on single line."""
    data = {"complex": {"nested": "data"}, "list": [1, 2, 3]}
    event = SSEEvent(
      event_type=EventType.OPERATION_COMPLETED,
      operation_id="op456",
      timestamp="2023-01-01T12:00:00Z",
      data=data,
      sequence_number=10,
    )

    sse_format = event.to_sse_format()

    # Extract the data line
    lines = sse_format.split("\n")
    data_line = None
    for line in lines:
      if line.startswith("data: "):
        data_line = line[6:]  # Remove "data: " prefix
        break

    assert data_line is not None
    # Should be valid JSON
    parsed = json.loads(data_line)
    assert parsed["operation_id"] == "op456"
    assert parsed["complex"]["nested"] == "data"
    assert parsed["list"] == [1, 2, 3]

  def test_to_dict(self):
    """Test converting event to dictionary."""
    data = {"key": "value"}
    event = SSEEvent(
      event_type=EventType.OPERATION_ERROR,
      operation_id="op789",
      timestamp="2023-01-01T12:00:00Z",
      data=data,
      sequence_number=2,
    )

    event_dict = event.to_dict()

    assert event_dict["event_type"] == EventType.OPERATION_ERROR
    assert event_dict["operation_id"] == "op789"
    assert event_dict["timestamp"] == "2023-01-01T12:00:00Z"
    assert event_dict["data"] == data
    assert event_dict["sequence_number"] == 2

  def test_from_dict(self):
    """Test creating event from dictionary."""
    event_data = {
      "event_type": EventType.OPERATION_STARTED,
      "operation_id": "op999",
      "timestamp": "2023-01-01T12:00:00Z",
      "data": {"initial": True},
      "sequence_number": 0,
    }

    event = SSEEvent.from_dict(event_data)

    assert event.event_type == EventType.OPERATION_STARTED
    assert event.operation_id == "op999"
    assert event.timestamp == "2023-01-01T12:00:00Z"
    assert event.data == {"initial": True}
    assert event.sequence_number == 0

  def test_serialization_roundtrip(self):
    """Test that serialization is reversible."""
    original_data = {"complex": {"data": [1, 2, 3]}, "unicode": "B5AB"}
    original_event = SSEEvent(
      event_type=EventType.SYNC_PROGRESS,
      operation_id="op_roundtrip",
      timestamp="2023-01-01T12:00:00Z",
      data=original_data,
      sequence_number=42,
    )

    # Convert to dict and back
    event_dict = original_event.to_dict()
    restored_event = SSEEvent.from_dict(event_dict)

    assert restored_event.event_type == original_event.event_type
    assert restored_event.operation_id == original_event.operation_id
    assert restored_event.timestamp == original_event.timestamp
    assert restored_event.data == original_event.data
    assert restored_event.sequence_number == original_event.sequence_number


class TestOperationMetadata:
  """Test OperationMetadata dataclass."""

  def test_operation_metadata_creation(self):
    """Test creating operation metadata."""
    metadata = OperationMetadata(
      operation_id="op123",
      operation_type="graph_creation",
      user_id="user456",
      graph_id="kg789",
      status=OperationStatus.RUNNING,
      created_at="2023-01-01T12:00:00Z",
      updated_at="2023-01-01T12:30:00Z",
      error_message="Test error",
      result_data={"result": "success"},
    )

    assert metadata.operation_id == "op123"
    assert metadata.operation_type == "graph_creation"
    assert metadata.user_id == "user456"
    assert metadata.graph_id == "kg789"
    assert metadata.status == OperationStatus.RUNNING
    assert metadata.created_at == "2023-01-01T12:00:00Z"
    assert metadata.updated_at == "2023-01-01T12:30:00Z"
    assert metadata.error_message == "Test error"
    assert metadata.result_data == {"result": "success"}

  def test_operation_metadata_optional_fields(self):
    """Test metadata with optional fields as None."""
    metadata = OperationMetadata(
      operation_id="op123",
      operation_type="sync",
      user_id="user456",
      graph_id=None,
      status=OperationStatus.PENDING,
      created_at="2023-01-01T12:00:00Z",
      updated_at="2023-01-01T12:00:00Z",
    )

    assert metadata.graph_id is None
    assert metadata.error_message is None
    assert metadata.result_data is None

  def test_operation_metadata_to_dict(self):
    """Test converting metadata to dictionary."""
    metadata = OperationMetadata(
      operation_id="op123",
      operation_type="backup",
      user_id="user456",
      graph_id="kg789",
      status=OperationStatus.COMPLETED,
      created_at="2023-01-01T12:00:00Z",
      updated_at="2023-01-01T13:00:00Z",
      result_data={"backup_size": "100MB"},
    )

    metadata_dict = metadata.to_dict()

    assert metadata_dict["operation_id"] == "op123"
    assert metadata_dict["operation_type"] == "backup"
    assert metadata_dict["user_id"] == "user456"
    assert metadata_dict["graph_id"] == "kg789"
    assert metadata_dict["status"] == OperationStatus.COMPLETED
    assert metadata_dict["created_at"] == "2023-01-01T12:00:00Z"
    assert metadata_dict["updated_at"] == "2023-01-01T13:00:00Z"
    assert metadata_dict["error_message"] is None
    assert metadata_dict["result_data"] == {"backup_size": "100MB"}


class TestSSEEventStorage:
  """Test SSEEventStorage class."""

  def test_initialization_defaults(self):
    """Test storage initialization with defaults."""
    storage = SSEEventStorage()

    assert storage._redis_client is None
    assert storage._async_redis is None
    assert storage._sync_redis is None
    assert storage.default_ttl == 3600
    assert storage.event_prefix == "sse:operation:events:"
    assert storage.metadata_prefix == "sse:operation:meta:"
    assert storage.sequence_prefix == "sse:operation:seq:"

  def test_initialization_custom(self):
    """Test storage initialization with custom values."""
    mock_redis = Mock()
    storage = SSEEventStorage(redis_client=mock_redis, default_ttl=7200)

    assert storage._redis_client == mock_redis
    assert storage.default_ttl == 7200

  def test_generate_operation_id(self):
    """Test operation ID generation."""
    storage = SSEEventStorage()

    op_id1 = storage.generate_operation_id()
    op_id2 = storage.generate_operation_id()

    assert isinstance(op_id1, str)
    assert isinstance(op_id2, str)
    assert op_id1 != op_id2
    assert len(op_id1) > 20  # UUIDs are longer than 20 chars

  @patch("robosystems.config.valkey_registry.create_async_redis_client")
  async def test_get_default_async_redis(self, mock_create_client):
    """Test getting default async Redis client."""
    mock_client = AsyncMock()
    mock_create_client.return_value = mock_client

    storage = SSEEventStorage()
    client = await storage._get_default_async_redis()

    assert client == mock_client
    mock_client.ping.assert_called_once()
    # Verify it was called with the correct database
    from robosystems.config.valkey_registry import ValkeyDatabase

    mock_create_client.assert_called_once_with(ValkeyDatabase.SSE_EVENTS)

  @patch("robosystems.config.valkey_registry.create_redis_client")
  def test_get_sync_redis(self, mock_create_client):
    """Test getting sync Redis client."""
    mock_client = Mock()
    mock_create_client.return_value = mock_client

    storage = SSEEventStorage()
    client = storage._get_sync_redis()

    assert client == mock_client
    mock_client.ping.assert_called_once()
    # Verify it was called with the correct database
    from robosystems.config.valkey_registry import ValkeyDatabase

    mock_create_client.assert_called_once_with(ValkeyDatabase.SSE_EVENTS)

  async def test_create_operation(self):
    """Test creating a new operation."""
    mock_redis = AsyncMock()
    storage = SSEEventStorage(redis_client=mock_redis)

    with patch("robosystems.middleware.sse.event_storage.datetime") as mock_datetime:
      mock_now = Mock()
      mock_now.isoformat.return_value = "2023-01-01T12:00:00Z"
      mock_datetime.now.return_value = mock_now

      operation_id = await storage.create_operation(
        operation_type="test_operation", user_id="user123", graph_id="kg456", ttl=7200
      )

      assert isinstance(operation_id, str)

      # Verify metadata was stored
      mock_redis.setex.assert_any_call(
        f"sse:operation:meta:{operation_id}",
        7200,
        json.dumps(
          {
            "operation_id": operation_id,
            "operation_type": "test_operation",
            "user_id": "user123",
            "graph_id": "kg456",
            "status": "pending",
            "created_at": "2023-01-01T12:00:00Z",
            "updated_at": "2023-01-01T12:00:00Z",
            "error_message": None,
            "result_data": None,
          }
        ),
      )

      # Verify sequence counter was initialized
      mock_redis.setex.assert_any_call(f"sse:operation:seq:{operation_id}", 7200, "0")

  async def test_create_operation_custom_id(self):
    """Test creating operation with custom ID."""
    mock_redis = AsyncMock()
    storage = SSEEventStorage(redis_client=mock_redis)

    with patch("robosystems.middleware.sse.event_storage.datetime") as mock_datetime:
      mock_now = Mock()
      mock_now.isoformat.return_value = "2023-01-01T12:00:00Z"
      mock_datetime.now.return_value = mock_now

      operation_id = await storage.create_operation(
        operation_type="custom_op", user_id="user789", operation_id="custom_op_123"
      )

      assert operation_id == "custom_op_123"

  async def test_store_event(self):
    """Test storing an event."""
    mock_redis = AsyncMock()
    mock_redis.exists.return_value = True
    mock_redis.incr.return_value = 5
    mock_redis.get.return_value = None  # No existing metadata

    storage = SSEEventStorage(redis_client=mock_redis)

    with patch("robosystems.middleware.sse.event_storage.datetime") as mock_datetime:
      mock_now = Mock()
      mock_now.isoformat.return_value = "2023-01-01T12:30:00Z"
      mock_datetime.now.return_value = mock_now

      event_data = {"message": "Progress update", "percentage": 75}
      event = await storage.store_event(
        operation_id="op123",
        event_type=EventType.OPERATION_PROGRESS,
        data=event_data,
        ttl=3600,
      )

      assert event.event_type == EventType.OPERATION_PROGRESS
      assert event.operation_id == "op123"
      assert event.timestamp == "2023-01-01T12:30:00Z"
      assert event.data == event_data
      assert event.sequence_number == 5

      # Verify Redis operations
      mock_redis.exists.assert_called_with("sse:operation:meta:op123")
      mock_redis.incr.assert_called_with("sse:operation:seq:op123")
      mock_redis.zadd.assert_called_once()
      mock_redis.expire.assert_called_with("sse:operation:events:op123", 3600)
      mock_redis.publish.assert_called_once()

  async def test_store_event_operation_not_found(self):
    """Test storing event for non-existent operation."""
    mock_redis = AsyncMock()
    mock_redis.exists.return_value = False

    storage = SSEEventStorage(redis_client=mock_redis)

    with pytest.raises(ValueError, match="Operation op123 not found"):
      await storage.store_event(
        operation_id="op123",
        event_type=EventType.OPERATION_PROGRESS,
        data={"test": "data"},
      )

  def test_store_event_sync(self):
    """Test synchronous event storage."""
    mock_redis = Mock()
    mock_redis.exists.return_value = True
    mock_redis.incr.return_value = 3
    mock_redis.get.return_value = None  # No existing metadata

    storage = SSEEventStorage()
    storage._sync_redis = mock_redis

    with patch("robosystems.middleware.sse.event_storage.datetime") as mock_datetime:
      mock_now = Mock()
      mock_now.isoformat.return_value = "2023-01-01T12:45:00Z"
      mock_datetime.now.return_value = mock_now

      event_data = {"error": "Something went wrong"}
      event = storage.store_event_sync(
        operation_id="op456", event_type=EventType.OPERATION_ERROR, data=event_data
      )

      assert event.event_type == EventType.OPERATION_ERROR
      assert event.operation_id == "op456"
      assert event.timestamp == "2023-01-01T12:45:00Z"
      assert event.data == event_data
      assert event.sequence_number == 3

      mock_redis.incr.assert_called_with("sse:operation:seq:op456")
      mock_redis.zadd.assert_called_once()
      mock_redis.expire.assert_called_once()
      mock_redis.publish.assert_called_once()

  def test_store_event_sync_operation_not_found(self):
    """Test sync event storage with missing operation (warns but continues)."""
    mock_redis = Mock()
    mock_redis.exists.return_value = False
    mock_redis.incr.return_value = 1
    mock_redis.get.return_value = None  # No existing metadata

    storage = SSEEventStorage()
    storage._sync_redis = mock_redis

    with patch("robosystems.middleware.sse.event_storage.logger") as mock_logger:
      with patch("robosystems.middleware.sse.event_storage.datetime") as mock_datetime:
        mock_now = Mock()
        mock_now.isoformat.return_value = "2023-01-01T12:45:00Z"
        mock_datetime.now.return_value = mock_now

        event = storage.store_event_sync(
          operation_id="missing_op",
          event_type=EventType.OPERATION_STARTED,
          data={"test": "data"},
        )

        assert event.operation_id == "missing_op"
        mock_logger.warning.assert_called_with(
          "Operation missing_op not found in metadata, continuing anyway"
        )

  async def test_get_events(self):
    """Test retrieving events for an operation."""
    mock_redis = AsyncMock()

    # Mock events data
    event1_data = {
      "event_type": EventType.OPERATION_STARTED,
      "operation_id": "op123",
      "timestamp": "2023-01-01T12:00:00Z",
      "data": {"message": "Started"},
      "sequence_number": 1,
    }
    event2_data = {
      "event_type": EventType.OPERATION_PROGRESS,
      "operation_id": "op123",
      "timestamp": "2023-01-01T12:15:00Z",
      "data": {"percentage": 50},
      "sequence_number": 2,
    }

    mock_redis.zrangebyscore.return_value = [
      json.dumps(event1_data),
      json.dumps(event2_data),
    ]

    storage = SSEEventStorage(redis_client=mock_redis)

    events = await storage.get_events("op123", from_sequence=1, limit=10)

    assert len(events) == 2
    assert events[0].event_type == EventType.OPERATION_STARTED
    assert events[0].sequence_number == 1
    assert events[1].event_type == EventType.OPERATION_PROGRESS
    assert events[1].sequence_number == 2

    mock_redis.zrangebyscore.assert_called_with(
      "sse:operation:events:op123", 1, "+inf", start=0, num=10
    )

  async def test_get_events_no_limit(self):
    """Test retrieving events without limit."""
    mock_redis = AsyncMock()
    mock_redis.zrangebyscore.return_value = []

    storage = SSEEventStorage(redis_client=mock_redis)

    await storage.get_events("op123", from_sequence=0)

    mock_redis.zrangebyscore.assert_called_with("sse:operation:events:op123", 0, "+inf")

  async def test_get_events_parse_error(self):
    """Test handling of parse errors in events."""
    mock_redis = AsyncMock()
    mock_redis.zrangebyscore.return_value = [
      '{"valid": "json"}',
      "invalid json",
      '{"another": "valid"}',
    ]

    storage = SSEEventStorage(redis_client=mock_redis)

    with patch("robosystems.middleware.sse.event_storage.logger") as mock_logger:
      await storage.get_events("op123")

      # Should skip invalid JSON and warn
      mock_logger.warning.assert_called()
      # The exact number of events depends on whether the valid JSON creates valid SSEEvent objects

  async def test_get_operation_metadata(self):
    """Test retrieving operation metadata."""
    mock_redis = AsyncMock()

    metadata_dict = {
      "operation_id": "op123",
      "operation_type": "test_op",
      "user_id": "user456",
      "graph_id": "kg789",
      "status": "running",
      "created_at": "2023-01-01T12:00:00Z",
      "updated_at": "2023-01-01T12:30:00Z",
      "error_message": None,
      "result_data": None,
    }

    mock_redis.get.return_value = json.dumps(metadata_dict)

    storage = SSEEventStorage(redis_client=mock_redis)

    metadata = await storage.get_operation_metadata("op123")

    assert metadata is not None
    assert metadata.operation_id == "op123"
    assert metadata.operation_type == "test_op"
    assert metadata.user_id == "user456"
    assert metadata.status == OperationStatus.RUNNING

  async def test_get_operation_metadata_not_found(self):
    """Test retrieving non-existent metadata."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None

    storage = SSEEventStorage(redis_client=mock_redis)

    metadata = await storage.get_operation_metadata("missing_op")

    assert metadata is None

  async def test_get_operation_metadata_parse_error(self):
    """Test handling metadata parse errors."""
    mock_redis = AsyncMock()
    mock_redis.get.return_value = "invalid json"

    storage = SSEEventStorage(redis_client=mock_redis)

    with patch("robosystems.middleware.sse.event_storage.logger") as mock_logger:
      metadata = await storage.get_operation_metadata("op123")

      assert metadata is None
      mock_logger.warning.assert_called()

  async def test_cancel_operation(self):
    """Test cancelling an operation."""
    mock_redis = AsyncMock()
    mock_redis.exists.return_value = True
    mock_redis.incr.return_value = 10

    storage = SSEEventStorage(redis_client=mock_redis)

    with patch.object(storage, "store_event") as mock_store_event:
      await storage.cancel_operation("op123", "User requested cancellation")

      mock_store_event.assert_called_once_with(
        "op123",
        EventType.OPERATION_CANCELLED,
        {"reason": "User requested cancellation"},
      )

  async def test_cleanup_expired_operations(self):
    """Test cleanup of expired operations."""
    mock_redis = AsyncMock()

    # Mock scan_iter to return some keys
    async def mock_scan_iter(match):
      keys = ["sse:operation:meta:op1", "sse:operation:meta:op2"]
      for key in keys:
        yield key

    mock_redis.scan_iter = mock_scan_iter
    mock_redis.exists.side_effect = [True, True]
    mock_redis.ttl.side_effect = [-1, 3600]  # First has no TTL, second has TTL

    storage = SSEEventStorage(redis_client=mock_redis)

    cleaned = await storage.cleanup_expired_operations()

    assert cleaned == 1  # Only one operation needed TTL setting
    mock_redis.expire.assert_called_once()

  def test_update_operation_metadata_sync_completed(self):
    """Test sync metadata update for completed operation."""
    mock_redis = Mock()

    existing_metadata = {
      "operation_id": "op123",
      "operation_type": "test_op",
      "user_id": "user456",
      "graph_id": "kg789",
      "status": "running",
      "created_at": "2023-01-01T12:00:00Z",
      "updated_at": "2023-01-01T12:30:00Z",
      "error_message": None,
      "result_data": None,
    }

    mock_redis.get.return_value = json.dumps(existing_metadata)

    storage = SSEEventStorage()
    storage._sync_redis = mock_redis

    with patch("robosystems.middleware.sse.event_storage.datetime") as mock_datetime:
      mock_now = Mock()
      mock_now.isoformat.return_value = "2023-01-01T13:00:00Z"
      mock_datetime.now.return_value = mock_now

      storage._update_operation_metadata_sync(
        "op123",
        EventType.OPERATION_COMPLETED,
        {"result": "success", "processed_items": 100},
      )

      # Verify metadata was updated
      mock_redis.setex.assert_called_once()
      call_args = mock_redis.setex.call_args
      stored_metadata = json.loads(call_args[0][2])

      assert stored_metadata["status"] == "completed"
      assert stored_metadata["updated_at"] == "2023-01-01T13:00:00Z"
      assert stored_metadata["result_data"] == {
        "result": "success",
        "processed_items": 100,
      }

  def test_update_operation_metadata_sync_no_existing_metadata(self):
    """Test sync metadata update when no metadata exists."""
    mock_redis = Mock()
    mock_redis.get.return_value = None

    storage = SSEEventStorage()
    storage._sync_redis = mock_redis

    with patch("robosystems.middleware.sse.event_storage.datetime") as mock_datetime:
      mock_now = Mock()
      mock_now.isoformat.return_value = "2023-01-01T13:00:00Z"
      mock_datetime.now.return_value = mock_now

      storage._update_operation_metadata_sync(
        "new_op",
        EventType.OPERATION_STARTED,
        {"user_id": "user123", "graph_id": "kg456"},
      )

      # Should create new metadata
      mock_redis.setex.assert_called_once()
      call_args = mock_redis.setex.call_args
      stored_metadata = json.loads(call_args[0][2])

      assert stored_metadata["operation_id"] == "new_op"
      assert stored_metadata["operation_type"] == "graph_creation"
      assert stored_metadata["user_id"] == "user123"
      assert stored_metadata["graph_id"] == "kg456"
      assert stored_metadata["status"] == "running"


class TestGetEventStorage:
  """Test global event storage instance."""

  @patch("robosystems.middleware.sse.event_storage._event_storage", None)
  def test_get_event_storage_creates_instance(self):
    """Test that get_event_storage creates instance if none exists."""
    storage = get_event_storage()

    assert isinstance(storage, SSEEventStorage)

  @patch("robosystems.middleware.sse.event_storage._event_storage", None)
  def test_get_event_storage_returns_same_instance(self):
    """Test that get_event_storage returns same instance on multiple calls."""
    storage1 = get_event_storage()
    storage2 = get_event_storage()

    assert storage1 is storage2

  def test_get_event_storage_existing_instance(self):
    """Test that existing instance is returned."""
    # Create a mock instance
    mock_storage = Mock(spec=SSEEventStorage)

    with patch("robosystems.middleware.sse.event_storage._event_storage", mock_storage):
      storage = get_event_storage()
      assert storage is mock_storage


class TestIntegrationScenarios:
  """Test integration scenarios combining multiple operations."""

  async def test_operation_lifecycle(self):
    """Test complete operation lifecycle."""
    mock_redis = AsyncMock()
    mock_redis.exists.return_value = True
    mock_redis.incr.side_effect = [1, 2, 3]  # Sequence numbers
    mock_redis.get.return_value = None  # No existing metadata

    storage = SSEEventStorage(redis_client=mock_redis)

    with patch("robosystems.middleware.sse.event_storage.datetime") as mock_datetime:
      mock_now = Mock()
      mock_now.isoformat.side_effect = [
        "2023-01-01T12:00:00Z",  # create_operation
        "2023-01-01T12:01:00Z",  # started event
        "2023-01-01T12:05:00Z",  # progress event
        "2023-01-01T12:10:00Z",  # completed event
      ]
      mock_datetime.now.return_value = mock_now

      # Create operation
      op_id = await storage.create_operation(
        operation_type="test_workflow", user_id="user123", graph_id="kg456"
      )

      # Store events for operation lifecycle
      started_event = await storage.store_event(
        op_id, EventType.OPERATION_STARTED, {"message": "Started processing"}
      )

      progress_event = await storage.store_event(
        op_id, EventType.OPERATION_PROGRESS, {"percentage": 50, "step": "processing"}
      )

      completed_event = await storage.store_event(
        op_id,
        EventType.OPERATION_COMPLETED,
        {"result": "success", "items_processed": 100},
      )

      # Verify events
      assert started_event.sequence_number == 1
      assert progress_event.sequence_number == 2
      assert completed_event.sequence_number == 3

      # Verify all events have same operation_id
      assert started_event.operation_id == op_id
      assert progress_event.operation_id == op_id
      assert completed_event.operation_id == op_id

  async def test_error_scenario(self):
    """Test error handling scenario."""
    mock_redis = AsyncMock()
    mock_redis.exists.return_value = True
    mock_redis.incr.side_effect = [1, 2]
    mock_redis.get.return_value = None  # No existing metadata

    storage = SSEEventStorage(redis_client=mock_redis)

    with patch("robosystems.middleware.sse.event_storage.datetime") as mock_datetime:
      mock_now = Mock()
      mock_now.isoformat.side_effect = [
        "2023-01-01T12:00:00Z",  # create_operation
        "2023-01-01T12:01:00Z",  # started event
        "2023-01-01T12:02:00Z",  # error event
      ]
      mock_datetime.now.return_value = mock_now

      # Create operation
      op_id = await storage.create_operation(
        operation_type="failing_operation", user_id="user123"
      )

      # Start operation
      await storage.store_event(
        op_id, EventType.OPERATION_STARTED, {"message": "Starting operation"}
      )

      # Store error event
      error_event = await storage.store_event(
        op_id,
        EventType.OPERATION_ERROR,
        {"error": "Database connection failed", "error_code": "DB_CONN_ERR"},
      )

      assert error_event.data["error"] == "Database connection failed"
      assert error_event.data["error_code"] == "DB_CONN_ERR"
