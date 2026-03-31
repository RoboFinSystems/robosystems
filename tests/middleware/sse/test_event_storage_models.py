"""SSE event storage model tests (no Redis required)."""

import json

from robosystems.middleware.sse.event_storage import (
  EventType,
  OperationMetadata,
  OperationStatus,
  SSEEvent,
  SSEEventStorage,
)


class TestEventType:
  def test_standard_events(self):
    assert EventType.OPERATION_STARTED.value == "operation_started"
    assert EventType.OPERATION_PROGRESS.value == "operation_progress"
    assert EventType.OPERATION_ERROR.value == "operation_error"
    assert EventType.OPERATION_COMPLETED.value == "operation_completed"
    assert EventType.OPERATION_CANCELLED.value == "operation_cancelled"

  def test_custom_events(self):
    assert EventType.GRAPH_CREATION_PROGRESS.value == "graph_creation_progress"
    assert EventType.AGENT_ANALYSIS_PROGRESS.value == "agent_analysis_progress"
    assert EventType.BACKUP_PROGRESS.value == "backup_progress"


class TestOperationStatus:
  def test_values(self):
    assert OperationStatus.PENDING.value == "pending"
    assert OperationStatus.RUNNING.value == "running"
    assert OperationStatus.COMPLETED.value == "completed"
    assert OperationStatus.FAILED.value == "failed"
    assert OperationStatus.CANCELLED.value == "cancelled"


class TestSSEEvent:
  def test_to_sse_format(self):
    event = SSEEvent(
      event_type=EventType.OPERATION_PROGRESS,
      operation_id="op123",
      timestamp="2024-01-15T12:00:00Z",
      data={"message": "Processing...", "progress_percent": 50},
      sequence_number=3,
    )
    formatted = event.to_sse_format()
    assert "event: operation_progress" in formatted
    assert "data: " in formatted
    assert "op123" in formatted
    assert formatted.endswith("\n\n")

  def test_to_sse_format_valid_json_data(self):
    event = SSEEvent(
      event_type=EventType.OPERATION_STARTED,
      operation_id="op456",
      timestamp="2024-01-15T12:00:00Z",
      data={"key": "value"},
      sequence_number=1,
    )
    formatted = event.to_sse_format()
    data_line = next(line for line in formatted.split("\n") if line.startswith("data:"))
    json_str = data_line[len("data: ") :]
    parsed = json.loads(json_str)
    assert parsed["operation_id"] == "op456"

  def test_to_dict(self):
    event = SSEEvent(
      event_type=EventType.OPERATION_COMPLETED,
      operation_id="op789",
      timestamp="2024-01-15T12:00:00Z",
      data={"result": "success"},
      sequence_number=5,
    )
    d = event.to_dict()
    assert d["operation_id"] == "op789"
    assert d["sequence_number"] == 5

  def test_from_dict(self):
    original = SSEEvent(
      event_type=EventType.OPERATION_ERROR,
      operation_id="op123",
      timestamp="2024-01-15T12:00:00Z",
      data={"error": "timeout"},
      sequence_number=2,
    )
    d = original.to_dict()
    restored = SSEEvent.from_dict(d)
    assert restored.operation_id == original.operation_id
    assert restored.sequence_number == original.sequence_number

  def test_default_sequence_number(self):
    event = SSEEvent(
      event_type=EventType.OPERATION_STARTED,
      operation_id="op1",
      timestamp="2024-01-01T00:00:00Z",
      data={},
    )
    assert event.sequence_number == 0


class TestOperationMetadata:
  def test_to_dict(self):
    meta = OperationMetadata(
      operation_id="op123",
      operation_type="graph_creation",
      user_id="user1",
      graph_id="kg456",
      status=OperationStatus.RUNNING,
      created_at="2024-01-15T12:00:00Z",
      updated_at="2024-01-15T12:01:00Z",
    )
    d = meta.to_dict()
    assert d["operation_id"] == "op123"
    assert d["operation_type"] == "graph_creation"
    assert d["graph_id"] == "kg456"

  def test_optional_fields(self):
    meta = OperationMetadata(
      operation_id="op1",
      operation_type="test",
      user_id="u1",
      graph_id=None,
      status=OperationStatus.PENDING,
      created_at="2024-01-01",
      updated_at="2024-01-01",
      error_message="something failed",
      result_data={"key": "value"},
    )
    d = meta.to_dict()
    assert d["error_message"] == "something failed"
    assert d["result_data"]["key"] == "value"


class TestSSEEventStorageKeyPrefixes:
  def test_prefixes(self):
    storage = SSEEventStorage.__new__(SSEEventStorage)
    storage.event_prefix = "sse:operation:events:"
    storage.metadata_prefix = "sse:operation:meta:"
    storage.sequence_prefix = "sse:operation:seq:"
    assert storage.event_prefix.startswith("sse:")
    assert storage.metadata_prefix.startswith("sse:")

  def test_generate_operation_id(self):
    storage = SSEEventStorage.__new__(SSEEventStorage)
    op_id = storage.generate_operation_id()
    assert isinstance(op_id, str)
    assert op_id.startswith("op_")
    assert len(op_id) == 29  # "op_" + 26-char ULID

  def test_unique_ids(self):
    storage = SSEEventStorage.__new__(SSEEventStorage)
    ids = {storage.generate_operation_id() for _ in range(100)}
    assert len(ids) == 100
