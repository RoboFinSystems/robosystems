"""Tests for graph_api task_sse module."""

import json
from unittest.mock import AsyncMock, patch

import pytest

from robosystems.graph_api.core.task_sse import (
  TaskType,
  _calculate_duration,
  _get_completion_message,
  _get_failure_message,
  _get_progress_message,
  generate_task_sse_events,
)


class TestTaskSSE:
  """Test cases for task SSE event generation."""

  @pytest.mark.asyncio
  async def test_generate_task_sse_events_connected(self):
    """Test that SSE generates initial connected event."""
    task_manager = AsyncMock()
    task_manager.get_task.return_value = {
      "status": "running",
      "progress_percent": 0,
    }

    events = []
    async for event in generate_task_sse_events(
      task_manager, "test-task-123", TaskType.INGESTION
    ):
      events.append(event)
      if len(events) >= 1:
        break  # Stop after first event

    assert len(events) == 1
    assert events[0]["event"] == "connected"
    data = json.loads(events[0]["data"])
    assert data["task_id"] == "test-task-123"
    assert data["task_type"] == "ingestion"
    assert "timestamp" in data

  @pytest.mark.asyncio
  async def test_generate_task_sse_events_progress_update(self):
    """Test that SSE generates progress events."""
    task_manager = AsyncMock()
    # Simulate progress updates
    task_manager.get_task.side_effect = [
      {
        "status": "running",
        "progress_percent": 0,
        "records_processed": 0,
        "estimated_records": 100,
      },
      {
        "status": "running",
        "progress_percent": 25,
        "records_processed": 25,
        "estimated_records": 100,
      },
      {
        "status": "completed",
        "progress_percent": 100,
        "result": {"records_loaded": 100},
      },
    ]

    events = []
    async for event in generate_task_sse_events(
      task_manager, "test-task-123", TaskType.INGESTION
    ):
      events.append(event)
      if event["event"] == "completed":
        break

    # Should have: connected, progress (25%), completed
    event_types = [e["event"] for e in events]
    assert "connected" in event_types
    assert "progress" in event_types
    assert "completed" in event_types

    # Check progress event - should have at least one with 25%
    progress_events = [e for e in events if e["event"] == "progress"]
    assert len(progress_events) >= 1

    # Find the progress event with 25%
    found_25_percent = False
    for event in progress_events:
      data = json.loads(event["data"])
      if data["progress_percent"] == 25:
        found_25_percent = True
        assert data["records_processed"] == 25
        break
    assert found_25_percent, "Should have a progress event with 25%"

  @pytest.mark.asyncio
  async def test_generate_task_sse_events_task_not_found(self):
    """Test SSE handling when task is not found."""
    task_manager = AsyncMock()
    task_manager.get_task.return_value = None

    events = []
    async for event in generate_task_sse_events(
      task_manager, "non-existent-task", TaskType.BACKUP
    ):
      events.append(event)
      if event["event"] == "error":
        break

    # Should have connected and error events
    assert len(events) == 2
    assert events[0]["event"] == "connected"
    assert events[1]["event"] == "error"
    error_data = json.loads(events[1]["data"])
    assert "not found" in error_data["error"]

  @pytest.mark.asyncio
  async def test_generate_task_sse_events_task_failed(self):
    """Test SSE handling when task fails."""
    task_manager = AsyncMock()
    task_manager.get_task.side_effect = [
      {
        "status": "running",
        "progress_percent": 50,
      },
      {
        "status": "failed",
        "error": "Database connection lost",
        "metadata": {"database": "test-db"},
      },
    ]

    events = []
    async for event in generate_task_sse_events(
      task_manager, "test-task-123", TaskType.RESTORE
    ):
      events.append(event)
      if event["event"] == "failed":
        break

    # Check failed event
    failed_events = [e for e in events if e["event"] == "failed"]
    assert len(failed_events) == 1
    failed_data = json.loads(failed_events[0]["data"])
    assert failed_data["status"] == "failed"
    assert failed_data["error"] == "Database connection lost"
    assert "Failed to restore" in failed_data["message"]

  @pytest.mark.asyncio
  async def test_generate_task_sse_events_heartbeat(self):
    """Test that SSE generates heartbeat events."""
    task_manager = AsyncMock()
    # Keep task running to generate heartbeats
    task_manager.get_task.return_value = {
      "status": "running",
      "progress_percent": 50,
    }

    events = []
    with patch("robosystems.graph_api.core.task_sse.time.time") as mock_time:
      # Simulate time passing for heartbeat
      mock_time.side_effect = [0, 0, 35, 35, 70]  # Trigger heartbeat after 35 seconds

      async for event in generate_task_sse_events(
        task_manager, "test-task-123", TaskType.BACKUP, heartbeat_interval=30
      ):
        events.append(event)
        if len(events) >= 3:  # connected + heartbeat + another event
          break

    # Check for heartbeat event
    heartbeat_events = [e for e in events if e["event"] == "heartbeat"]
    assert len(heartbeat_events) >= 1
    heartbeat_data = json.loads(heartbeat_events[0]["data"])
    assert heartbeat_data["task_id"] == "test-task-123"
    assert heartbeat_data["status"] == "running"

  @pytest.mark.asyncio
  async def test_generate_task_sse_events_exception_handling(self):
    """Test SSE handling of exceptions."""
    task_manager = AsyncMock()
    task_manager.get_task.side_effect = Exception("Database error")

    events = []
    async for event in generate_task_sse_events(
      task_manager, "test-task-123", TaskType.EXPORT
    ):
      events.append(event)
      if event["event"] == "error":
        break

    # Should have connected and error events
    error_events = [e for e in events if e["event"] == "error"]
    assert len(error_events) == 1
    error_data = json.loads(error_events[0]["data"])
    assert "Database error" in error_data["error"]

  def test_get_progress_message_ingestion(self):
    """Test progress message generation for ingestion tasks."""
    task = {"metadata": {"table_name": "customers"}}
    message = _get_progress_message(TaskType.INGESTION, task)
    assert message == "Processing customers..."

  def test_get_progress_message_backup(self):
    """Test progress message generation for backup tasks."""
    task = {"metadata": {"database": "prod-db"}}
    message = _get_progress_message(TaskType.BACKUP, task)
    assert message == "Backing up prod-db..."

  def test_get_progress_message_restore(self):
    """Test progress message generation for restore tasks."""
    task = {"metadata": {"database": "staging-db"}}
    message = _get_progress_message(TaskType.RESTORE, task)
    assert message == "Restoring staging-db..."

  def test_get_progress_message_generic(self):
    """Test progress message generation for generic tasks."""
    task = {}
    message = _get_progress_message(TaskType.MIGRATION, task)
    assert message == "Processing migration task..."

  def test_get_completion_message_ingestion(self):
    """Test completion message generation for ingestion tasks."""
    task = {
      "metadata": {"table_name": "orders"},
      "result": {"records_loaded": 1500},
    }
    message = _get_completion_message(TaskType.INGESTION, task)
    assert message == "Successfully ingested 1,500 records into orders"

  def test_get_completion_message_backup(self):
    """Test completion message generation for backup tasks."""
    task = {
      "metadata": {"database": "analytics-db"},
      "result": {"backup_size_mb": 256.5},
    }
    message = _get_completion_message(TaskType.BACKUP, task)
    assert message == "Successfully backed up analytics-db (256.5 MB)"

  def test_get_completion_message_restore(self):
    """Test completion message generation for restore tasks."""
    task = {"metadata": {"database": "test-db"}}
    message = _get_completion_message(TaskType.RESTORE, task)
    assert message == "Successfully restored test-db"

  def test_get_completion_message_generic(self):
    """Test completion message generation for generic tasks."""
    task = {}
    message = _get_completion_message(TaskType.EXPORT, task)
    assert message == "Successfully completed export task"

  def test_get_failure_message_ingestion(self):
    """Test failure message generation for ingestion tasks."""
    task = {"metadata": {"table_name": "products"}}
    message = _get_failure_message(TaskType.INGESTION, task)
    assert message == "Failed to ingest data into products"

  def test_get_failure_message_backup(self):
    """Test failure message generation for backup tasks."""
    task = {"metadata": {"database": "main-db"}}
    message = _get_failure_message(TaskType.BACKUP, task)
    assert message == "Failed to backup main-db"

  def test_get_failure_message_restore(self):
    """Test failure message generation for restore tasks."""
    task = {"metadata": {"database": "archive-db"}}
    message = _get_failure_message(TaskType.RESTORE, task)
    assert message == "Failed to restore archive-db"

  def test_get_failure_message_generic(self):
    """Test failure message generation for generic tasks."""
    task = {}
    message = _get_failure_message(TaskType.MIGRATION, task)
    assert message == "Failed to complete migration task"

  def test_calculate_duration_valid(self):
    """Test duration calculation with valid timestamps."""
    task = {
      "started_at": "2024-01-01T10:00:00",
      "completed_at": "2024-01-01T10:05:30",
    }
    duration = _calculate_duration(task)
    assert duration == 330.0  # 5 minutes 30 seconds

  def test_calculate_duration_missing_timestamps(self):
    """Test duration calculation with missing timestamps."""
    task = {"started_at": "2024-01-01T10:00:00"}
    duration = _calculate_duration(task)
    assert duration == 0.0

  def test_calculate_duration_invalid_timestamps(self):
    """Test duration calculation with invalid timestamps."""
    task = {
      "started_at": "invalid-date",
      "completed_at": "also-invalid",
    }
    duration = _calculate_duration(task)
    assert duration == 0.0

  @pytest.mark.asyncio
  async def test_generate_task_sse_events_with_metadata(self):
    """Test SSE events include metadata."""
    task_manager = AsyncMock()
    task_manager.get_task.return_value = {
      "status": "completed",
      "progress_percent": 100,
      "result": {"records_loaded": 500},
      "metadata": {
        "database": "test-db",
        "table_name": "users",
        "source": "csv_import",
      },
    }

    events = []
    async for event in generate_task_sse_events(
      task_manager, "test-task-123", TaskType.INGESTION
    ):
      events.append(event)
      if event["event"] == "completed":
        break

    # Check completed event has metadata
    completed_events = [e for e in events if e["event"] == "completed"]
    assert len(completed_events) == 1
    completed_data = json.loads(completed_events[0]["data"])
    assert completed_data["metadata"]["database"] == "test-db"
    assert completed_data["metadata"]["table_name"] == "users"
    assert completed_data["metadata"]["source"] == "csv_import"
