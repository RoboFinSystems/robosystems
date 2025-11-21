"""Tests for create_entity_graph Celery tasks."""

import pytest
from unittest.mock import MagicMock, patch, Mock

from robosystems.tasks.graph_operations.create_entity_graph import (
  create_entity_with_new_graph_task,
  create_entity_with_new_graph_sse_task,
)


@pytest.fixture(autouse=True)
def mock_celery_async_result():
  """Mock Celery AsyncResult to avoid Redis connection during tests."""
  with patch(
    "robosystems.tasks.graph_operations.create_entity_graph.celery_app.AsyncResult"
  ) as mock_result_class:
    mock_result = Mock()
    mock_result.state = "PENDING"
    mock_result_class.return_value = mock_result
    yield mock_result_class


class TestCreateEntityWithNewGraphTask:
  """Test cases for create_entity_with_new_graph_task."""

  @patch("robosystems.tasks.graph_operations.create_entity_graph.db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.EntityGraphServiceSync"
  )
  def test_create_entity_success(self, mock_service_class, mock_db_session):
    """Test successful entity creation with new graph."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_entity_with_new_graph.return_value = {
      "graph_id": "kg123456",
      "entity_id": "entity-789",
      "entity_name": "Test Company Inc",
      "entity_type": "company",
      "status": "created",
    }
    mock_service_class.return_value = mock_service

    # Entity data
    entity_data = {
      "name": "Test Company Inc",
      "type": "company",
      "tax_id": "12-3456789",
      "address": "123 Main St",
      "industry": "Technology",
    }

    # Run task
    result = create_entity_with_new_graph_task.apply(args=[entity_data, "user-123"])  # type: ignore[attr-defined]

    # Assertions
    assert result.successful()
    result_data = result.get()
    assert result_data["graph_id"] == "kg123456"
    assert result_data["entity_id"] == "entity-789"
    assert result_data["entity_name"] == "Test Company Inc"
    assert result_data["status"] == "created"

    # Verify service was called correctly
    mock_service.create_entity_with_new_graph.assert_called_once()
    call_kwargs = mock_service.create_entity_with_new_graph.call_args[1]
    assert call_kwargs["entity_data_dict"] == entity_data
    assert call_kwargs["user_id"] == "user-123"
    assert "cancellation_callback" in call_kwargs

  @patch("robosystems.tasks.graph_operations.create_entity_graph.db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.EntityGraphServiceSync"
  )
  def test_create_entity_with_metadata(self, mock_service_class, mock_db_session):
    """Test entity creation with additional metadata."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_entity_with_new_graph.return_value = {
      "graph_id": "kg456789",
      "entity_id": "entity-abc",
      "entity_name": "Advanced Analytics Corp",
      "metadata": {"employees": 500, "founded": 2015, "headquarters": "San Francisco"},
    }
    mock_service_class.return_value = mock_service

    # Entity data with metadata
    entity_data = {
      "name": "Advanced Analytics Corp",
      "type": "company",
      "tax_id": "98-7654321",
      "metadata": {"employees": 500, "founded": 2015, "headquarters": "San Francisco"},
    }

    # Run task
    result = create_entity_with_new_graph_task.apply(args=[entity_data, "user-456"])  # type: ignore[attr-defined]

    # Assertions
    assert result.successful()
    result_data = result.get()
    assert result_data["entity_name"] == "Advanced Analytics Corp"
    assert result_data["metadata"]["employees"] == 500
    assert result_data["metadata"]["founded"] == 2015

  @patch("robosystems.tasks.graph_operations.create_entity_graph.db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.EntityGraphServiceSync"
  )
  def test_create_entity_service_failure(self, mock_service_class, mock_db_session):
    """Test entity creation when service fails."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_entity_with_new_graph.side_effect = ValueError(
      "Duplicate tax ID found"
    )
    mock_service_class.return_value = mock_service

    entity_data = {
      "name": "Duplicate Corp",
      "type": "company",
      "tax_id": "11-1111111",  # Duplicate
    }

    # Run task - in eager mode, exceptions are raised immediately
    with pytest.raises(ValueError, match="Duplicate tax ID found"):
      create_entity_with_new_graph_task.apply(args=[entity_data, "user-789"]).get()  # type: ignore[attr-defined]

  @patch("robosystems.tasks.graph_operations.create_entity_graph.db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.EntityGraphServiceSync"
  )
  def test_create_entity_different_types(self, mock_service_class, mock_db_session):
    """Test entity creation for different entity types."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service_class.return_value = mock_service

    entity_types = ["company", "individual", "trust", "partnership"]

    for entity_type in entity_types:
      mock_service.create_entity_with_new_graph.return_value = {
        "graph_id": f"kg{entity_type}123",
        "entity_id": f"entity-{entity_type}",
        "entity_type": entity_type,
        "status": "created",
      }

      entity_data = {
        "name": f"Test {entity_type.title()}",
        "type": entity_type,
        "tax_id": f"99-{entity_type[:3]}",
      }

      # Run task
      result = create_entity_with_new_graph_task.apply(  # type: ignore[attr-defined]
        args=[entity_data, f"user-{entity_type}"]
      )

      # Assertions
      assert result.successful()
      result_data = result.get()
      assert result_data["entity_type"] == entity_type
      assert result_data["graph_id"] == f"kg{entity_type}123"

  @patch("robosystems.tasks.graph_operations.create_entity_graph.db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.EntityGraphServiceSync"
  )
  def test_create_entity_cancellation_callback(
    self, mock_service_class, mock_db_session
  ):
    """Test that cancellation callback is passed to service."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_entity_with_new_graph.return_value = {
      "graph_id": "kg-cancel-test",
      "entity_id": "entity-cancel",
      "status": "created",
    }
    mock_service_class.return_value = mock_service

    entity_data = {"name": "Cancellation Test Corp", "type": "company"}

    # Run task
    result = create_entity_with_new_graph_task.apply(  # type: ignore[attr-defined]
      args=[entity_data, "user-cancel-test"]
    )

    # Verify cancellation callback was passed
    assert result.successful()
    mock_service.create_entity_with_new_graph.assert_called_once()
    call_kwargs = mock_service.create_entity_with_new_graph.call_args[1]
    assert "cancellation_callback" in call_kwargs
    assert callable(call_kwargs["cancellation_callback"])


class TestCreateEntityWithNewGraphSSETask:
  """Test cases for create_entity_with_new_graph_sse_task."""

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.create_entity_graph.db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.EntityGraphServiceSync"
  )
  def test_create_entity_sse_success(
    self, mock_service_class, mock_db_session, mock_tracker_class
  ):
    """Test successful entity creation with SSE progress tracking."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_entity_with_new_graph.return_value = {
      "graph_id": "kgsse123",
      "entity_id": "entity-sse",
      "entity_name": "SSE Test Corp",
      "status": "created",
    }
    mock_service_class.return_value = mock_service

    # Mock SSE tracker
    mock_tracker = MagicMock()
    mock_tracker.create_progress_callback.return_value = lambda msg: None
    mock_tracker_class.return_value = mock_tracker

    entity_data = {"name": "SSE Test Corp", "type": "company", "tax_id": "55-5555555"}

    # Run task
    result = create_entity_with_new_graph_sse_task.apply(  # type: ignore[attr-defined]
      args=[entity_data, "user-sse", "operation-123"]
    )

    # Assertions
    assert result.successful()
    result_data = result.get()
    assert result_data["graph_id"] == "kgsse123"
    assert result_data["entity_name"] == "SSE Test Corp"

    # Verify SSE tracker was used
    mock_tracker_class.assert_called_once_with("operation-123")
    mock_tracker.emit_progress.assert_any_call(
      "Starting entity and graph creation...", 0
    )
    mock_tracker.emit_progress.assert_any_call("Validating entity data...", 10)
    mock_tracker.emit_completion.assert_called_once()

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.create_entity_graph.db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.EntityGraphServiceSync"
  )
  def test_create_entity_sse_with_error(
    self, mock_service_class, mock_db_session, mock_tracker_class
  ):
    """Test entity creation with SSE when an error occurs."""
    # Setup mocks
    mock_service = MagicMock()
    error = RuntimeError("Database connection failed")
    mock_service.create_entity_with_new_graph.side_effect = error
    mock_service_class.return_value = mock_service

    # Mock SSE tracker
    mock_tracker = MagicMock()
    mock_tracker.create_progress_callback.return_value = lambda msg: None
    mock_tracker_class.return_value = mock_tracker

    entity_data = {"name": "Error Test Corp", "type": "company", "tax_id": "99-9999999"}

    # Run task - in eager mode, exceptions are raised immediately
    with pytest.raises(RuntimeError, match="Database connection failed"):
      create_entity_with_new_graph_sse_task.apply(  # type: ignore[attr-defined]
        args=[entity_data, "user-error", "operation-error"]
      ).get()

    # Verify error was emitted to SSE
    mock_tracker.emit_error.assert_called_once()
    error_call_args = mock_tracker.emit_error.call_args[0]
    assert isinstance(error_call_args[0], RuntimeError)

    # Verify additional context was provided
    error_context = mock_tracker.emit_error.call_args[1]["additional_context"]
    assert error_context["user_id"] == "user-error"
    assert error_context["entity_name"] == "Error Test Corp"

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.create_entity_graph.db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.EntityGraphServiceSync"
  )
  def test_create_entity_sse_progress_tracking(
    self, mock_service_class, mock_db_session, mock_tracker_class
  ):
    """Test that SSE progress events are emitted correctly."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_entity_with_new_graph.return_value = {
      "graph_id": "kgprogress",
      "entity_id": "entity-progress",
      "status": "created",
    }
    mock_service_class.return_value = mock_service

    # Mock SSE tracker
    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    entity_data = {"name": "Progress Corp", "type": "company"}

    # Run task
    result = create_entity_with_new_graph_sse_task.apply(  # type: ignore[attr-defined]
      args=[entity_data, "user-progress", "op-progress"]
    )

    # Assertions
    assert result.successful()

    # Verify progress tracking was used (emit_progress should have been called)
    assert (
      mock_tracker.emit_progress.call_count >= 2
    )  # At least validation and started events

    # Verify service was called with progress_callback
    mock_service.create_entity_with_new_graph.assert_called_once()
    call_args = mock_service.create_entity_with_new_graph.call_args
    assert "progress_callback" in call_args.kwargs
    assert callable(call_args.kwargs["progress_callback"])

    # Verify completion event includes context
    mock_tracker.emit_completion.assert_called_once()
    completion_call = mock_tracker.emit_completion.call_args
    assert completion_call[0][0]["graph_id"] == "kgprogress"

    additional_context = completion_call[1]["additional_context"]
    assert additional_context["graph_id"] == "kgprogress"
    assert additional_context["entity_name"] == "Progress Corp"

  @patch("robosystems.tasks.graph_operations.create_entity_graph.db_session")
  @patch(
    "robosystems.tasks.graph_operations.create_entity_graph.EntityGraphServiceSync"
  )
  def test_create_entity_minimal_data(self, mock_service_class, mock_db_session):
    """Test entity creation with minimal required data."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_entity_with_new_graph.return_value = {
      "graph_id": "kgminimal",
      "entity_id": "entity-min",
      "status": "created",
    }
    mock_service_class.return_value = mock_service

    # Minimal entity data
    entity_data = {
      "name": "Minimal Corp",
      "type": "company",
      # No tax_id, address, or other optional fields
    }

    # Run task
    result = create_entity_with_new_graph_task.apply(args=[entity_data, "user-minimal"])  # type: ignore[attr-defined]

    # Assertions
    assert result.successful()
    result_data = result.get()
    assert result_data["graph_id"] == "kgminimal"
    assert result_data["status"] == "created"
