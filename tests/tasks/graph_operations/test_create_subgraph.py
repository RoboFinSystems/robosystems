"""Tests for create_subgraph Celery task."""

import pytest
from unittest.mock import MagicMock, patch, Mock, AsyncMock

from robosystems.tasks.graph_operations.create_subgraph import (
  create_subgraph_with_fork_sse_task,
  create_subgraph_task,
)


@pytest.fixture(autouse=True)
def mock_celery_async_result():
  """Mock Celery AsyncResult to avoid Redis connection during tests."""
  with patch(
    "robosystems.tasks.graph_operations.create_subgraph.celery_app.AsyncResult"
  ) as mock_result_class:
    mock_result = Mock()
    mock_result.state = "PENDING"
    mock_result_class.return_value = mock_result
    yield mock_result_class


class TestCreateSubgraphWithForkSSETask:
  """Test cases for create_subgraph_with_fork_sse_task."""

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_subgraph.SubgraphService")
  def test_successful_creation_no_fork(self, mock_service_class, mock_get_db):
    """Test successful subgraph creation without fork (SSE version)."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Setup parent graph and user mocks
    mock_parent_graph = MagicMock()
    mock_parent_graph.graph_id = "parent-123"
    mock_parent_graph.user_id = "user-123"

    mock_user = MagicMock()
    mock_user.id = "user-123"

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_parent_graph,
      mock_user,
    ]

    # Setup service mock with AsyncMock methods
    mock_service = MagicMock()
    mock_service.create_subgraph = AsyncMock(
      return_value={
        "graph_id": "parent-123_dev",
        "status": "created",
        "name": "dev",
        "parent_graph_id": "parent-123",
      }
    )
    mock_service_class.return_value = mock_service

    # Task data
    task_data = {
      "user_id": "user-123",
      "parent_graph_id": "parent-123",
      "name": "dev",
      "description": "Development subgraph",
      "subgraph_type": "static",
      "metadata": {"purpose": "testing"},
      "fork_parent": False,
    }

    # Run task with SSE
    with patch(
      "robosystems.middleware.sse.task_progress.TaskSSEProgressTracker"
    ) as mock_tracker_class:
      mock_tracker = MagicMock()
      mock_tracker_class.return_value = mock_tracker

      result = create_subgraph_with_fork_sse_task.apply(
        args=[task_data, "operation-123"]
      ).get()

      assert result["graph_id"] == "parent-123_dev"
      assert result["status"] == "created"
      assert result["fork_status"] is None

      # Verify progress tracking
      assert mock_tracker.emit_progress.call_count >= 3
      mock_tracker.emit_completion.assert_called_once()

      # Verify service was called correctly
      mock_service.create_subgraph.assert_called_once()
      call_kwargs = mock_service.create_subgraph.call_args[1]
      assert call_kwargs["name"] == "dev"
      assert call_kwargs["fork_parent"] is False

      mock_session.close.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_subgraph.SubgraphService")
  def test_successful_creation_with_fork(self, mock_service_class, mock_get_db):
    """Test successful subgraph creation with fork (SSE version)."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Setup parent graph and user mocks
    mock_parent_graph = MagicMock()
    mock_parent_graph.graph_id = "parent-456"

    mock_user = MagicMock()
    mock_user.id = "user-456"

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_parent_graph,
      mock_user,
    ]

    # Setup service mock
    mock_service = MagicMock()
    mock_service.create_subgraph = AsyncMock(
      return_value={
        "graph_id": "parent-456_prod",
        "status": "created",
        "name": "prod",
      }
    )
    mock_service.fork_parent_data = AsyncMock(
      return_value={
        "status": "completed",
        "row_count": 1000,
        "tables_copied": 5,
      }
    )
    mock_service_class.return_value = mock_service

    # Task data with fork
    task_data = {
      "user_id": "user-456",
      "parent_graph_id": "parent-456",
      "name": "prod",
      "description": "Production subgraph",
      "fork_parent": True,
      "fork_options": {"tables": ["Entity", "Transaction"]},
    }

    # Run task with SSE
    with patch(
      "robosystems.middleware.sse.task_progress.TaskSSEProgressTracker"
    ) as mock_tracker_class:
      mock_tracker = MagicMock()
      mock_tracker_class.return_value = mock_tracker

      result = create_subgraph_with_fork_sse_task.apply(
        args=[task_data, "operation-456"]
      ).get()

      assert result["graph_id"] == "parent-456_prod"
      assert result["fork_status"]["status"] == "completed"
      assert result["fork_status"]["row_count"] == 1000

      # Verify fork was called
      mock_service.fork_parent_data.assert_called_once()
      fork_call = mock_service.fork_parent_data.call_args[1]
      assert fork_call["parent_graph_id"] == "parent-456"
      assert fork_call["subgraph_id"] == "parent-456_prod"

      # Verify progress tracking includes fork steps
      assert mock_tracker.emit_progress.call_count >= 5
      mock_tracker.emit_completion.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  def test_parent_graph_not_found(self, mock_get_db):
    """Test error handling when parent graph is not found."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Parent graph not found
    mock_session.query.return_value.filter.return_value.first.return_value = None

    task_data = {
      "user_id": "user-789",
      "parent_graph_id": "nonexistent",
      "name": "test",
    }

    # Run task and expect error
    with patch(
      "robosystems.middleware.sse.task_progress.TaskSSEProgressTracker"
    ) as mock_tracker_class:
      mock_tracker = MagicMock()
      mock_tracker_class.return_value = mock_tracker

      with pytest.raises(Exception, match="Parent graph nonexistent not found"):
        create_subgraph_with_fork_sse_task.apply(
          args=[task_data, "operation-789"]
        ).get()

      # Verify error was emitted via SSE (called twice: in if block and except block)
      assert mock_tracker.emit_error.call_count == 2
      mock_session.close.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  def test_user_not_found(self, mock_get_db):
    """Test error handling when user is not found."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Parent graph found, user not found
    mock_parent_graph = MagicMock()
    mock_parent_graph.graph_id = "parent-999"

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_parent_graph,
      None,  # User not found
    ]

    task_data = {
      "user_id": "nonexistent-user",
      "parent_graph_id": "parent-999",
      "name": "test",
    }

    # Run task and expect error
    with patch(
      "robosystems.middleware.sse.task_progress.TaskSSEProgressTracker"
    ) as mock_tracker_class:
      mock_tracker = MagicMock()
      mock_tracker_class.return_value = mock_tracker

      with pytest.raises(Exception, match="User nonexistent-user not found"):
        create_subgraph_with_fork_sse_task.apply(
          args=[task_data, "operation-999"]
        ).get()

      # Verify error was emitted via SSE (called twice: in if block and except block)
      assert mock_tracker.emit_error.call_count == 2
      mock_session.close.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_subgraph.SubgraphService")
  def test_fork_failure_but_subgraph_created(self, mock_service_class, mock_get_db):
    """Test handling of fork failure after subgraph creation."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Setup parent graph and user mocks
    mock_parent_graph = MagicMock()
    mock_parent_graph.graph_id = "parent-111"

    mock_user = MagicMock()
    mock_user.id = "user-111"

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_parent_graph,
      mock_user,
    ]

    # Setup service mock - creation succeeds, fork fails
    mock_service = MagicMock()
    mock_service.create_subgraph = AsyncMock(
      return_value={
        "graph_id": "parent-111_test",
        "status": "created",
      }
    )
    mock_service.fork_parent_data = AsyncMock(
      side_effect=RuntimeError("Fork operation failed")
    )
    mock_service_class.return_value = mock_service

    task_data = {
      "user_id": "user-111",
      "parent_graph_id": "parent-111",
      "name": "test",
      "fork_parent": True,
    }

    # Run task - should succeed with fork_status showing error
    with patch(
      "robosystems.middleware.sse.task_progress.TaskSSEProgressTracker"
    ) as mock_tracker_class:
      mock_tracker = MagicMock()
      mock_tracker_class.return_value = mock_tracker

      result = create_subgraph_with_fork_sse_task.apply(
        args=[task_data, "operation-111"]
      ).get()

      # Subgraph was created even though fork failed
      assert result["graph_id"] == "parent-111_test"
      assert result["fork_status"]["status"] == "failed"
      assert "Fork operation failed" in result["fork_status"]["error"]

      # Completion should still be emitted
      mock_tracker.emit_completion.assert_called_once()


class TestCreateSubgraphTask:
  """Test cases for create_subgraph_task (non-SSE version)."""

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_subgraph.SubgraphService")
  def test_successful_creation_simple(self, mock_service_class, mock_get_db):
    """Test successful subgraph creation (simple version)."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Setup parent graph and user mocks
    mock_parent_graph = MagicMock()
    mock_parent_graph.graph_id = "parent-222"

    mock_user = MagicMock()
    mock_user.id = "user-222"

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_parent_graph,
      mock_user,
    ]

    # Setup service mock
    mock_service = MagicMock()
    mock_service.create_subgraph = AsyncMock(
      return_value={
        "graph_id": "parent-222_simple",
        "status": "created",
        "name": "simple",
      }
    )
    mock_service_class.return_value = mock_service

    # Task data
    task_data = {
      "user_id": "user-222",
      "parent_graph_id": "parent-222",
      "name": "simple",
      "description": "Simple subgraph",
      "fork_parent": False,
    }

    # Run simple task
    result = create_subgraph_task.apply(args=[task_data]).get()

    assert result["graph_id"] == "parent-222_simple"
    assert result["status"] == "created"

    # Verify service was called
    mock_service.create_subgraph.assert_called_once()
    call_kwargs = mock_service.create_subgraph.call_args[1]
    assert call_kwargs["name"] == "simple"
    assert call_kwargs["fork_parent"] is False

    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_subgraph.SubgraphService")
  def test_creation_with_fork_simple(self, mock_service_class, mock_get_db):
    """Test subgraph creation with fork (simple version handles internally)."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Setup parent graph and user mocks
    mock_parent_graph = MagicMock()
    mock_parent_graph.graph_id = "parent-333"

    mock_user = MagicMock()
    mock_user.id = "user-333"

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_parent_graph,
      mock_user,
    ]

    # Setup service mock
    mock_service = MagicMock()
    mock_service.create_subgraph = AsyncMock(
      return_value={
        "graph_id": "parent-333_forked",
        "status": "created",
        "fork_status": "completed",
        "row_count": 500,
      }
    )
    mock_service_class.return_value = mock_service

    # Task data with fork
    task_data = {
      "user_id": "user-333",
      "parent_graph_id": "parent-333",
      "name": "forked",
      "fork_parent": True,
      "fork_options": {"tables": ["Entity"]},
    }

    # Run simple task
    result = create_subgraph_task.apply(args=[task_data]).get()

    assert result["graph_id"] == "parent-333_forked"
    assert result["fork_status"] == "completed"

    # Verify fork_parent was passed to service
    call_kwargs = mock_service.create_subgraph.call_args[1]
    assert call_kwargs["fork_parent"] is True
    assert call_kwargs["fork_options"]["tables"] == ["Entity"]

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  def test_parent_graph_not_found_simple(self, mock_get_db):
    """Test error handling when parent graph not found (simple version)."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Parent graph not found
    mock_session.query.return_value.filter.return_value.first.return_value = None

    task_data = {
      "user_id": "user-444",
      "parent_graph_id": "nonexistent",
      "name": "test",
    }

    # Run task and expect error
    with pytest.raises(Exception, match="Parent graph nonexistent not found"):
      create_subgraph_task.apply(args=[task_data]).get()

    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  def test_user_not_found_simple(self, mock_get_db):
    """Test error handling when user not found (simple version)."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Parent graph found, user not found
    mock_parent_graph = MagicMock()
    mock_parent_graph.graph_id = "parent-555"

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_parent_graph,
      None,  # User not found
    ]

    task_data = {
      "user_id": "nonexistent-user",
      "parent_graph_id": "parent-555",
      "name": "test",
    }

    # Run task and expect error
    with pytest.raises(Exception, match="User nonexistent-user not found"):
      create_subgraph_task.apply(args=[task_data]).get()

    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_subgraph.SubgraphService")
  def test_service_creation_failure(self, mock_service_class, mock_get_db):
    """Test handling of service creation failures."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Setup parent graph and user mocks
    mock_parent_graph = MagicMock()
    mock_parent_graph.graph_id = "parent-666"

    mock_user = MagicMock()
    mock_user.id = "user-666"

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_parent_graph,
      mock_user,
    ]

    # Setup service mock to fail
    mock_service = MagicMock()
    mock_service.create_subgraph = AsyncMock(
      side_effect=ValueError("Invalid subgraph name")
    )
    mock_service_class.return_value = mock_service

    task_data = {
      "user_id": "user-666",
      "parent_graph_id": "parent-666",
      "name": "invalid-name-with-hyphens",
    }

    # Run task and expect error
    with pytest.raises(ValueError, match="Invalid subgraph name"):
      create_subgraph_task.apply(args=[task_data]).get()

    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_subgraph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_subgraph.SubgraphService")
  def test_minimal_task_data(self, mock_service_class, mock_get_db):
    """Test subgraph creation with minimal required data."""
    # Setup database session mock
    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    # Setup parent graph and user mocks
    mock_parent_graph = MagicMock()
    mock_parent_graph.graph_id = "parent-777"

    mock_user = MagicMock()
    mock_user.id = "user-777"

    mock_session.query.return_value.filter.return_value.first.side_effect = [
      mock_parent_graph,
      mock_user,
    ]

    # Setup service mock
    mock_service = MagicMock()
    mock_service.create_subgraph = AsyncMock(
      return_value={
        "graph_id": "parent-777_minimal",
        "status": "created",
      }
    )
    mock_service_class.return_value = mock_service

    # Minimal task data
    task_data = {
      "user_id": "user-777",
      "parent_graph_id": "parent-777",
      "name": "minimal",
    }

    # Run task
    result = create_subgraph_task.apply(args=[task_data]).get()

    assert result["graph_id"] == "parent-777_minimal"

    # Verify defaults were used
    call_kwargs = mock_service.create_subgraph.call_args[1]
    assert call_kwargs["description"] is None
    assert call_kwargs["subgraph_type"] == "static"  # Default
    assert call_kwargs["metadata"] is None
    assert call_kwargs["fork_parent"] is False
