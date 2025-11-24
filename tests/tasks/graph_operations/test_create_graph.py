"""Tests for create_graph Celery task."""

import pytest
from unittest.mock import MagicMock, patch, Mock

from robosystems.tasks.graph_operations.create_graph import create_graph_task


@pytest.fixture(autouse=True)
def mock_celery_async_result():
  """Mock Celery AsyncResult to avoid Redis connection during tests."""
  with patch(
    "robosystems.tasks.graph_operations.create_graph.celery_app.AsyncResult"
  ) as mock_result_class:
    mock_result = Mock()
    mock_result.state = "PENDING"
    mock_result_class.return_value = mock_result
    yield mock_result_class


class TestCreateGraphTask:
  """Test cases for create_graph_task."""

  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_success(self, mock_service_class):
    """Test successful graph creation."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_graph.return_value = {
      "graph_id": "kg123456",
      "status": "created",
      "database_path": "/data/graphs/kg123456",
      "schema_extensions": ["roboledger"],
      "tier": "standard",
    }
    mock_service_class.return_value = mock_service

    # Task data
    task_data = {
      "user_id": "user-123",
      "graph_id": "kg123456",
      "schema_extensions": ["roboledger"],
      "metadata": {
        "name": "Test Graph",
        "description": "A test graph",
        "type": "financial",
        "tags": ["test", "demo"],
      },
      "tier": "standard",
    }

    # Run task function directly for testing
    result_data = create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]
    assert result_data["graph_id"] == "kg123456"
    assert result_data["status"] == "created"
    assert result_data["tier"] == "standard"

    # Verify service was called with correct parameters
    mock_service.create_graph.assert_called_once()
    call_args = mock_service.create_graph.call_args[1]
    assert call_args["graph_id"] == "kg123456"
    assert call_args["user_id"] == "user-123"
    assert call_args["schema_extensions"] == ["roboledger"]
    assert call_args["tier"] == "standard"

  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_with_custom_schema(self, mock_service_class):
    """Test graph creation with custom schema."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_graph.return_value = {
      "graph_id": "kg789012",
      "status": "created",
      "custom_schema": True,
    }
    mock_service_class.return_value = mock_service

    # Task data with custom schema
    custom_schema = {
      "nodes": {
        "Product": {
          "properties": {"name": "STRING", "price": "DOUBLE", "category": "STRING"}
        }
      },
      "edges": {
        "PURCHASED": {
          "source": "Customer",
          "target": "Product",
          "properties": {"quantity": "INT32", "date": "DATE"},
        }
      },
    }

    task_data = {
      "user_id": "user-456",
      "custom_schema": custom_schema,
      "metadata": {"name": "Custom Graph"},
    }

    # Run task function directly for testing
    result_data = create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]
    assert result_data["graph_id"] == "kg789012"
    assert result_data["custom_schema"] is True

    # Verify custom schema was passed
    call_args = mock_service.create_graph.call_args[1]
    assert call_args["custom_schema"] == custom_schema

  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_with_initial_data(self, mock_service_class):
    """Test graph creation with initial data population."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_graph.return_value = {
      "graph_id": "kg345678",
      "status": "created",
      "data_populated": True,
      "node_count": 50,
      "edge_count": 100,
    }
    mock_service_class.return_value = mock_service

    # Task data with initial data
    initial_data = {
      "nodes": [
        {"type": "Person", "id": "p1", "properties": {"name": "Alice"}},
        {"type": "Person", "id": "p2", "properties": {"name": "Bob"}},
      ],
      "edges": [
        {"type": "KNOWS", "from": "p1", "to": "p2", "properties": {"since": 2020}}
      ],
    }

    task_data = {
      "user_id": "user-789",
      "initial_data": initial_data,
      "tier": "enterprise",
    }

    # Run task function directly for testing
    result_data = create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]
    assert result_data["data_populated"] is True
    assert result_data["node_count"] == 50
    assert result_data["edge_count"] == 100

    # Verify initial data was passed
    call_args = mock_service.create_graph.call_args[1]
    assert call_args["initial_data"] == initial_data
    assert call_args["tier"] == "enterprise"

  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_service_failure(self, mock_service_class):
    """Test graph creation when service fails."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_graph.side_effect = ValueError("Invalid tier specified")
    mock_service_class.return_value = mock_service

    task_data = {"user_id": "user-error", "tier": "invalid_tier"}

    # Run task function directly for testing (expecting exception)
    with pytest.raises(ValueError, match="Invalid tier specified"):
      create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]

  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_all_tiers(self, mock_service_class):
    """Test graph creation for all tier types."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service_class.return_value = mock_service

    # Test each tier
    for tier in ["standard", "enterprise", "premium"]:
      mock_service.create_graph.return_value = {
        "graph_id": f"kg{tier}123",
        "status": "created",
        "tier": tier,
      }

      task_data = {"user_id": f"user-{tier}", "tier": tier}

      # Run task function directly for testing
      result_data = create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]
      assert result_data["tier"] == tier
      assert result_data["graph_id"] == f"kg{tier}123"

      # Verify tier was passed correctly
      call_args = mock_service.create_graph.call_args[1]
      assert call_args["tier"] == tier

  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_minimal_data(self, mock_service_class):
    """Test graph creation with minimal required data."""
    # Setup mocks
    mock_service = MagicMock()
    mock_service.create_graph.return_value = {
      "graph_id": "kgminimal",
      "status": "created",
      "tier": "standard",  # Default tier
    }
    mock_service_class.return_value = mock_service

    # Minimal task data - only user_id
    task_data = {"user_id": "user-minimal"}

    # Run task function directly for testing
    result_data = create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]
    assert result_data["graph_id"] == "kgminimal"

    # Verify defaults were used
    call_args = mock_service.create_graph.call_args[1]
    assert call_args["graph_id"] is None  # No specific ID requested
    assert call_args["schema_extensions"] == []  # Default empty list
    assert call_args["metadata"] == {}  # Default empty dict
    assert call_args["tier"] == "ladybug-standard"  # Default tier
    assert call_args["initial_data"] is None
    assert call_args["custom_schema"] is None

  @patch("robosystems.tasks.graph_operations.create_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.GraphSubscriptionService")
  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_with_billing_subscription(
    self, mock_service_class, mock_subscription_service_class, mock_get_db
  ):
    """Test graph creation creates billing subscription."""
    mock_service = MagicMock()
    mock_service.create_graph.return_value = {
      "graph_id": "kg123456",
      "status": "created",
    }
    mock_service_class.return_value = mock_service

    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    mock_subscription_service = MagicMock()
    mock_subscription = MagicMock()
    mock_subscription.id = "sub-123"
    mock_subscription_service.create_graph_subscription.return_value = mock_subscription
    mock_subscription_service_class.return_value = mock_subscription_service

    task_data = {
      "user_id": "user-123",
      "tier": "ladybug-large",
    }

    result = create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]

    assert result["graph_id"] == "kg123456"
    mock_subscription_service.create_graph_subscription.assert_called_once_with(
      user_id="user-123",
      graph_id="kg123456",
      plan_name="ladybug-large",
    )
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.GraphSubscriptionService")
  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_skip_billing(
    self, mock_service_class, mock_subscription_service_class, mock_get_db
  ):
    """Test graph creation with skip_billing flag."""
    mock_service = MagicMock()
    mock_service.create_graph.return_value = {
      "graph_id": "kg123456",
      "status": "created",
    }
    mock_service_class.return_value = mock_service

    task_data = {
      "user_id": "user-123",
      "skip_billing": True,
    }

    result = create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]

    assert result["graph_id"] == "kg123456"
    mock_subscription_service_class.assert_not_called()
    mock_get_db.assert_not_called()

  @patch("robosystems.tasks.graph_operations.create_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.GraphSubscriptionService")
  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_billing_error_continues(
    self, mock_service_class, mock_subscription_service_class, mock_get_db
  ):
    """Test graph creation continues despite billing subscription error."""
    mock_service = MagicMock()
    mock_service.create_graph.return_value = {
      "graph_id": "kg123456",
      "status": "created",
    }
    mock_service_class.return_value = mock_service

    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    mock_subscription_service = MagicMock()
    mock_subscription_service.create_graph_subscription.side_effect = Exception(
      "Billing API error"
    )
    mock_subscription_service_class.return_value = mock_subscription_service

    task_data = {
      "user_id": "user-123",
      "tier": "ladybug-standard",
    }

    result = create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]

    assert result["graph_id"] == "kg123456"
    assert result["status"] == "created"
    mock_session.close.assert_called_once()

  @patch("robosystems.tasks.graph_operations.create_graph.celery_app")
  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_create_graph_task_cancellation(self, mock_service_class, mock_celery_app):
    """Test graph creation handles task cancellation."""
    mock_service = MagicMock()
    mock_service_class.return_value = mock_service

    mock_result = MagicMock()
    mock_result.state = "REVOKED"
    mock_celery_app.AsyncResult.return_value = mock_result

    def cancellation_check(*args, **kwargs):
      callback = kwargs.get("cancellation_callback")
      if callback:
        callback()
      return {"graph_id": "kg123456", "status": "created"}

    mock_service.create_graph.side_effect = cancellation_check

    task_data = {"user_id": "user-123"}

    with pytest.raises(Exception, match="Task was cancelled"):
      create_graph_task.apply(args=[task_data]).get()  # type: ignore[attr-defined]


class TestCreateGraphSSETask:
  """Test cases for create_graph_sse_task."""

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.create_graph.get_db_session")
  @patch("robosystems.tasks.graph_operations.create_graph.GraphSubscriptionService")
  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_sse_graph_creation_success(
    self,
    mock_service_class,
    mock_subscription_service_class,
    mock_get_db,
    mock_tracker_class,
  ):
    """Test successful SSE graph creation with progress tracking."""
    from robosystems.tasks.graph_operations.create_graph import create_graph_sse_task

    mock_service = MagicMock()
    mock_service.create_graph.return_value = {
      "graph_id": "kg123456",
      "status": "created",
    }
    mock_service_class.return_value = mock_service

    mock_session = MagicMock()
    mock_get_db.return_value = iter([mock_session])

    mock_subscription_service = MagicMock()
    mock_subscription = MagicMock()
    mock_subscription.id = "sub-123"
    mock_subscription_service.create_graph_subscription.return_value = mock_subscription
    mock_subscription_service_class.return_value = mock_subscription_service

    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    task_data = {
      "user_id": "user-123",
      "tier": "ladybug-standard",
      "metadata": {"graph_name": "Test Graph"},
    }

    result = create_graph_sse_task.apply(  # type: ignore[attr-defined]
      args=[task_data, "op-123"]
    ).get()

    assert result["graph_id"] == "kg123456"
    mock_tracker.emit_progress.assert_any_call("Starting graph creation...", 0)
    mock_tracker.emit_progress.assert_any_call("Validating graph configuration...", 10)
    mock_tracker.emit_progress.assert_any_call("Creating billing subscription...", 95)
    mock_tracker.emit_completion.assert_called_once()

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_sse_graph_creation_with_progress_callback(
    self, mock_service_class, mock_tracker_class
  ):
    """Test SSE graph creation invokes progress callback."""
    from robosystems.tasks.graph_operations.create_graph import create_graph_sse_task

    mock_service = MagicMock()

    def create_with_progress(*args, **kwargs):
      progress_cb = kwargs.get("progress_callback")
      if progress_cb:
        progress_cb("Creating database...", 30)
        progress_cb("Installing schema...", 60)
      return {"graph_id": "kg123456", "status": "created"}

    mock_service.create_graph.side_effect = create_with_progress
    mock_service_class.return_value = mock_service

    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    task_data = {
      "user_id": "user-123",
      "skip_billing": True,
    }

    create_graph_sse_task.apply(args=[task_data, "op-123"]).get()  # type: ignore[attr-defined]

    mock_tracker.emit_progress.assert_any_call("Creating database...", 30)
    mock_tracker.emit_progress.assert_any_call("Installing schema...", 60)

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_sse_graph_creation_error(self, mock_service_class, mock_tracker_class):
    """Test SSE graph creation handles errors with progress tracking."""
    from robosystems.tasks.graph_operations.create_graph import create_graph_sse_task

    mock_service = MagicMock()
    error = ValueError("Invalid configuration")
    mock_service.create_graph.side_effect = error
    mock_service_class.return_value = mock_service

    mock_tracker = MagicMock()
    mock_tracker_class.return_value = mock_tracker

    task_data = {
      "user_id": "user-123",
      "metadata": {"graph_name": "Test Graph"},
    }

    with pytest.raises(ValueError, match="Invalid configuration"):
      create_graph_sse_task.apply(args=[task_data, "op-123"]).get()  # type: ignore[attr-defined]

    mock_tracker.emit_error.assert_called_once()
    call_args = mock_tracker.emit_error.call_args
    assert call_args[0][0] == error
    assert call_args[1]["additional_context"]["user_id"] == "user-123"
    assert call_args[1]["additional_context"]["graph_name"] == "Test Graph"

  @patch("robosystems.middleware.sse.task_progress.TaskSSEProgressTracker")
  @patch("robosystems.tasks.graph_operations.create_graph.GenericGraphServiceSync")
  def test_sse_graph_creation_cancellation(
    self, mock_service_class, mock_tracker_class
  ):
    """Test SSE graph creation handles cancellation."""
    from robosystems.tasks.graph_operations.create_graph import create_graph_sse_task

    mock_service = MagicMock()
    mock_service_class.return_value = mock_service

    mock_tracker = MagicMock()
    mock_tracker.check_cancellation.side_effect = Exception("Task cancelled")
    mock_tracker_class.return_value = mock_tracker

    def check_cancel(*args, **kwargs):
      callback = kwargs.get("cancellation_callback")
      if callback:
        callback()
      return {"graph_id": "kg123456", "status": "created"}

    mock_service.create_graph.side_effect = check_cancel

    task_data = {"user_id": "user-123"}

    with pytest.raises(Exception, match="Task cancelled"):
      create_graph_sse_task.apply(args=[task_data, "op-123"]).get()  # type: ignore[attr-defined]

    mock_tracker.emit_error.assert_called_once()
