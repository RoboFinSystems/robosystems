"""Tests for create_graph Celery task."""

import pytest
from unittest.mock import MagicMock, patch

from robosystems.tasks.graph_operations.create_graph import create_graph_task


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
    assert call_args["tier"] == "kuzu-standard"  # Default tier
    assert call_args["initial_data"] is None
    assert call_args["custom_schema"] is None
