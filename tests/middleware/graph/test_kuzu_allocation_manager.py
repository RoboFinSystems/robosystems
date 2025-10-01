"""Tests for KuzuAllocationManager."""

import pytest
from unittest.mock import MagicMock, patch

from robosystems.middleware.graph.allocation_manager import KuzuAllocationManager
from robosystems.middleware.graph.subgraph_utils import parse_subgraph_id


class TestKuzuAllocationManager:
  """Test suite for KuzuAllocationManager."""

  def setup_method(self):
    """Set up test fixtures."""
    self.environment = "test"
    self.max_databases = 50

    # Mock DynamoDB
    self.mock_dynamodb = MagicMock()
    self.mock_graph_table = MagicMock()
    self.mock_instance_table = MagicMock()

    # Patch the get_dynamodb_resource function
    self.patcher = patch(
      "robosystems.middleware.graph.allocation_manager.get_dynamodb_resource"
    )
    self.mock_get_dynamodb = self.patcher.start()

    # Configure the mock to return our mock dynamodb resource
    mock_dynamodb_resource = MagicMock()
    mock_dynamodb_resource.Table.side_effect = lambda name: (
      self.mock_graph_table if "graph-registry" in name else self.mock_instance_table
    )
    self.mock_get_dynamodb.return_value = mock_dynamodb_resource

    # Also patch boto3.client for autoscaling and cloudwatch
    self.boto3_patcher = patch(
      "robosystems.middleware.graph.allocation_manager.boto3.client"
    )
    self.mock_boto3_client = self.boto3_patcher.start()
    self.mock_autoscaling = MagicMock()
    self.mock_cloudwatch = MagicMock()

    def client_side_effect(service_name, **kwargs):
      if service_name == "autoscaling":
        return self.mock_autoscaling
      elif service_name == "cloudwatch":
        return self.mock_cloudwatch
      return MagicMock()

    self.mock_boto3_client.side_effect = client_side_effect

    self.manager = KuzuAllocationManager(
      environment=self.environment, max_databases_per_instance=self.max_databases
    )

  def teardown_method(self):
    """Clean up test fixtures."""
    self.patcher.stop()
    self.boto3_patcher.stop()

  def test_initialization(self):
    """Test manager initialization."""
    assert self.manager.environment == self.environment
    assert self.manager.max_databases_per_instance == self.max_databases
    assert self.manager.graph_table == self.mock_graph_table
    assert self.manager.instance_table == self.mock_instance_table

  @patch("time.time")
  @patch(
    "robosystems.middleware.graph.allocation_manager.KuzuAllocationManager._publish_allocation_metrics"
  )
  async def test_allocate_database_success(self, mock_publish_metrics, mock_time):
    """Test successful database allocation."""
    mock_time.return_value = 1234567890
    mock_publish_metrics.return_value = None

    # Mock finding available instance
    self.mock_instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-123456",
          "private_ip": "10.0.1.100",
          "availability_zone": "us-east-1a",
          "status": "healthy",
          "database_count": 25,
          "max_databases": self.max_databases,
          "created_at": "2023-01-01T00:00:00",
        }
      ]
    }

    # Mock successful database creation
    self.mock_graph_table.put_item.return_value = {}
    self.mock_instance_table.update_item.return_value = {}

    result = await self.manager.allocate_database("test-entity-123")

    assert result is not None
    # Graph ID should now be kg prefix with UUID
    assert result.graph_id.startswith("kg")
    assert len(result.graph_id) == 18  # kg + 16 hex chars
    assert result.instance_id == "i-123456"
    assert result.private_ip == "10.0.1.100"

    # Verify DynamoDB operations
    self.mock_graph_table.put_item.assert_called_once()
    self.mock_instance_table.update_item.assert_called_once()

  @patch(
    "robosystems.middleware.graph.allocation_manager.KuzuAllocationManager._trigger_scale_up"
  )
  async def test_allocate_database_no_capacity(self, mock_trigger_scale_up):
    """Test database allocation when no instances have capacity."""
    mock_trigger_scale_up.return_value = None

    # Mock no available instances (FilterExpression would filter these out)
    self.mock_instance_table.scan.return_value = {
      "Items": []  # No instances with capacity available
    }

    with pytest.raises(Exception, match="No Standard tier capacity available"):
      await self.manager.allocate_database("test-entity-123")

    # Should not create any database records
    self.mock_graph_table.put_item.assert_not_called()

  @patch(
    "robosystems.middleware.graph.allocation_manager.KuzuAllocationManager._trigger_scale_up"
  )
  async def test_allocate_database_no_instances(self, mock_trigger_scale_up):
    """Test database allocation when no instances exist."""
    mock_trigger_scale_up.return_value = None

    # Mock no instances
    self.mock_instance_table.scan.return_value = {"Items": []}

    with pytest.raises(Exception, match="No Standard tier capacity available"):
      await self.manager.allocate_database("test-entity-123")

    self.mock_graph_table.put_item.assert_not_called()

  @patch(
    "robosystems.middleware.graph.allocation_manager.KuzuAllocationManager._publish_allocation_metrics"
  )
  async def test_deallocate_database_success(self, mock_publish_metrics):
    """Test successful database deallocation."""
    mock_publish_metrics.return_value = None

    # Mock finding existing database
    self.mock_graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "test-entity-123",
        "instance_id": "i-123456",
        "private_ip": "10.0.1.100",
      }
    }

    # Mock successful update (marking as deleted)
    self.mock_graph_table.update_item.return_value = {}
    self.mock_instance_table.update_item.return_value = {}

    result = await self.manager.deallocate_database("test-entity-123")

    assert result is True
    self.mock_graph_table.update_item.assert_called_once()
    # Instance table update_item should be called too (decrementing count)
    assert self.mock_instance_table.update_item.call_count == 1

  async def test_deallocate_database_not_found(self):
    """Test database deallocation when database doesn't exist."""
    # Mock database not found
    self.mock_graph_table.get_item.return_value = {}

    result = await self.manager.deallocate_database("nonexistent-db")

    assert result is False
    self.mock_graph_table.update_item.assert_not_called()

  async def test_get_database_location_success(self):
    """Test successful database location lookup."""
    # Mock finding database
    self.mock_graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "test-entity-123",
        "instance_id": "i-123456",
        "private_ip": "10.0.1.100",
        "availability_zone": "us-east-1a",
        "created_at": "2023-01-01T00:00:00",
        "status": "active",
      }
    }

    result = await self.manager.find_database_location("test-entity-123")

    assert result is not None
    assert result.graph_id == "test-entity-123"
    assert result.instance_id == "i-123456"
    assert result.private_ip == "10.0.1.100"

  async def test_get_database_location_not_found(self):
    """Test database location lookup when database doesn't exist."""
    # Mock database not found
    self.mock_graph_table.get_item.return_value = {}

    result = await self.manager.find_database_location("nonexistent-db")

    assert result is None

  async def test_get_allocation_metrics(self):
    """Test allocation metrics retrieval."""
    # Mock instance data
    self.mock_instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-123456",
          "database_count": 25,
          "max_databases": 50,
          "status": "healthy",
          "private_ip": "10.0.1.100",
          "availability_zone": "us-east-1a",
          "created_at": "2023-01-01T00:00:00",
        },
        {
          "instance_id": "i-789012",
          "database_count": 40,
          "max_databases": 50,
          "status": "healthy",
          "private_ip": "10.0.1.200",
          "availability_zone": "us-east-1b",
          "created_at": "2023-01-01T00:00:00",
        },
      ]
    }

    metrics = await self.manager.get_allocation_metrics()

    assert metrics["total_instances"] == 2
    assert metrics["total_capacity"] == 100
    assert metrics["total_databases"] == 65
    assert metrics["overall_utilization_percent"] == 65.0
    assert metrics["scale_up_needed"] is False  # 65% < 80%

  async def test_get_allocation_metrics_no_instances(self):
    """Test allocation metrics when no instances exist."""
    # Mock no instances
    self.mock_instance_table.scan.return_value = {"Items": []}

    metrics = await self.manager.get_allocation_metrics()

    assert metrics["total_instances"] == 0
    assert metrics["total_capacity"] == 0
    assert metrics["overall_utilization_percent"] == 0.0

  def test_initialization_parameters(self):
    """Test that initialization parameters are properly set."""
    # Test that the manager sets all the expected attributes
    assert self.manager.environment == "test"
    assert self.manager.max_databases_per_instance == 50
    # The default ASG name is constructed based on the environment
    assert (
      self.manager.default_asg_name == "RoboSystemsKuzuWritersStandardTest-writers-asg"
    )


class TestKuzuAllocationManagerSubgraphs:
  """Test suite for KuzuAllocationManager subgraph functionality."""

  def setup_method(self):
    """Set up test fixtures."""
    self.environment = "test"
    self.max_databases = 50

    # Mock DynamoDB
    self.mock_dynamodb = MagicMock()
    self.mock_graph_table = MagicMock()
    self.mock_instance_table = MagicMock()

    # Patch the get_dynamodb_resource function
    self.patcher = patch(
      "robosystems.middleware.graph.allocation_manager.get_dynamodb_resource"
    )
    self.mock_get_dynamodb = self.patcher.start()

    # Configure the mock to return our mock dynamodb resource
    mock_dynamodb_resource = MagicMock()
    mock_dynamodb_resource.Table.side_effect = lambda name: (
      self.mock_graph_table if "graph-registry" in name else self.mock_instance_table
    )
    self.mock_get_dynamodb.return_value = mock_dynamodb_resource

    # Also patch boto3.client for autoscaling and cloudwatch
    self.boto3_patcher = patch(
      "robosystems.middleware.graph.allocation_manager.boto3.client"
    )
    self.mock_boto3_client = self.boto3_patcher.start()
    self.mock_autoscaling = MagicMock()
    self.mock_cloudwatch = MagicMock()

    def client_side_effect(service_name, **kwargs):
      if service_name == "autoscaling":
        return self.mock_autoscaling
      elif service_name == "cloudwatch":
        return self.mock_cloudwatch
      return MagicMock()

    self.mock_boto3_client.side_effect = client_side_effect

    self.manager = KuzuAllocationManager(
      environment=self.environment, max_databases_per_instance=self.max_databases
    )

  def teardown_method(self):
    """Clean up test fixtures."""
    self.patcher.stop()
    self.boto3_patcher.stop()

  async def test_find_database_location_subgraph(self):
    """Test finding location for a subgraph routes to parent."""
    # Parent graph exists
    parent_graph_id = "kg5f2e5e0da65d45d69645"
    subgraph_id = f"{parent_graph_id}_dev"

    # Mock parent graph location
    self.mock_graph_table.get_item.return_value = {
      "Item": {
        "graph_id": parent_graph_id,
        "instance_id": "i-parent123",
        "private_ip": "10.0.1.100",
        "availability_zone": "us-east-1a",
        "status": "active",
        "created_at": "2024-01-01T00:00:00",
      }
    }

    # Find location for subgraph
    location = await self.manager.find_database_location(subgraph_id)

    # Should return parent's location
    assert location is not None
    assert location.instance_id == "i-parent123"
    assert location.private_ip == "10.0.1.100"

    # The implementation first tries the subgraph ID directly
    self.mock_graph_table.get_item.assert_called_once_with(
      Key={"graph_id": subgraph_id}
    )

  async def test_find_database_location_subgraph_parent_not_found(self):
    """Test finding location for subgraph when parent doesn't exist."""
    parent_graph_id = "kg5f2e5e0da65d45d69645"
    subgraph_id = f"{parent_graph_id}_dev"

    # Mock parent not found
    self.mock_graph_table.get_item.return_value = {}

    # Find location for subgraph
    location = await self.manager.find_database_location(subgraph_id)

    # Should return None
    assert location is None

    # The implementation tries the subgraph ID directly
    self.mock_graph_table.get_item.assert_called_once_with(
      Key={"graph_id": subgraph_id}
    )

  async def test_allocate_database_for_subgraph_attempts_allocation(self):
    """Test that the allocate_database method doesn't block subgraphs."""
    # NOTE: The actual blocking of subgraph allocation happens at a higher level
    # The allocation manager itself doesn't check for subgraphs in allocate_database
    # This test verifies the current behavior
    subgraph_id = "kg5f2e5e0da65d45d69645_dev"

    # Mock no available instances - will cause allocation to fail
    self.mock_instance_table.scan.return_value = {"Items": []}

    # Mock autoscaling to prevent errors in scale-up path
    self.mock_autoscaling.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [
        {
          "DesiredCapacity": 2,
          "MaxSize": 10,
          "MinSize": 1,
        }
      ]
    }

    # Allocation will fail due to no capacity
    with pytest.raises(Exception, match="No Standard tier capacity available"):
      await self.manager.allocate_database(subgraph_id)

  def test_parse_subgraph_integration(self):
    """Test integration with subgraph parsing utilities."""
    # Test valid subgraph
    subgraph_id = "kg5f2e5e0da65d45d69645_dev"
    info = parse_subgraph_id(subgraph_id)

    assert info is not None
    assert info.parent_graph_id == "kg5f2e5e0da65d45d69645"
    assert info.subgraph_name == "dev"
    assert info.database_name == subgraph_id

    # Test non-subgraph
    parent_id = "kg5f2e5e0da65d45d69645"
    info = parse_subgraph_id(parent_id)
    assert info is None

    # Test shared repository (cannot be subgraph)
    shared_id = "sec"
    info = parse_subgraph_id(shared_id)
    assert info is None
