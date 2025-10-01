"""Simple working tests for allocation manager."""

import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from robosystems.middleware.graph.allocation_manager import (
  KuzuAllocationManager,
  DatabaseStatus,
  InstanceStatus,
  DatabaseLocation,
  InstanceInfo,
)


class TestAllocationManagerBasic:
  """Basic tests for KuzuAllocationManager."""

  @pytest.fixture
  def mock_dynamodb(self):
    """Create a mock DynamoDB client."""
    client = MagicMock()
    client.get_item = MagicMock()
    client.put_item = MagicMock()
    client.update_item = MagicMock()
    client.delete_item = MagicMock()
    client.scan = MagicMock()
    client.query = MagicMock()
    return client

  @pytest.fixture
  def allocation_manager(self, mock_dynamodb):
    """Create a KuzuAllocationManager instance."""
    with patch("robosystems.middleware.graph.allocation_manager.boto3") as mock_boto3:
      mock_boto3.client.return_value = mock_dynamodb

      # Mock the initialization properly
      with patch.object(
        KuzuAllocationManager, "__init__", lambda x, environment="test": None
      ):
        manager = KuzuAllocationManager(environment="test")
        manager.dynamodb_client = mock_dynamodb
        manager.table_name = "test-allocations"
        manager.instances = {}
        manager._lock = MagicMock()
        manager.environment = "test"
        return manager

  def test_database_status_enum(self):
    """Test DatabaseStatus enum values."""
    assert DatabaseStatus.ACTIVE.value == "active"
    assert DatabaseStatus.CREATING.value == "creating"
    assert DatabaseStatus.MIGRATING.value == "migrating"
    assert DatabaseStatus.FAILED.value == "failed"
    assert DatabaseStatus.DELETED.value == "deleted"

  def test_instance_status_enum(self):
    """Test InstanceStatus enum values."""
    assert InstanceStatus.HEALTHY.value == "healthy"
    assert InstanceStatus.UNHEALTHY.value == "unhealthy"
    assert InstanceStatus.TERMINATING.value == "terminating"

  def test_database_location_creation(self):
    """Test DatabaseLocation dataclass."""
    location = DatabaseLocation(
      graph_id="db123",
      instance_id="i-1234567890",
      private_ip="10.0.1.100",
      availability_zone="us-east-1a",
      created_at=datetime.now(timezone.utc),
      status=DatabaseStatus.ACTIVE,
    )

    assert location.instance_id == "i-1234567890"
    assert location.graph_id == "db123"
    assert location.private_ip == "10.0.1.100"

  def test_instance_info_creation(self):
    """Test InstanceInfo dataclass."""
    info = InstanceInfo(
      instance_id="i-1234567890",
      private_ip="10.0.1.100",
      availability_zone="us-east-1a",
      status=InstanceStatus.HEALTHY,
      database_count=5,
      max_databases=10,
      created_at=datetime.now(timezone.utc),
    )

    assert info.instance_id == "i-1234567890"
    assert info.status == InstanceStatus.HEALTHY
    assert info.max_databases == 10
    assert info.database_count == 5
    assert info.available_capacity == 5
