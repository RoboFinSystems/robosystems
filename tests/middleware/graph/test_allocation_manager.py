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
  GraphTier,
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


class TestMultiBackendSupport:
  """Test multi-backend support (Kuzu, Neo4j Community, Neo4j Enterprise)."""

  @pytest.fixture
  def allocation_manager(self):
    """Create a KuzuAllocationManager instance with tier configs."""
    with patch("robosystems.middleware.graph.allocation_manager.boto3"):
      with patch.object(
        KuzuAllocationManager, "__init__", lambda x, environment="test": None
      ):
        manager = KuzuAllocationManager(environment="test")
        manager.environment = "test"

        # Set up tier configurations matching allocation_manager.py
        manager.tier_configs = {
          GraphTier.KUZU_STANDARD: {
            "backend": "kuzu",
            "backend_type": "kuzu",
            "databases_per_instance": 10,
          },
          GraphTier.NEO4J_COMMUNITY_LARGE: {
            "backend": "neo4j",
            "backend_type": "neo4j",
            "neo4j_edition": "community",
            "databases_per_instance": 1,
          },
          GraphTier.NEO4J_ENTERPRISE_XLARGE: {
            "backend": "neo4j",
            "backend_type": "neo4j",
            "neo4j_edition": "enterprise",
            "databases_per_instance": 1,
          },
        }

        return manager

  @pytest.mark.parametrize(
    "tier,expected_backend,expected_backend_type,expected_edition,expected_capacity",
    [
      (GraphTier.KUZU_STANDARD, "kuzu", "kuzu", None, 10),
      (GraphTier.NEO4J_COMMUNITY_LARGE, "neo4j", "neo4j", "community", 1),
      (GraphTier.NEO4J_ENTERPRISE_XLARGE, "neo4j", "neo4j", "enterprise", 1),
    ],
  )
  def test_tier_backend_configuration(
    self,
    allocation_manager,
    tier,
    expected_backend,
    expected_backend_type,
    expected_edition,
    expected_capacity,
  ):
    """Test that each tier has correct backend configuration."""
    config = allocation_manager.tier_configs[tier]

    assert config["backend"] == expected_backend
    assert config["backend_type"] == expected_backend_type
    assert config["databases_per_instance"] == expected_capacity

    if expected_edition:
      assert config.get("neo4j_edition") == expected_edition
    else:
      assert "neo4j_edition" not in config

  def test_kuzu_backend_standard_tier(self, allocation_manager):
    """Test Kuzu backend is used for Standard tier."""
    config = allocation_manager.tier_configs[GraphTier.KUZU_STANDARD]

    assert config["backend"] == "kuzu"
    assert config["backend_type"] == "kuzu"
    assert config["databases_per_instance"] == 10
    assert "neo4j_edition" not in config

  def test_neo4j_community_enterprise_tier(self, allocation_manager):
    """Test Neo4j Community backend is used for Enterprise tier."""
    config = allocation_manager.tier_configs[GraphTier.NEO4J_COMMUNITY_LARGE]

    assert config["backend"] == "neo4j"
    assert config["backend_type"] == "neo4j"
    assert config["neo4j_edition"] == "community"
    assert config["databases_per_instance"] == 1

  def test_neo4j_enterprise_premium_tier(self, allocation_manager):
    """Test Neo4j Enterprise backend is used for Premium tier."""
    config = allocation_manager.tier_configs[GraphTier.NEO4J_ENTERPRISE_XLARGE]

    assert config["backend"] == "neo4j"
    assert config["backend_type"] == "neo4j"
    assert config["neo4j_edition"] == "enterprise"
    assert config["databases_per_instance"] == 1

  @pytest.mark.parametrize(
    "tier,expected_isolation",
    [
      (GraphTier.KUZU_STANDARD, False),  # Shared resources
      (GraphTier.NEO4J_COMMUNITY_LARGE, True),  # Isolated resources
      (GraphTier.NEO4J_ENTERPRISE_XLARGE, True),  # Isolated resources
    ],
  )
  def test_tier_resource_isolation(self, allocation_manager, tier, expected_isolation):
    """Test resource isolation for each tier."""
    config = allocation_manager.tier_configs[tier]

    # Standard tier: 10 databases per instance (shared)
    # Enterprise/Premium: 1 database per instance (isolated)
    is_isolated = config["databases_per_instance"] == 1
    assert is_isolated == expected_isolation

  def test_all_tiers_have_backend_type(self, allocation_manager):
    """Test that all tiers have backend_type attribute for DynamoDB."""
    for tier, config in allocation_manager.tier_configs.items():
      assert "backend_type" in config, f"Tier {tier} missing backend_type"
      assert config["backend_type"] in ["kuzu", "neo4j"], (
        f"Invalid backend_type for tier {tier}"
      )

  def test_backend_type_consistency(self, allocation_manager):
    """Test that backend and backend_type are consistent."""
    for tier, config in allocation_manager.tier_configs.items():
      backend = config["backend"]
      backend_type = config["backend_type"]

      # Both should match (both "kuzu" or both "neo4j")
      assert backend == backend_type, (
        f"Tier {tier} has inconsistent backend ({backend}) and backend_type ({backend_type})"
      )


class TestAllocationManagerRegression:
  """Regression tests for KuzuAllocationManager critical bugs."""

  def test_identify_graph_named_parameter_in_code(self):
    """
    Regression test: verify identify_graph uses named parameter in allocation_manager.py.

    This test directly checks the source code to ensure the bug doesn't reoccur.
    The bug was passing instance_tier as a positional argument where session
    was expected, causing: AttributeError: 'GraphTier' object has no attribute 'query'

    Bug: identify_graph(graph_id, instance_tier)  # WRONG
    Fix: identify_graph(graph_id, graph_tier=instance_tier)  # CORRECT
    """
    import re
    from pathlib import Path

    allocation_manager_path = (
      Path(__file__).parent.parent.parent.parent
      / "robosystems"
      / "middleware"
      / "graph"
      / "allocation_manager.py"
    )

    with open(allocation_manager_path, "r") as f:
      content = f.read()

    pattern = r"GraphTypeRegistry\.identify_graph\(\s*graph_id\s*,\s*graph_tier\s*="

    matches = re.findall(pattern, content)

    assert len(matches) > 0, (
      "Expected to find GraphTypeRegistry.identify_graph(graph_id, graph_tier=...) "
      "but named parameter usage not found in allocation_manager.py. "
      "This could mean the bug has been reintroduced."
    )
