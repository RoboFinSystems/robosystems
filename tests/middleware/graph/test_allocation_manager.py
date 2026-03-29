"""Simple working tests for allocation manager."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from robosystems.middleware.graph.allocation_manager import (
  VALID_ENTITY_ID_PATTERN,
  VALID_INSTANCE_ID_PATTERN,
  DatabaseLocation,
  DatabaseStatus,
  GraphTier,
  InstanceInfo,
  InstanceStatus,
  LadybugAllocationManager,
)


class TestAllocationManagerBasic:
  """Basic tests for LadybugAllocationManager."""

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
    """Create a LadybugAllocationManager instance."""
    with patch("robosystems.middleware.graph.allocation_manager.boto3") as mock_boto3:
      mock_boto3.client.return_value = mock_dynamodb

      # Mock the initialization properly
      with patch.object(
        LadybugAllocationManager, "__init__", lambda x, environment="test": None
      ):
        manager = LadybugAllocationManager(environment="test")
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
      created_at=datetime.now(UTC),
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
      created_at=datetime.now(UTC),
    )

    assert info.instance_id == "i-1234567890"
    assert info.status == InstanceStatus.HEALTHY
    assert info.max_databases == 10
    assert info.database_count == 5
    assert info.available_capacity == 5


class TestMultiBackendSupport:
  """Test backend configuration for LadybugDB tiers."""

  @pytest.fixture
  def allocation_manager(self):
    """Create a LadybugAllocationManager instance with tier configs."""
    with patch("robosystems.middleware.graph.allocation_manager.boto3"):
      with patch.object(
        LadybugAllocationManager, "__init__", lambda x, environment="test": None
      ):
        manager = LadybugAllocationManager(environment="test")
        manager.environment = "test"

        # Set up tier configurations matching allocation_manager.py
        manager.tier_configs = {
          GraphTier.LADYBUG_STANDARD: {
            "backend": "ladybug",
            "backend_type": "ladybug",
            "databases_per_instance": 1,
          },
        }

        return manager

  def test_lbug_backend_standard_tier(self, allocation_manager):
    """Test LadybugDB backend is used for Standard tier."""
    config = allocation_manager.tier_configs[GraphTier.LADYBUG_STANDARD]

    assert config["backend"] == "ladybug"
    assert config["backend_type"] == "ladybug"
    assert config["databases_per_instance"] == 1

  def test_all_tiers_have_backend_type(self, allocation_manager):
    """Test that all tiers have backend_type attribute for DynamoDB."""
    for tier, config in allocation_manager.tier_configs.items():
      assert "backend_type" in config, f"Tier {tier} missing backend_type"
      assert config["backend_type"] == "ladybug", (
        f"Invalid backend_type for tier {tier}"
      )

  def test_backend_type_consistency(self, allocation_manager):
    """Test that backend and backend_type are consistent."""
    for tier, config in allocation_manager.tier_configs.items():
      backend = config["backend"]
      backend_type = config["backend_type"]

      assert backend == backend_type, (
        f"Tier {tier} has inconsistent backend ({backend}) and backend_type ({backend_type})"
      )


class TestAllocationManagerRegression:
  """Regression tests for LadybugAllocationManager critical bugs."""

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

    with open(allocation_manager_path) as f:
      content = f.read()

    pattern = r"GraphTypeRegistry\.identify_graph\(\s*graph_id\s*,\s*graph_tier\s*="

    matches = re.findall(pattern, content)

    assert len(matches) > 0, (
      "Expected to find GraphTypeRegistry.identify_graph(graph_id, graph_tier=...) "
      "but named parameter usage not found in allocation_manager.py. "
      "This could mean the bug has been reintroduced."
    )


# ---------------------------------------------------------------------------
# Helper to build botocore ClientError instances for mock assertions
# ---------------------------------------------------------------------------


def _client_error(code="ConditionalCheckFailedException", message="error"):
  """Create a botocore ClientError for testing."""
  return ClientError({"Error": {"Code": code, "Message": message}}, "operation_name")


def _make_manager(**overrides):
  """
  Create a LadybugAllocationManager with mocked internals.

  Returns a fully-wired manager instance suitable for unit tests.
  Every DynamoDB table, AWS client, and internal attribute is a MagicMock
  unless explicitly provided via ``overrides``.
  """
  with patch.object(
    LadybugAllocationManager, "__init__", lambda x, environment="test": None
  ):
    manager = LadybugAllocationManager(environment="test")
    manager.environment = overrides.get("environment", "test")
    manager.graph_table = overrides.get("graph_table", MagicMock())
    manager.instance_table = overrides.get("instance_table", MagicMock())
    manager.volume_table = overrides.get("volume_table", MagicMock())
    manager.autoscaling = overrides.get("autoscaling", MagicMock())
    manager.cloudwatch = overrides.get("cloudwatch", MagicMock())
    manager.max_databases_per_instance = overrides.get("max_databases_per_instance", 10)
    manager.tier_configs = overrides.get(
      "tier_configs",
      {
        GraphTier.LADYBUG_STANDARD: {
          "backend": "ladybug",
          "backend_type": "ladybug",
          "databases_per_instance": 10,
        },
        GraphTier.LADYBUG_LARGE: {
          "backend": "ladybug",
          "backend_type": "ladybug",
          "databases_per_instance": 1,
        },
        GraphTier.LADYBUG_XLARGE: {
          "backend": "ladybug",
          "backend_type": "ladybug",
          "databases_per_instance": 1,
        },
        GraphTier.LADYBUG_SHARED: {
          "backend": "ladybug",
          "backend_type": "ladybug",
          "databases_per_instance": 1,
        },
      },
    )
    manager._scale_up_timestamps = overrides.get("_scale_up_timestamps", {})
    manager.default_asg_name = overrides.get(
      "default_asg_name", "robosystems-ladybug-standard-writers-test-asg"
    )
    manager._lock = MagicMock()
    manager.instances = {}
    manager.dynamodb_client = MagicMock()
    manager.table_name = "test-allocations"
    return manager


# ===========================================================================
# get_tier_config
# ===========================================================================


class TestGetTierConfig:
  """Tests for LadybugAllocationManager.get_tier_config."""

  @pytest.mark.unit
  def test_returns_config_for_known_tier(self):
    """Known tier returns its own config dict."""
    manager = _make_manager()
    config = manager.get_tier_config(GraphTier.LADYBUG_LARGE)
    assert config["databases_per_instance"] == 1
    assert config["backend_type"] == "ladybug"

  @pytest.mark.unit
  def test_falls_back_to_standard_for_unknown_tier(self):
    """An unknown tier that is not in tier_configs falls back to LADYBUG_STANDARD."""
    manager = _make_manager()
    # Remove LADYBUG_LARGE so we can test fallback if we had an unknown key
    # Since GraphTier is an enum we cannot create a truly unknown member,
    # but we CAN remove an entry and verify the fallback works via dict.get.
    del manager.tier_configs[GraphTier.LADYBUG_LARGE]
    config = manager.get_tier_config(GraphTier.LADYBUG_LARGE)
    assert config is manager.tier_configs[GraphTier.LADYBUG_STANDARD]

  @pytest.mark.unit
  def test_standard_tier_matches_max_databases_setting(self):
    """Standard tier databases_per_instance should reflect the manager setting."""
    manager = _make_manager(max_databases_per_instance=25)
    manager.tier_configs[GraphTier.LADYBUG_STANDARD]["databases_per_instance"] = 25
    config = manager.get_tier_config(GraphTier.LADYBUG_STANDARD)
    assert config["databases_per_instance"] == 25


# ===========================================================================
# find_database_location
# ===========================================================================


class TestFindDatabaseLocation:
  """Tests for LadybugAllocationManager.find_database_location."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_location_for_existing_graph(self):
    """A graph that exists in DynamoDB returns a fully-populated DatabaseLocation."""
    manager = _make_manager()
    now_iso = datetime.now(UTC).isoformat()
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kgaabbccdd11223344",
        "instance_id": "i-0123456789abcdef0",
        "private_ip": "10.0.1.50",
        "availability_zone": "us-east-1a",
        "created_at": now_iso,
        "status": "active",
        "backend_type": "ladybug",
      }
    }
    manager.graph_table.update_item.return_value = None

    with patch(
      "robosystems.middleware.graph.allocation_manager.parse_subgraph_id",
      return_value=None,
    ):
      location = await manager.find_database_location("kgaabbccdd11223344")

    assert location is not None
    assert location.graph_id == "kgaabbccdd11223344"
    assert location.instance_id == "i-0123456789abcdef0"
    assert location.private_ip == "10.0.1.50"
    assert location.status == DatabaseStatus.ACTIVE

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_none_when_graph_not_found(self):
    """When graph_table has no Item key, returns None."""
    manager = _make_manager()
    manager.graph_table.get_item.return_value = {}  # No "Item" key

    with patch(
      "robosystems.middleware.graph.allocation_manager.parse_subgraph_id",
      return_value=None,
    ):
      location = await manager.find_database_location("kgaabbccdd11223344")

    assert location is None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_subgraph_resolves_to_parent_location(self):
    """Subgraph graph_id resolves through the parent's DynamoDB entry."""
    manager = _make_manager()
    now_iso = datetime.now(UTC).isoformat()

    # First call with subgraph returns SubgraphInfo, second call with parent returns None
    subgraph_info = MagicMock()
    subgraph_info.parent_graph_id = "kgaabbccdd11223344"

    # graph_table.get_item returns parent entry on the recursive call
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kgaabbccdd11223344",
        "instance_id": "i-0123456789abcdef0",
        "private_ip": "10.0.1.50",
        "availability_zone": "us-east-1a",
        "created_at": now_iso,
        "status": "active",
        "backend_type": "ladybug",
      }
    }
    manager.graph_table.update_item.return_value = None

    with patch(
      "robosystems.middleware.graph.allocation_manager.parse_subgraph_id",
      side_effect=[subgraph_info, None],
    ):
      location = await manager.find_database_location("kgaabbccdd11223344_dev")

    assert location is not None
    assert location.graph_id == "kgaabbccdd11223344_dev"
    assert location.instance_id == "i-0123456789abcdef0"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_subgraph_returns_none_when_parent_not_found(self):
    """If parent graph is not found, subgraph resolution returns None."""
    manager = _make_manager()
    subgraph_info = MagicMock()
    subgraph_info.parent_graph_id = "kgaabbccdd11223344"

    manager.graph_table.get_item.return_value = {}  # Parent not found

    with patch(
      "robosystems.middleware.graph.allocation_manager.parse_subgraph_id",
      side_effect=[subgraph_info, None],
    ):
      location = await manager.find_database_location("kgaabbccdd11223344_dev")

    assert location is None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_private_ip_fallback_to_instance_table(self):
    """When graph_table Item has no private_ip, falls back to instance_table."""
    manager = _make_manager()
    now_iso = datetime.now(UTC).isoformat()
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kgaabbccdd11223344",
        "instance_id": "i-0123456789abcdef0",
        # private_ip intentionally missing
        "availability_zone": "us-east-1a",
        "created_at": now_iso,
        "status": "active",
      }
    }
    manager.instance_table.get_item.return_value = {
      "Item": {
        "instance_id": "i-0123456789abcdef0",
        "private_ip": "10.0.2.99",
        "availability_zone": "us-east-1b",
      }
    }
    manager.graph_table.update_item.return_value = None

    with patch(
      "robosystems.middleware.graph.allocation_manager.parse_subgraph_id",
      return_value=None,
    ):
      location = await manager.find_database_location("kgaabbccdd11223344")

    assert location is not None
    assert location.private_ip == "10.0.2.99"
    assert location.availability_zone == "us-east-1b"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_none_when_private_ip_unresolvable(self):
    """If neither graph_table nor instance_table has private_ip, returns None."""
    manager = _make_manager()
    now_iso = datetime.now(UTC).isoformat()
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kgaabbccdd11223344",
        "instance_id": "i-0123456789abcdef0",
        # no private_ip
        "created_at": now_iso,
        "status": "active",
      }
    }
    # instance_table also has no item
    manager.instance_table.get_item.return_value = {}
    manager.graph_table.update_item.return_value = None

    with patch(
      "robosystems.middleware.graph.allocation_manager.parse_subgraph_id",
      return_value=None,
    ):
      location = await manager.find_database_location("kgaabbccdd11223344")

    assert location is None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_none_on_client_error(self):
    """DynamoDB ClientError during get_item returns None gracefully."""
    manager = _make_manager()
    manager.graph_table.get_item.side_effect = _client_error(
      "InternalServerError", "boom"
    )

    with patch(
      "robosystems.middleware.graph.allocation_manager.parse_subgraph_id",
      return_value=None,
    ):
      location = await manager.find_database_location("kgaabbccdd11223344")

    assert location is None


# ===========================================================================
# deallocate_database
# ===========================================================================


class TestDeallocateDatabase:
  """Tests for LadybugAllocationManager.deallocate_database."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_successful_deallocation(self):
    """Normal deallocation marks deleted and decrements count."""
    manager = _make_manager()
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kgaabbccdd11223344",
        "instance_id": "i-0123456789abcdef0",
        "status": "active",
        "entity_id": "user123",
      }
    }
    manager.graph_table.update_item.return_value = None
    manager.instance_table.update_item.return_value = None

    result = await manager.deallocate_database("kgaabbccdd11223344")
    assert result is True
    manager.graph_table.update_item.assert_called_once()
    manager.instance_table.update_item.assert_called_once()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_deallocation_returns_false_when_not_found(self):
    """If graph_table has no entry, returns False."""
    manager = _make_manager()
    manager.graph_table.get_item.return_value = {}

    result = await manager.deallocate_database("kgnonexistent0000000")
    assert result is False

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_deallocation_skips_already_deleted(self):
    """If database status is already 'deleted', returns True immediately."""
    manager = _make_manager()
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kgaabbccdd11223344",
        "instance_id": "i-0123456789abcdef0",
        "status": "deleted",
      }
    }

    result = await manager.deallocate_database("kgaabbccdd11223344")
    assert result is True
    # update_item should NOT be called since status is already deleted
    manager.graph_table.update_item.assert_not_called()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_deallocation_handles_concurrent_delete(self):
    """If another process already marked it deleted (ConditionalCheckFailedException), returns True."""
    manager = _make_manager()
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kgaabbccdd11223344",
        "instance_id": "i-0123456789abcdef0",
        "status": "active",
      }
    }
    # Mark-deleted step raises conditional check (another process deleted it first)
    manager.graph_table.update_item.side_effect = _client_error(
      "ConditionalCheckFailedException", "already deleted"
    )

    result = await manager.deallocate_database("kgaabbccdd11223344")
    assert result is True

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_deallocation_handles_zero_count_gracefully(self):
    """If instance count is already 0 during decrement, logs warning but still returns True."""
    manager = _make_manager()
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kgaabbccdd11223344",
        "instance_id": "i-0123456789abcdef0",
        "status": "active",
        "entity_id": "user123",
      }
    }
    manager.graph_table.update_item.return_value = None  # Mark-deleted succeeds
    # Decrement count fails because count is already 0
    manager.instance_table.update_item.side_effect = _client_error(
      "ConditionalCheckFailedException", "count already 0"
    )

    result = await manager.deallocate_database("kgaabbccdd11223344")
    assert result is True

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_deallocation_returns_false_on_unexpected_error(self):
    """DynamoDB ClientError at the top-level get_item returns False."""
    manager = _make_manager()
    manager.graph_table.get_item.side_effect = _client_error(
      "InternalServerError", "boom"
    )

    result = await manager.deallocate_database("kgaabbccdd11223344")
    assert result is False


# ===========================================================================
# get_instance_databases
# ===========================================================================


class TestGetInstanceDatabases:
  """Tests for LadybugAllocationManager.get_instance_databases."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_graph_ids_from_query(self):
    """Successful query returns a list of graph_id strings."""
    manager = _make_manager()
    manager.graph_table.query.return_value = {
      "Items": [
        {"graph_id": "kgaabb0000000001"},
        {"graph_id": "kgaabb0000000002"},
      ]
    }

    result = await manager.get_instance_databases("i-0123456789abcdef0")
    assert result == ["kgaabb0000000001", "kgaabb0000000002"]

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_empty_list_on_error(self):
    """ClientError during query returns an empty list."""
    manager = _make_manager()
    manager.graph_table.query.side_effect = _client_error(
      "InternalServerError", "query failed"
    )

    result = await manager.get_instance_databases("i-0123456789abcdef0")
    assert result == []

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_raises_value_error_for_empty_instance_id(self):
    """Empty string raises ValueError."""
    manager = _make_manager()
    with pytest.raises(ValueError, match="Instance ID must be a non-empty string"):
      await manager.get_instance_databases("")

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_raises_value_error_for_invalid_instance_id_format(self):
    """Instance ID not matching AWS format raises ValueError."""
    manager = _make_manager()
    with pytest.raises(ValueError, match="Invalid instance ID format"):
      await manager.get_instance_databases("not-an-instance-id")

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_raises_value_error_for_none_instance_id(self):
    """None instance_id raises ValueError."""
    manager = _make_manager()
    with pytest.raises(ValueError, match="Instance ID must be a non-empty string"):
      await manager.get_instance_databases(None)


# ===========================================================================
# get_all_instances
# ===========================================================================


class TestGetAllInstances:
  """Tests for LadybugAllocationManager.get_all_instances."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_healthy_instances(self):
    """Successful scan returns Items list from DynamoDB."""
    manager = _make_manager()
    items = [
      {"instance_id": "i-aaa", "status": "healthy"},
      {"instance_id": "i-bbb", "status": "healthy"},
    ]
    manager.instance_table.scan.return_value = {"Items": items}

    result = await manager.get_all_instances()
    assert len(result) == 2
    assert result[0]["instance_id"] == "i-aaa"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_empty_list_on_error(self):
    """ClientError during scan returns empty list."""
    manager = _make_manager()
    manager.instance_table.scan.side_effect = _client_error(
      "InternalServerError", "scan failed"
    )

    result = await manager.get_all_instances()
    assert result == []

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_empty_list_when_no_items(self):
    """Scan with no items returns empty list."""
    manager = _make_manager()
    manager.instance_table.scan.return_value = {"Items": []}

    result = await manager.get_all_instances()
    assert result == []


# ===========================================================================
# get_allocation_metrics
# ===========================================================================


class TestGetAllocationMetrics:
  """Tests for LadybugAllocationManager.get_allocation_metrics."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_aggregates_metrics_correctly(self):
    """Metrics aggregate capacity and usage across instances."""
    manager = _make_manager()
    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-aaa",
          "status": "healthy",
          "max_databases": 10,
          "database_count": 8,
        },
        {
          "instance_id": "i-bbb",
          "status": "healthy",
          "max_databases": 10,
          "database_count": 2,
        },
      ]
    }

    metrics = await manager.get_allocation_metrics()
    assert metrics["total_instances"] == 2
    assert metrics["total_capacity"] == 20
    assert metrics["total_databases"] == 10
    assert metrics["overall_utilization_percent"] == 50.0
    assert metrics["scale_up_needed"] is False
    assert len(metrics["instances"]) == 2

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_scale_up_needed_above_80_percent(self):
    """scale_up_needed is True when overall utilization exceeds 80%."""
    manager = _make_manager()
    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-aaa",
          "status": "healthy",
          "max_databases": 10,
          "database_count": 9,
        },
      ]
    }

    metrics = await manager.get_allocation_metrics()
    assert metrics["overall_utilization_percent"] == 90.0
    assert metrics["scale_up_needed"] is True

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_empty_instances_returns_zero_utilization(self):
    """No instances results in 0% utilization and no scale_up_needed."""
    manager = _make_manager()
    manager.instance_table.scan.return_value = {"Items": []}

    metrics = await manager.get_allocation_metrics()
    assert metrics["total_instances"] == 0
    assert metrics["total_capacity"] == 0
    assert metrics["overall_utilization_percent"] == 0
    assert metrics["scale_up_needed"] is False

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_error_returns_error_dict(self):
    """ClientError raised during metric aggregation returns dict with 'error' key.

    Note: get_allocation_metrics catches ClientError at its own level.
    Since get_all_instances catches its own errors and returns [], we need
    the ClientError to come from the aggregation logic itself. We patch
    get_all_instances to raise ClientError directly to test this path.
    """
    manager = _make_manager()

    with patch.object(
      manager,
      "get_all_instances",
      side_effect=_client_error("InternalServerError", "scan failed"),
    ):
      metrics = await manager.get_allocation_metrics()

    assert "error" in metrics
    assert "timestamp" in metrics


# ===========================================================================
# check_tier_capacity
# ===========================================================================


class TestCheckTierCapacity:
  """Tests for LadybugAllocationManager.check_tier_capacity."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_ready_when_instance_available(self):
    """If _find_best_instance returns a result, capacity is 'ready'."""
    manager = _make_manager()
    now = datetime.now(UTC)
    # Scan returns an instance with available capacity
    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-0123456789abcdef0",
          "private_ip": "10.0.1.1",
          "availability_zone": "us-east-1a",
          "database_count": 2,
          "max_databases": 10,
          "created_at": now.isoformat(),
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        }
      ]
    }

    result = await manager.check_tier_capacity(GraphTier.LADYBUG_STANDARD)
    assert result == "ready"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_scalable_when_asg_has_headroom(self):
    """No instances but ASG has headroom returns 'scalable'."""
    manager = _make_manager()
    # No instances found
    manager.instance_table.scan.return_value = {"Items": []}
    # ASG has headroom
    manager.autoscaling.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [{"DesiredCapacity": 1, "MaxSize": 5}]
    }

    result = await manager.check_tier_capacity(GraphTier.LADYBUG_STANDARD)
    assert result == "scalable"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_at_capacity_when_no_headroom(self):
    """No instances and ASG at max returns 'at_capacity'."""
    manager = _make_manager()
    # No instances found
    manager.instance_table.scan.return_value = {"Items": []}
    # ASG at max
    manager.autoscaling.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [{"DesiredCapacity": 5, "MaxSize": 5}]
    }

    result = await manager.check_tier_capacity(GraphTier.LADYBUG_STANDARD)
    assert result == "at_capacity"


# ===========================================================================
# _find_best_instance
# ===========================================================================


class TestFindBestInstance:
  """Tests for LadybugAllocationManager._find_best_instance."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_selects_instance_with_most_capacity(self):
    """Returns the instance with the highest available_capacity."""
    manager = _make_manager()
    now_iso = datetime.now(UTC).isoformat()
    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-0000000000000aaaa",
          "private_ip": "10.0.1.1",
          "availability_zone": "us-east-1a",
          "database_count": 8,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
        {
          "instance_id": "i-0000000000000bbbb",
          "private_ip": "10.0.1.2",
          "availability_zone": "us-east-1b",
          "database_count": 2,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
      ]
    }

    best = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert best is not None
    assert best.instance_id == "i-0000000000000bbbb"
    assert best.available_capacity == 8

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_excludes_specified_instance(self):
    """Excluded instance is skipped even if it has the most capacity."""
    manager = _make_manager()
    now_iso = datetime.now(UTC).isoformat()
    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-0000000000000aaaa",
          "private_ip": "10.0.1.1",
          "availability_zone": "us-east-1a",
          "database_count": 0,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
        {
          "instance_id": "i-0000000000000bbbb",
          "private_ip": "10.0.1.2",
          "availability_zone": "us-east-1b",
          "database_count": 5,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
      ]
    }

    best = await manager._find_best_instance(
      GraphTier.LADYBUG_STANDARD, exclude_instance="i-0000000000000aaaa"
    )
    assert best is not None
    assert best.instance_id == "i-0000000000000bbbb"

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_none_when_all_at_capacity(self):
    """All instances full returns None."""
    manager = _make_manager()
    now_iso = datetime.now(UTC).isoformat()
    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-0000000000000aaaa",
          "private_ip": "10.0.1.1",
          "availability_zone": "us-east-1a",
          "database_count": 10,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
      ]
    }

    best = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert best is None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_none_when_no_instances(self):
    """Empty scan returns None."""
    manager = _make_manager()
    manager.instance_table.scan.return_value = {"Items": []}

    best = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert best is None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_returns_none_on_client_error(self):
    """ClientError during scan returns None."""
    manager = _make_manager()
    manager.instance_table.scan.side_effect = _client_error(
      "InternalServerError", "scan failed"
    )

    best = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert best is None

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_defaults_to_standard_tier_when_none(self):
    """Passing None for instance_tier defaults to ladybug-standard."""
    manager = _make_manager()
    now_iso = datetime.now(UTC).isoformat()
    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-0000000000000aaaa",
          "private_ip": "10.0.1.1",
          "availability_zone": "us-east-1a",
          "database_count": 0,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
      ]
    }

    best = await manager._find_best_instance(None)
    assert best is not None
    # Verify the scan used ladybug-standard
    call_kwargs = manager.instance_table.scan.call_args
    assert call_kwargs[1]["ExpressionAttributeValues"][":tier"] == "ladybug-standard"


# ===========================================================================
# _trigger_scale_up
# ===========================================================================


class TestTriggerScaleUp:
  """Tests for LadybugAllocationManager._trigger_scale_up."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_scale_up_increments_desired_capacity(self):
    """Successfully increments ASG desired capacity by 1."""
    manager = _make_manager()
    manager.autoscaling.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [{"DesiredCapacity": 2, "MaxSize": 5}]
    }
    manager.autoscaling.set_desired_capacity.return_value = {}

    result = await manager._trigger_scale_up(GraphTier.LADYBUG_STANDARD)
    assert result is True
    manager.autoscaling.set_desired_capacity.assert_called_once()
    call_kwargs = manager.autoscaling.set_desired_capacity.call_args[1]
    assert call_kwargs["DesiredCapacity"] == 3

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_scale_up_returns_false_at_max_capacity(self):
    """Already at max returns False without calling set_desired_capacity."""
    manager = _make_manager()
    manager.autoscaling.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [{"DesiredCapacity": 5, "MaxSize": 5}]
    }

    result = await manager._trigger_scale_up(GraphTier.LADYBUG_STANDARD)
    assert result is False
    manager.autoscaling.set_desired_capacity.assert_not_called()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_scale_up_rate_limited_within_5_minutes(self):
    """Second call within 5 minutes is rate-limited and returns False."""
    manager = _make_manager()
    # Set timestamp to recent (within 5 min)
    manager._scale_up_timestamps["ladybug-standard"] = datetime.now(UTC) - timedelta(
      seconds=60
    )

    result = await manager._trigger_scale_up(GraphTier.LADYBUG_STANDARD)
    assert result is False
    # describe_auto_scaling_groups should NOT be called when rate-limited
    manager.autoscaling.describe_auto_scaling_groups.assert_not_called()

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_scale_up_allowed_after_cooldown(self):
    """Call after 5-minute cooldown is allowed."""
    manager = _make_manager()
    # Set timestamp to 6 minutes ago
    manager._scale_up_timestamps["ladybug-standard"] = datetime.now(UTC) - timedelta(
      seconds=360
    )
    manager.autoscaling.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [{"DesiredCapacity": 2, "MaxSize": 5}]
    }
    manager.autoscaling.set_desired_capacity.return_value = {}

    result = await manager._trigger_scale_up(GraphTier.LADYBUG_STANDARD)
    assert result is True

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_scale_up_returns_false_when_asg_not_found(self):
    """ASG not found returns False."""
    manager = _make_manager()
    manager.autoscaling.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": []
    }

    result = await manager._trigger_scale_up(GraphTier.LADYBUG_STANDARD)
    assert result is False

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_scale_up_returns_false_on_client_error(self):
    """ClientError returns False."""
    manager = _make_manager()
    manager.autoscaling.describe_auto_scaling_groups.side_effect = _client_error(
      "AutoScalingGroupNotFound", "not found"
    )

    result = await manager._trigger_scale_up(GraphTier.LADYBUG_STANDARD)
    assert result is False


# ===========================================================================
# _get_stack_name_for_tier
# ===========================================================================


class TestGetStackNameForTier:
  """Tests for LadybugAllocationManager._get_stack_name_for_tier."""

  @pytest.mark.unit
  def test_returns_correct_stack_name_for_prod(self):
    """Prod environment returns proper stack name."""
    manager = _make_manager(environment="prod")
    result = manager._get_stack_name_for_tier("ladybug-standard")
    assert result == "RoboSystemsGraphLadybugStandardProd"

  @pytest.mark.unit
  def test_returns_correct_stack_name_for_staging(self):
    """Staging environment returns proper stack name."""
    manager = _make_manager(environment="staging")
    result = manager._get_stack_name_for_tier("ladybug-large")
    assert result == "RoboSystemsGraphLadybugLargeStaging"

  @pytest.mark.unit
  def test_returns_none_for_dev_environment(self):
    """Dev environment returns None (no CloudFormation stacks)."""
    manager = _make_manager(environment="dev")
    result = manager._get_stack_name_for_tier("ladybug-standard")
    assert result is None

  @pytest.mark.unit
  def test_returns_none_for_test_environment(self):
    """Test environment returns None."""
    manager = _make_manager(environment="test")
    result = manager._get_stack_name_for_tier("ladybug-standard")
    assert result is None

  @pytest.mark.unit
  def test_returns_none_for_unknown_tier(self):
    """Unknown tier name returns None even in prod."""
    manager = _make_manager(environment="prod")
    result = manager._get_stack_name_for_tier("ladybug-mega")
    assert result is None

  @pytest.mark.unit
  def test_all_known_tiers_have_stack_names_in_prod(self):
    """All four canonical tiers have stack names in prod."""
    manager = _make_manager(environment="prod")
    known_tiers = [
      "ladybug-standard",
      "ladybug-large",
      "ladybug-xlarge",
      "ladybug-shared",
    ]
    for tier in known_tiers:
      result = manager._get_stack_name_for_tier(tier)
      assert result is not None, f"Tier {tier} should have a stack name in prod"
      assert result.startswith("RoboSystemsGraph")


# ===========================================================================
# allocate_database (input validation)
# ===========================================================================


class TestAllocateDatabaseValidation:
  """Tests for allocate_database input validation paths."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_empty_entity_id_raises_value_error(self):
    """Empty entity_id raises ValueError."""
    manager = _make_manager()
    with pytest.raises(ValueError, match="Entity ID must be a non-empty string"):
      await manager.allocate_database(entity_id="")

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_invalid_entity_id_format_raises_value_error(self):
    """Entity ID with special characters raises ValueError."""
    manager = _make_manager()
    with pytest.raises(ValueError, match="Invalid entity ID format"):
      await manager.allocate_database(entity_id="user@evil; DROP TABLE")

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_subgraph_graph_id_raises_helpful_error(self):
    """Passing a subgraph-style graph_id raises ValueError with helpful message."""
    manager = _make_manager()

    with patch(
      "robosystems.middleware.graph.allocation_manager._get_valid_graph_id_regex"
    ) as mock_regex:
      # Make the regex NOT match (simulating a subgraph ID that fails validation)
      mock_compiled = MagicMock()
      mock_compiled.match.return_value = None
      mock_regex.return_value = mock_compiled

      with patch(
        "robosystems.middleware.graph.allocation_manager.is_subgraph_id",
        return_value=True,
      ):
        with pytest.raises(
          ValueError, match="Subgraph IDs are not stored in the DynamoDB registry"
        ):
          await manager.allocate_database(
            entity_id="user123",
            graph_id="kgaabbccdd11223344_dev",
          )

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_invalid_graph_id_format_raises_value_error(self):
    """Invalid graph_id that is not a subgraph raises a generic ValueError."""
    manager = _make_manager()

    with patch(
      "robosystems.middleware.graph.allocation_manager._get_valid_graph_id_regex"
    ) as mock_regex:
      mock_compiled = MagicMock()
      mock_compiled.match.return_value = None
      mock_regex.return_value = mock_compiled

      with patch(
        "robosystems.middleware.graph.allocation_manager.is_subgraph_id",
        return_value=False,
      ):
        with pytest.raises(ValueError, match="Invalid graph ID format"):
          await manager.allocate_database(
            entity_id="user123",
            graph_id="bad!!!id",
          )


# ===========================================================================
# Validation pattern tests
# ===========================================================================


class TestValidationPatterns:
  """Tests for compiled regex patterns used in input validation."""

  @pytest.mark.unit
  def test_valid_entity_id_patterns(self):
    """Valid entity IDs match the pattern."""
    valid_ids = ["user123", "my-entity", "abc_def", "A", "a1b2c3"]
    for eid in valid_ids:
      assert VALID_ENTITY_ID_PATTERN.match(eid), f"Expected '{eid}' to be valid"

  @pytest.mark.unit
  def test_invalid_entity_id_patterns(self):
    """Invalid entity IDs do not match the pattern."""
    invalid_ids = ["", "user@evil", "has space", "semi;colon", "a" * 129]
    for eid in invalid_ids:
      assert not VALID_ENTITY_ID_PATTERN.match(eid), f"Expected '{eid}' to be invalid"

  @pytest.mark.unit
  def test_valid_instance_id_patterns(self):
    """Valid AWS instance IDs match the pattern."""
    valid_ids = ["i-01234567", "i-0123456789abcdef0"]
    for iid in valid_ids:
      assert VALID_INSTANCE_ID_PATTERN.match(iid), f"Expected '{iid}' to be valid"

  @pytest.mark.unit
  def test_invalid_instance_id_patterns(self):
    """Invalid instance IDs do not match the pattern."""
    invalid_ids = ["", "i-short", "i-UPPERCASE", "not-an-id", "i-01234567890123456789"]
    for iid in invalid_ids:
      assert not VALID_INSTANCE_ID_PATTERN.match(iid), f"Expected '{iid}' to be invalid"


# ===========================================================================
# InstanceInfo property tests
# ===========================================================================


class TestInstanceInfoProperties:
  """Additional property tests for InstanceInfo dataclass."""

  @pytest.mark.unit
  def test_utilization_percent_at_zero(self):
    """Zero databases means 0% utilization."""
    info = InstanceInfo(
      instance_id="i-01234567",
      private_ip="10.0.0.1",
      availability_zone="us-east-1a",
      status=InstanceStatus.HEALTHY,
      database_count=0,
      max_databases=10,
      created_at=datetime.now(UTC),
    )
    assert info.utilization_percent == 0.0

  @pytest.mark.unit
  def test_utilization_percent_at_full(self):
    """Full capacity means 100% utilization."""
    info = InstanceInfo(
      instance_id="i-01234567",
      private_ip="10.0.0.1",
      availability_zone="us-east-1a",
      status=InstanceStatus.HEALTHY,
      database_count=10,
      max_databases=10,
      created_at=datetime.now(UTC),
    )
    assert info.utilization_percent == 100.0

  @pytest.mark.unit
  def test_utilization_percent_with_zero_max(self):
    """Zero max_databases yields 0% (no division by zero)."""
    info = InstanceInfo(
      instance_id="i-01234567",
      private_ip="10.0.0.1",
      availability_zone="us-east-1a",
      status=InstanceStatus.HEALTHY,
      database_count=0,
      max_databases=0,
      created_at=datetime.now(UTC),
    )
    assert info.utilization_percent == 0.0

  @pytest.mark.unit
  def test_available_capacity_never_negative(self):
    """available_capacity clamps to 0 even if database_count exceeds max_databases."""
    info = InstanceInfo(
      instance_id="i-01234567",
      private_ip="10.0.0.1",
      availability_zone="us-east-1a",
      status=InstanceStatus.HEALTHY,
      database_count=15,
      max_databases=10,
      created_at=datetime.now(UTC),
    )
    assert info.available_capacity == 0
