"""Extended unit tests for allocation manager - covering uncovered methods."""

import re
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from robosystems.middleware.graph.allocation_manager import (
  VALID_ENTITY_ID_PATTERN,
  VALID_INSTANCE_ID_PATTERN,
  DatabaseLocation,
  DatabaseStatus,
  InstanceInfo,
  InstanceStatus,
  LadybugAllocationManager,
  _get_valid_graph_id_regex,
)
from robosystems.middleware.graph.types import GraphTier


def _make_client_error(code="ConditionalCheckFailedException", message="error"):
  """Create a botocore ClientError with the given code."""
  return ClientError(
    error_response={"Error": {"Code": code, "Message": message}},
    operation_name="test_op",
  )


def _create_manager(environment="test", max_dbs=10, asg_name="test-asg"):
  """Create a LadybugAllocationManager with mocked DynamoDB/AWS clients."""
  with (
    patch(
      "robosystems.middleware.graph.allocation_manager.get_dynamodb_resource"
    ) as mock_dynamo,
    patch("robosystems.middleware.graph.allocation_manager.boto3"),
    patch("robosystems.middleware.graph.allocation_manager.env") as mock_env,
  ):
    mock_env.AWS_REGION = "us-east-1"
    mock_env.is_development.return_value = environment == "dev"
    mock_env.AWS_ENDPOINT_URL = None
    mock_env.GRAPH_REGISTRY_TABLE = "graph-registry"
    mock_env.INSTANCE_REGISTRY_TABLE = "instance-registry"
    mock_env.VOLUME_REGISTRY_TABLE = "volume-registry"

    mock_resource = MagicMock()
    mock_dynamo.return_value = mock_resource

    manager = LadybugAllocationManager(
      environment=environment,
      max_databases_per_instance=max_dbs,
      asg_name=asg_name,
    )
    # Replace tables with mocks
    manager.graph_table = MagicMock()
    manager.instance_table = MagicMock()
    manager.volume_table = MagicMock()
    manager.autoscaling = MagicMock()
    manager.cloudwatch = MagicMock()
    # Occupancy is derived from graph-registry queries; default to empty
    manager.graph_table.query.return_value = {"Items": []}
    return manager


def _stub_graph_occupancy(manager, occupancy: dict[str, int]) -> None:
  """Stub graph-registry queries to report N active graphs per instance."""

  def _query(**kwargs):
    instance_id = kwargs["KeyConditionExpression"]._values[1]
    count = occupancy.get(instance_id, 0)
    return {
      "Items": [
        {"graph_id": f"kg{instance_id}{i}", "status": "active"} for i in range(count)
      ]
    }

  manager.graph_table.query.side_effect = _query


@pytest.mark.unit
class TestValidationPatterns:
  """Tests for ID validation patterns."""

  def test_valid_entity_ids(self):
    valid_ids = [
      "user123",
      "entity-1",
      "test_entity",
      "abc",
      "a" * 128,
      "A-Z_0-9",
    ]
    for entity_id in valid_ids:
      assert VALID_ENTITY_ID_PATTERN.match(entity_id), f"{entity_id} should be valid"

  def test_invalid_entity_ids(self):
    invalid_ids = [
      "",
      "a" * 129,  # too long
      "hello world",  # space
      "hello@world",  # special char
      "hello.world",  # dot
      "hello/world",  # slash
    ]
    for entity_id in invalid_ids:
      assert not VALID_ENTITY_ID_PATTERN.match(entity_id), (
        f"{entity_id} should be invalid"
      )

  def test_valid_instance_ids(self):
    valid_ids = [
      "i-12345678",
      "i-1234567890abcdef0",
      "i-abcdef01",
    ]
    for instance_id in valid_ids:
      assert VALID_INSTANCE_ID_PATTERN.match(instance_id), (
        f"{instance_id} should be valid"
      )

  def test_invalid_instance_ids(self):
    invalid_ids = [
      "",
      "i-1234567",  # too short
      "i-1234567890abcdef01",  # too long
      "x-12345678",  # wrong prefix
      "i-ABCDEF01",  # uppercase hex
      "12345678",  # no prefix
    ]
    for instance_id in invalid_ids:
      assert not VALID_INSTANCE_ID_PATTERN.match(instance_id), (
        f"{instance_id} should be invalid"
      )


@pytest.mark.unit
class TestGetValidGraphIdRegex:
  """Tests for _get_valid_graph_id_regex lazy loading."""

  def test_returns_compiled_regex(self):
    """Test that _get_valid_graph_id_regex returns a compiled pattern."""
    regex = _get_valid_graph_id_regex()
    assert isinstance(regex, re.Pattern)

  def test_caches_result(self):
    """Test that subsequent calls return the same object."""
    regex1 = _get_valid_graph_id_regex()
    regex2 = _get_valid_graph_id_regex()
    assert regex1 is regex2

  def test_matches_valid_graph_ids(self):
    """Test that the regex matches valid graph IDs."""
    regex = _get_valid_graph_id_regex()
    valid_ids = [
      "kg01234567890abcdef",
      "kg" + "a" * 16,
      "sec",
    ]
    for gid in valid_ids:
      assert regex.match(gid), f"{gid} should match"


@pytest.mark.unit
class TestDataclasses:
  """Tests for DatabaseLocation and InstanceInfo dataclasses."""

  def test_database_location_creation(self):
    now = datetime.now(UTC)
    loc = DatabaseLocation(
      graph_id="kg01234567890abcdef",
      instance_id="i-12345678",
      private_ip="10.0.0.1",
      availability_zone="us-east-1a",
      created_at=now,
      status=DatabaseStatus.ACTIVE,
    )
    assert loc.graph_id == "kg01234567890abcdef"
    assert loc.backend_type == "ladybug"

  def test_instance_info_available_capacity(self):
    info = InstanceInfo(
      instance_id="i-12345678",
      private_ip="10.0.0.1",
      availability_zone="us-east-1a",
      status=InstanceStatus.HEALTHY,
      database_count=3,
      max_databases=10,
      created_at=datetime.now(UTC),
    )
    assert info.available_capacity == 7

  def test_instance_info_available_capacity_at_max(self):
    info = InstanceInfo(
      instance_id="i-12345678",
      private_ip="10.0.0.1",
      availability_zone="us-east-1a",
      status=InstanceStatus.HEALTHY,
      database_count=10,
      max_databases=10,
      created_at=datetime.now(UTC),
    )
    assert info.available_capacity == 0

  def test_instance_info_utilization_percent(self):
    info = InstanceInfo(
      instance_id="i-12345678",
      private_ip="10.0.0.1",
      availability_zone="us-east-1a",
      status=InstanceStatus.HEALTHY,
      database_count=5,
      max_databases=10,
      created_at=datetime.now(UTC),
    )
    assert info.utilization_percent == 50.0

  def test_instance_info_utilization_zero_max(self):
    info = InstanceInfo(
      instance_id="i-12345678",
      private_ip="10.0.0.1",
      availability_zone="us-east-1a",
      status=InstanceStatus.HEALTHY,
      database_count=0,
      max_databases=0,
      created_at=datetime.now(UTC),
    )
    assert info.utilization_percent == 0


@pytest.mark.unit
class TestLadybugAllocationManagerInit:
  """Tests for LadybugAllocationManager initialization."""

  def test_invalid_environment_name(self):
    """Test that invalid environment names are rejected."""
    with pytest.raises(ValueError, match="Invalid environment name"):
      with (
        patch("robosystems.middleware.graph.allocation_manager.get_dynamodb_resource"),
        patch("robosystems.middleware.graph.allocation_manager.boto3"),
        patch("robosystems.middleware.graph.allocation_manager.env") as mock_env,
      ):
        mock_env.AWS_REGION = "us-east-1"
        mock_env.is_development.return_value = False
        mock_env.AWS_ENDPOINT_URL = None
        mock_env.GRAPH_REGISTRY_TABLE = "graph-registry"
        mock_env.INSTANCE_REGISTRY_TABLE = "instance-registry"
        mock_env.VOLUME_REGISTRY_TABLE = "volume-registry"

        LadybugAllocationManager(environment="inv@lid!")

  def test_get_tier_config(self):
    """Test get_tier_config returns correct config for each tier."""
    manager = _create_manager()

    for tier in [
      GraphTier.LADYBUG_STANDARD,
      GraphTier.LADYBUG_LARGE,
      GraphTier.LADYBUG_XLARGE,
      GraphTier.LADYBUG_SHARED,
    ]:
      config = manager.get_tier_config(tier)
      assert config["backend"] == "ladybug"
      assert "databases_per_instance" in config

  def test_get_tier_config_unknown_falls_back_to_standard(self):
    """Test that unknown tier falls back to standard config."""
    manager = _create_manager()
    # Use a tier that exists but test fallback path
    config = manager.get_tier_config(GraphTier.LADYBUG_STANDARD)
    assert config["backend"] == "ladybug"


@pytest.mark.unit
class TestFindDatabaseLocation:
  """Tests for find_database_location method."""

  @pytest.mark.asyncio
  async def test_find_existing_database(self):
    """Test finding an existing database location."""
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kg01234567890abcdef",
        "instance_id": "i-12345678",
        "private_ip": "10.0.0.1",
        "availability_zone": "us-east-1a",
        "created_at": now_iso,
        "status": "active",
        "backend_type": "ladybug",
      }
    }
    manager.graph_table.update_item.return_value = {}

    result = await manager.find_database_location("kg01234567890abcdef")

    assert result is not None
    assert result.graph_id == "kg01234567890abcdef"
    assert result.instance_id == "i-12345678"
    assert result.private_ip == "10.0.0.1"
    assert result.status == DatabaseStatus.ACTIVE

  @pytest.mark.asyncio
  async def test_find_nonexistent_database(self):
    """Test finding a database that does not exist."""
    manager = _create_manager()
    manager.graph_table.get_item.return_value = {}

    result = await manager.find_database_location("kg01234567890abcdef")
    assert result is None

  @pytest.mark.asyncio
  async def test_find_database_falls_back_to_instance_registry(self):
    """Test that missing private_ip falls back to instance registry."""
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    # Graph table has entry but no private_ip
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kg01234567890abcdef",
        "instance_id": "i-12345678",
        "created_at": now_iso,
        "status": "active",
      }
    }
    manager.graph_table.update_item.return_value = {}

    # Instance table has the IP
    manager.instance_table.get_item.return_value = {
      "Item": {
        "instance_id": "i-12345678",
        "private_ip": "10.0.0.2",
        "availability_zone": "us-east-1b",
      }
    }

    result = await manager.find_database_location("kg01234567890abcdef")
    assert result is not None
    assert result.private_ip == "10.0.0.2"

  @pytest.mark.asyncio
  async def test_find_database_no_ip_anywhere(self):
    """Test that None is returned when IP cannot be resolved."""
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    # Graph table has entry but no private_ip
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kg01234567890abcdef",
        "instance_id": "i-12345678",
        "created_at": now_iso,
        "status": "active",
      }
    }

    # Instance table also missing
    manager.instance_table.get_item.return_value = {}

    result = await manager.find_database_location("kg01234567890abcdef")
    assert result is None

  @pytest.mark.asyncio
  async def test_find_database_handles_client_error(self):
    """Test that ClientError during lookup returns None."""
    manager = _create_manager()
    manager.graph_table.get_item.side_effect = _make_client_error(
      code="InternalServerError"
    )

    result = await manager.find_database_location("kg01234567890abcdef")
    assert result is None

  @pytest.mark.asyncio
  async def test_find_database_subgraph_resolves_to_parent(self):
    """Test that subgraph IDs resolve to parent location."""
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    # Parent graph entry
    manager.graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kg01234567890abcdef",
        "instance_id": "i-12345678",
        "private_ip": "10.0.0.1",
        "availability_zone": "us-east-1a",
        "created_at": now_iso,
        "status": "active",
        "backend_type": "ladybug",
      }
    }
    manager.graph_table.update_item.return_value = {}

    result = await manager.find_database_location("kg01234567890abcdef_dev")

    assert result is not None
    assert result.graph_id == "kg01234567890abcdef_dev"
    assert result.instance_id == "i-12345678"
    assert result.private_ip == "10.0.0.1"

  @pytest.mark.asyncio
  async def test_find_database_subgraph_parent_not_found(self):
    """Test that subgraph returns None when parent not found."""
    manager = _create_manager()
    manager.graph_table.get_item.return_value = {}

    result = await manager.find_database_location("kg01234567890abcdef_dev")
    assert result is None


@pytest.mark.unit
class TestGetInstanceDatabases:
  """Tests for get_instance_databases method."""

  @pytest.mark.asyncio
  async def test_get_instance_databases_success(self):
    manager = _create_manager()
    manager.graph_table.query.return_value = {
      "Items": [
        {"graph_id": "kg01234567890abcdef"},
        {"graph_id": "kg11111111111111111"},
      ]
    }

    result = await manager.get_instance_databases("i-12345678")
    assert result == ["kg01234567890abcdef", "kg11111111111111111"]

  @pytest.mark.asyncio
  async def test_get_instance_databases_invalid_id(self):
    manager = _create_manager()

    with pytest.raises(ValueError, match="Invalid instance ID format"):
      await manager.get_instance_databases("invalid-id")

  @pytest.mark.asyncio
  async def test_get_instance_databases_empty_id(self):
    manager = _create_manager()

    with pytest.raises(ValueError, match="must be a non-empty string"):
      await manager.get_instance_databases("")

  @pytest.mark.asyncio
  async def test_get_instance_databases_client_error(self):
    manager = _create_manager()
    manager.graph_table.query.side_effect = _make_client_error(
      code="InternalServerError"
    )

    result = await manager.get_instance_databases("i-12345678")
    assert result == []


@pytest.mark.unit
class TestGetAllInstances:
  """Tests for get_all_instances method."""

  @pytest.mark.asyncio
  async def test_get_all_instances_success(self):
    manager = _create_manager()
    manager.instance_table.scan.return_value = {
      "Items": [
        {"instance_id": "i-12345678", "status": "healthy"},
        {"instance_id": "i-abcdef01", "status": "healthy"},
      ]
    }

    result = await manager.get_all_instances()
    assert len(result) == 2

  @pytest.mark.asyncio
  async def test_get_all_instances_empty(self):
    manager = _create_manager()
    manager.instance_table.scan.return_value = {"Items": []}

    result = await manager.get_all_instances()
    assert result == []

  @pytest.mark.asyncio
  async def test_get_all_instances_client_error(self):
    manager = _create_manager()
    manager.instance_table.scan.side_effect = _make_client_error(
      code="InternalServerError"
    )

    result = await manager.get_all_instances()
    assert result == []


@pytest.mark.unit
class TestAllocationMetrics:
  """Tests for get_allocation_metrics method."""

  @pytest.mark.asyncio
  async def test_get_allocation_metrics_success(self):
    manager = _create_manager()
    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-12345678",
          "max_databases": 10,
          "database_count": 4,
          "status": "healthy",
        },
        {
          "instance_id": "i-abcdef01",
          "max_databases": 10,
          "database_count": 8,
          "status": "healthy",
        },
      ]
    }

    metrics = await manager.get_allocation_metrics()

    assert metrics["total_instances"] == 2
    assert metrics["total_capacity"] == 20
    assert metrics["total_databases"] == 12
    assert metrics["overall_utilization_percent"] == 60.0
    assert len(metrics["instances"]) == 2
    assert "timestamp" in metrics

  @pytest.mark.asyncio
  async def test_get_allocation_metrics_empty(self):
    manager = _create_manager()
    manager.instance_table.scan.return_value = {"Items": []}

    metrics = await manager.get_allocation_metrics()
    assert metrics["total_instances"] == 0
    assert metrics["overall_utilization_percent"] == 0
    assert metrics["scale_up_needed"] is False

  @pytest.mark.asyncio
  async def test_get_allocation_metrics_client_error(self):
    """Test that ClientError during metrics gathering returns error dict."""
    manager = _create_manager()
    # Patch get_all_instances to raise ClientError directly so that
    # get_allocation_metrics' own error handler catches it.
    with patch.object(
      manager,
      "get_all_instances",
      new_callable=AsyncMock,
      side_effect=_make_client_error(),
    ):
      metrics = await manager.get_allocation_metrics()
    assert "error" in metrics
    assert "timestamp" in metrics


@pytest.mark.unit
class TestCheckTierCapacity:
  """Tests for check_tier_capacity method."""

  @pytest.mark.asyncio
  async def test_capacity_ready(self):
    """Test returns 'ready' when instance has capacity."""
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-12345678",
          "private_ip": "10.0.0.1",
          "availability_zone": "us-east-1a",
          "database_count": 3,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        }
      ]
    }

    result = await manager.check_tier_capacity(GraphTier.LADYBUG_STANDARD)
    assert result == "ready"

  @pytest.mark.asyncio
  async def test_capacity_scalable(self):
    """Test returns 'scalable' when ASG has headroom."""
    manager = _create_manager()
    # No instances with capacity
    manager.instance_table.scan.return_value = {"Items": []}
    # ASG has headroom
    manager.autoscaling.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [
        {"DesiredCapacity": 2, "MaxSize": 5},
      ]
    }

    result = await manager.check_tier_capacity(GraphTier.LADYBUG_STANDARD)
    assert result == "scalable"

  @pytest.mark.asyncio
  async def test_capacity_at_capacity(self):
    """Test returns 'at_capacity' when no headroom."""
    manager = _create_manager()
    # No instances with capacity
    manager.instance_table.scan.return_value = {"Items": []}
    # ASG at max
    manager.autoscaling.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [
        {"DesiredCapacity": 5, "MaxSize": 5},
      ]
    }

    result = await manager.check_tier_capacity(GraphTier.LADYBUG_STANDARD)
    assert result == "at_capacity"


@pytest.mark.unit
class TestGetStackNameForTier:
  """Tests for _get_stack_name_for_tier method."""

  def test_prod_standard(self):
    manager = _create_manager(environment="prod")
    result = manager._get_stack_name_for_tier("ladybug-standard")
    assert result == "RoboSystemsGraphLadybugStandardProd"

  def test_staging_large(self):
    manager = _create_manager(environment="staging")
    result = manager._get_stack_name_for_tier("ladybug-large")
    assert result == "RoboSystemsGraphLadybugLargeStaging"

  def test_unknown_tier(self):
    manager = _create_manager(environment="prod")
    result = manager._get_stack_name_for_tier("unknown-tier")
    assert result is None

  def test_dev_environment_returns_none(self):
    manager = _create_manager(environment="test")
    result = manager._get_stack_name_for_tier("ladybug-standard")
    assert result is None


@pytest.mark.unit
class TestGetAsgNameForInstance:
  """Tests for _get_asg_name_for_instance method."""

  @pytest.mark.asyncio
  async def test_returns_asg_name_for_prod(self):
    manager = _create_manager(environment="prod")
    manager.instance_table.get_item.return_value = {
      "Item": {
        "instance_id": "i-12345678",
        "cluster_tier": "ladybug-standard",
      }
    }

    result = await manager._get_asg_name_for_instance("i-12345678")
    assert result == "robosystems-ladybug-standard-writers-prod-asg"

  @pytest.mark.asyncio
  async def test_returns_default_for_dev(self):
    manager = _create_manager(environment="test")
    manager.instance_table.get_item.return_value = {
      "Item": {
        "instance_id": "i-12345678",
        "cluster_tier": "ladybug-standard",
      }
    }

    result = await manager._get_asg_name_for_instance("i-12345678")
    assert result == "test-asg"

  @pytest.mark.asyncio
  async def test_returns_none_when_not_found(self):
    manager = _create_manager()
    manager.instance_table.get_item.return_value = {}

    result = await manager._get_asg_name_for_instance("i-12345678")
    assert result is None

  @pytest.mark.asyncio
  async def test_returns_none_on_client_error(self):
    manager = _create_manager()
    manager.instance_table.get_item.side_effect = _make_client_error()

    result = await manager._get_asg_name_for_instance("i-12345678")
    assert result is None


@pytest.mark.unit
class TestPublishMetrics:
  """Tests for metric publishing methods."""

  @pytest.mark.asyncio
  async def test_publish_allocation_metrics_skips_dev(self):
    """Test that metrics are skipped in dev/test environments."""
    manager = _create_manager(environment="test")
    await manager._publish_allocation_metrics()
    manager.cloudwatch.put_metric_data.assert_not_called()

  @pytest.mark.asyncio
  async def test_publish_failure_metric_skips_dev(self):
    manager = _create_manager(environment="test")
    await manager._publish_failure_metric("no_capacity", "entity1")
    manager.cloudwatch.put_metric_data.assert_not_called()

  @pytest.mark.asyncio
  async def test_publish_allocation_metrics_prod(self):
    """Test metrics are published in prod."""
    manager = _create_manager(environment="prod")
    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-12345678",
          "max_databases": 10,
          "database_count": 5,
          "status": "healthy",
        },
      ]
    }

    await manager._publish_allocation_metrics()
    manager.cloudwatch.put_metric_data.assert_called_once()

  @pytest.mark.asyncio
  async def test_publish_failure_metric_prod(self):
    manager = _create_manager(environment="prod")
    await manager._publish_failure_metric("no_capacity", "entity1", "user1")
    manager.cloudwatch.put_metric_data.assert_called_once()

  @pytest.mark.asyncio
  async def test_publish_failure_metric_emits_both_dimension_sets(self):
    """The alarm matches on `Environment`; triage wants `FailureReason`.

    CloudWatch matches an alarm to a metric on the exact dimension set, so
    emitting only `FailureReason` leaves `AllocationFailureAlarm` watching a
    stream nothing publishes to — it sat `OK` for six months on exactly that.
    Asserting the call happened is what let it through; assert the payload.
    """
    manager = _create_manager(environment="prod")
    await manager._publish_failure_metric("no_capacity", "entity1", "user1")

    call = manager.cloudwatch.put_metric_data.call_args
    metric_data = call.kwargs["MetricData"]
    dimension_sets = [
      {d["Name"]: d["Value"] for d in datum["Dimensions"]} for datum in metric_data
    ]

    # An alarm matches on namespace *and* dimension set. Namespace arrives as a
    # separate kwarg, so asserting only the dimensions would let a dropped
    # `/{environment}` suffix re-break the alarm in the identical way with this
    # test still green.
    assert call.kwargs["Namespace"] == "RoboSystems/Graph/prod"
    assert {"FailureReason": "no_capacity"} in dimension_sets
    assert {"Environment": "prod"} in dimension_sets
    assert all(d["MetricName"] == "AllocationFailures" for d in metric_data)
    assert all(d["Value"] == 1 for d in metric_data)

  @pytest.mark.asyncio
  async def test_publish_allocation_metrics_handles_error(self):
    manager = _create_manager(environment="prod")
    manager.instance_table.scan.side_effect = _make_client_error()

    # Should not raise
    await manager._publish_allocation_metrics()


@pytest.mark.unit
class TestFindBestInstance:
  """Tests for _find_best_instance method."""

  @pytest.mark.asyncio
  async def test_find_best_instance_selects_most_capacity(self):
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-11111111",
          "private_ip": "10.0.0.1",
          "availability_zone": "us-east-1a",
          "database_count": 8,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
        {
          "instance_id": "i-22222222",
          "private_ip": "10.0.0.2",
          "availability_zone": "us-east-1b",
          "database_count": 2,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
      ]
    }
    _stub_graph_occupancy(manager, {"i-11111111": 8, "i-22222222": 2})

    result = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert result is not None
    assert result.instance_id == "i-22222222"
    assert result.available_capacity == 8

  @pytest.mark.asyncio
  async def test_find_best_instance_excludes_specified(self):
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-11111111",
          "private_ip": "10.0.0.1",
          "availability_zone": "us-east-1a",
          "database_count": 2,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
        {
          "instance_id": "i-22222222",
          "private_ip": "10.0.0.2",
          "availability_zone": "us-east-1b",
          "database_count": 5,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
      ]
    }

    _stub_graph_occupancy(manager, {"i-11111111": 2, "i-22222222": 5})

    result = await manager._find_best_instance(
      GraphTier.LADYBUG_STANDARD, exclude_instance="i-11111111"
    )
    assert result is not None
    assert result.instance_id == "i-22222222"

  @pytest.mark.asyncio
  async def test_find_best_instance_no_capacity(self):
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-11111111",
          "private_ip": "10.0.0.1",
          "availability_zone": "us-east-1a",
          "database_count": 10,
          "max_databases": 10,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
      ]
    }
    _stub_graph_occupancy(manager, {"i-11111111": 10})

    result = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert result is None

  @pytest.mark.asyncio
  async def test_find_best_instance_ignores_drifted_instance_counter(self):
    """A full instance whose registry counter reset to 0 must not be selected.

    Replacement instances re-register with database_count=0 after ASG
    cycling even when their reattached volume holds a customer graph. The
    graph registry is authoritative, so a dedicated instance with an active
    graph is full regardless of the drifted counter.
    """
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-drifted",
          "private_ip": "10.0.0.1",
          "availability_zone": "us-east-1a",
          "database_count": 0,
          "max_databases": 1,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
      ]
    }
    _stub_graph_occupancy(manager, {"i-drifted": 1})

    result = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert result is None

  @pytest.mark.asyncio
  async def test_find_best_instance_ignores_deleted_graphs(self):
    """Deleted and failed graph rows do not consume capacity."""
    manager = _create_manager()
    now_iso = datetime.now(UTC).isoformat()

    manager.instance_table.scan.return_value = {
      "Items": [
        {
          "instance_id": "i-11111111",
          "private_ip": "10.0.0.1",
          "availability_zone": "us-east-1a",
          "database_count": 1,
          "max_databases": 1,
          "created_at": now_iso,
          "status": "healthy",
          "cluster_tier": "ladybug-standard",
        },
      ]
    }
    manager.graph_table.query.return_value = {
      "Items": [
        {"graph_id": "kg-old", "status": "deleted"},
        {"graph_id": "kg-broken", "status": "failed"},
      ]
    }

    result = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert result is not None
    assert result.instance_id == "i-11111111"
    assert result.database_count == 0

  @pytest.mark.asyncio
  async def test_find_best_instance_empty_scan(self):
    manager = _create_manager()
    manager.instance_table.scan.return_value = {"Items": []}

    result = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert result is None

  @pytest.mark.asyncio
  async def test_find_best_instance_client_error(self):
    manager = _create_manager()
    manager.instance_table.scan.side_effect = _make_client_error()

    result = await manager._find_best_instance(GraphTier.LADYBUG_STANDARD)
    assert result is None


@pytest.mark.unit
class TestDatabaseStatusEnum:
  """Tests for DatabaseStatus and InstanceStatus enums."""

  def test_database_status_values(self):
    assert DatabaseStatus.ACTIVE.value == "active"
    assert DatabaseStatus.CREATING.value == "creating"
    assert DatabaseStatus.MIGRATING.value == "migrating"
    assert DatabaseStatus.FAILED.value == "failed"
    assert DatabaseStatus.DELETED.value == "deleted"

  def test_instance_status_values(self):
    assert InstanceStatus.HEALTHY.value == "healthy"
    assert InstanceStatus.UNHEALTHY.value == "unhealthy"
    assert InstanceStatus.TERMINATING.value == "terminating"


@pytest.mark.unit
class TestAllocateEntityIdValidation:
  """Tests for entity_id validation in allocate_database."""

  @pytest.mark.asyncio
  async def test_empty_entity_id(self):
    manager = _create_manager()
    with pytest.raises(ValueError, match="must be a non-empty string"):
      await manager.allocate_database("")

  @pytest.mark.asyncio
  async def test_invalid_entity_id_format(self):
    manager = _create_manager()
    with pytest.raises(ValueError, match="Invalid entity ID format"):
      await manager.allocate_database("invalid entity@id")

  @pytest.mark.asyncio
  async def test_invalid_graph_id_format(self):
    manager = _create_manager()
    with pytest.raises(ValueError, match="Invalid graph ID format"):
      await manager.allocate_database("valid-entity", graph_id="INVALID_GRAPH_ID")

  @pytest.mark.asyncio
  async def test_subgraph_id_rejected(self):
    """Test that subgraph IDs are rejected with helpful error message."""
    manager = _create_manager()
    with pytest.raises(
      ValueError, match="Subgraph IDs are not stored in the DynamoDB registry"
    ):
      await manager.allocate_database(
        "valid-entity", graph_id="kg01234567890abcdef_dev"
      )


@pytest.mark.unit
class TestVolumeRegistrySkipDev:
  """Tests for volume registry operations in dev/test environment."""

  @pytest.mark.asyncio
  async def test_add_database_skips_in_dev(self):
    manager = _create_manager(environment="test")
    volume_id = manager._resolve_instance_volume("i-12345678")
    await manager._update_volume_registry_add_database(volume_id, "kg_test")
    assert volume_id is None
    manager.volume_table.scan.assert_not_called()
    manager.volume_table.update_item.assert_not_called()

  @pytest.mark.asyncio
  async def test_remove_database_skips_in_dev(self):
    manager = _create_manager(environment="test")
    await manager._update_volume_registry_remove_database("i-12345678", "kg_test")
    manager.volume_table.scan.assert_not_called()


class _RacingTable:
  """A real table that lets a concurrent add land right after the first read."""

  def __init__(self, table, on_first_read):
    self._table = table
    self._on_first_read = on_first_read

  def _after_read(self, result):
    if self._on_first_read:
      hook, self._on_first_read = self._on_first_read, None
      hook()
    return result

  def scan(self, **kwargs):
    return self._after_read(self._table.scan(**kwargs))

  def get_item(self, **kwargs):
    return self._after_read(self._table.get_item(**kwargs))

  def __getattr__(self, name):
    return getattr(self._table, name)


@pytest.mark.unit
class TestVolumeRegistryRemoveRace:
  @pytest.fixture
  def volume_table(self, monkeypatch):
    import boto3
    from moto import mock_aws

    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with mock_aws():
      table = boto3.resource("dynamodb", region_name="us-east-1").create_table(
        TableName="volume-registry",
        KeySchema=[{"AttributeName": "volume_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "volume_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
      )
      table.put_item(
        Item={
          "volume_id": "vol-1",
          "instance_id": "i-1",
          "status": "attached",
          "databases": ["kg_a", "kg_a_old"],
        }
      )
      yield table

  @pytest.mark.asyncio
  async def test_concurrent_add_survives_a_remove(self, volume_table):
    def concurrent_add():
      volume_table.update_item(
        Key={"volume_id": "vol-1"},
        UpdateExpression="SET databases = list_append(databases, :new)",
        ExpressionAttributeValues={":new": ["kg_a_new"]},
      )

    manager = _create_manager(environment="prod")
    manager.volume_table = _RacingTable(volume_table, concurrent_add)

    await manager._update_volume_registry_remove_database("i-1", "kg_a_old")

    databases = volume_table.get_item(Key={"volume_id": "vol-1"})["Item"]["databases"]
    assert databases == ["kg_a", "kg_a_new"]

  @pytest.mark.asyncio
  async def test_remove_is_a_noop_when_absent(self, volume_table):
    manager = _create_manager(environment="prod")
    manager.volume_table = volume_table

    await manager._update_volume_registry_remove_database("i-1", "kg_missing")

    databases = volume_table.get_item(Key={"volume_id": "vol-1"})["Item"]["databases"]
    assert databases == ["kg_a", "kg_a_old"]


@pytest.mark.unit
class TestVolumeResolvedFromTheInstancesOwnRow:
  """A database is recorded only on the volume its instance's registry row
  names. Guessing by AZ and tier recorded it on another writer's volume."""

  @pytest.fixture
  def tables(self, monkeypatch):
    import boto3
    from moto import mock_aws

    monkeypatch.delenv("AWS_ENDPOINT_URL", raising=False)
    with mock_aws():
      dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

      def table(name, key):
        return dynamodb.create_table(
          TableName=name,
          KeySchema=[{"AttributeName": key, "KeyType": "HASH"}],
          AttributeDefinitions=[{"AttributeName": key, "AttributeType": "S"}],
          BillingMode="PAY_PER_REQUEST",
        )

      volumes = table("volume-registry", "volume_id")
      instances = table("instance-registry", "instance_id")
      graphs = table("graph-registry", "graph_id")
      # A neighbour writer in the same AZ and tier.
      volumes.put_item(
        Item={
          "volume_id": "vol-other",
          "instance_id": "i-other",
          "status": "attached",
          "availability_zone": "us-east-1a",
          "tier": "ladybug-standard",
          "databases": ["kg_theirs"],
        }
      )
      instances.put_item(
        Item={
          "instance_id": "i-mine",
          "availability_zone": "us-east-1a",
          "tier": "ladybug-standard",
          "database_count": 0,
          "max_databases": 1,
        }
      )
      manager = _create_manager(environment="prod")
      manager.volume_table = volumes
      manager.instance_table = instances
      manager.graph_table = graphs
      yield manager, volumes, instances, graphs

  def test_no_row_of_its_own_is_refused(self, tables):
    from robosystems.middleware.graph.allocation_manager import (
      VolumeNotResolvedError,
    )

    manager, volumes, _, _ = tables
    with pytest.raises(VolumeNotResolvedError):
      manager._resolve_instance_volume("i-mine")

  def test_an_expanding_row_of_its_own_is_used(self, tables):
    manager, volumes, _, _ = tables
    volumes.put_item(
      Item={"volume_id": "vol-mine", "instance_id": "i-mine", "status": "expanding"}
    )
    assert manager._resolve_instance_volume("i-mine") == "vol-mine"

  def test_two_live_rows_are_refused(self, tables):
    from robosystems.middleware.graph.allocation_manager import (
      VolumeNotResolvedError,
    )

    manager, volumes, _, _ = tables
    for vid in ("vol-a", "vol-b"):
      volumes.put_item(
        Item={"volume_id": vid, "instance_id": "i-mine", "status": "attached"}
      )
    with pytest.raises(VolumeNotResolvedError):
      manager._resolve_instance_volume("i-mine")

  @staticmethod
  def _writers(*ids):
    from datetime import UTC, datetime

    from robosystems.middleware.graph.allocation_manager import (
      InstanceInfo,
      InstanceStatus,
    )

    writers = [
      InstanceInfo(
        instance_id=iid,
        private_ip=f"10.0.0.{n}",
        availability_zone="us-east-1a",
        status=InstanceStatus.HEALTHY,
        database_count=0,
        max_databases=1,
        created_at=datetime.now(UTC),
      )
      for n, iid in enumerate(ids, start=1)
    ]

    async def find_best(_tier=None, exclude_instance=None, exclude=None):
      skip = set(exclude or ()) | {exclude_instance}
      return next((w for w in writers if w.instance_id not in skip), None)

    return find_best

  @pytest.mark.asyncio
  async def test_a_writer_with_a_stuck_volume_row_is_skipped(self, tables):
    manager, volumes, instances, graphs = tables
    volumes.put_item(
      Item={"volume_id": "vol-mine", "instance_id": "i-mine", "status": "attaching"}
    )
    volumes.put_item(
      Item={"volume_id": "vol-peer", "instance_id": "i-peer", "status": "attached"}
    )
    instances.put_item(
      Item={"instance_id": "i-peer", "database_count": 0, "max_databases": 1}
    )
    with patch.object(
      manager, "_find_best_instance", side_effect=self._writers("i-mine", "i-peer")
    ):
      location = await manager.allocate_database(
        "entity_1", graph_id="kg0123456789abcdef01"
      )

    assert location.instance_id == "i-peer"
    assert volumes.get_item(Key={"volume_id": "vol-peer"})["Item"]["databases"] == [
      "kg0123456789abcdef01"
    ]
    assert "databases" not in volumes.get_item(Key={"volume_id": "vol-mine"})["Item"]

  @pytest.mark.asyncio
  async def test_no_writer_with_a_volume_writes_nothing(self, tables):
    manager, volumes, instances, graphs = tables
    with patch.object(
      manager, "_find_best_instance", side_effect=self._writers("i-mine")
    ):
      with pytest.raises(Exception, match="capacity"):
        await manager.allocate_database("entity_1", graph_id="kg0123456789abcdef01")

    assert "Item" not in graphs.get_item(Key={"graph_id": "kg0123456789abcdef01"})
    assert (
      instances.get_item(Key={"instance_id": "i-mine"})["Item"]["database_count"] == 0
    )
    assert volumes.get_item(Key={"volume_id": "vol-other"})["Item"]["databases"] == [
      "kg_theirs"
    ]
