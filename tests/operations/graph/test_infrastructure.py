"""Tests for Graph infrastructure operations (InstanceMonitor)."""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from robosystems.operations.graph.infrastructure import (
  TIER_CAPACITY_MAP,
  CleanupResult,
  HealthCheckResult,
  InstanceMonitor,
  MetricsResult,
  _get_tier_capacity,
  _is_valid_ec2_instance_id,
)

MODULE = "robosystems.operations.graph.infrastructure"


@pytest.fixture
def monitor():
  """Create an InstanceMonitor with mocked AWS clients."""
  with patch(f"{MODULE}.env") as mock_env:
    mock_env.ENVIRONMENT = "test"
    m = InstanceMonitor(
      instance_registry_table="test-instance",
      graph_registry_table="test-graph",
      volume_registry_table="test-volume",
      environment="test",
    )
  m._ec2 = MagicMock()
  m._dynamodb = MagicMock()
  m._cloudwatch = MagicMock()
  return m


def _make_dynamo_table(items=None, scan_kwargs=None):
  """Create a mock DynamoDB table that returns items from scan."""
  table = MagicMock()
  response = {"Items": items or []}
  if scan_kwargs:
    response.update(scan_kwargs)
  table.scan.return_value = response
  table.update_item.return_value = None
  table.delete_item.return_value = None
  return table


# ---------------------------------------------------------------------------
# TestHelperFunctions
# ---------------------------------------------------------------------------


class TestHelperFunctions:
  @pytest.mark.unit
  def test_get_tier_capacity_known_tier(self):
    assert _get_tier_capacity("ladybug-standard") == 1
    assert _get_tier_capacity("ladybug-shared") == 10
    assert _get_tier_capacity("ladybug-large") == 1
    assert _get_tier_capacity("ladybug-xlarge") == 1

  @pytest.mark.unit
  def test_get_tier_capacity_unknown_tier(self):
    result = _get_tier_capacity("nonexistent-tier")
    assert result == TIER_CAPACITY_MAP["ladybug-standard"]

  @pytest.mark.unit
  def test_get_tier_capacity_none_tier(self):
    result = _get_tier_capacity(None)
    assert result == TIER_CAPACITY_MAP["ladybug-standard"]

  @pytest.mark.unit
  def test_is_valid_ec2_instance_id_valid(self):
    assert _is_valid_ec2_instance_id("i-1234567890abcdef0") is True
    assert _is_valid_ec2_instance_id("i-abcdef01") is True
    assert _is_valid_ec2_instance_id("i-0123456789abcdef0") is True

  @pytest.mark.unit
  def test_is_valid_ec2_instance_id_invalid(self):
    assert _is_valid_ec2_instance_id("") is False
    assert _is_valid_ec2_instance_id(None) is False
    assert _is_valid_ec2_instance_id("not-an-id") is False
    assert _is_valid_ec2_instance_id("i-XXXX") is False
    assert _is_valid_ec2_instance_id("i-123") is False  # too short
    assert _is_valid_ec2_instance_id("abc") is False


# ---------------------------------------------------------------------------
# TestDataclasses
# ---------------------------------------------------------------------------


class TestDataclasses:
  @pytest.mark.unit
  def test_health_check_result_defaults(self):
    result = HealthCheckResult(timestamp="2026-01-01T00:00:00+00:00")
    assert result.total_instances == 0
    assert result.healthy == 0
    assert result.unhealthy == 0
    assert result.terminated == 0
    assert result.removed == 0
    assert result.invalid_ids == 0
    assert result.errors == 0
    assert result.error_message is None

  @pytest.mark.unit
  def test_cleanup_result_defaults(self):
    result = CleanupResult(timestamp="2026-01-01T00:00:00+00:00")
    assert result.removed_count == 0
    assert result.updated_count == 0
    assert result.errors == 0
    assert result.error_message is None

  @pytest.mark.unit
  def test_metrics_result_defaults(self):
    result = MetricsResult(timestamp="2026-01-01T00:00:00+00:00")
    assert result.metrics_published == 0
    assert result.errors == 0
    assert result.error_message is None


# ---------------------------------------------------------------------------
# TestCheckInstanceHealth
# ---------------------------------------------------------------------------


class TestCheckInstanceHealth:
  @pytest.mark.unit
  def test_happy_path_mixed_states(self, monitor):
    """Running and terminated instances produce correct counts."""
    instance_table = _make_dynamo_table(
      items=[
        {
          "instance_id": "i-1234567890abcdef0",
          "status": "healthy",
          "tier": "ladybug-standard",
        },
        {
          "instance_id": "i-abcdef1234567890a",
          "status": "healthy",
          "tier": "ladybug-large",
        },
      ]
    )
    volume_table = _make_dynamo_table()

    def table_router(name):
      if name == "test-instance":
        return instance_table
      if name == "test-volume":
        return volume_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    # First instance running, second terminated
    monitor._ec2.describe_instances.return_value = {
      "Reservations": [
        {
          "Instances": [
            {"InstanceId": "i-1234567890abcdef0", "State": {"Name": "running"}},
            {"InstanceId": "i-abcdef1234567890a", "State": {"Name": "terminated"}},
          ]
        }
      ]
    }

    result = monitor.check_instance_health()

    assert result.total_instances == 2
    assert result.healthy == 1
    assert result.terminated == 1
    assert result.removed == 1
    assert result.error_message is None

  @pytest.mark.unit
  def test_empty_registry(self, monitor):
    """Empty instance registry returns zero counts."""
    instance_table = _make_dynamo_table(items=[])
    monitor._dynamodb.Table.return_value = instance_table

    result = monitor.check_instance_health()

    assert result.total_instances == 0
    assert result.healthy == 0
    assert result.unhealthy == 0
    assert result.terminated == 0
    assert result.error_message is None

  @pytest.mark.unit
  def test_invalid_instance_ids_detected(self, monitor):
    """Invalid EC2 instance IDs are counted and handled."""
    instance_table = _make_dynamo_table(
      items=[
        {
          "instance_id": "bad-id-format",
          "status": "healthy",
          "tier": "ladybug-standard",
        },
        {
          "instance_id": "i-1234567890abcdef0",
          "status": "healthy",
          "tier": "ladybug-standard",
        },
      ]
    )
    volume_table = _make_dynamo_table()

    def table_router(name):
      if name == "test-instance":
        return instance_table
      if name == "test-volume":
        return volume_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    # Only the valid ID goes to EC2
    monitor._ec2.describe_instances.return_value = {
      "Reservations": [
        {
          "Instances": [
            {"InstanceId": "i-1234567890abcdef0", "State": {"Name": "running"}},
          ]
        }
      ]
    }

    result = monitor.check_instance_health()

    assert result.invalid_ids == 1
    assert result.total_instances == 2
    # Valid instance is running -> healthy; invalid gets "invalid_id" state -> terminated+removed
    assert result.healthy == 1
    assert result.terminated == 1
    assert result.removed == 1

  @pytest.mark.unit
  def test_ec2_invalid_instance_id_not_found_fallback(self, monitor):
    """InvalidInstanceID.NotFound triggers individual instance checks."""
    instance_table = _make_dynamo_table(
      items=[
        {
          "instance_id": "i-1234567890abcdef0",
          "status": "healthy",
          "tier": "ladybug-standard",
        },
        {
          "instance_id": "i-abcdef1234567890a",
          "status": "healthy",
          "tier": "ladybug-standard",
        },
      ]
    )
    volume_table = _make_dynamo_table()

    def table_router(name):
      if name == "test-instance":
        return instance_table
      if name == "test-volume":
        return volume_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    # Batch call fails with InvalidInstanceID.NotFound
    error_response = {
      "Error": {"Code": "InvalidInstanceID.NotFound", "Message": "Not found"}
    }
    monitor._ec2.describe_instances.side_effect = [
      ClientError(error_response, "DescribeInstances"),
      # Individual fallback calls
      {
        "Reservations": [
          {
            "Instances": [
              {"InstanceId": "i-1234567890abcdef0", "State": {"Name": "running"}}
            ]
          }
        ]
      },
      {
        "Reservations": [
          {
            "Instances": [
              {"InstanceId": "i-abcdef1234567890a", "State": {"Name": "running"}}
            ]
          }
        ]
      },
    ]

    result = monitor.check_instance_health()

    assert result.total_instances == 2
    assert result.healthy == 2
    assert result.error_message is None

  @pytest.mark.unit
  def test_exception_sets_error_message(self, monitor):
    """Top-level exception is captured in error_message."""
    monitor._dynamodb.Table.side_effect = Exception("DynamoDB is down")

    result = monitor.check_instance_health()

    assert result.error_message == "DynamoDB is down"


# ---------------------------------------------------------------------------
# TestCleanupStaleGraphs
# ---------------------------------------------------------------------------


class TestCleanupStaleGraphs:
  @pytest.mark.unit
  def test_removes_graph_deleted_over_7_days(self, monitor):
    """Graph deleted more than 7 days ago is removed."""
    old_deleted_at = (datetime.now(UTC) - timedelta(days=10)).isoformat()

    graph_table = _make_dynamo_table(
      items=[
        {
          "graph_id": "kg_stale",
          "status": "deleted",
          "deleted_at": old_deleted_at,
          "instance_id": "i-1234567890abcdef0",
        },
      ]
    )
    # Instance table returns the instance so instance_id is "valid"
    instance_table = _make_dynamo_table(items=[{"instance_id": "i-1234567890abcdef0"}])

    def table_router(name):
      if name == "test-graph":
        return graph_table
      if name == "test-instance":
        return instance_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    result = monitor.cleanup_stale_graphs()

    assert result.removed_count == 1
    graph_table.delete_item.assert_called_once_with(Key={"graph_id": "kg_stale"})
    assert result.error_message is None

  @pytest.mark.unit
  def test_marks_graph_with_missing_instance_instead_of_removing(self, monitor):
    """A live graph whose instance is absent from the instance registry is
    marked and counted, never deleted — the row is its routing, and the
    instance registry drifts on ASG cycling."""
    graph_table = _make_dynamo_table(
      items=[
        {
          "graph_id": "kg_orphan",
          "status": "active",
          "instance_id": "i-doesnotexist00001",
        },
      ]
    )
    instance_table = _make_dynamo_table(items=[])

    def table_router(name):
      if name == "test-graph":
        return graph_table
      if name == "test-instance":
        return instance_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    result = monitor.cleanup_stale_graphs()

    assert result.removed_count == 0
    assert result.orphaned_count == 1
    assert result.updated_count == 1
    graph_table.delete_item.assert_not_called()
    update = graph_table.update_item.call_args.kwargs
    assert update["Key"] == {"graph_id": "kg_orphan"}
    assert "SET instance_missing_since" in update["UpdateExpression"]
    assert "attribute_not_exists" in update["ConditionExpression"]
    metric = monitor._cloudwatch.put_metric_data.call_args.kwargs
    assert metric["Namespace"] == "RoboSystems/Graph/test"
    assert metric["MetricData"][0]["MetricName"] == "OrphanedGraphRegistrations"
    assert metric["MetricData"][0]["Value"] == 1

  @pytest.mark.unit
  def test_instance_registry_scan_is_paginated(self, monitor):
    """An instance on the second page of the instance scan is still valid.
    Without pagination every graph on a later-page instance would be stamped
    orphaned and the sweep would raise the very alarm it feeds."""
    graph_table = _make_dynamo_table(
      items=[
        {"graph_id": "kg_p2", "status": "active", "instance_id": "i-onpagetwo000001"},
      ]
    )
    instance_table = MagicMock()
    instance_table.scan.side_effect = [
      {"Items": [{"instance_id": "i-onpageone000001"}], "LastEvaluatedKey": {"k": 1}},
      {"Items": [{"instance_id": "i-onpagetwo000001"}]},
    ]

    def table_router(name):
      if name == "test-graph":
        return graph_table
      if name == "test-instance":
        return instance_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    result = monitor.cleanup_stale_graphs()

    assert instance_table.scan.call_count == 2
    assert instance_table.scan.call_args_list[1].kwargs["ExclusiveStartKey"] == {"k": 1}
    assert result.orphaned_count == 0
    graph_table.update_item.assert_not_called()

  @pytest.mark.unit
  def test_already_marked_orphan_is_counted_not_restamped(self, monitor):
    graph_table = _make_dynamo_table(
      items=[
        {
          "graph_id": "kg_orphan",
          "status": "active",
          "instance_id": "i-doesnotexist00001",
          "instance_missing_since": "2026-08-01T00:00:00+00:00",
        },
      ]
    )
    instance_table = _make_dynamo_table(items=[])
    monitor._dynamodb.Table.side_effect = lambda name: (
      graph_table if name == "test-graph" else instance_table
    )

    result = monitor.cleanup_stale_graphs()

    assert result.orphaned_count == 1
    assert result.updated_count == 0
    graph_table.update_item.assert_not_called()
    graph_table.delete_item.assert_not_called()

  @pytest.mark.unit
  def test_marker_is_cleared_when_instance_returns(self, monitor):
    graph_table = _make_dynamo_table(
      items=[
        {
          "graph_id": "kg_back",
          "status": "active",
          "instance_id": "i-1234567890abcdef0",
          "instance_missing_since": "2026-08-01T00:00:00+00:00",
        },
      ]
    )
    instance_table = _make_dynamo_table(items=[{"instance_id": "i-1234567890abcdef0"}])
    monitor._dynamodb.Table.side_effect = lambda name: (
      graph_table if name == "test-graph" else instance_table
    )

    result = monitor.cleanup_stale_graphs()

    assert result.orphaned_count == 0
    assert result.updated_count == 1
    update = graph_table.update_item.call_args.kwargs
    assert update["UpdateExpression"] == "REMOVE instance_missing_since"
    metric = monitor._cloudwatch.put_metric_data.call_args.kwargs
    assert metric["MetricData"][0]["Value"] == 0

  @pytest.mark.unit
  def test_shared_repository_with_parked_master_is_not_orphaned(self, monitor):
    """A shared repo's row is bookkeeping, not routing, and its master is
    parked to zero between ingestion runs — so a missing instance is the
    normal resting state, not drift. Uses the real registry ids so the
    exemption cannot drift from the predicate the router uses."""
    graph_table = _make_dynamo_table(
      items=[
        {"graph_id": "sec", "status": "active", "instance_id": "i-parkedmaster0001"},
        {
          "graph_id": "sec_historical",
          "status": "active",
          "instance_id": "i-parkedmaster0001",
        },
      ]
    )
    instance_table = _make_dynamo_table(items=[])
    monitor._dynamodb.Table.side_effect = lambda name: (
      graph_table if name == "test-graph" else instance_table
    )

    result = monitor.cleanup_stale_graphs()

    assert result.orphaned_count == 0
    assert result.updated_count == 0
    graph_table.update_item.assert_not_called()
    graph_table.delete_item.assert_not_called()
    metric = monitor._cloudwatch.put_metric_data.call_args.kwargs
    assert metric["MetricData"][0]["Value"] == 0

  @pytest.mark.unit
  def test_shared_repository_marker_from_before_the_exemption_is_cleared(self, monitor):
    """A row stamped by a sweep that predates the exemption self-heals rather
    than carrying the stale marker forever."""
    graph_table = _make_dynamo_table(
      items=[
        {
          "graph_id": "sec",
          "status": "active",
          "instance_id": "i-parkedmaster0001",
          "instance_missing_since": "2026-08-16T02:01:26+00:00",
        },
      ]
    )
    instance_table = _make_dynamo_table(items=[])
    monitor._dynamodb.Table.side_effect = lambda name: (
      graph_table if name == "test-graph" else instance_table
    )

    result = monitor.cleanup_stale_graphs()

    assert result.orphaned_count == 0
    assert result.updated_count == 1
    update = graph_table.update_item.call_args.kwargs
    assert update["Key"] == {"graph_id": "sec"}
    assert update["UpdateExpression"] == "REMOVE instance_missing_since"

  @pytest.mark.unit
  def test_user_graph_is_still_orphaned_when_a_shared_repo_is_present(self, monitor):
    """The exemption is scoped to shared repos — a user graph on the same
    sweep is still marked and counted."""
    graph_table = _make_dynamo_table(
      items=[
        {"graph_id": "sec", "status": "active", "instance_id": "i-parkedmaster0001"},
        {
          "graph_id": "kg_orphan",
          "status": "active",
          "instance_id": "i-doesnotexist00001",
        },
      ]
    )
    instance_table = _make_dynamo_table(items=[])
    monitor._dynamodb.Table.side_effect = lambda name: (
      graph_table if name == "test-graph" else instance_table
    )

    result = monitor.cleanup_stale_graphs()

    assert result.orphaned_count == 1
    update = graph_table.update_item.call_args.kwargs
    assert update["Key"] == {"graph_id": "kg_orphan"}
    metric = monitor._cloudwatch.put_metric_data.call_args.kwargs
    assert metric["MetricData"][0]["Value"] == 1

  @pytest.mark.unit
  def test_exception_sets_error_message(self, monitor):
    """Top-level exception is captured in error_message."""
    monitor._dynamodb.Table.side_effect = Exception("scan failed")

    result = monitor.cleanup_stale_graphs()

    assert result.error_message == "scan failed"


# ---------------------------------------------------------------------------
# TestCleanupStaleVolumes
# ---------------------------------------------------------------------------


class TestCleanupStaleVolumes:
  @pytest.mark.unit
  def test_updates_stuck_attaching_volume(self, monitor):
    """Volume stuck attaching to non-existent instance gets status 'failed'."""
    volume_table = _make_dynamo_table(
      items=[
        {
          "volume_id": "vol-stuck",
          "status": "attaching",
          "instance_id": "i-doesnotexist00001",
        },
      ]
    )
    instance_table = _make_dynamo_table(items=[])

    def table_router(name):
      if name == "test-volume":
        return volume_table
      if name == "test-instance":
        return instance_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    result = monitor.cleanup_stale_volumes()

    assert result.updated_count == 1
    volume_table.update_item.assert_called_once()
    call_kwargs = volume_table.update_item.call_args[1]
    assert call_kwargs["Key"] == {"volume_id": "vol-stuck"}
    assert call_kwargs["ExpressionAttributeValues"][":status"] == "failed"

  @pytest.mark.unit
  def test_removes_old_unattached_volume(self, monitor):
    """Unattached volume older than 30 days is removed."""
    old_created = (datetime.now(UTC) - timedelta(days=45)).isoformat()

    volume_table = _make_dynamo_table(
      items=[
        {
          "volume_id": "vol-old",
          "status": "available",
          "instance_id": "unattached",
          "created_at": old_created,
        },
      ]
    )
    instance_table = _make_dynamo_table(items=[])

    def table_router(name):
      if name == "test-volume":
        return volume_table
      if name == "test-instance":
        return instance_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    result = monitor.cleanup_stale_volumes()

    assert result.removed_count == 1
    volume_table.delete_item.assert_called_once_with(Key={"volume_id": "vol-old"})

  @pytest.mark.unit
  def test_exception_sets_error_message(self, monitor):
    """Top-level exception is captured in error_message."""
    monitor._dynamodb.Table.side_effect = Exception("volume scan failed")

    result = monitor.cleanup_stale_volumes()

    assert result.error_message == "volume scan failed"


# ---------------------------------------------------------------------------
# TestCollectMetrics
# ---------------------------------------------------------------------------


class TestCollectMetrics:
  @pytest.mark.unit
  def test_happy_path_publishes_metrics_in_batches(self, monitor):
    """Metrics are published to CloudWatch in batches of 20."""
    created_at = (datetime.now(UTC) - timedelta(hours=2)).isoformat()

    instance_table = _make_dynamo_table(
      items=[
        {
          "instance_id": "i-1234567890abcdef0",
          "status": "healthy",
          "tier": "ladybug-standard",
          "total_capacity": 50,
          "database_count": 10,
          "available_capacity": 40,
          "created_at": created_at,
        },
      ]
    )
    graph_table = MagicMock()
    # Instance table scan for healthy instances
    # Graph table scan for active count
    graph_table.scan.return_value = {"Count": 10}

    def table_router(name):
      if name == "test-instance":
        return instance_table
      if name == "test-graph":
        return graph_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    result = monitor.collect_metrics()

    assert result.metrics_published > 0
    assert result.error_message is None
    # Verify put_metric_data was called with correct namespace
    monitor._cloudwatch.put_metric_data.assert_called()
    call_kwargs = monitor._cloudwatch.put_metric_data.call_args[1]
    assert call_kwargs["Namespace"] == "RoboSystems/Graph/test"

  @pytest.mark.unit
  def test_no_instances_no_metrics(self, monitor):
    """When no healthy instances exist, minimal or no metrics are published."""
    instance_table = _make_dynamo_table(items=[])
    graph_table = _make_dynamo_table(items=[])

    def table_router(name):
      if name == "test-instance":
        return instance_table
      if name == "test-graph":
        return graph_table
      return _make_dynamo_table()

    monitor._dynamodb.Table.side_effect = table_router

    result = monitor.collect_metrics()

    # No healthy instances -> total_capacity == 0 -> no cluster metrics
    assert result.metrics_published == 0
    assert result.error_message is None

  @pytest.mark.unit
  def test_exception_sets_error_message(self, monitor):
    """Top-level exception is captured in error_message."""
    monitor._dynamodb.Table.side_effect = Exception("cloudwatch down")

    result = monitor.collect_metrics()

    assert result.error_message == "cloudwatch down"
