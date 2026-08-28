# pyright: reportGeneralTypeIssues=false, reportArgumentType=false
"""Tests for GraphTierUpgradeTask worker task.

All AWS services (DynamoDB, Lambda, AutoScaling) and HTTP calls
are fully mocked. Tests verify the multi-step orchestration logic,
progress reporting, and rollback behavior.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

TASK_MODULE = "robosystems.operations.graph.tasks.graph_tier_upgrade"


def _make_task(params=None, graph_id="kg1a2b3c4d5e6f78"):
  """Create a GraphTierUpgradeTask instance with mocked infrastructure."""
  from robosystems.operations.graph.tasks.graph_tier_upgrade import (
    GraphTierUpgradeTask,
  )

  default_params = {
    "old_tier": "ladybug-standard",
    "new_tier": "ladybug-large",
    "subscription_id": "bsub_abc123",
  }
  if params:
    default_params.update(params)

  manager = MagicMock()
  manager.emit_progress = AsyncMock()
  manager.get_operation_status = AsyncMock(return_value="pending")

  task = GraphTierUpgradeTask(
    task_id="op_test123",
    graph_id=graph_id,
    user_id="user_123",
    params=default_params,
    manager=manager,
  )
  return task


def _make_dynamodb_mocks():
  """Create mock DynamoDB tables."""
  graph_table = MagicMock()
  volume_table = MagicMock()
  instance_table = MagicMock()

  # Graph registry lookup
  graph_table.get_item.return_value = {
    "Item": {
      "graph_id": "kg1a2b3c4d5e6f78",
      "instance_id": "i-old123",
      "private_ip": "10.0.1.5",
      "status": "active",
    }
  }

  # Volume registry query (via instance-index GSI)
  volume_table.query.return_value = {
    "Items": [
      {
        "volume_id": "vol-abc123",
        "instance_id": "i-old123",
        "status": "attached",
        "tier": "ladybug-standard",
        "databases": ["kg1a2b3c4d5e6f78"],
      }
    ]
  }

  # Volume reattachment polling: first call returns available, second returns attached
  volume_table.get_item.side_effect = [
    {"Item": {"volume_id": "vol-abc123", "status": "available"}},
    {
      "Item": {
        "volume_id": "vol-abc123",
        "status": "attached",
        "instance_id": "i-new456",
      }
    },
  ]

  return graph_table, volume_table, instance_table


def _make_lambda_responses():
  """Create mock Lambda invoke responses."""

  def make_response(body):
    payload = MagicMock()
    payload.read.return_value = json.dumps(body).encode()
    return {"Payload": payload}

  snapshot_response = make_response(
    {"statusCode": 200, "snapshot_id": "snap-upgrade123", "volume_id": "vol-abc123"}
  )
  detach_response = make_response(
    {"statusCode": 200, "volume_id": "vol-abc123", "databases": ["kg1a2b3c4d5e6f78"]}
  )

  return snapshot_response, detach_response


@pytest.fixture
def mock_dynamodb():
  return _make_dynamodb_mocks()


@pytest.mark.unit
@pytest.mark.asyncio
class TestGraphTierUpgradeTask:
  """Tests for the multi-step upgrade orchestration."""

  async def test_happy_path_completes_all_steps(self, mock_dynamodb):
    """Full upgrade flow completes successfully with all steps."""
    graph_table, volume_table, instance_table = mock_dynamodb
    snapshot_response, detach_response = _make_lambda_responses()

    # After reattachment, graph registry returns new instance info
    graph_table.get_item.side_effect = [
      # Step 1: initial lookup
      {
        "Item": {
          "graph_id": "kg1a2b3c4d5e6f78",
          "instance_id": "i-old123",
          "private_ip": "10.0.1.5",
          "status": "active",
        }
      },
      # Step 8: verify new instance
      {
        "Item": {
          "graph_id": "kg1a2b3c4d5e6f78",
          "instance_id": "i-new456",
          "private_ip": "10.0.2.8",
          "status": "active",
        }
      },
    ]

    # Volume reattachment: immediately attached
    volume_table.get_item.return_value = {
      "Item": {
        "volume_id": "vol-abc123",
        "status": "attached",
        "instance_id": "i-new456",
      }
    }

    mock_lambda = MagicMock()
    mock_lambda.invoke.side_effect = [snapshot_response, detach_response]

    mock_asg = MagicMock()
    mock_asg.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [
        {
          "DesiredCapacity": 0,
          "MaxSize": 5,
          "Instances": [],
        }
      ]
    }

    dynamodb_resource = MagicMock()
    dynamodb_resource.Table.side_effect = [
      graph_table,
      volume_table,
      instance_table,
    ]

    task = _make_task()

    with (
      patch(f"{TASK_MODULE}._get_dynamodb_resource", return_value=dynamodb_resource),
      patch(f"{TASK_MODULE}._get_lambda_client", return_value=mock_lambda),
      patch(f"{TASK_MODULE}._get_autoscaling_client", return_value=mock_asg),
      patch(
        f"{TASK_MODULE}.get_volume_manager_function_arn",
        return_value="arn:aws:lambda:us-east-1:123:function:vol-mgr",
      ),
      patch(f"{TASK_MODULE}.env") as mock_env,
      patch.object(task, "_drain_instance", new_callable=AsyncMock),
      patch.object(task, "_verify_graph_health", new_callable=AsyncMock),
      patch.object(task, "_finalize_subscription"),
      patch(
        "robosystems.dagster.reporting.report_asset_materialization",
        new_callable=AsyncMock,
      ),
    ):
      mock_env.GRAPH_REGISTRY_TABLE = "test-graph-registry"
      mock_env.VOLUME_REGISTRY_TABLE = "test-volume-registry"
      mock_env.INSTANCE_REGISTRY_TABLE = "test-instance-registry"
      mock_env.ENVIRONMENT = "staging"

      result = await task.execute()

    assert result["status"] == "completed"
    assert result["old_tier"] == "ladybug-standard"
    assert result["new_tier"] == "ladybug-large"
    assert result["new_instance_id"] == "i-new456"
    assert result["snapshot_id"] == "snap-upgrade123"

    # Verify DynamoDB was updated
    graph_table.update_item.assert_called()
    volume_table.update_item.assert_called()

    # Verify Lambda was invoked twice (snapshot + detach)
    assert mock_lambda.invoke.call_count == 2

    # Verify ASG capacity was increased
    mock_asg.set_desired_capacity.assert_called_once()

  async def test_no_lambda_arn_skips_snapshot_and_detach(self, mock_dynamodb):
    """When Volume Manager ARN is empty (dev), snapshot and detach are skipped."""
    graph_table, volume_table, instance_table = mock_dynamodb

    graph_table.get_item.side_effect = [
      {
        "Item": {
          "graph_id": "kg1a2b3c4d5e6f78",
          "instance_id": "i-old123",
          "private_ip": "10.0.1.5",
        }
      },
      {
        "Item": {
          "graph_id": "kg1a2b3c4d5e6f78",
          "instance_id": "i-new456",
          "private_ip": "10.0.2.8",
        }
      },
    ]

    volume_table.get_item.return_value = {
      "Item": {
        "volume_id": "vol-abc123",
        "status": "attached",
        "instance_id": "i-new456",
      }
    }

    mock_lambda = MagicMock()
    mock_asg = MagicMock()
    mock_asg.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [
        {"DesiredCapacity": 1, "MaxSize": 5, "Instances": [{"HealthStatus": "Healthy"}]}
      ]
    }

    dynamodb_resource = MagicMock()
    dynamodb_resource.Table.side_effect = [graph_table, volume_table, instance_table]

    task = _make_task()

    with (
      patch(f"{TASK_MODULE}._get_dynamodb_resource", return_value=dynamodb_resource),
      patch(f"{TASK_MODULE}._get_lambda_client", return_value=mock_lambda),
      patch(f"{TASK_MODULE}._get_autoscaling_client", return_value=mock_asg),
      patch(f"{TASK_MODULE}.get_volume_manager_function_arn", return_value=""),
      patch(f"{TASK_MODULE}.env") as mock_env,
      patch.object(task, "_drain_instance", new_callable=AsyncMock),
      patch.object(task, "_verify_graph_health", new_callable=AsyncMock),
      patch.object(task, "_finalize_subscription"),
      patch(
        "robosystems.dagster.reporting.report_asset_materialization",
        new_callable=AsyncMock,
      ),
    ):
      mock_env.GRAPH_REGISTRY_TABLE = "test-graph-registry"
      mock_env.VOLUME_REGISTRY_TABLE = "test-volume-registry"
      mock_env.INSTANCE_REGISTRY_TABLE = "test-instance-registry"
      mock_env.ENVIRONMENT = "dev"

      result = await task.execute()

    assert result["status"] == "completed"
    assert result["snapshot_id"] is None
    mock_lambda.invoke.assert_not_called()

  async def test_rollback_on_detach_failure(self, mock_dynamodb):
    """Rollback is triggered when volume detach fails."""
    graph_table, volume_table, instance_table = mock_dynamodb
    snapshot_response, _ = _make_lambda_responses()

    graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kg1a2b3c4d5e6f78",
        "instance_id": "i-old123",
        "private_ip": "10.0.1.5",
      }
    }

    # Detach fails
    detach_fail_payload = MagicMock()
    detach_fail_payload.read.return_value = json.dumps(
      {"statusCode": 500, "error": "Volume busy"}
    ).encode()
    detach_fail_response = {"Payload": detach_fail_payload}

    mock_lambda = MagicMock()
    mock_lambda.invoke.side_effect = [snapshot_response, detach_fail_response]

    dynamodb_resource = MagicMock()
    dynamodb_resource.Table.side_effect = [graph_table, volume_table, instance_table]

    task = _make_task()

    with (
      patch(f"{TASK_MODULE}._get_dynamodb_resource", return_value=dynamodb_resource),
      patch(f"{TASK_MODULE}._get_lambda_client", return_value=mock_lambda),
      patch(f"{TASK_MODULE}._get_autoscaling_client"),
      patch(
        f"{TASK_MODULE}.get_volume_manager_function_arn",
        return_value="arn:aws:lambda:us-east-1:123:function:vol-mgr",
      ),
      patch(f"{TASK_MODULE}.env") as mock_env,
      patch.object(task, "_drain_instance", new_callable=AsyncMock),
      patch.object(task, "_rollback", new_callable=AsyncMock) as mock_rollback,
    ):
      mock_env.GRAPH_REGISTRY_TABLE = "test-graph-registry"
      mock_env.VOLUME_REGISTRY_TABLE = "test-volume-registry"
      mock_env.INSTANCE_REGISTRY_TABLE = "test-instance-registry"
      mock_env.ENVIRONMENT = "staging"

      with pytest.raises(ValueError, match="Volume detach failed"):
        await task.execute()

    mock_rollback.assert_called_once()
    rollback_kwargs = mock_rollback.call_args[1]
    assert rollback_kwargs["old_tier"] == "ladybug-standard"
    assert rollback_kwargs["old_instance_id"] == "i-old123"
    assert rollback_kwargs["volume_detached"] is False

  async def test_rollback_on_reattachment_timeout(self, mock_dynamodb):
    """The compensator actually runs on a reattachment timeout — this is the
    one test in the file that executes ``_rollback`` rather than asserting a
    mock of it was called. Post-conditions: the volume is re-attached to the
    old instance, its registry tier reverted, the graph registry taken off
    ``migrating``, and the subscription taken off ``upgrading`` at the old
    plan and price."""
    graph_table, volume_table, instance_table = mock_dynamodb
    snapshot_response, detach_response = _make_lambda_responses()

    graph_table.get_item.return_value = {
      "Item": {
        "graph_id": "kg1a2b3c4d5e6f78",
        "instance_id": "i-old123",
        "private_ip": "10.0.1.5",
      }
    }

    # Volume never gets attached
    volume_table.get_item.return_value = {
      "Item": {"volume_id": "vol-abc123", "status": "available"}
    }

    attach_response = MagicMock()
    attach_response_payload = MagicMock()
    attach_response_payload.read.return_value = json.dumps({"statusCode": 200}).encode()
    attach_response = {"Payload": attach_response_payload}

    mock_lambda = MagicMock()
    mock_lambda.invoke.side_effect = [
      snapshot_response,
      detach_response,
      attach_response,
    ]

    mock_asg = MagicMock()
    mock_asg.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [
        {"DesiredCapacity": 1, "MaxSize": 5, "Instances": [{"HealthStatus": "Healthy"}]}
      ]
    }

    dynamodb_resource = MagicMock()
    dynamodb_resource.Table.side_effect = [graph_table, volume_table, instance_table]

    # PostgreSQL side of the rollback: a subscription mid-upgrade, the graph
    # row, and its credit pool.
    subscription = MagicMock()
    subscription.status = "upgrading"
    subscription.stripe_subscription_id = None
    graph_row = MagicMock()
    graph_credits = MagicMock()
    db = MagicMock()
    db.query.return_value.filter_by.return_value.first.return_value = subscription

    def _db_gen():
      yield db

    task = _make_task()

    with (
      patch(f"{TASK_MODULE}._get_dynamodb_resource", return_value=dynamodb_resource),
      patch(f"{TASK_MODULE}._get_lambda_client", return_value=mock_lambda),
      patch(f"{TASK_MODULE}._get_autoscaling_client", return_value=mock_asg),
      patch(
        f"{TASK_MODULE}.get_volume_manager_function_arn",
        return_value="arn:aws:lambda:us-east-1:123:function:vol-mgr",
      ),
      patch(f"{TASK_MODULE}.env") as mock_env,
      patch(f"{TASK_MODULE}.REATTACH_TIMEOUT_SECONDS", 0),
      patch(f"{TASK_MODULE}.REATTACH_POLL_INTERVAL_SECONDS", 0),
      patch.object(task, "_drain_instance", new_callable=AsyncMock),
      patch("robosystems.database.get_db_session", side_effect=_db_gen),
      patch("robosystems.models.core.graph.Graph.get_by_id", return_value=graph_row),
      patch(
        "robosystems.models.core.graph.graph_credits.GraphCredits.get_by_graph_id",
        return_value=graph_credits,
      ),
      patch(
        "robosystems.config.billing.BillingConfig.get_subscription_plan",
        return_value={"base_price_cents": 4900},
      ),
      patch("robosystems.config.billing.get_tier_credit_allocation", return_value=1000),
    ):
      mock_env.GRAPH_REGISTRY_TABLE = "test-graph-registry"
      mock_env.VOLUME_REGISTRY_TABLE = "test-volume-registry"
      mock_env.INSTANCE_REGISTRY_TABLE = "test-instance-registry"
      mock_env.ENVIRONMENT = "staging"

      with pytest.raises(TimeoutError, match="not reattached"):
        await task.execute()

    # Volume re-attached to the OLD instance
    attach_call = json.loads(mock_lambda.invoke.call_args_list[-1].kwargs["Payload"])
    assert attach_call == {
      "action": "attach_volume",
      "volume_id": "vol-abc123",
      "instance_id": "i-old123",
    }
    # Volume registry tier reverted
    tier_reverts = [
      c.kwargs
      for c in volume_table.update_item.call_args_list
      if c.kwargs.get("ExpressionAttributeValues", {}).get(":tier")
      == "ladybug-standard"
    ]
    assert tier_reverts, "volume registry tier was not reverted"
    # Graph registry back to active, migration markers removed
    graph_revert = graph_table.update_item.call_args_list[-1].kwargs
    assert graph_revert["ExpressionAttributeValues"][":status"] == "active"
    assert "REMOVE migration_required" in graph_revert["UpdateExpression"]
    # Subscription off `upgrading` at the old plan and price; graph + pool reverted
    assert subscription.status == "active"
    assert subscription.plan_name == "ladybug-standard"
    assert subscription.base_price_cents == 4900
    assert graph_row.graph_tier == "ladybug-standard"
    assert graph_credits.monthly_allocation == 1000
    db.commit.assert_called_once()

  async def test_graph_not_found_raises_error(self, mock_dynamodb):
    """Raises error when graph is not in DynamoDB registry."""
    graph_table, volume_table, instance_table = mock_dynamodb
    graph_table.get_item.return_value = {}

    dynamodb_resource = MagicMock()
    dynamodb_resource.Table.side_effect = [graph_table, volume_table, instance_table]

    task = _make_task()

    with (
      patch(f"{TASK_MODULE}._get_dynamodb_resource", return_value=dynamodb_resource),
      patch(f"{TASK_MODULE}._get_lambda_client"),
      patch(f"{TASK_MODULE}._get_autoscaling_client"),
      patch(f"{TASK_MODULE}.get_volume_manager_function_arn", return_value=""),
      patch(f"{TASK_MODULE}.env") as mock_env,
      patch.object(task, "_rollback", new_callable=AsyncMock) as mock_rollback,
    ):
      mock_env.GRAPH_REGISTRY_TABLE = "test-graph-registry"
      mock_env.VOLUME_REGISTRY_TABLE = "test-volume-registry"
      mock_env.INSTANCE_REGISTRY_TABLE = "test-instance-registry"

      with pytest.raises(ValueError, match="not found in graph registry"):
        await task.execute()

    mock_rollback.assert_called_once()

  async def test_no_volume_found_raises_error(self, mock_dynamodb):
    """Raises error when no attached volume found for instance."""
    graph_table, volume_table, instance_table = mock_dynamodb
    volume_table.query.return_value = {"Items": []}

    dynamodb_resource = MagicMock()
    dynamodb_resource.Table.side_effect = [graph_table, volume_table, instance_table]

    task = _make_task()

    with (
      patch(f"{TASK_MODULE}._get_dynamodb_resource", return_value=dynamodb_resource),
      patch(f"{TASK_MODULE}._get_lambda_client"),
      patch(f"{TASK_MODULE}._get_autoscaling_client"),
      patch(f"{TASK_MODULE}.get_volume_manager_function_arn", return_value=""),
      patch(f"{TASK_MODULE}.env") as mock_env,
      patch.object(task, "_rollback", new_callable=AsyncMock) as mock_rollback,
    ):
      mock_env.GRAPH_REGISTRY_TABLE = "test-graph-registry"
      mock_env.VOLUME_REGISTRY_TABLE = "test-volume-registry"
      mock_env.INSTANCE_REGISTRY_TABLE = "test-instance-registry"

      with pytest.raises(ValueError, match="No attached volume"):
        await task.execute()

    mock_rollback.assert_called_once()

  async def test_progress_reporting(self, mock_dynamodb):
    """Verifies progress is reported at key milestones."""
    graph_table, volume_table, instance_table = mock_dynamodb
    snapshot_response, detach_response = _make_lambda_responses()

    graph_table.get_item.side_effect = [
      {
        "Item": {
          "graph_id": "kg1a2b3c4d5e6f78",
          "instance_id": "i-old123",
          "private_ip": "10.0.1.5",
        }
      },
      {
        "Item": {
          "graph_id": "kg1a2b3c4d5e6f78",
          "instance_id": "i-new456",
          "private_ip": "10.0.2.8",
        }
      },
    ]
    volume_table.get_item.return_value = {
      "Item": {
        "volume_id": "vol-abc123",
        "status": "attached",
        "instance_id": "i-new456",
      }
    }

    mock_lambda = MagicMock()
    mock_lambda.invoke.side_effect = [snapshot_response, detach_response]
    mock_asg = MagicMock()
    mock_asg.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [
        {"DesiredCapacity": 1, "MaxSize": 5, "Instances": [{"HealthStatus": "Healthy"}]}
      ]
    }

    dynamodb_resource = MagicMock()
    dynamodb_resource.Table.side_effect = [graph_table, volume_table, instance_table]

    task = _make_task()

    with (
      patch(f"{TASK_MODULE}._get_dynamodb_resource", return_value=dynamodb_resource),
      patch(f"{TASK_MODULE}._get_lambda_client", return_value=mock_lambda),
      patch(f"{TASK_MODULE}._get_autoscaling_client", return_value=mock_asg),
      patch(
        f"{TASK_MODULE}.get_volume_manager_function_arn",
        return_value="arn:aws:lambda:us-east-1:123:function:vol-mgr",
      ),
      patch(f"{TASK_MODULE}.env") as mock_env,
      patch.object(task, "_drain_instance", new_callable=AsyncMock),
      patch.object(task, "_verify_graph_health", new_callable=AsyncMock),
      patch.object(task, "_finalize_subscription"),
      patch(
        "robosystems.dagster.reporting.report_asset_materialization",
        new_callable=AsyncMock,
      ),
    ):
      mock_env.GRAPH_REGISTRY_TABLE = "test-graph-registry"
      mock_env.VOLUME_REGISTRY_TABLE = "test-volume-registry"
      mock_env.INSTANCE_REGISTRY_TABLE = "test-instance-registry"
      mock_env.ENVIRONMENT = "staging"

      await task.execute()

    # Verify progress was reported multiple times
    progress_calls = task.manager.emit_progress.call_args_list
    assert len(progress_calls) >= 5

    # Check that final progress is 100%
    last_call = progress_calls[-1]
    assert last_call[1].get("progress_percent") == 100 or last_call[0][2] == 100


@pytest.mark.unit
@pytest.mark.asyncio
class TestDrainInstance:
  """Tests for the _drain_instance helper method."""

  async def test_drain_with_no_private_ip_skips(self):
    task = _make_task()
    await task._drain_instance("")

  async def test_drain_succeeds_when_connections_reach_zero(self):
    task = _make_task()

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"active_connections": 0}

    with patch("httpx.AsyncClient") as MockClient:
      client = AsyncMock()
      client.post = AsyncMock(return_value=MagicMock(status_code=202))
      client.get = AsyncMock(return_value=mock_response)
      MockClient.return_value.__aenter__ = AsyncMock(return_value=client)
      MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

      await task._drain_instance("10.0.1.5")

    client.post.assert_called_once()
    client.get.assert_called()

  async def test_drain_treats_unreachable_api_as_drained(self):
    # The maintenance-window procedure stops the graph container first;
    # nothing can write through an API that is not listening.
    task = _make_task()

    with patch("httpx.AsyncClient") as MockClient:
      client = AsyncMock()
      client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
      client.get = AsyncMock()
      MockClient.return_value.__aenter__ = AsyncMock(return_value=client)
      MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

      await task._drain_instance("10.0.1.5")

    client.get.assert_not_called()

  async def test_drain_refuses_when_api_has_no_drain_endpoint(self):
    from robosystems.operations.graph.tasks.graph_tier_upgrade import (
      DrainRefusedError,
    )

    task = _make_task()
    post_response = MagicMock()
    post_response.status_code = 404

    with patch("httpx.AsyncClient") as MockClient:
      client = AsyncMock()
      client.post = AsyncMock(return_value=post_response)
      client.get = AsyncMock()
      MockClient.return_value.__aenter__ = AsyncMock(return_value=client)
      MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

      with pytest.raises(DrainRefusedError, match="no drain endpoint"):
        await task._drain_instance("10.0.1.5")

    client.get.assert_not_called()

  async def test_drain_refuses_on_timeout_with_connections_open(self):
    from robosystems.operations.graph.tasks.graph_tier_upgrade import (
      DrainRefusedError,
    )

    task = _make_task()
    post_response = MagicMock()
    post_response.status_code = 202
    poll_response = MagicMock()
    poll_response.status_code = 200
    poll_response.json.return_value = {"active_connections": 3}

    with patch("httpx.AsyncClient") as MockClient:
      client = AsyncMock()
      client.post = AsyncMock(return_value=post_response)
      client.get = AsyncMock(return_value=poll_response)
      MockClient.return_value.__aenter__ = AsyncMock(return_value=client)
      MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

      with pytest.raises(DrainRefusedError, match="still open"):
        await task._drain_instance("10.0.1.5")

    client.get.assert_called()


@pytest.mark.unit
@pytest.mark.asyncio
class TestEnsureAsgCapacity:
  """Tests for the _ensure_asg_capacity helper method."""

  async def test_increases_desired_when_under_max(self):
    task = _make_task()
    mock_asg = MagicMock()
    mock_asg.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [{"DesiredCapacity": 0, "MaxSize": 5, "Instances": []}]
    }

    await task._ensure_asg_capacity(mock_asg, "ladybug-large")

    mock_asg.set_desired_capacity.assert_called_once()

  async def test_does_not_increase_when_at_max(self):
    task = _make_task()
    mock_asg = MagicMock()
    mock_asg.describe_auto_scaling_groups.return_value = {
      "AutoScalingGroups": [
        {
          "DesiredCapacity": 5,
          "MaxSize": 5,
          "Instances": [{"HealthStatus": "Healthy"}],
        }
      ]
    }

    await task._ensure_asg_capacity(mock_asg, "ladybug-large")

    mock_asg.set_desired_capacity.assert_not_called()

  async def test_asg_not_found_logs_warning(self):
    task = _make_task()
    mock_asg = MagicMock()
    mock_asg.describe_auto_scaling_groups.return_value = {"AutoScalingGroups": []}

    with patch(f"{TASK_MODULE}.env") as mock_env:
      mock_env.ENVIRONMENT = "staging"
      await task._ensure_asg_capacity(mock_asg, "ladybug-large")

    mock_asg.set_desired_capacity.assert_not_called()


@pytest.mark.unit
@pytest.mark.asyncio
class TestFinalizeSubscription:
  """Tests for _finalize_subscription helper."""

  def test_updates_subscription_status_to_active(self):
    task = _make_task()

    mock_subscription = MagicMock()
    mock_subscription.status = "upgrading"

    mock_db = MagicMock()
    mock_db_gen = iter([mock_db])

    with patch("robosystems.database.get_db_session") as mock_get_db:
      mock_get_db.return_value = mock_db_gen
      mock_db.query.return_value.filter_by.return_value.first.return_value = (
        mock_subscription
      )

      task._finalize_subscription("bsub_abc123")

    assert mock_subscription.status == "active"
    mock_db.commit.assert_called_once()

  def test_skips_if_not_upgrading(self):
    task = _make_task()

    mock_subscription = MagicMock()
    mock_subscription.status = "active"

    mock_db = MagicMock()
    mock_db_gen = iter([mock_db])

    with patch("robosystems.database.get_db_session") as mock_get_db:
      mock_get_db.return_value = mock_db_gen
      mock_db.query.return_value.filter_by.return_value.first.return_value = (
        mock_subscription
      )

      task._finalize_subscription("bsub_abc123")

    mock_db.commit.assert_not_called()
