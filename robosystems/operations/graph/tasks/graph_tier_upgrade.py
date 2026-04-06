"""Worker task for graph tier upgrade/downgrade.

Handles the async EBS volume migration between instance types when a
graph's tier changes (e.g., ladybug-standard -> ladybug-large).

Flow:
1. Update DynamoDB registries (graph status=migrating, volume tier=new)
2. Create EBS snapshot via Volume Manager Lambda (safety net)
3. Drain connections on old instance
4. Detach EBS volume (sits available with updated tier tag)
5. Ensure target tier ASG has capacity
6. Poll for volume reattachment by new instance
7. Verify graph accessible on new instance
8. Finalize: subscription -> active, graph registry -> active
"""

import asyncio
import json
import logging
from datetime import UTC, datetime
from typing import Any

import boto3
import httpx
from botocore.exceptions import ClientError

from robosystems.config import env
from robosystems.config.env import get_volume_manager_function_arn
from robosystems.worker.tasks import register_task
from robosystems.worker.tasks.base import BaseTask

logger = logging.getLogger(__name__)

DRAIN_TIMEOUT_SECONDS = 120
DRAIN_POLL_INTERVAL_SECONDS = 5
REATTACH_TIMEOUT_SECONDS = 300
REATTACH_POLL_INTERVAL_SECONDS = 10
GRAPH_API_PORT = 8001


def _get_dynamodb_resource():
  """Get DynamoDB resource with proper endpoint configuration."""
  region = env.AWS_REGION
  if env.is_development() and env.AWS_ENDPOINT_URL:
    return boto3.resource(
      "dynamodb", endpoint_url=env.AWS_ENDPOINT_URL, region_name=region
    )
  return boto3.resource("dynamodb", region_name=region)


def _get_lambda_client():
  """Get Lambda client."""
  region = env.AWS_REGION
  if env.is_development() and env.AWS_ENDPOINT_URL:
    return boto3.client("lambda", endpoint_url=env.AWS_ENDPOINT_URL, region_name=region)
  return boto3.client("lambda", region_name=region)


def _get_autoscaling_client():
  """Get Auto Scaling client."""
  region = env.AWS_REGION
  if env.is_development() and env.AWS_ENDPOINT_URL:
    return boto3.client(
      "autoscaling", endpoint_url=env.AWS_ENDPOINT_URL, region_name=region
    )
  return boto3.client("autoscaling", region_name=region)


@register_task("graph_tier_upgrade")
class GraphTierUpgradeTask(BaseTask):
  """Migrate a graph's EBS volume to a new tier's instance type."""

  async def execute(self) -> dict[str, Any]:
    old_tier = self.params["old_tier"]
    new_tier = self.params["new_tier"]
    subscription_id = self.params["subscription_id"]
    graph_id = self.graph_id

    dynamodb = _get_dynamodb_resource()
    graph_table = dynamodb.Table(env.GRAPH_REGISTRY_TABLE)
    volume_table = dynamodb.Table(env.VOLUME_REGISTRY_TABLE)
    instance_table = dynamodb.Table(env.INSTANCE_REGISTRY_TABLE)
    lambda_client = _get_lambda_client()
    asg_client = _get_autoscaling_client()
    volume_manager_arn = get_volume_manager_function_arn()

    # Volume manager is required in staging/prod — without it we can't
    # snapshot, detach, or reattach the EBS volume
    if not volume_manager_arn and not env.is_development():
      raise ValueError(
        "Volume Manager Lambda ARN not found. Cannot perform tier upgrade "
        "without volume management infrastructure."
      )

    # Track state for rollback
    old_instance_id = None
    old_private_ip = None
    volume_id = None
    snapshot_id = None
    volume_detached = False

    try:
      # Step 1: Look up current instance info from DynamoDB
      await self.report_progress("Looking up graph instance...", percent=5)
      graph_item = graph_table.get_item(Key={"graph_id": graph_id}).get("Item")
      if not graph_item:
        raise ValueError(f"Graph {graph_id} not found in graph registry")

      old_instance_id = graph_item["instance_id"]
      old_private_ip = graph_item.get("private_ip", "")

      # Find volume for this instance (query GSI instead of full table scan)
      volume_response = volume_table.query(
        IndexName="instance-index",
        KeyConditionExpression="instance_id = :iid",
        FilterExpression="#status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":iid": old_instance_id, ":status": "attached"},
      )
      volumes = volume_response.get("Items", [])
      if not volumes:
        raise ValueError(f"No attached volume found for instance {old_instance_id}")
      volume_id = volumes[0]["volume_id"]

      # Step 2: Update DynamoDB registries
      await self.report_progress("Updating registries...", percent=10)

      graph_table.update_item(
        Key={"graph_id": graph_id},
        UpdateExpression=(
          "SET #status = :status, migration_required = :mr, "
          "migration_source = :ms, last_updated = :ts"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
          ":status": "migrating",
          ":mr": True,
          ":ms": old_instance_id,
          ":ts": datetime.now(UTC).isoformat(),
        },
      )

      volume_table.update_item(
        Key={"volume_id": volume_id},
        UpdateExpression="SET tier = :tier, last_modified = :ts",
        ExpressionAttributeValues={
          ":tier": new_tier,
          ":ts": datetime.now(UTC).isoformat(),
        },
      )

      # Step 3: Create EBS snapshot (safety net)
      await self.report_progress("Creating EBS snapshot...", percent=15)

      if volume_manager_arn:
        snapshot_response = lambda_client.invoke(
          FunctionName=volume_manager_arn,
          InvocationType="RequestResponse",
          Payload=json.dumps(
            {
              "action": "snapshot_for_upgrade",
              "volume_id": volume_id,
              "graph_id": graph_id,
              "old_tier": old_tier,
              "new_tier": new_tier,
            }
          ),
        )
        snapshot_result = json.loads(snapshot_response["Payload"].read())
        if snapshot_result.get("statusCode") != 200:
          raise ValueError(
            f"Snapshot creation failed: {snapshot_result.get('error', 'unknown')}"
          )
        snapshot_id = snapshot_result.get("snapshot_id")
        logger.info(f"EBS snapshot created: {snapshot_id}")
      else:
        logger.warning("VOLUME_MANAGER_FUNCTION_ARN not set, skipping snapshot")

      # Step 4: Drain connections on old instance
      await self.report_progress("Draining connections...", percent=35)
      await self._drain_instance(old_private_ip)

      # Step 5: Detach volume
      await self.report_progress("Detaching volume...", percent=50)

      if volume_manager_arn:
        detach_response = lambda_client.invoke(
          FunctionName=volume_manager_arn,
          InvocationType="RequestResponse",
          Payload=json.dumps(
            {
              "action": "detach_volume",
              "volume_id": volume_id,
              "force": False,
            }
          ),
        )
        detach_result = json.loads(detach_response["Payload"].read())
        if detach_result.get("statusCode") != 200:
          raise ValueError(
            f"Volume detach failed: {detach_result.get('error', 'unknown')}"
          )
        volume_detached = True
        logger.info(f"Volume {volume_id} detached successfully")
      else:
        logger.warning("VOLUME_MANAGER_FUNCTION_ARN not set, skipping detach")
        volume_detached = True

      # Step 6: Ensure target tier ASG has capacity
      await self.report_progress("Provisioning new instance...", percent=60)
      await self._ensure_asg_capacity(asg_client, new_tier)

      # Step 7: Poll for volume reattachment
      await self.report_progress("Waiting for volume reattachment...", percent=70)
      new_instance_id = await self._wait_for_reattachment(
        volume_table, volume_id, old_instance_id
      )

      # Step 8: Verify graph is accessible on new instance
      await self.report_progress("Verifying graph...", percent=90)
      new_graph_item = graph_table.get_item(Key={"graph_id": graph_id}).get("Item")
      new_private_ip = new_graph_item.get("private_ip", "") if new_graph_item else ""

      if new_private_ip:
        await self._verify_graph_health(new_private_ip)

      # Step 9: Finalize — update PostgreSQL subscription status
      await self.report_progress("Finalizing upgrade...", percent=95)
      self._finalize_subscription(subscription_id)

      # Step 10: Update DynamoDB graph registry to active
      graph_table.update_item(
        Key={"graph_id": graph_id},
        UpdateExpression=(
          "SET #status = :status, last_updated = :ts "
          "REMOVE migration_required, migration_source"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
          ":status": "active",
          ":ts": datetime.now(UTC).isoformat(),
        },
      )

      # Decrement old instance database count (ASG protection removed if 0)
      try:
        instance_table.update_item(
          Key={"instance_id": old_instance_id},
          UpdateExpression="ADD database_count :dec SET last_deallocation = :ts",
          ConditionExpression="database_count > :zero",
          ExpressionAttributeValues={
            ":dec": -1,
            ":ts": datetime.now(UTC).isoformat(),
            ":zero": 0,
          },
        )
      except ClientError:
        logger.warning(f"Could not decrement database_count for {old_instance_id}")

      await self.report_progress("Upgrade complete", percent=100)

      from robosystems.dagster.reporting import report_asset_materialization

      await report_asset_materialization(
        asset_key="graph_tier_upgrade",
        description=f"Graph {graph_id} upgraded from {old_tier} to {new_tier}",
        metadata={
          "graph_id": graph_id,
          "old_tier": old_tier,
          "new_tier": new_tier,
          "old_instance_id": old_instance_id,
          "new_instance_id": new_instance_id,
          "snapshot_id": snapshot_id or "skipped",
          "operation_id": self.task_id,
        },
      )

      return {
        "status": "completed",
        "graph_id": graph_id,
        "old_tier": old_tier,
        "new_tier": new_tier,
        "new_instance_id": new_instance_id,
        "snapshot_id": snapshot_id,
      }

    except Exception as e:
      logger.error(
        f"Tier upgrade failed for {graph_id}: {e}",
        extra={"graph_id": graph_id, "old_tier": old_tier, "new_tier": new_tier},
        exc_info=True,
      )

      # Rollback
      await self._rollback(
        graph_id=graph_id,
        graph_table=graph_table,
        volume_table=volume_table,
        volume_id=volume_id,
        old_tier=old_tier,
        old_instance_id=old_instance_id,
        subscription_id=subscription_id,
        volume_detached=volume_detached,
        lambda_client=lambda_client if volume_manager_arn else None,
        volume_manager_arn=volume_manager_arn,
      )

      await self.report_progress(f"Upgrade failed: {e}", percent=100)
      raise

  async def _drain_instance(self, private_ip: str) -> None:
    """Drain connections on the old instance via Graph API admin endpoint."""
    if not private_ip:
      logger.warning("No private IP for drain, skipping")
      return

    base_url = f"http://{private_ip}:{GRAPH_API_PORT}"

    async with httpx.AsyncClient(timeout=10) as client:
      try:
        await client.post(f"{base_url}/admin/drain")
      except httpx.HTTPError as e:
        logger.warning(f"Drain request failed (may be expected): {e}")
        return

      # Poll for connections to reach 0
      elapsed = 0
      while elapsed < DRAIN_TIMEOUT_SECONDS:
        try:
          resp = await client.get(f"{base_url}/admin/connections")
          if resp.status_code == 200:
            data = resp.json()
            if data.get("active_connections", 1) == 0:
              logger.info("All connections drained")
              return
        except httpx.HTTPError:
          pass  # Instance may be shutting down — keep polling until timeout

        await asyncio.sleep(DRAIN_POLL_INTERVAL_SECONDS)
        elapsed += DRAIN_POLL_INTERVAL_SECONDS

    logger.warning("Drain timeout reached, proceeding with detach")

  async def _ensure_asg_capacity(self, asg_client: Any, tier: str) -> None:
    """Ensure the target tier ASG has capacity for a new instance."""
    environment = env.ENVIRONMENT
    asg_name = f"robosystems-{tier}-writers-{environment}-asg"

    try:
      response = asg_client.describe_auto_scaling_groups(
        AutoScalingGroupNames=[asg_name]
      )
      if not response["AutoScalingGroups"]:
        logger.warning(f"ASG {asg_name} not found, skipping capacity check")
        return

      asg = response["AutoScalingGroups"][0]
      desired = asg["DesiredCapacity"]
      max_size = asg["MaxSize"]

      # Check if all instances are at capacity
      healthy_instances = [
        i for i in asg.get("Instances", []) if i["HealthStatus"] == "Healthy"
      ]

      if desired < max_size:
        asg_client.set_desired_capacity(
          AutoScalingGroupName=asg_name,
          DesiredCapacity=desired + 1,
        )
        logger.info(f"Increased {asg_name} desired capacity to {desired + 1}")
      elif not healthy_instances:
        logger.warning(
          f"ASG {asg_name} at max capacity ({max_size}) with no healthy instances"
        )
      else:
        logger.info(f"ASG {asg_name} has {len(healthy_instances)} healthy instances")

    except ClientError as e:
      logger.warning(f"Failed to check/update ASG capacity: {e}")

  async def _wait_for_reattachment(
    self, volume_table: Any, volume_id: str, old_instance_id: str | None = None
  ) -> str:
    """Poll DynamoDB volume registry until volume is reattached to a new instance."""
    elapsed = 0
    while elapsed < REATTACH_TIMEOUT_SECONDS:
      if await self.is_cancelled():
        raise ValueError("Upgrade cancelled by user")

      response = volume_table.get_item(Key={"volume_id": volume_id})
      item = response.get("Item", {})

      if item.get("status") == "attached":
        new_instance_id = item.get("instance_id", "unknown")
        # Verify volume actually moved to a different instance
        if old_instance_id and new_instance_id == old_instance_id:
          logger.warning(
            f"Volume {volume_id} reattached to same instance {old_instance_id}, "
            f"waiting for new instance..."
          )
        else:
          logger.info(f"Volume {volume_id} reattached to {new_instance_id}")
          return new_instance_id

      await asyncio.sleep(REATTACH_POLL_INTERVAL_SECONDS)
      elapsed += REATTACH_POLL_INTERVAL_SECONDS

      if elapsed % 60 == 0:
        await self.report_progress(
          f"Waiting for volume reattachment... ({elapsed}s)",
          percent=70 + (elapsed / REATTACH_TIMEOUT_SECONDS * 20),
        )

    raise TimeoutError(
      f"Volume {volume_id} was not reattached within {REATTACH_TIMEOUT_SECONDS}s"
    )

  async def _verify_graph_health(self, private_ip: str) -> None:
    """Verify graph is accessible on the new instance."""
    base_url = f"http://{private_ip}:{GRAPH_API_PORT}"
    max_retries = 6

    async with httpx.AsyncClient(timeout=10) as client:
      for attempt in range(max_retries):
        try:
          resp = await client.get(f"{base_url}/health")
          if resp.status_code == 200:
            logger.info(f"Graph API healthy on {private_ip}")
            return
        except httpx.HTTPError:
          pass  # New instance may still be booting — retry

        if attempt < max_retries - 1:
          await asyncio.sleep(5)

    raise RuntimeError(
      f"Graph API health check failed on {private_ip} after {max_retries} attempts. "
      f"New instance may not have started correctly."
    )

  def _finalize_subscription(self, subscription_id: str) -> None:
    """Update subscription status back to active in PostgreSQL."""
    from robosystems.database import get_db_session
    from robosystems.models.core.billing import BillingSubscription

    db_gen = get_db_session()
    db = next(db_gen)
    try:
      subscription = db.query(BillingSubscription).filter_by(id=subscription_id).first()
      if subscription and subscription.status == "upgrading":
        subscription.status = "active"
        subscription.updated_at = datetime.now(UTC)
        db.commit()
        logger.info(f"Subscription {subscription_id} status restored to active")
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

  async def _rollback(
    self,
    graph_id: str,
    graph_table: Any,
    volume_table: Any,
    volume_id: str | None,
    old_tier: str,
    old_instance_id: str | None,
    subscription_id: str,
    volume_detached: bool,
    lambda_client: Any | None,
    volume_manager_arn: str = "",
  ) -> None:
    """Roll back tier upgrade on failure."""
    logger.info(f"Rolling back tier upgrade for {graph_id}")

    # Revert volume registry tier
    if volume_id:
      try:
        volume_table.update_item(
          Key={"volume_id": volume_id},
          UpdateExpression="SET tier = :tier, last_modified = :ts",
          ExpressionAttributeValues={
            ":tier": old_tier,
            ":ts": datetime.now(UTC).isoformat(),
          },
        )
      except ClientError as e:
        logger.error(f"Failed to revert volume tier: {e}")

    # Reattach volume to old instance if it was detached
    if volume_detached and volume_id and old_instance_id and lambda_client:
      try:
        lambda_client.invoke(
          FunctionName=volume_manager_arn,
          InvocationType="RequestResponse",
          Payload=json.dumps(
            {
              "action": "attach_volume",
              "volume_id": volume_id,
              "instance_id": old_instance_id,
            }
          ),
        )
        logger.info(f"Reattached volume {volume_id} to {old_instance_id}")
      except Exception as e:
        logger.error(
          f"Failed to reattach volume {volume_id} to {old_instance_id}: {e}. "
          f"Manual intervention required."
        )

    # Revert DynamoDB graph registry
    try:
      graph_table.update_item(
        Key={"graph_id": graph_id},
        UpdateExpression=(
          "SET #status = :status, last_updated = :ts "
          "REMOVE migration_required, migration_source"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
          ":status": "active",
          ":ts": datetime.now(UTC).isoformat(),
        },
      )
    except ClientError as e:
      logger.error(f"Failed to revert graph registry: {e}")

    # Revert PostgreSQL (subscription + graph tier + price)
    try:
      from robosystems.config.billing import BillingConfig, get_tier_credit_allocation
      from robosystems.database import get_db_session
      from robosystems.models.core.billing import BillingSubscription
      from robosystems.models.core.graph import Graph
      from robosystems.models.core.graph.graph_credits import GraphCredits

      old_plan_config = BillingConfig.get_subscription_plan(old_tier)
      old_price_cents = old_plan_config["base_price_cents"] if old_plan_config else 0

      db_gen = get_db_session()
      db = next(db_gen)
      try:
        subscription = (
          db.query(BillingSubscription).filter_by(id=subscription_id).first()
        )
        graph = Graph.get_by_id(graph_id, db)

        if subscription and subscription.status == "upgrading":
          subscription.plan_name = old_tier
          subscription.base_price_cents = old_price_cents
          subscription.status = "active"
          subscription.updated_at = datetime.now(UTC)

          # Revert Stripe pricing if linked
          if subscription.stripe_subscription_id:
            try:
              from robosystems.operations.providers.payment_provider import (
                get_payment_provider,
              )

              provider = get_payment_provider("stripe")
              old_stripe_price_id = provider.get_or_create_price(
                plan_name=old_tier,
                resource_type="graph",
              )
              provider.change_subscription_price(
                subscription_id=subscription.stripe_subscription_id,
                new_price_id=old_stripe_price_id,
              )
              logger.info(f"Reverted Stripe pricing to {old_tier} for {graph_id}")
            except Exception as stripe_err:
              logger.error(
                f"Failed to revert Stripe pricing for {graph_id}: {stripe_err}. "
                f"Manual Stripe intervention may be required."
              )

        if graph:
          graph.graph_tier = old_tier
          graph.updated_at = datetime.now(UTC)

        graph_credits = GraphCredits.get_by_graph_id(graph_id, db)
        if graph_credits:
          graph_credits.monthly_allocation = get_tier_credit_allocation(old_tier)

        db.commit()
        logger.info(f"Reverted PostgreSQL state for {graph_id}")
      finally:
        try:
          next(db_gen)
        except StopIteration:
          pass
    except Exception as e:
      logger.error(f"Failed to revert PostgreSQL state: {e}")
