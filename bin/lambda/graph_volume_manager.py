"""
Enhanced Graph Volume Manager Lambda Function

Manages the lifecycle of EBS volumes for Graph database instances with proper
volume reattachment on instance replacement.

Key improvements:
- Tracks database-to-volume mapping
- Reattaches existing volumes with data on instance launch
- Prevents data loss during instance replacement
"""

import json
import logging
import os
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
from botocore.exceptions import ClientError

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
ec2 = boto3.client("ec2")
dynamodb = boto3.resource("dynamodb")
sns = boto3.client("sns")
cloudwatch = boto3.client("cloudwatch")
ssm = boto3.client("ssm")

# Environment variables
ENVIRONMENT = os.environ["ENVIRONMENT"]
TABLE_NAME = os.environ["VOLUME_REGISTRY_TABLE"]
GRAPH_REGISTRY_TABLE = os.environ.get(
  "GRAPH_REGISTRY_TABLE", f"robosystems-graph-{ENVIRONMENT}-graph-registry"
)
ALERT_TOPIC = os.environ["ALERT_TOPIC_ARN"]
# Retention for fallback cleanup of orphaned volume snapshots (tagged AutoDelete: true)
# Note: DLM handles primary snapshot lifecycle with 3-day retention
RETENTION_DAYS = int(os.environ.get("SNAPSHOT_RETENTION_DAYS", "7"))

# How long a volume may sit in `claiming` before another launch may take it.
# Only reached if the Lambda dies between claiming a volume and attaching it —
# the ordinary failure path releases the claim itself. Must exceed the worst
# case of attach_and_register_volume (instance_running waiter 5 min, plus the
# volume_available waiter and attach retries) so a slow-but-live attach is
# never stolen out from under itself.
STALE_CLAIM_SECONDS = int(os.environ.get("STALE_CLAIM_SECONDS", "900"))

# Volume defaults (used as fallback when tier not found in tier_config)
DEFAULT_SIZE = 50  # GB
DEFAULT_TYPE = "gp3"
DEFAULT_IOPS = 3000
DEFAULT_THROUGHPUT = 125  # MB/s

# DynamoDB tables
table = dynamodb.Table(TABLE_NAME)  # Volume registry
graph_table = dynamodb.Table(GRAPH_REGISTRY_TABLE)  # Graph registry


def handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """Main Lambda handler"""
  action = event.get("action")

  try:
    if action == "instance_launch":
      # NEW: Handle instance launch with volume reattachment
      return handle_instance_launch(event)
    elif action == "get_or_create_volume":
      return get_or_create_volume(event)
    elif action == "attach_volume":
      return attach_volume(event)
    elif action == "detach_volume":
      return detach_volume(event)
    elif action == "expand_volume":
      return expand_volume(event)
    elif action == "cleanup_orphaned":
      return cleanup_orphaned_volumes()
    elif action == "cleanup_snapshots":
      return cleanup_old_snapshots(event)
    elif action == "restore_from_snapshot":
      return restore_from_snapshot(event)
    elif action == "register_volume":
      return register_volume(event)
    elif action == "snapshot_for_upgrade":
      return snapshot_for_upgrade(event)
    elif action == "sync_registry":
      # NEW: Synchronize registry with actual EC2 volumes
      return sync_registry_with_ec2(event)
    else:
      return {"statusCode": 400, "error": f"Unknown action: {action}"}
  except Exception as e:
    logger.error(f"Error in {action}: {e!s}", exc_info=True)
    send_alert("Volume Manager Error", f"Action: {action}\nError: {e!s}")
    return {"statusCode": 500, "error": "Internal server error"}


def handle_instance_launch(event: dict[str, Any]) -> dict[str, Any]:
  """
  Handle new instance launch - reattach existing volumes or create new ones.
  This is the critical fix for volume persistence.
  """
  # Validate required fields (except availability_zone which we'll look up)
  required_fields = ["instance_id", "node_type"]
  for field in required_fields:
    if field not in event or not event[field]:
      error_msg = f"Missing or empty required field: {field}"
      logger.error(error_msg)
      return {"statusCode": 400, "error": error_msg}

  instance_id = event["instance_id"]
  node_type = event["node_type"]  # writer, shared_master, shared_replica
  tier = event.get("tier", "ladybug-standard")
  databases = event.get("databases", [])  # List of databases this instance should have

  # Always fetch the actual AZ from the instance - don't trust what's provided
  try:
    instance_info = ec2.describe_instances(InstanceIds=[instance_id])
    if (
      not instance_info["Reservations"]
      or not instance_info["Reservations"][0]["Instances"]
    ):
      error_msg = f"Instance {instance_id} not found"
      logger.error(error_msg)
      return {"statusCode": 404, "error": error_msg}

    az = instance_info["Reservations"][0]["Instances"][0]["Placement"][
      "AvailabilityZone"
    ]
    logger.info(f"Fetched AZ for instance {instance_id}: {az}")

  except Exception as e:
    error_msg = f"Failed to get AZ for instance {instance_id}: {e}"
    logger.error(error_msg)
    return {"statusCode": 500, "error": error_msg}

  logger.info(
    f"Handling instance launch for {instance_id}, node_type={node_type}, tier={tier}, az={az}, databases={databases}"
  )

  # For shared repositories, always look for existing SEC volume first
  # Even if databases list is empty on instance launch, we want to reattach SEC data
  if node_type == "shared_master" or (
    node_type == "writer" and tier == "ladybug-shared"
  ):
    # Look for existing SEC volume
    existing_volume = find_volume_with_database("sec", az, tier)
    if existing_volume:
      logger.info(
        f"Found existing SEC volume {existing_volume['volume_id']} for shared repository"
      )
      # Preserve the SEC database in the volume's database list
      return attach_and_register_volume(
        existing_volume["volume_id"], instance_id, ["sec"]
      )
    else:
      # No existing SEC volume, will create new one
      if not databases:
        databases = ["sec"]  # Ensure SEC is in the database list for shared nodes
      elif "sec" not in databases:
        databases.append("sec")

  # Return anything stranded mid-claim to the pool before scanning, or a volume
  # whose claimant died would stay invisible to the `available` filter forever.
  reclaim_stale_claims(az, tier)

  # Check for any available volumes in the same AZ and tier
  # CRITICAL: Must match availability zone exactly to avoid attachment failures
  all_items = []
  last_evaluated_key = None

  # Handle pagination for large number of volumes
  while True:
    scan_params = {
      "FilterExpression": "availability_zone = :az AND tier = :tier AND #status = :status",
      "ExpressionAttributeNames": {"#status": "status"},
      "ExpressionAttributeValues": {":az": az, ":tier": tier, ":status": "available"},
    }

    if last_evaluated_key:
      scan_params["ExclusiveStartKey"] = last_evaluated_key

    response = table.scan(**scan_params)
    all_items.extend(response.get("Items", []))

    last_evaluated_key = response.get("LastEvaluatedKey")
    if not last_evaluated_key:
      break

  logger.info(f"Found {len(all_items)} available volumes in AZ {az} with tier {tier}")

  if all_items:
    # Double-check AZ matches (defensive programming)
    valid_volumes = [v for v in all_items if v.get("availability_zone") == az]

    if valid_volumes:
      # Try candidates in preference order, claiming each atomically before
      # attaching. A lost claim means a concurrent launch took that volume, so
      # move on rather than fail — the attach retry loop cannot help there,
      # since it waits on a volume that is now legitimately held.
      for volume, volume_databases in ordered_volume_candidates(
        valid_volumes, databases
      ):
        volume_id = volume["volume_id"]

        if not claim_volume(volume_id, instance_id):
          continue

        logger.info(
          f"Claimed volume {volume_id} with databases: {volume.get('databases')} in AZ {az}"
        )
        try:
          return attach_and_register_volume(volume_id, instance_id, volume_databases)
        except Exception:
          # Put it back so the next launch can use it; the instance still fails,
          # which is the pre-existing behaviour for a genuine attach failure.
          release_claim(volume_id, instance_id)
          raise

      logger.info(
        f"All {len(valid_volumes)} candidate volumes in AZ {az} were claimed by "
        "other instances; creating a new volume"
      )
    else:
      logger.warning(
        f"Found {len(all_items)} volumes but none in AZ {az}, creating new volume"
      )

  # No existing volume found, create new one
  logger.info("No existing volume found, creating new volume")
  return create_and_attach_volume(instance_id, tier, az, databases, node_type)


def find_volume_with_database(database: str, az: str, tier: str) -> dict | None:
  """Find a volume that contains a specific database"""
  response = table.scan(
    FilterExpression="contains(databases, :db) AND availability_zone = :az AND tier = :tier AND #status = :status",
    ExpressionAttributeNames={"#status": "status"},
    ExpressionAttributeValues={
      ":db": database,
      ":az": az,
      ":tier": tier,
      ":status": "available",
    },
  )

  if response["Items"]:
    return response["Items"][0]
  return None


def update_graph_registry_for_instance(
  instance_id: str, databases: list[str], private_ip: str | None = None
) -> dict[str, int]:
  """
  Update graph registry entries when databases move to a new instance.

  This is critical for routing - when an instance is replaced, we need to update
  the graph registry so queries route to the correct IP address.

  Args:
    instance_id: The new instance ID
    databases: List of database/graph IDs on this instance
    private_ip: The new instance's private IP (will be fetched if not provided)

  Returns:
    Dict with counts of updated, skipped, and failed entries
  """
  if not databases:
    logger.info("No databases to update in graph registry")
    return {"updated": 0, "skipped": 0, "failed": 0}

  # Get private IP if not provided
  if not private_ip:
    try:
      response = ec2.describe_instances(InstanceIds=[instance_id])
      if response["Reservations"] and response["Reservations"][0]["Instances"]:
        private_ip = response["Reservations"][0]["Instances"][0].get("PrivateIpAddress")
        if not private_ip:
          logger.warning(f"Instance {instance_id} does not have a private IP yet")
          # Don't fail - the instance might still be initializing
    except Exception as e:
      logger.error(f"Failed to get private IP for instance {instance_id}: {e}")

  results = {"updated": 0, "skipped": 0, "failed": 0}

  for db_id in databases:
    try:
      # Skip subgraph IDs (format: kg123_workspacename) - they're not in graph registry
      # Subgraphs share their parent's instance, parent registry entry handles routing
      if "_" in db_id and db_id.startswith("kg"):
        logger.debug(f"Skipping subgraph {db_id} - not stored in graph registry")
        results["skipped"] += 1
        continue

      # Check if database exists in graph registry
      response = graph_table.get_item(Key={"graph_id": db_id})
      if "Item" not in response:
        logger.warning(
          f"Database {db_id} not found in graph registry - may be new or deleted"
        )
        results["skipped"] += 1
        continue

      # Update the graph registry entry with new instance info
      update_expr = "SET instance_id = :iid, last_updated = :timestamp"
      expr_values: dict[str, Any] = {
        ":iid": instance_id,
        ":timestamp": datetime.now(UTC).isoformat(),
      }

      # Only update private_ip if we have it
      if private_ip:
        update_expr += ", private_ip = :ip"
        expr_values[":ip"] = private_ip

      graph_table.update_item(
        Key={"graph_id": db_id},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=expr_values,
      )

      logger.info(
        f"Updated graph registry for {db_id}: instance={instance_id}, ip={private_ip}"
      )
      results["updated"] += 1

    except Exception as e:
      logger.error(f"Failed to update graph registry for {db_id}: {e}")
      results["failed"] += 1

  # Log summary
  logger.info(
    f"Graph registry update complete: {results['updated']} updated, "
    f"{results['skipped']} skipped, {results['failed']} failed"
  )

  # Alert if any failures
  if results["failed"] > 0:
    send_alert(
      "Graph Registry Update Failures",
      f"Failed to update {results['failed']} graph registry entries for instance {instance_id}. "
      f"Databases: {databases}. This may cause routing issues!",
    )

  return results


def claim_volume(volume_id: str, instance_id: str) -> bool:
  """Atomically take a volume out of the `available` pool.

  Returns True when this caller won the volume, False when another instance
  claimed it first.

  Selection above is a scan followed by a deterministic sort, so two instances
  launching concurrently in the same AZ and tier see the same list and pick the
  same candidate. Without this conditional write both proceed to attach; EC2
  makes one of them lose with VolumeInUse, and the attach retry loop then waits
  out a volume that will never free before failing the instance. That is
  survivable while instances cycle one at a time, but it becomes the normal case
  under any concurrent replacement — a batched rolling update, an ASG instance
  refresh, a spot reclaim during a deploy, or two graphs provisioned together.
  """
  try:
    table.update_item(
      Key={"volume_id": volume_id},
      UpdateExpression=(
        "SET #status = :claiming, instance_id = :instance_id, claimed_at = :timestamp"
      ),
      ConditionExpression="#status = :available",
      ExpressionAttributeNames={"#status": "status"},
      ExpressionAttributeValues={
        ":claiming": "claiming",
        ":available": "available",
        ":instance_id": instance_id,
        ":timestamp": datetime.now(UTC).isoformat(),
      },
    )
    return True
  except ClientError as e:
    if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
      raise
    logger.info(
      f"Volume {volume_id} was claimed by another instance before {instance_id} "
      "could take it; trying the next candidate"
    )
    # Emitted so the contention rate is observable before concurrency is raised.
    # At MaxBatchSize 1 this should stay at zero outside of scale-out overlap; a
    # non-zero baseline means the race is already live at the current fleet size.
    try:
      cloudwatch.put_metric_data(
        Namespace=f"RoboSystems/Graph/{ENVIRONMENT}",
        MetricData=[
          {"MetricName": "VolumeClaimContention", "Value": 1, "Unit": "Count"}
        ],
      )
    except Exception as metric_error:
      logger.warning(f"Failed to publish claim contention metric: {metric_error}")
    return False


def release_claim(volume_id: str, instance_id: str) -> None:
  """Return a claimed-but-unattached volume to the pool.

  Guarded on this instance still holding the claim so a release can never undo
  a later claim by someone else.
  """
  try:
    table.update_item(
      Key={"volume_id": volume_id},
      UpdateExpression="SET #status = :available REMOVE claimed_at",
      ConditionExpression="#status = :claiming AND instance_id = :instance_id",
      ExpressionAttributeNames={"#status": "status"},
      ExpressionAttributeValues={
        ":available": "available",
        ":claiming": "claiming",
        ":instance_id": instance_id,
      },
    )
    logger.info(f"Released claim on volume {volume_id} held by {instance_id}")
  except ClientError as e:
    if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
      raise
    logger.warning(
      f"Claim on {volume_id} was no longer held by {instance_id}; nothing released"
    )


def reclaim_stale_claims(az: str, tier: str) -> int:
  """Return volumes stranded in `claiming` to the pool.

  A claim is only stranded when the Lambda died between claiming and attaching,
  since every handled failure releases its own claim. Without this sweep such a
  volume is invisible to future scans forever, because they filter on
  `available` — the graph it holds would be stranded with it.
  """
  cutoff = (datetime.now(UTC) - timedelta(seconds=STALE_CLAIM_SECONDS)).isoformat()
  reclaimed = 0

  last_evaluated_key = None
  while True:
    scan_params: dict[str, Any] = {
      "FilterExpression": (
        "availability_zone = :az AND tier = :tier AND #status = :claiming "
        "AND claimed_at < :cutoff"
      ),
      "ExpressionAttributeNames": {"#status": "status"},
      "ExpressionAttributeValues": {
        ":az": az,
        ":tier": tier,
        ":claiming": "claiming",
        ":cutoff": cutoff,
      },
    }
    if last_evaluated_key:
      scan_params["ExclusiveStartKey"] = last_evaluated_key

    response = table.scan(**scan_params)
    for item in response.get("Items", []):
      volume_id = item["volume_id"]
      try:
        table.update_item(
          Key={"volume_id": volume_id},
          UpdateExpression="SET #status = :available REMOVE claimed_at",
          # Re-check staleness at write time: a concurrent launch may have
          # legitimately re-claimed it between the scan and here.
          ConditionExpression="#status = :claiming AND claimed_at < :cutoff",
          ExpressionAttributeNames={"#status": "status"},
          ExpressionAttributeValues={
            ":available": "available",
            ":claiming": "claiming",
            ":cutoff": cutoff,
          },
        )
        reclaimed += 1
        logger.warning(
          f"Reclaimed volume {volume_id} stranded in `claiming` since "
          f"{item.get('claimed_at')}"
        )
      except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
          raise

    last_evaluated_key = response.get("LastEvaluatedKey")
    if not last_evaluated_key:
      break

  return reclaimed


def ordered_volume_candidates(
  valid_volumes: list[dict], databases: Any
) -> list[tuple[dict, Any]]:
  """Volumes to try in preference order, each paired with the databases to register.

  Order is unchanged: a volume already holding one of this instance's databases,
  then any volume carrying data, then an empty one. What changed is that every
  candidate is returned rather than only the best, so a caller that loses a
  claim race falls through to the next instead of failing.
  """
  matching: list[tuple[dict, Any]] = []
  with_data: list[tuple[dict, Any]] = []
  empty: list[tuple[dict, Any]] = []

  for volume in valid_volumes:
    volume_dbs = volume.get("databases") or []
    if databases and any(db in volume_dbs for db in databases):
      matching.append((volume, volume_dbs))
    elif volume_dbs:
      with_data.append((volume, volume_dbs))
    else:
      empty.append((volume, databases))

  return matching + with_data + empty


def attach_and_register_volume(
  volume_id: str, instance_id: str, databases: Any
) -> dict[str, Any]:
  """Attach a volume to an instance and update registry"""
  device = "/dev/xvdf"

  # Wait for instance to be running
  logger.info(f"Waiting for instance {instance_id} to be in running state...")
  waiter = ec2.get_waiter("instance_running")
  try:
    waiter.wait(InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60})
  except Exception as e:
    logger.error(f"Instance {instance_id} did not reach running state: {e}")
    return {"statusCode": 500, "error": f"Instance not ready: {e!s}"}

  # Wait for volume to actually reach `available`. EBS detach is async — the
  # detachment Lambda may have ack'd before the volume left `detaching`. Without
  # this wait, AttachVolume races the previous instance's detach and EC2 returns
  # VolumeInUse. Log-and-proceed on timeout; the retry loop below handles the
  # residual race.
  vol_waiter = ec2.get_waiter("volume_available")
  try:
    vol_waiter.wait(VolumeIds=[volume_id], WaiterConfig={"Delay": 5, "MaxAttempts": 24})
  except Exception as e:
    logger.warning(
      f"Volume {volume_id} did not reach available in 2 min: {e}; will attempt attach"
    )

  # Attach the volume with retry logic.
  # Retry both IncorrectState (instance not ready) and VolumeInUse (volume still
  # detaching from prior instance). Use a longer sleep for VolumeInUse since
  # actual detach completion takes 15-30s in observed AWS host-failure recoveries.
  max_retries = 3
  retryable_tokens = ("IncorrectState", "VolumeInUse")
  response = None
  for attempt in range(max_retries):
    try:
      logger.info(
        f"Attempting to attach volume {volume_id} to instance {instance_id} (attempt {attempt + 1}/{max_retries})"
      )
      response = ec2.attach_volume(
        VolumeId=volume_id, InstanceId=instance_id, Device=device
      )
      break
    except Exception as e:
      err_str = str(e)
      if (
        any(token in err_str for token in retryable_tokens)
        and attempt < max_retries - 1
      ):
        sleep_s = 30 if "VolumeInUse" in err_str else 10
        logger.warning(
          f"Volume not ready ({e}); sleeping {sleep_s}s before retry {attempt + 2}/{max_retries}"
        )
        time.sleep(sleep_s)
        continue
      logger.error(f"Failed to attach volume after {attempt + 1} attempts: {e}")
      raise

  if response is None:
    raise RuntimeError("Failed to attach volume: no response received")

  # Wait for attachment
  waiter = ec2.get_waiter("volume_in_use")
  waiter.wait(VolumeIds=[volume_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60})

  # Preserve the registry's existing databases list when present. This list is
  # maintained by allocation_manager.py (add on graph create, remove on graph
  # drop). Userdata for non-shared writers passes [] on every launch, so a naive
  # overwrite wipes the volume↔graph linkage on every instance replacement —
  # exactly the bug that orphaned kg19dcbe757481af06fc9b on 2026-05-08.
  current = table.get_item(Key={"volume_id": volume_id}).get("Item", {})
  existing_dbs = current.get("databases") or []
  caller_dbs = databases if isinstance(databases, list) else [databases]
  final_dbs = existing_dbs if existing_dbs else caller_dbs

  # Update registry
  table.update_item(
    Key={"volume_id": volume_id},
    UpdateExpression="SET instance_id = :instance_id, #status = :status, last_attached = :timestamp, databases = :databases",
    ExpressionAttributeNames={"#status": "status"},
    ExpressionAttributeValues={
      ":instance_id": instance_id,
      ":status": "attached",
      ":timestamp": datetime.now(UTC).isoformat(),
      ":databases": final_dbs,
    },
  )

  logger.info(f"Successfully attached volume {volume_id} to instance {instance_id}")

  # CRITICAL: Update graph registry with new instance info for routing.
  # Use final_dbs (preserved list) so a replacement instance reroutes ALL graphs
  # the volume actually holds, not just the (often empty) list userdata passed.
  graph_update_result = update_graph_registry_for_instance(instance_id, final_dbs)
  logger.info(f"Graph registry update result: {graph_update_result}")

  # Signal the instance that volume is ready. Use final_dbs (preserved list),
  # not the caller-provided databases — otherwise the OS-side databases.json
  # re-introduces the orphaning bug we just fixed in the registry.
  try:
    ssm.send_command(
      InstanceIds=[instance_id],
      DocumentName="AWS-RunShellScript",
      Parameters={
        "commands": [
          "echo 'VOLUME_READY' > /tmp/volume_status",
          f"echo '{json.dumps(final_dbs)}' > /tmp/databases.json",
        ]
      },
    )
  except Exception as e:
    logger.warning(f"Failed to signal instance: {e}")

  return {
    "statusCode": 200,
    "volume_id": volume_id,
    "attachment_state": response["State"],
    "databases": final_dbs,
  }


def create_and_attach_volume(
  instance_id: str, tier: str, az: str, databases: list[str], node_type: str
) -> dict[str, Any]:
  """Create a new volume and attach it to the instance"""
  # Initial volume sizes. These are starting points, not caps: the volume
  # monitor expands at 80% usage every 15 minutes, and EBS volumes can grow
  # but never shrink - so provisioning small and growing on demand costs
  # strictly less than provisioning for a ceiling most graphs never reach.
  # The product cap is enforced separately by instance_storage_limit_gb in
  # .github/configs/graph.yml (20/50/100 GB); these do not need to match it,
  # since expansion at 80% fires before the write-path cap is reached.
  # gp3's 3000 IOPS / 125 MBps baseline is size-independent, so a smaller
  # volume carries no performance penalty.
  tier_config = {
    "ladybug-standard": {"size": 20, "iops": 3000},
    "ladybug-large": {"size": 50, "iops": 3000},
    "ladybug-xlarge": {"size": 50, "iops": 3000},
    "ladybug-shared": {"size": 200, "iops": 3000},
  }

  config = tier_config.get(tier, {"size": DEFAULT_SIZE, "iops": DEFAULT_IOPS})

  # Determine volume name based on node type
  if node_type in ["shared_master", "shared_replica"]:
    volume_name = f"robosystems-graph-shared-{ENVIRONMENT}-data"
  else:
    volume_name = f"robosystems-graph-writer-{ENVIRONMENT}-data"

  # Create volume
  volume_response = ec2.create_volume(
    AvailabilityZone=az,
    Size=config["size"],
    VolumeType=DEFAULT_TYPE,
    Iops=config["iops"],
    Throughput=DEFAULT_THROUGHPUT,
    Encrypted=True,
    TagSpecifications=[
      {
        "ResourceType": "volume",
        "Tags": [
          {"Key": "Name", "Value": volume_name},
          {"Key": "Environment", "Value": ENVIRONMENT},
          {"Key": "Tier", "Value": tier},
          {"Key": "NodeType", "Value": node_type},
          {"Key": "Service", "Value": "RoboSystems"},
          {"Key": "Component", "Value": "GraphWriter"},
          {"Key": "VolumeType", "Value": "GraphData"},
          {"Key": "ManagedBy", "Value": "GraphVolumeManager"},
          {"Key": "CreatedAt", "Value": datetime.now(UTC).isoformat()},
          {"Key": "DatabaseId", "Value": databases[0] if databases else "unassigned"},
          {"Key": "InstanceId", "Value": instance_id},
        ],
      }
    ],
  )

  volume_id = volume_response["VolumeId"]
  logger.info(f"Created new volume: {volume_id}")

  # Register in DynamoDB
  table.put_item(
    Item={
      "volume_id": volume_id,
      "instance_id": instance_id,
      "availability_zone": az,
      "tier": tier,
      "status": "attaching",
      "databases": databases,
      "created_at": datetime.now(UTC).isoformat(),
      "node_type": node_type,
    }
  )

  # Wait for volume to be available
  waiter = ec2.get_waiter("volume_available")
  waiter.wait(VolumeIds=[volume_id])

  # Attach the volume
  return attach_and_register_volume(volume_id, instance_id, databases)


def get_or_create_volume(event: dict[str, Any]) -> dict[str, Any]:
  """Legacy function - redirects to handle_instance_launch"""
  logger.warning("get_or_create_volume called - redirecting to handle_instance_launch")
  return handle_instance_launch(event)


def attach_volume(event: dict[str, Any]) -> dict[str, Any]:
  """Attach a volume to an instance"""
  volume_id = event["volume_id"]
  instance_id = event["instance_id"]
  device = event.get("device", "/dev/xvdf")

  # Attach the volume
  response = ec2.attach_volume(
    VolumeId=volume_id, InstanceId=instance_id, Device=device
  )

  # Update registry
  table.update_item(
    Key={"volume_id": volume_id},
    UpdateExpression="SET instance_id = :instance_id, #status = :status, last_attached = :timestamp",
    ExpressionAttributeNames={"#status": "status"},
    ExpressionAttributeValues={
      ":instance_id": instance_id,
      ":status": "attached",
      ":timestamp": datetime.now(UTC).isoformat(),
    },
  )

  return {"statusCode": 200, "attachment_state": response["State"]}


def detach_volume(event: dict[str, Any]) -> dict[str, Any]:
  """Safely detach a volume.

  Before detaching, take a defensive 3-day-retention snapshot of any volume
  that holds unique data (writers + shared_master). This captures the
  volume's last-known-good state at the moment of instance replacement,
  giving us fast recovery when rebuild-from-source is slow or broken.
  Replicas (and empty pool volumes) are skipped.
  """
  volume_id = event["volume_id"]
  force = event.get("force", False)

  # Load registry metadata so we can decide whether to snapshot.
  item = table.get_item(Key={"volume_id": volume_id}).get("Item", {})
  databases = item.get("databases") or []
  node_type = item.get("node_type", "")
  tier = item.get("tier", "")

  _maybe_snapshot_pre_detach(volume_id, node_type, tier, databases)

  # Detach the volume
  try:
    ec2.detach_volume(VolumeId=volume_id, Force=force)
  except Exception as e:
    logger.error(f"Failed to detach volume: {e}")
    if not force:
      raise

  # Update registry - PRESERVE databases list!
  table.update_item(
    Key={"volume_id": volume_id},
    UpdateExpression="SET instance_id = :instance_id, #status = :status, last_detached = :timestamp, databases = :databases",
    ExpressionAttributeNames={"#status": "status"},
    ExpressionAttributeValues={
      ":instance_id": "unattached",
      ":status": "available",
      ":timestamp": datetime.now(UTC).isoformat(),
      ":databases": databases,  # Preserve the databases list
    },
  )

  logger.info(f"Volume {volume_id} detached, preserving databases: {databases}")

  return {"statusCode": 200, "volume_id": volume_id, "databases": databases}


def _maybe_snapshot_pre_detach(
  volume_id: str, node_type: str, tier: str, databases: list[str]
) -> None:
  """Defensively snapshot a volume before detach.

  Skipped for shared_replica (rebuilt from S3 on boot, no unique state) and
  for empty pool volumes (no data to preserve). CreateSnapshot is async —
  AWS copies blocks from the volume in the background even after detach,
  so we don't wait. Failure logs + alerts but does NOT block the detach.

  For shared_master the snapshot is a *rolling single*: after the new snapshot
  is created we delete any prior pre-detach snapshots of the same volume, so at
  most one exists at a time. The master detaches on every park cycle and its
  graph is fully rebuilt nightly (each snapshot delta ≈ the whole volume), so
  retaining a window's worth accumulates expensively — and the master's real
  backup is S3 anyway, making the EBS snapshot a single last-known-good fallback
  rather than a history. Prune runs only AFTER a successful create, so there is
  never a moment with zero fallback. Dedicated writers keep the every-detach,
  3-day multi-snapshot history unchanged (unique data, infrequent detaches).
  """
  if node_type == "shared_replica":
    logger.info(f"Skipping pre-detach snapshot for {volume_id}: shared_replica")
    return
  if not databases:
    logger.info(f"Skipping pre-detach snapshot for {volume_id}: no databases")
    return

  # AWS snapshot Description is capped at 255 chars; keep it concise.
  # The full graph list lives in the Databases tag (JSON-encoded) below — the
  # description just needs to be human-identifiable at a glance.
  description = (
    f"Pre-detach {volume_id} node_type={node_type} tier={tier} dbs={len(databases)}"
  )[:255]

  try:
    snapshot = ec2.create_snapshot(
      VolumeId=volume_id,
      Description=description,
      TagSpecifications=[
        {
          "ResourceType": "snapshot",
          "Tags": [
            {"Key": "Name", "Value": f"pre-detach-{volume_id}"},
            {"Key": "Environment", "Value": ENVIRONMENT},
            {"Key": "Type", "Value": "pre_detach"},
            {"Key": "AutoDelete", "Value": "true"},
            {"Key": "RetentionDays", "Value": "3"},
            {"Key": "VolumeId", "Value": volume_id},
            {"Key": "NodeType", "Value": node_type},
            {"Key": "Tier", "Value": tier},
            {"Key": "Databases", "Value": json.dumps(databases)},
          ],
        }
      ],
    )
  except Exception as e:
    # Don't block detach on snapshot failure — alert and continue.
    logger.error(f"Failed to create pre-detach snapshot for {volume_id}: {e}")
    send_alert(
      "Pre-detach Snapshot Failed",
      f"Failed to snapshot {volume_id} (node_type={node_type}, "
      f"databases={databases}) before detach: {e}",
    )
    return

  snapshot_id = snapshot["SnapshotId"]
  logger.info(
    f"Created pre-detach snapshot {snapshot_id} for {volume_id} "
    f"(node_type={node_type}, databases={databases})"
  )

  # Rolling single: keep only the just-created snapshot for the shared master.
  if node_type == "shared_master":
    _prune_prior_pre_detach_snapshots(volume_id, keep_snapshot_id=snapshot_id)


def _prune_prior_pre_detach_snapshots(volume_id: str, keep_snapshot_id: str) -> None:
  """Delete every pre-detach snapshot of ``volume_id`` except ``keep_snapshot_id``.

  Enforces the shared master's rolling-single policy. Best effort and never
  raises: a snapshot mid-use (e.g. a volume being created from it) cannot be
  deleted and is left for the retention cleanup to reap. Pruning must never
  block the detach.
  """
  try:
    response = ec2.describe_snapshots(
      OwnerIds=["self"],
      Filters=[
        {"Name": "volume-id", "Values": [volume_id]},
        {"Name": "tag:Type", "Values": ["pre_detach"]},
        {"Name": "tag:Environment", "Values": [ENVIRONMENT]},
      ],
    )
  except Exception as e:
    logger.error(f"Failed to list prior pre-detach snapshots for {volume_id}: {e}")
    return

  for snap in response.get("Snapshots", []):
    snap_id = snap["SnapshotId"]
    if snap_id == keep_snapshot_id:
      continue
    try:
      ec2.delete_snapshot(SnapshotId=snap_id)
      logger.info(f"Pruned prior pre-detach snapshot {snap_id} for {volume_id}")
    except Exception as e:
      logger.warning(
        f"Could not prune pre-detach snapshot {snap_id} for {volume_id}: {e}"
      )


def expand_volume(event: dict[str, Any]) -> dict[str, Any]:
  """Expand a volume size"""
  volume_id = event["volume_id"]
  new_size = event["new_size"]

  # Modify volume (ensure size is an integer)
  response = ec2.modify_volume(VolumeId=volume_id, Size=int(new_size))

  # Update registry
  table.update_item(
    Key={"volume_id": volume_id},
    UpdateExpression="SET size = :size, last_modified = :timestamp",
    ExpressionAttributeValues={
      ":size": new_size,
      ":timestamp": datetime.now(UTC).isoformat(),
    },
  )

  return {
    "statusCode": 200,
    "modification_state": response["VolumeModification"]["ModificationState"],
  }


def snapshot_for_upgrade(event: dict[str, Any]) -> dict[str, Any]:
  """Create an EBS snapshot before a tier upgrade and wait for completion.

  This provides a safety net for the upgrade process. If the volume migration
  fails, the snapshot can be used to restore the data.
  """
  volume_id = event["volume_id"]
  graph_id = event["graph_id"]
  old_tier = event["old_tier"]
  new_tier = event["new_tier"]

  logger.info(
    f"Creating upgrade snapshot for volume {volume_id} "
    f"(graph={graph_id}, {old_tier} -> {new_tier})"
  )

  snapshot = ec2.create_snapshot(
    VolumeId=volume_id,
    Description=f"Tier upgrade: {old_tier} -> {new_tier} for {graph_id}",
    TagSpecifications=[
      {
        "ResourceType": "snapshot",
        "Tags": [
          {
            "Key": "Name",
            "Value": f"upgrade-{graph_id}-{old_tier}-to-{new_tier}",
          },
          {"Key": "Environment", "Value": ENVIRONMENT},
          {"Key": "Type", "Value": "tier_upgrade"},
          {"Key": "GraphId", "Value": graph_id},
          {"Key": "UpgradeFrom", "Value": old_tier},
          {"Key": "UpgradeTo", "Value": new_tier},
          {"Key": "AutoDelete", "Value": "true"},
        ],
      }
    ],
  )

  snapshot_id = snapshot["SnapshotId"]
  logger.info(f"Snapshot {snapshot_id} created, waiting for completion...")

  waiter = ec2.get_waiter("snapshot_completed")
  waiter.wait(
    SnapshotIds=[snapshot_id],
    WaiterConfig={"Delay": 15, "MaxAttempts": 40},  # 10 min max
  )

  logger.info(f"Snapshot {snapshot_id} completed for upgrade {old_tier} -> {new_tier}")

  return {
    "statusCode": 200,
    "snapshot_id": snapshot_id,
    "volume_id": volume_id,
  }


def cleanup_orphaned_volumes() -> dict[str, Any]:
  """Clean up orphaned volumes"""
  # Find volumes that have been detached for more than 24 hours
  cutoff_time = datetime.now(UTC) - timedelta(hours=24)

  response = table.scan(
    FilterExpression="#status = :status AND last_detached < :cutoff",
    ExpressionAttributeNames={"#status": "status"},
    ExpressionAttributeValues={
      ":status": "available",
      ":cutoff": cutoff_time.isoformat(),
    },
  )

  orphaned = []
  for item in response["Items"]:
    # Don't delete volumes with databases!
    if item.get("databases"):
      logger.warning(
        f"Volume {item['volume_id']} has databases {item['databases']} - NOT deleting"
      )
      continue

    volume_id = item["volume_id"]
    node_type = item.get("node_type", "")
    try:
      # Skip snapshots for shared volumes - they're backed up to S3
      if node_type not in ("shared_master", "shared_replica"):
        ec2.create_snapshot(
          VolumeId=volume_id,
          Description=f"Orphaned volume cleanup - {volume_id}",
          TagSpecifications=[
            {
              "ResourceType": "snapshot",
              "Tags": [
                {"Key": "Name", "Value": f"orphaned-{volume_id}"},
                {"Key": "Environment", "Value": ENVIRONMENT},
                {"Key": "AutoDelete", "Value": "true"},
              ],
            }
          ],
        )
      else:
        logger.info(
          f"Skipping snapshot for shared volume {volume_id} (backed up to S3)"
        )

      # Delete volume
      ec2.delete_volume(VolumeId=volume_id)

      # Remove from registry
      table.delete_item(Key={"volume_id": volume_id})

      orphaned.append(volume_id)
      logger.info(f"Cleaned up orphaned volume: {volume_id}")
    except Exception as e:
      logger.error(f"Failed to cleanup {volume_id}: {e}")

  # Publish metric
  cloudwatch.put_metric_data(
    Namespace=f"RoboSystems/Graph/{ENVIRONMENT}",
    MetricData=[
      {
        "MetricName": "OrphanedVolumes",
        "Value": len(orphaned),
        "Unit": "Count",
      }
    ],
  )

  return {"statusCode": 200, "orphaned_volumes": orphaned}


def cleanup_old_snapshots(event: dict[str, Any]) -> dict[str, Any]:
  """Clean up snapshots older than their retention period.

  Per-snapshot retention via the `RetentionDays` tag takes precedence over the
  global RETENTION_DAYS env var. This lets pre-detach snapshots use 3-day
  retention without affecting orphan-cleanup snapshots (default 7 days).
  """
  now = datetime.now(UTC)

  # Find snapshots to delete. describe_snapshots returns up to 1000 per page;
  # paginate via NextToken so we don't silently miss snapshots at scale (which
  # would leak past the retention window). Pre-detach snapshots add per-detach
  # snapshot churn so this matters more than it did pre-PR-664.
  all_snapshots = []
  next_token = None
  while True:
    kwargs: dict[str, Any] = {
      "OwnerIds": ["self"],
      "Filters": [
        {"Name": "tag:Environment", "Values": [ENVIRONMENT]},
        {"Name": "tag:AutoDelete", "Values": ["true"]},
      ],
    }
    if next_token:
      kwargs["NextToken"] = next_token
    response = ec2.describe_snapshots(**kwargs)
    all_snapshots.extend(response.get("Snapshots", []))
    next_token = response.get("NextToken")
    if not next_token:
      break

  deleted = []
  for snapshot in all_snapshots:
    tags = {tag["Key"]: tag["Value"] for tag in snapshot.get("Tags", [])}
    try:
      retention_days = int(tags.get("RetentionDays", RETENTION_DAYS))
    except ValueError:
      logger.warning(
        f"Snapshot {snapshot['SnapshotId']} has malformed RetentionDays tag "
        f"{tags.get('RetentionDays')!r}; using default {RETENTION_DAYS}"
      )
      retention_days = RETENTION_DAYS

    cutoff = now - timedelta(days=retention_days)
    if snapshot["StartTime"].replace(tzinfo=UTC) < cutoff:
      try:
        ec2.delete_snapshot(SnapshotId=snapshot["SnapshotId"])
        deleted.append(snapshot["SnapshotId"])
        logger.info(
          f"Deleted snapshot {snapshot['SnapshotId']} "
          f"(retention={retention_days}d, age={(now - snapshot['StartTime'].replace(tzinfo=UTC)).days}d)"
        )
      except Exception as e:
        logger.error(f"Failed to delete snapshot {snapshot['SnapshotId']}: {e}")

  return {"statusCode": 200, "deleted_snapshots": deleted}


def restore_from_snapshot(event: dict[str, Any]) -> dict[str, Any]:
  """Restore a volume from a snapshot"""
  snapshot_id = event["snapshot_id"]
  az = event["availability_zone"]

  # Get snapshot info
  snapshot_response = ec2.describe_snapshots(SnapshotIds=[snapshot_id])
  snapshot = snapshot_response["Snapshots"][0]

  # Extract databases from tags
  databases = []
  for tag in snapshot.get("Tags", []):
    if tag["Key"] == "Databases":
      databases = json.loads(tag["Value"])
      break

  # Create volume from snapshot
  volume_response = ec2.create_volume(
    AvailabilityZone=az,
    SnapshotId=snapshot_id,
    VolumeType=DEFAULT_TYPE,
    Iops=DEFAULT_IOPS,
    Throughput=DEFAULT_THROUGHPUT,
    TagSpecifications=[
      {
        "ResourceType": "volume",
        "Tags": [
          {"Key": "Name", "Value": f"restored-from-{snapshot_id}"},
          {"Key": "Environment", "Value": ENVIRONMENT},
          {"Key": "RestoredFrom", "Value": snapshot_id},
          {"Key": "Databases", "Value": json.dumps(databases)},
        ],
      }
    ],
  )

  volume_id = volume_response["VolumeId"]

  # Register in DynamoDB
  table.put_item(
    Item={
      "volume_id": volume_id,
      "instance_id": "unattached",
      "availability_zone": az,
      "tier": "ladybug-standard",  # Default tier
      "status": "available",
      "databases": databases,
      "created_at": datetime.now(UTC).isoformat(),
      "restored_from": snapshot_id,
    }
  )

  return {"statusCode": 200, "volume_id": volume_id, "databases": databases}


def register_volume(event: dict[str, Any]) -> dict[str, Any]:
  """Register an existing volume in the registry"""
  volume_id = event["volume_id"]
  databases = event.get("databases", [])
  tier = event.get("tier", "ladybug-standard")

  # Get volume info from EC2
  response = ec2.describe_volumes(VolumeIds=[volume_id])
  volume = response["Volumes"][0]

  # Register in DynamoDB
  table.put_item(
    Item={
      "volume_id": volume_id,
      "instance_id": volume["Attachments"][0]["InstanceId"]
      if volume["Attachments"]
      else "unattached",
      "availability_zone": volume["AvailabilityZone"],
      "tier": tier,
      "status": "attached" if volume["Attachments"] else "available",
      "databases": databases,
      "created_at": datetime.now(UTC).isoformat(),
      "size": volume["Size"],
    }
  )

  return {"statusCode": 200, "volume_id": volume_id, "registered": True}


def sync_registry_with_ec2(event: dict[str, Any]) -> dict[str, Any]:
  """Synchronize DynamoDB registry with actual EC2 volumes"""
  logger.info("Starting registry synchronization with EC2")

  # Get all volumes from DynamoDB registry
  registry_volumes = {}
  last_evaluated_key = None

  while True:
    scan_params = {}
    if last_evaluated_key:
      scan_params["ExclusiveStartKey"] = last_evaluated_key

    response = table.scan(**scan_params)
    for item in response.get("Items", []):
      registry_volumes[item["volume_id"]] = item

    last_evaluated_key = response.get("LastEvaluatedKey")
    if not last_evaluated_key:
      break

  logger.info(f"Found {len(registry_volumes)} volumes in registry")

  # Get all actual volumes from EC2 with our tags
  ec2_volumes = {}
  try:
    response = ec2.describe_volumes(
      Filters=[
        {"Name": "tag:Environment", "Values": [ENVIRONMENT]},
        {"Name": "tag:ManagedBy", "Values": ["GraphVolumeManager"]},
      ]
    )

    for volume in response["Volumes"]:
      volume_id = volume["VolumeId"]
      ec2_volumes[volume_id] = volume
  except Exception as e:
    logger.error(f"Failed to describe EC2 volumes: {e}")
    return {"statusCode": 500, "error": "Failed to query EC2 volumes"}

  logger.info(f"Found {len(ec2_volumes)} managed volumes in EC2")

  # Find discrepancies
  volumes_to_remove = []  # In registry but not in EC2
  volumes_to_add = []  # In EC2 but not in registry
  volumes_to_update = []  # Status mismatch

  # Check for volumes in registry but not in EC2
  for volume_id in registry_volumes:
    if volume_id not in ec2_volumes:
      logger.warning(f"Volume {volume_id} in registry but not in EC2")
      volumes_to_remove.append(volume_id)
    else:
      # Check if status matches
      ec2_volume = ec2_volumes[volume_id]
      registry_item = registry_volumes[volume_id]

      # Determine actual status
      actual_status = "attached" if ec2_volume["Attachments"] else "available"
      registry_status = registry_item.get("status")

      if actual_status != registry_status:
        logger.warning(
          f"Volume {volume_id} status mismatch: registry={registry_status}, actual={actual_status}"
        )
        volumes_to_update.append(
          {
            "volume_id": volume_id,
            "actual_status": actual_status,
            "instance_id": ec2_volume["Attachments"][0]["InstanceId"]
            if ec2_volume["Attachments"]
            else "unattached",
          }
        )

  # Check for volumes in EC2 but not in registry
  for volume_id in ec2_volumes:
    if volume_id not in registry_volumes:
      logger.warning(f"Volume {volume_id} in EC2 but not in registry")
      volumes_to_add.append(volume_id)

  # Apply corrections
  corrections_applied = {"removed": [], "added": [], "updated": []}

  # Remove stale entries
  for volume_id in volumes_to_remove:
    try:
      table.delete_item(Key={"volume_id": volume_id})
      corrections_applied["removed"].append(volume_id)
      logger.info(f"Removed stale entry for {volume_id}")
    except Exception as e:
      logger.error(f"Failed to remove {volume_id}: {e}")

  # Add missing entries
  for volume_id in volumes_to_add:
    try:
      volume = ec2_volumes[volume_id]

      # Extract info from tags
      tags = {tag["Key"]: tag["Value"] for tag in volume.get("Tags", [])}
      databases = json.loads(tags.get("Databases", "[]"))
      tier = tags.get("Tier", "ladybug-standard")
      node_type = tags.get("NodeType", "writer")

      table.put_item(
        Item={
          "volume_id": volume_id,
          "instance_id": volume["Attachments"][0]["InstanceId"]
          if volume["Attachments"]
          else "unattached",
          "availability_zone": volume["AvailabilityZone"],
          "tier": tier,
          "status": "attached" if volume["Attachments"] else "available",
          "databases": databases,
          "created_at": volume["CreateTime"].isoformat()
          if hasattr(volume["CreateTime"], "isoformat")
          else str(volume["CreateTime"]),
          "size": volume["Size"],
          "node_type": node_type,
        }
      )
      corrections_applied["added"].append(volume_id)
      logger.info(f"Added missing entry for {volume_id}")
    except Exception as e:
      logger.error(f"Failed to add {volume_id}: {e}")

  # Update mismatched entries
  for update in volumes_to_update:
    try:
      table.update_item(
        Key={"volume_id": update["volume_id"]},
        UpdateExpression="SET #status = :status, instance_id = :instance_id, last_synced = :timestamp",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
          ":status": update["actual_status"],
          ":instance_id": update["instance_id"],
          ":timestamp": datetime.now(UTC).isoformat(),
        },
      )
      corrections_applied["updated"].append(update["volume_id"])
      logger.info(f"Updated status for {update['volume_id']}")
    except Exception as e:
      logger.error(f"Failed to update {update['volume_id']}: {e}")

  # Send alert if corrections were made
  total_corrections = (
    len(corrections_applied["removed"])
    + len(corrections_applied["added"])
    + len(corrections_applied["updated"])
  )
  if total_corrections > 0:
    message = f"""Registry synchronization completed with {total_corrections} corrections:

Removed {len(corrections_applied["removed"])} stale entries: {corrections_applied["removed"]}
Added {len(corrections_applied["added"])} missing entries: {corrections_applied["added"]}
Updated {len(corrections_applied["updated"])} mismatched entries: {corrections_applied["updated"]}
"""
    send_alert("Registry Synchronization", message)

  # Publish metrics
  cloudwatch.put_metric_data(
    Namespace=f"RoboSystems/Graph/{ENVIRONMENT}",
    MetricData=[
      {
        "MetricName": "RegistryStaleEntries",
        "Value": len(corrections_applied["removed"]),
        "Unit": "Count",
      },
      {
        "MetricName": "RegistryMissingEntries",
        "Value": len(corrections_applied["added"]),
        "Unit": "Count",
      },
      {
        "MetricName": "RegistryMismatchedEntries",
        "Value": len(corrections_applied["updated"]),
        "Unit": "Count",
      },
    ],
  )

  return {
    "statusCode": 200,
    "registry_volumes": len(registry_volumes),
    "ec2_volumes": len(ec2_volumes),
    "corrections": corrections_applied,
    "summary": f"Applied {total_corrections} corrections to registry",
  }


def send_alert(subject: str, message: str) -> None:
  """Send an alert via SNS"""
  try:
    sns.publish(
      TopicArn=ALERT_TOPIC,
      Subject=f"[Graph Volume Manager] {subject}",
      Message=message,
    )
  except Exception as e:
    logger.error(f"Failed to send alert: {e}")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """AWS Lambda entry point"""
  return handler(event, context)
