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
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3

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
      # Sort volumes by preference:
      # 1. Volumes with matching databases (if databases were specified)
      # 2. Volumes with any data
      # 3. Empty volumes

      if databases:
        # Look for volumes with matching databases
        matching_db_volumes = [
          v
          for v in valid_volumes
          if v.get("databases")
          and any(db in v.get("databases", []) for db in databases)
        ]
        if matching_db_volumes:
          volume = matching_db_volumes[0]
          logger.info(
            f"Reusing volume {volume['volume_id']} with matching databases: {volume.get('databases')} in AZ {az}"
          )
          return attach_and_register_volume(
            volume["volume_id"], instance_id, volume.get("databases", databases)
          )

      # Prefer volumes with data
      volumes_with_data = [v for v in valid_volumes if v.get("databases")]
      if volumes_with_data:
        volume = volumes_with_data[0]
        logger.info(
          f"Reusing volume {volume['volume_id']} with databases: {volume.get('databases')} in AZ {az}"
        )
        # Preserve existing databases on the volume
        existing_databases = volume.get("databases", [])
        return attach_and_register_volume(
          volume["volume_id"], instance_id, existing_databases
        )
      else:
        volume = valid_volumes[0]
        logger.info(f"Reusing empty volume {volume['volume_id']} in AZ {az}")
        return attach_and_register_volume(volume["volume_id"], instance_id, databases)
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

  # Attach the volume with retry logic
  max_retries = 3
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
      if "IncorrectState" in str(e) and attempt < max_retries - 1:
        logger.warning("Volume or instance not ready, retrying in 10 seconds...")
        import time

        time.sleep(10)
        continue
      else:
        logger.error(f"Failed to attach volume after {attempt + 1} attempts: {e}")
        raise

  if response is None:
    raise RuntimeError("Failed to attach volume: no response received")

  # Wait for attachment
  waiter = ec2.get_waiter("volume_in_use")
  waiter.wait(VolumeIds=[volume_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60})

  # Update registry
  table.update_item(
    Key={"volume_id": volume_id},
    UpdateExpression="SET instance_id = :instance_id, #status = :status, last_attached = :timestamp, databases = :databases",
    ExpressionAttributeNames={"#status": "status"},
    ExpressionAttributeValues={
      ":instance_id": instance_id,
      ":status": "attached",
      ":timestamp": datetime.now(UTC).isoformat(),
      ":databases": databases if isinstance(databases, list) else [databases],
    },
  )

  logger.info(f"Successfully attached volume {volume_id} to instance {instance_id}")

  # CRITICAL: Update graph registry with new instance info for routing
  # This ensures queries route to the correct IP after instance replacement
  db_list = databases if isinstance(databases, list) else [databases]
  graph_update_result = update_graph_registry_for_instance(instance_id, db_list)
  logger.info(f"Graph registry update result: {graph_update_result}")

  # Signal the instance that volume is ready
  try:
    ssm.send_command(
      InstanceIds=[instance_id],
      DocumentName="AWS-RunShellScript",
      Parameters={
        "commands": [
          "echo 'VOLUME_READY' > /tmp/volume_status",
          f"echo '{json.dumps(databases)}' > /tmp/databases.json",
        ]
      },
    )
  except Exception as e:
    logger.warning(f"Failed to signal instance: {e}")

  return {
    "statusCode": 200,
    "volume_id": volume_id,
    "attachment_state": response["State"],
    "databases": databases,
  }


def create_and_attach_volume(
  instance_id: str, tier: str, az: str, databases: list[str], node_type: str
) -> dict[str, Any]:
  """Create a new volume and attach it to the instance"""
  # Tier configurations (updated to match .github/configs/graph.yml)
  tier_config = {
    "ladybug-standard": {"size": 50, "iops": 3000},
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
  """Safely detach a volume"""
  volume_id = event["volume_id"]
  force = event.get("force", False)

  # Get current databases on the volume before detaching
  response = table.get_item(Key={"volume_id": volume_id})
  databases = response.get("Item", {}).get("databases", [])

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
  """Clean up snapshots older than retention period"""
  cutoff_time = datetime.now(UTC) - timedelta(days=RETENTION_DAYS)

  # Find snapshots to delete
  response = ec2.describe_snapshots(
    OwnerIds=["self"],
    Filters=[
      {"Name": "tag:Environment", "Values": [ENVIRONMENT]},
      {"Name": "tag:AutoDelete", "Values": ["true"]},
    ],
  )

  deleted = []
  for snapshot in response["Snapshots"]:
    if snapshot["StartTime"].replace(tzinfo=UTC) < cutoff_time:
      try:
        ec2.delete_snapshot(SnapshotId=snapshot["SnapshotId"])
        deleted.append(snapshot["SnapshotId"])
        logger.info(f"Deleted old snapshot: {snapshot['SnapshotId']}")
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
