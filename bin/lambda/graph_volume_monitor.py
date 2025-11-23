"""
Graph Volume Monitor and Auto-Expansion Lambda Function

This Lambda handles proactive volume monitoring and expansion for Graph instances (LadybugDB and Neo4j):
- Monitors disk usage across all instances
- Automatically expands volumes when thresholds are exceeded
- Grows filesystems after EBS expansion
- Handles both scheduled checks and alarm-triggered expansions
"""

import boto3
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import urllib3

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
ENVIRONMENT = os.environ.get("ENVIRONMENT", "staging")
VOLUME_REGISTRY_TABLE = os.environ.get("VOLUME_REGISTRY_TABLE")
INSTANCE_REGISTRY_TABLE = os.environ.get("INSTANCE_REGISTRY_TABLE")
ALERT_TOPIC_ARN = os.environ.get("ALERT_TOPIC_ARN")
EXPANSION_THRESHOLD = float(os.environ.get("EXPANSION_THRESHOLD", "0.8"))  # 80%
EXPANSION_FACTOR = float(os.environ.get("EXPANSION_FACTOR", "1.5"))  # 50% increase
MIN_EXPANSION_GB = int(os.environ.get("MIN_EXPANSION_GB", "50"))
MAX_VOLUME_SIZE_GB = int(os.environ.get("MAX_VOLUME_SIZE_GB", "16384"))  # EBS limit
GRAPH_API_PORT = os.environ.get("GRAPH_API_PORT", "8001")
GRAPH_API_KEY = os.environ.get("GRAPH_API_KEY", "")

# HTTP client for API calls
http = urllib3.PoolManager()


def handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
  """Main Lambda handler"""

  # Determine the action based on event source
  if "source" in event and event["source"] == "aws.cloudwatch":
    # Triggered by CloudWatch alarm
    return handle_alarm_trigger(event)
  elif "action" in event:
    # Direct invocation with specific action
    action = event["action"]
    if action == "monitor_all":
      return monitor_all_instances()
    elif action == "monitor_instance":
      return monitor_single_instance(event)
    elif action == "expand_volume":
      return expand_volume_with_filesystem(event)
    elif action == "grow_filesystem":
      return grow_filesystem_only(event)
    elif action == "fix_stuck_volumes":
      return fix_stuck_optimizing_volumes()
    else:
      return {"statusCode": 400, "error": f"Unknown action: {action}"}
  else:
    # Default scheduled check
    return monitor_all_instances()


def handle_alarm_trigger(event: Dict[str, Any]) -> Dict[str, Any]:
  """Handle CloudWatch alarm trigger"""
  try:
    # Parse the alarm message
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    alarm_name = message.get("AlarmName", "")

    logger.info(f"Triggered by alarm: {alarm_name}")

    # For disk usage alarms, trigger immediate monitoring
    if "disk-usage" in alarm_name.lower():
      return monitor_all_instances(expand_immediately=True)

    return {"statusCode": 200, "message": "Alarm processed"}

  except Exception as e:
    logger.error(f"Error processing alarm: {e}")
    return {"statusCode": 500, "error": str(e)}


def monitor_all_instances(expand_immediately: bool = False) -> Dict[str, Any]:
  """Monitor all Graph instances and expand volumes as needed"""

  results = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "instances_checked": 0,
    "volumes_expanded": [],
    "errors": [],
    "registry_synced": False,
  }

  try:
    # First, sync registry with actual EC2 state to clean up stale entries
    try:
      sync_volume_registry()
      results["registry_synced"] = True
      logger.info("Volume registry synchronized with EC2 state")
    except Exception as e:
      logger.warning(f"Failed to sync volume registry: {e}")
      results["errors"].append(f"Registry sync failed: {str(e)}")

    # Discover all Graph instances
    instances = discover_lbug_instances()
    results["instances_checked"] = len(instances)

    logger.info(f"Monitoring {len(instances)} Graph instances")

    for instance in instances:
      try:
        check_result = check_and_expand_volume(
          instance, expand_immediately=expand_immediately
        )

        if check_result.get("expanded"):
          results["volumes_expanded"].append(check_result)

      except Exception as e:
        error_msg = f"Failed to check instance {instance['instance_id']}: {str(e)}"
        logger.error(error_msg)
        results["errors"].append(error_msg)

    # Publish metrics to CloudWatch
    publish_monitoring_metrics(results)

    # Send alerts if needed
    if results["volumes_expanded"]:
      send_expansion_alert(results["volumes_expanded"])

    if results["errors"]:
      send_error_alert(results["errors"])

  except Exception as e:
    logger.error(f"Monitoring failed: {e}")
    results["errors"].append(str(e))

  return results


def discover_lbug_instances() -> List[Dict]:
  """Discover all running Graph instances"""

  instances = []

  try:
    # Query EC2 for Graph instances
    response = ec2.describe_instances(
      Filters=[
        {"Name": "tag:Service", "Values": ["RoboSystems"]},
        {
          "Name": "tag:LadybugRole",
          "Values": ["writer", "shared_master", "shared_replica"],
        },
        {"Name": "instance-state-name", "Values": ["running"]},
        {"Name": "tag:Environment", "Values": [ENVIRONMENT]},
      ]
    )

    for reservation in response["Reservations"]:
      for instance in reservation["Instances"]:
        instance_id = instance["InstanceId"]
        private_ip = instance.get("PrivateIpAddress")

        # Get tags as dict
        tags = {tag["Key"]: tag["Value"] for tag in instance.get("Tags", [])}

        instances.append(
          {
            "instance_id": instance_id,
            "private_ip": private_ip,
            "lbug_role": tags.get("LadybugRole", "unknown"),
            "tier": tags.get("WriterTier", tags.get("Tier", "unknown")),
            "tags": tags,
          }
        )

  except Exception as e:
    logger.error(f"Failed to discover instances: {e}")
    raise

  return instances


def check_and_expand_volume(instance: Dict, expand_immediately: bool = False) -> Dict:
  """Check a single instance and expand if needed"""

  result = {
    "instance_id": instance["instance_id"],
    "tier": instance["tier"],
    "checked_at": datetime.now(timezone.utc).isoformat(),
  }

  try:
    # Get volume metrics from instance
    metrics = get_volume_metrics_from_instance(instance)

    if not metrics:
      result["error"] = "Failed to get volume metrics"
      return result

    # Check data volume usage
    data_volume = metrics.get("volumes", {}).get("data_volume", {})

    if "error" in data_volume:
      result["error"] = data_volume["error"]
      return result

    usage_percent = data_volume.get("usage_percent", 0)  # Already a decimal from API
    current_size = int(data_volume.get("volume_size_gb", 50))  # Ensure it's an int

    # Get the actual EBS volume ID from EC2 (since API doesn't provide it)
    volume_id = get_data_volume_id(instance["instance_id"])

    # Determine if expansion is needed
    threshold = 0.7 if expand_immediately else EXPANSION_THRESHOLD

    # Log the current status
    logger.info(
      f"Instance {instance['instance_id']}: "
      f"Usage={usage_percent:.1%}, Size={current_size}GB, "
      f"Volume={volume_id}, Threshold={threshold:.0%}"
    )

    # Add metrics to result for visibility
    result["metrics"] = {
      "usage_percent": usage_percent,
      "current_size_gb": current_size,
      "volume_id": volume_id,
      "threshold": threshold,
    }

    if usage_percent > threshold and volume_id:
      logger.info(
        f"Volume expansion needed for {instance['instance_id']}: "
        f"{usage_percent:.1%} > {threshold:.0%} threshold"
      )

      # Check if volume is stuck in optimizing state
      modification_state = check_volume_modification_state(volume_id)

      if modification_state == "optimizing":
        logger.warning(
          f"Volume {volume_id} is stuck in optimizing state, attempting filesystem growth only"
        )
        # Try to grow filesystem without expanding volume
        grow_result = trigger_filesystem_growth(instance)
        result["action"] = "filesystem_growth_only"
        result["filesystem_grown"] = grow_result.get("success", False)
        result["reason"] = "Volume stuck in optimizing state"
        return result
      elif modification_state == "modifying":
        logger.info(f"Volume {volume_id} is already being modified, skipping")
        result["action"] = "skipped"
        result["reason"] = "Volume modification in progress"
        return result

      # Calculate new size
      new_size = calculate_new_volume_size(current_size, usage_percent)

      if new_size > current_size:
        # Perform expansion
        expansion_result = perform_volume_expansion(
          volume_id, current_size, new_size, instance["instance_id"]
        )

        result["expanded"] = True
        result["expansion_details"] = {
          "volume_id": volume_id,
          "previous_size_gb": current_size,
          "new_size_gb": new_size,
          "usage_before": f"{usage_percent:.1%}",
          **expansion_result,
        }

        # Only grow filesystem if volume expansion succeeded and we can wait for it
        if expansion_result.get("success"):
          modification_id = expansion_result.get("modification_id")

          # Wait for volume modification to complete (up to 5 minutes)
          logger.info(f"Waiting for volume {volume_id} modification to complete...")
          modification_completed = wait_for_volume_modification(
            volume_id, modification_id, max_wait_seconds=300
          )

          if modification_completed:
            # Now trigger filesystem growth
            logger.info("Volume modification complete, growing filesystem...")
            grow_result = trigger_filesystem_growth(instance)
            result["expansion_details"]["filesystem_grown"] = grow_result.get(
              "success", False
            )

            # If filesystem growth command was sent, wait and check its status
            if grow_result.get("success"):
              command_id = grow_result.get("command_id")
              if command_id:
                # Wait a bit for the command to complete
                time.sleep(30)

                # Check command status
                try:
                  cmd_status = ssm.get_command_invocation(
                    CommandId=command_id, InstanceId=instance["instance_id"]
                  )

                  if cmd_status["Status"] == "Success":
                    logger.info("Filesystem growth completed successfully")
                    result["expansion_details"]["filesystem_status"] = "grown"
                  else:
                    logger.warning(f"Filesystem growth status: {cmd_status['Status']}")
                    result["expansion_details"]["filesystem_status"] = cmd_status[
                      "Status"
                    ]

                except Exception as e:
                  logger.warning(f"Could not check filesystem growth status: {e}")
          else:
            logger.warning(
              "Volume modification did not complete in time, skipping filesystem growth"
            )
            result["expansion_details"]["filesystem_grown"] = False
            result["expansion_details"]["filesystem_status"] = "skipped_timeout"

  except Exception as e:
    result["error"] = str(e)

  return result


def check_volume_modification_state(volume_id: str) -> Optional[str]:
  """Check if a volume has an active modification in progress"""

  try:
    response = ec2.describe_volumes_modifications(
      VolumeIds=[volume_id],
      Filters=[
        {
          "Name": "modification-state",
          "Values": ["modifying", "optimizing"],
        }
      ],
    )

    if response["VolumesModifications"]:
      modification = response["VolumesModifications"][0]
      state = modification["ModificationState"]

      # Check if it's stuck in optimizing (been optimizing for more than 30 minutes)
      start_time = modification.get("StartTime")
      if state == "optimizing" and start_time:
        time_elapsed = (
          datetime.now(timezone.utc) - start_time.replace(tzinfo=timezone.utc)
        ).total_seconds()
        if time_elapsed > 1800:  # 30 minutes
          logger.warning(
            f"Volume {volume_id} has been optimizing for {time_elapsed / 60:.0f} minutes"
          )
          return "optimizing"

      return state

    return None

  except Exception as e:
    logger.error(f"Failed to check volume modification state: {e}")
    return None


def get_data_volume_id(instance_id: str) -> Optional[str]:
  """Get the data volume ID for a Graph instance"""

  try:
    # Get volumes attached to the instance
    response = ec2.describe_volumes(
      Filters=[
        {"Name": "attachment.instance-id", "Values": [instance_id]},
        {
          "Name": "attachment.device",
          "Values": ["/dev/xvdf", "/dev/sdf"],
        },  # Data volume device
      ]
    )

    if response["Volumes"]:
      return response["Volumes"][0]["VolumeId"]

    # If not found at standard device, check DynamoDB registry
    if VOLUME_REGISTRY_TABLE:
      table = dynamodb.Table(VOLUME_REGISTRY_TABLE)
      response = table.scan(
        FilterExpression="instance_id = :instance_id AND #status = :status",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
          ":instance_id": instance_id,
          ":status": "attached",
        },
      )

      if response["Items"]:
        return response["Items"][0]["volume_id"]

    logger.warning(f"No data volume found for instance {instance_id}")
    return None

  except Exception as e:
    logger.error(f"Failed to get volume ID for {instance_id}: {e}")
    return None


def get_volume_metrics_from_instance(instance: Dict) -> Optional[Dict]:
  """Query Graph API for volume metrics"""

  try:
    # Use the /metrics endpoint
    url = f"http://{instance['private_ip']}:{GRAPH_API_PORT}/metrics"

    # Add API key to headers if configured
    headers = {}
    if GRAPH_API_KEY:
      headers["X-Graph-API-Key"] = GRAPH_API_KEY

    response = http.request(
      "GET",
      url,
      headers=headers,
      timeout=10.0,
      retries=urllib3.Retry(total=2, backoff_factor=0.3),
    )

    if response.status == 200:
      # Parse the metrics response and extract system/volume information
      metrics_data = json.loads(response.data.decode("utf-8"))

      # Extract the volume metrics from the system metrics
      # The API returns system.volumes.data_volume with the disk usage info
      system_metrics = metrics_data.get("system", {})
      volumes = system_metrics.get("volumes", {})

      # Return in the format expected by the Lambda
      return {
        "volumes": volumes,
        "system": system_metrics,
        "databases": metrics_data.get("databases", {}),
      }
    else:
      logger.error(
        f"API returned status {response.status} for {instance['instance_id']}"
      )
      return None

  except Exception as e:
    logger.error(f"Failed to get metrics from {instance['instance_id']}: {e}")
    return None


def perform_volume_expansion(
  volume_id: str, current_size: int, new_size: int, instance_id: str
) -> Dict:
  """Perform the actual EBS volume expansion"""

  try:
    # Ensure new_size is an integer (AWS API requires int, not float)
    new_size = int(new_size)

    # Call EC2 modify_volume
    response = ec2.modify_volume(VolumeId=volume_id, Size=new_size)

    modification_id = response["VolumeModification"]["VolumeModificationId"]
    modification_state = response["VolumeModification"]["ModificationState"]

    logger.info(
      f"Volume {volume_id} expansion initiated: {current_size}GB -> {new_size}GB (ID: {modification_id})"
    )

    # Update DynamoDB registry if table exists
    if VOLUME_REGISTRY_TABLE:
      table = dynamodb.Table(VOLUME_REGISTRY_TABLE)
      table.update_item(
        Key={"volume_id": volume_id},
        UpdateExpression="SET size_gb = :size, #status = :status, last_modified = :timestamp",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
          ":size": new_size,
          ":status": "expanding",
          ":timestamp": datetime.now(timezone.utc).isoformat(),
        },
      )

    # Publish CloudWatch metric
    cloudwatch.put_metric_data(
      Namespace="RoboSystems/Graph",
      MetricData=[
        {
          "MetricName": "VolumeExpansions",
          "Value": 1,
          "Unit": "Count",
          "Dimensions": [
            {"Name": "Environment", "Value": ENVIRONMENT},
            {"Name": "InstanceId", "Value": instance_id},
          ],
        }
      ],
    )

    return {
      "success": True,
      "modification_id": modification_id,
      "modification_state": modification_state,
    }

  except Exception as e:
    error_str = str(e)
    logger.error(f"Failed to expand volume {volume_id}: {error_str}")

    # Check for specific error conditions
    if "IncorrectModificationState" in error_str:
      if "OPTIMIZING" in error_str:
        logger.warning(
          f"Volume {volume_id} is stuck in OPTIMIZING state, cannot expand"
        )
        return {
          "success": False,
          "error": "volume_stuck_optimizing",
          "message": error_str,
        }
      else:
        return {
          "success": False,
          "error": "modification_in_progress",
          "message": error_str,
        }

    return {"success": False, "error": str(e)}


def wait_for_volume_modification(
  volume_id: str, modification_id: str, max_wait_seconds: int = 300
) -> bool:
  """Wait for volume modification to complete with robust error handling"""

  start_time = time.time()
  last_progress = 0
  no_progress_count = 0

  while time.time() - start_time < max_wait_seconds:
    try:
      response = ec2.describe_volumes_modifications(
        VolumeIds=[volume_id],
        Filters=[
          {
            "Name": "modification-state",
            "Values": ["modifying", "optimizing", "completed", "failed"],
          }
        ],
      )

      if response["VolumesModifications"]:
        modification = response["VolumesModifications"][0]
        state = modification["ModificationState"]
        progress = modification.get("Progress", 0)

        logger.info(f"Volume {volume_id} modification state: {state} ({progress}%)")

        if state == "completed":
          logger.info(f"Volume {volume_id} modification completed successfully")
          return True
        elif state == "failed":
          logger.error(f"Volume modification failed for {volume_id}")
          # Try to get failure reason
          status_message = modification.get("StatusMessage", "Unknown error")
          logger.error(f"Failure reason: {status_message}")
          return False
        elif state == "optimizing":
          # Optimizing means the resize is done, just doing final optimization
          logger.info(
            f"Volume {volume_id} is optimizing, considering it ready for filesystem growth"
          )
          return True

        # Check if progress is stuck
        if progress == last_progress:
          no_progress_count += 1
          if no_progress_count > 6:  # No progress for 1 minute
            logger.warning(f"Volume modification appears stuck at {progress}%")
        else:
          no_progress_count = 0
          last_progress = progress
      else:
        # No modification found - might already be complete
        logger.warning(f"No active modification found for volume {volume_id}")
        # Check if volume is already at the target size
        try:
          vol_response = ec2.describe_volumes(VolumeIds=[volume_id])
          if vol_response["Volumes"]:
            current_size = vol_response["Volumes"][0]["Size"]
            logger.info(f"Volume {volume_id} current size: {current_size}GB")
            return True  # Assume it's already done
        except Exception as ve:
          logger.error(f"Could not check volume status: {ve}")

      # Wait 10 seconds before checking again
      time.sleep(10)

    except Exception as e:
      logger.error(f"Error checking volume modification status: {e}")
      # Don't immediately fail - network errors can be transient
      time.sleep(10)

  logger.warning(
    f"Timeout waiting for volume {volume_id} modification to complete after {max_wait_seconds}s"
  )
  return False


def trigger_filesystem_growth(instance: Dict) -> Dict:
  """Trigger filesystem growth via SSM command with retry logic"""

  try:
    # Send SSM command to grow filesystem
    response = ssm.send_command(
      InstanceIds=[instance["instance_id"]],
      DocumentName="AWS-RunShellScript",
      Parameters={
        "commands": [
          "#!/bin/bash",
          "set -e",  # Exit on error
          "",
          "# Function to grow filesystem with retries",
          "grow_filesystem() {",
          "  local retries=3",
          "  local wait_time=5",
          "  ",
          "  for i in $(seq 1 $retries); do",
          '    echo "Attempt $i of $retries to grow filesystem"',
          "    ",
          "    # Get the device",
          "    DEVICE=$(df /mnt/lbug-data | tail -1 | awk '{print $1}')",
          '    echo "Device detected: $DEVICE"',
          "    ",
          "    # Get current and available size",
          "    CURRENT_SIZE=$(df -BG /mnt/lbug-data | tail -1 | awk '{print $2}' | sed 's/G//')",
          "    DEVICE_SIZE=$(lsblk -b -n -o SIZE $DEVICE 2>/dev/null | head -1)",
          "    DEVICE_SIZE_GB=$((DEVICE_SIZE / 1024 / 1024 / 1024))",
          '    echo "Current filesystem: ${CURRENT_SIZE}GB, Device size: ${DEVICE_SIZE_GB}GB"',
          "    ",
          "    # Check if growth is needed",
          '    if [ "$CURRENT_SIZE" -ge "$DEVICE_SIZE_GB" ]; then',
          '      echo "Filesystem already at maximum size"',
          "      return 0",
          "    fi",
          "    ",
          "    # Try to grow the filesystem",
          "    if sudo xfs_growfs /mnt/lbug-data 2>/dev/null; then",
          '      echo "XFS filesystem grown successfully"',
          "      df -h /mnt/lbug-data",
          "      return 0",
          "    elif sudo resize2fs $DEVICE 2>/dev/null; then",
          '      echo "EXT filesystem grown successfully"',
          "      df -h /mnt/lbug-data",
          "      return 0",
          "    else",
          '      echo "Growth attempt $i failed, waiting ${wait_time}s..."',
          "      sleep $wait_time",
          "    fi",
          "  done",
          "  ",
          "  return 1",
          "}",
          "",
          "# Main execution",
          'echo "Starting filesystem growth process..."',
          "",
          "# Get device for partition check",
          "DEVICE=$(df /mnt/lbug-data | tail -1 | awk '{print $1}')",
          "",
          "# Check if device has partitions",
          "if [[ $DEVICE =~ [0-9]+$ ]] || [[ $DEVICE =~ p[0-9]+$ ]]; then",
          '  echo "Device has partitions, using growpart"',
          "  # Extract base device and partition number",
          "  if [[ $DEVICE =~ ^(/dev/nvme[0-9]+n[0-9]+)p([0-9]+)$ ]]; then",
          "    # NVMe partition (e.g., /dev/nvme0n1p1)",
          "    BASE_DEVICE=${BASH_REMATCH[1]}",
          "    PARTITION=${BASH_REMATCH[2]}",
          "  elif [[ $DEVICE =~ ^(/dev/[a-z]+)([0-9]+)$ ]]; then",
          "    # Standard partition (e.g., /dev/xvdf1)",
          "    BASE_DEVICE=${BASH_REMATCH[1]}",
          "    PARTITION=${BASH_REMATCH[2]}",
          "  fi",
          "  if [[ -n $BASE_DEVICE ]] && [[ -n $PARTITION ]]; then",
          '    sudo growpart $BASE_DEVICE $PARTITION || echo "growpart not needed or failed"',
          "  fi",
          "else",
          '  echo "Device has no partitions (whole device), skipping growpart"',
          "fi",
          "",
          "# Now grow the filesystem with retries",
          "if grow_filesystem; then",
          '  echo "SUCCESS: Filesystem growth completed"',
          "  exit 0",
          "else",
          '  echo "ERROR: Failed to grow filesystem after multiple attempts"',
          "  exit 1",
          "fi",
        ]
      },
      TimeoutSeconds=120,  # Increased timeout for better reliability
    )

    command_id = response["Command"]["CommandId"]

    logger.info(
      f"Filesystem growth triggered on {instance['instance_id']}: {command_id}"
    )

    return {"success": True, "command_id": command_id}

  except Exception as e:
    logger.error(f"Failed to trigger filesystem growth: {e}")
    return {"success": False, "error": str(e)}


def calculate_new_volume_size(current_size: int, usage_percent: float) -> int:
  """Calculate the new volume size based on current usage"""

  # Calculate size needed to get back to 60% usage
  target_usage = 0.6
  required_size = int(current_size * usage_percent / target_usage)

  # Apply expansion factor
  expanded_size = int(current_size * EXPANSION_FACTOR)

  # Use the larger of the two
  new_size = max(required_size, expanded_size)

  # Ensure minimum expansion
  new_size = max(new_size, current_size + MIN_EXPANSION_GB)

  # Cap at AWS limit
  new_size = min(new_size, MAX_VOLUME_SIZE_GB)

  # Round up to nearest 10GB for cleaner sizes
  new_size = ((new_size + 9) // 10) * 10

  return new_size


def monitor_single_instance(event: Dict) -> Dict:
  """Monitor a specific instance"""

  instance_id = event.get("instance_id")

  if not instance_id:
    return {"statusCode": 400, "error": "instance_id required"}

  try:
    # Get instance details
    response = ec2.describe_instances(InstanceIds=[instance_id])

    if not response["Reservations"]:
      return {"statusCode": 404, "error": "Instance not found"}

    instance_data = response["Reservations"][0]["Instances"][0]
    tags = {tag["Key"]: tag["Value"] for tag in instance_data.get("Tags", [])}

    instance = {
      "instance_id": instance_id,
      "private_ip": instance_data.get("PrivateIpAddress"),
      "lbug_role": tags.get("LadybugRole", "unknown"),
      "tier": tags.get("WriterTier", tags.get("Tier", "unknown")),
      "tags": tags,
    }

    result = check_and_expand_volume(instance, expand_immediately=True)

    return {"statusCode": 200, **result}

  except Exception as e:
    logger.error(f"Failed to monitor instance {instance_id}: {e}")
    return {"statusCode": 500, "error": str(e)}


def expand_volume_with_filesystem(event: Dict) -> Dict:
  """Direct volume expansion with filesystem growth"""

  volume_id = event.get("volume_id")
  new_size_gb = event.get("new_size_gb")
  instance_id = event.get("instance_id")

  if not all([volume_id, new_size_gb]):
    return {"statusCode": 400, "error": "volume_id and new_size_gb required"}

  try:
    # Ensure new_size_gb is an integer
    new_size_gb = int(new_size_gb)

    # Get current volume info
    volumes = ec2.describe_volumes(VolumeIds=[volume_id])["Volumes"]

    if not volumes:
      return {"statusCode": 404, "error": "Volume not found"}

    current_size = volumes[0]["Size"]

    if new_size_gb <= current_size:
      return {
        "statusCode": 400,
        "error": f"New size must be larger than current size ({current_size}GB)",
      }

    # Perform expansion
    expansion_result = perform_volume_expansion(
      volume_id, current_size, new_size_gb, instance_id or "unknown"
    )

    # If instance_id provided, grow filesystem
    if instance_id and expansion_result.get("success"):
      grow_result = trigger_filesystem_growth({"instance_id": instance_id})
      expansion_result["filesystem_growth"] = grow_result

    return {
      "statusCode": 200 if expansion_result.get("success") else 500,
      **expansion_result,
    }

  except Exception as e:
    logger.error(f"Failed to expand volume {volume_id}: {e}")
    return {"statusCode": 500, "error": str(e)}


def publish_monitoring_metrics(results: Dict):
  """Publish monitoring metrics to CloudWatch"""

  try:
    namespace = "RoboSystems/Graph"

    metric_data = [
      {
        "MetricName": "VolumeMonitorExecutions",
        "Value": 1,
        "Unit": "Count",
        "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
      },
      {
        "MetricName": "InstancesMonitored",
        "Value": results.get("instances_checked", 0),
        "Unit": "Count",
        "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
      },
      {
        "MetricName": "VolumesExpanded",
        "Value": len(results.get("volumes_expanded", [])),
        "Unit": "Count",
        "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
      },
      {
        "MetricName": "VolumeMonitorErrors",
        "Value": len(results.get("errors", [])),
        "Unit": "Count",
        "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
      },
    ]

    cloudwatch.put_metric_data(Namespace=namespace, MetricData=metric_data)

    logger.info(f"Published {len(metric_data)} metrics to CloudWatch")

  except Exception as e:
    logger.error(f"Failed to publish metrics: {e}")


def send_expansion_alert(expansions: List[Dict]):
  """Send SNS alert about volume expansions"""

  if not expansions or not ALERT_TOPIC_ARN:
    return

  try:
    message = "LadybugDB Volume Auto-Expansion Report\n\n"

    for expansion in expansions:
      details = expansion.get("expansion_details", {})
      message += f"Instance: {expansion['instance_id']}\n"
      message += f"Tier: {expansion['tier']}\n"
      message += f"Volume: {details.get('volume_id')}\n"
      message += (
        f"Size: {details.get('previous_size_gb')}GB → {details.get('new_size_gb')}GB\n"
      )
      message += f"Usage before: {details.get('usage_before')}\n\n"

    sns.publish(
      TopicArn=ALERT_TOPIC_ARN,
      Subject=f"[{ENVIRONMENT.upper()}] LadybugDB Volumes Auto-Expanded",
      Message=message,
    )

  except Exception as e:
    logger.error(f"Failed to send expansion alert: {e}")


def send_error_alert(errors: List[str]):
  """Send SNS alert about monitoring errors"""

  if not errors or not ALERT_TOPIC_ARN:
    return

  try:
    message = "LadybugDB Volume Monitoring Errors\n\n"

    for error in errors[:10]:  # Limit to first 10
      message += f"• {error}\n"

    if len(errors) > 10:
      message += f"\n... and {len(errors) - 10} more errors"

    sns.publish(
      TopicArn=ALERT_TOPIC_ARN,
      Subject=f"[{ENVIRONMENT.upper()}] Volume Monitoring Errors",
      Message=message,
    )

  except Exception as e:
    logger.error(f"Failed to send error alert: {e}")


def grow_filesystem_only(event: Dict) -> Dict:
  """Manually trigger filesystem growth for a specific instance"""

  instance_id = event.get("instance_id")

  if not instance_id:
    return {"statusCode": 400, "error": "instance_id required"}

  try:
    logger.info(f"Manually triggering filesystem growth for {instance_id}")

    # Trigger filesystem growth
    grow_result = trigger_filesystem_growth({"instance_id": instance_id})

    if grow_result.get("success"):
      command_id = grow_result.get("command_id")

      # Wait for command to complete
      time.sleep(30)

      try:
        cmd_status = ssm.get_command_invocation(
          CommandId=command_id, InstanceId=instance_id
        )

        return {
          "statusCode": 200,
          "instance_id": instance_id,
          "command_id": command_id,
          "status": cmd_status["Status"],
          "output": cmd_status.get("StandardOutputContent", ""),
          "error": cmd_status.get("StandardErrorContent", ""),
        }

      except Exception as e:
        logger.warning(f"Could not check command status: {e}")
        return {
          "statusCode": 200,
          "instance_id": instance_id,
          "command_id": command_id,
          "status": "unknown",
          "message": "Command sent but status unknown",
        }
    else:
      return {
        "statusCode": 500,
        "error": grow_result.get("error", "Failed to trigger filesystem growth"),
      }

  except Exception as e:
    logger.error(f"Failed to grow filesystem for {instance_id}: {e}")
    return {"statusCode": 500, "error": str(e)}


def fix_stuck_optimizing_volumes() -> Dict:
  """Find and fix volumes stuck in optimizing state"""

  results = {
    "timestamp": datetime.now(timezone.utc).isoformat(),
    "volumes_checked": 0,
    "volumes_fixed": [],
    "errors": [],
  }

  try:
    # Find all volumes in optimizing state
    response = ec2.describe_volumes_modifications(
      Filters=[{"Name": "modification-state", "Values": ["optimizing"]}]
    )

    results["volumes_checked"] = len(response["VolumesModifications"])
    logger.info(f"Found {results['volumes_checked']} volumes in optimizing state")

    for modification in response["VolumesModifications"]:
      volume_id = modification["VolumeId"]
      start_time = modification.get("StartTime")
      progress = modification.get("Progress", 0)

      # Check if stuck (optimizing for more than 30 minutes)
      if start_time:
        time_elapsed = (
          datetime.now(timezone.utc) - start_time.replace(tzinfo=timezone.utc)
        ).total_seconds()

        if time_elapsed > 1800:  # 30 minutes
          logger.info(
            f"Volume {volume_id} stuck in optimizing for {time_elapsed / 60:.0f} minutes at {progress}%"
          )

          # Find the instance this volume is attached to
          volume_info = ec2.describe_volumes(VolumeIds=[volume_id])["Volumes"][0]

          if volume_info["Attachments"]:
            instance_id = volume_info["Attachments"][0]["InstanceId"]

            # Try to grow the filesystem
            logger.info(f"Attempting filesystem growth for instance {instance_id}")
            grow_result = trigger_filesystem_growth({"instance_id": instance_id})

            results["volumes_fixed"].append(
              {
                "volume_id": volume_id,
                "instance_id": instance_id,
                "optimizing_duration_minutes": int(time_elapsed / 60),
                "progress": progress,
                "filesystem_growth_triggered": grow_result.get("success", False),
              }
            )
          else:
            logger.warning(f"Volume {volume_id} is not attached to any instance")

  except Exception as e:
    logger.error(f"Failed to fix stuck volumes: {e}")
    results["errors"].append(str(e))

  # Send alert if any volumes were fixed
  if results["volumes_fixed"]:
    send_stuck_volume_alert(results["volumes_fixed"])

  return results


def send_stuck_volume_alert(fixed_volumes: List[Dict]):
  """Send alert about stuck volumes that were fixed"""

  if not fixed_volumes or not ALERT_TOPIC_ARN:
    return

  try:
    message = "Stuck Volume Remediation Report\n\n"
    message += f"Found and attempted to fix {len(fixed_volumes)} stuck volumes:\n\n"

    for vol in fixed_volumes:
      message += f"Volume: {vol['volume_id']}\n"
      message += f"Instance: {vol['instance_id']}\n"
      message += f"Stuck for: {vol['optimizing_duration_minutes']} minutes\n"
      message += f"Progress: {vol['progress']}%\n"
      message += (
        f"Filesystem growth: {'✓' if vol['filesystem_growth_triggered'] else '✗'}\n\n"
      )

    sns.publish(
      TopicArn=ALERT_TOPIC_ARN,
      Subject=f"[{ENVIRONMENT.upper()}] Stuck Volumes Fixed",
      Message=message,
    )

  except Exception as e:
    logger.error(f"Failed to send stuck volume alert: {e}")


def sync_volume_registry():
  """Synchronize volume registry with actual EC2 state"""

  if not VOLUME_REGISTRY_TABLE:
    logger.info("No volume registry table configured, skipping sync")
    return

  table = dynamodb.Table(VOLUME_REGISTRY_TABLE)
  updated_count = 0

  try:
    # Scan all volumes in registry
    response = table.scan()
    registry_volumes = response.get("Items", [])

    # Handle pagination
    while "LastEvaluatedKey" in response:
      response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
      registry_volumes.extend(response.get("Items", []))

    logger.info(f"Checking {len(registry_volumes)} volumes in registry")

    for volume in registry_volumes:
      volume_id = volume.get("volume_id")
      registry_status = volume.get("status")
      instance_id = volume.get("instance_id")

      # Skip if already marked as available
      if registry_status == "available":
        continue

      try:
        # Check actual volume state in EC2
        ec2_response = ec2.describe_volumes(VolumeIds=[volume_id])

        if not ec2_response["Volumes"]:
          # Volume doesn't exist in EC2 - remove from registry
          logger.warning(f"Volume {volume_id} not found in EC2, removing from registry")
          table.delete_item(Key={"volume_id": volume_id})
          updated_count += 1
          continue

        ec2_volume = ec2_response["Volumes"][0]
        ec2_state = ec2_volume["State"]
        ec2_attachments = ec2_volume.get("Attachments", [])

        # Check if volume is actually attached
        if ec2_state == "available" and registry_status == "attached":
          # Volume is detached but registry shows attached - update it
          logger.info(
            f"Volume {volume_id} is detached but registry shows attached, updating"
          )

          # Preserve databases list if it exists
          databases = volume.get("databases", [])

          table.update_item(
            Key={"volume_id": volume_id},
            UpdateExpression="SET #status = :status, instance_id = :instance_id, last_detached = :timestamp, databases = :databases",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
              ":status": "available",
              ":instance_id": "unattached",
              ":timestamp": datetime.now(timezone.utc).isoformat(),
              ":databases": databases,
            },
          )
          updated_count += 1

        elif ec2_attachments and registry_status == "attached":
          # Check if attached to a different or terminated instance
          current_instance = ec2_attachments[0].get("InstanceId")

          if current_instance != instance_id:
            logger.warning(
              f"Volume {volume_id} attached to {current_instance} but registry shows {instance_id}"
            )

            # Check if the registered instance still exists
            try:
              instance_response = ec2.describe_instances(InstanceIds=[instance_id])
              if instance_response["Reservations"]:
                instance_state = instance_response["Reservations"][0]["Instances"][0][
                  "State"
                ]["Name"]
                if instance_state in ["terminated", "terminating"]:
                  # Original instance is terminated, update registry
                  logger.info(
                    f"Instance {instance_id} is {instance_state}, updating volume registry"
                  )

                  table.update_item(
                    Key={"volume_id": volume_id},
                    UpdateExpression="SET instance_id = :instance_id, last_attached = :timestamp",
                    ExpressionAttributeValues={
                      ":instance_id": current_instance,
                      ":timestamp": datetime.now(timezone.utc).isoformat(),
                    },
                  )
                  updated_count += 1
            except Exception:
              # Instance doesn't exist, update to current attachment
              table.update_item(
                Key={"volume_id": volume_id},
                UpdateExpression="SET instance_id = :instance_id",
                ExpressionAttributeValues={":instance_id": current_instance},
              )
              updated_count += 1

      except Exception as e:
        if "InvalidVolume.NotFound" in str(e):
          # Volume doesn't exist - remove from registry
          logger.warning(f"Volume {volume_id} not found, removing from registry")
          table.delete_item(Key={"volume_id": volume_id})
          updated_count += 1
        else:
          logger.error(f"Error checking volume {volume_id}: {e}")

    if updated_count > 0:
      logger.info(f"Updated {updated_count} volume registry entries")

      # Publish metric
      cloudwatch.put_metric_data(
        Namespace="RoboSystems/Graph",
        MetricData=[
          {
            "MetricName": "RegistryUpdates",
            "Value": updated_count,
            "Unit": "Count",
            "Dimensions": [{"Name": "Environment", "Value": ENVIRONMENT}],
          }
        ],
      )

  except Exception as e:
    logger.error(f"Failed to sync volume registry: {e}")
    raise
