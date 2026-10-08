"""
Detach LadybugDB EBS data volumes when an EC2 instance terminates.

Triggered by an Auto Scaling terminate lifecycle hook (via SNS). Stops the
graph container and unmounts the data volume over SSM, asks the volume manager
to detach it, and waits for the volume to reach `available` before releasing
the hook. Signals CONTINUE when every volume detached cleanly, otherwise
ABANDON. On a termination hook both let the instance terminate and its
replacement launch; ABANDON only skips any other termination hooks.
"""

import json
import os
import time

import boto3

# Initialize AWS clients
ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")
asg = boto3.client("autoscaling")
lambda_client = boto3.client("lambda")
dynamodb = boto3.client("dynamodb")

# The database must be stopped before its volume is detached: an unmount fails
# while the container holds the mount, and a detach under a live writer loses
# whatever it had not flushed. Bounded so the detach and its own 3-minute wait
# still fit the Lambda and the hook's 300s heartbeat.
STOP_AND_UNMOUNT_BUDGET_SECONDS = 50
STOP_AND_UNMOUNT_POLL_SECONDS = 2
# SSM's own limit on the script, inside the poll's budget: unset, it is an hour.
STOP_AND_UNMOUNT_EXECUTION_TIMEOUT_SECONDS = 45
STOP_AND_UNMOUNT_COMMANDS = [
  "set -a; . /etc/environment; set +a",
  # First, so the health check cannot start the container again mid-detach.
  "systemctl stop crond || true",
  'CONTAINER="$(/usr/local/bin/run-graph-container.sh --print-container-name)"'
  ' && docker stop -t 30 "$CONTAINER" || true',
  # The agent tails the database's logs on the volume, which holds the mount.
  "systemctl stop amazon-cloudwatch-agent || true",
  "sync",
  "umount /data 2>/dev/null || true",  # Legacy mount point
  # Last, so a mount that is still busy fails the command and is logged.
  "if mountpoint -q /mnt/ladybug-data; then umount /mnt/ladybug-data; fi",
]


def format_missing_field_error(field_name: str, available_keys: list) -> dict:
  """Format consistent error response for missing fields."""
  return {
    "statusCode": 400,
    "body": f"Missing {field_name}",
    "error": f"No {field_name} found. Available keys: {available_keys}",
  }


def handler(event, context):
  """Process an instance-termination lifecycle event delivered over SNS."""
  try:
    message = json.loads(event["Records"][0]["Sns"]["Message"])
    print(f"Received message: {json.dumps(message, indent=2)}")

    # Handle different message formats
    instance_id = message.get("EC2InstanceId") or message.get("InstanceId")
    lifecycle_hook = message.get("LifecycleHookName")
    asg_name = message.get("AutoScalingGroupName")

    if not instance_id:
      print(
        f"ERROR: No instance ID found in message. Available keys: {list(message.keys())}"
      )
      return format_missing_field_error("instance ID", list(message.keys()))

    if not lifecycle_hook:
      print(
        f"ERROR: No lifecycle hook found in message. Available keys: {list(message.keys())}"
      )
      return format_missing_field_error("lifecycle hook", list(message.keys()))

    if not asg_name:
      print(
        f"ERROR: No ASG name found in message. Available keys: {list(message.keys())}"
      )
      return format_missing_field_error("ASG name", list(message.keys()))

  except Exception as e:
    print(f"ERROR parsing message: {e}")
    print(f"Raw event: {json.dumps(event, indent=2)}")
    return {"statusCode": 500, "body": f"Message parsing error: {e!s}"}

  try:
    print(f"Processing termination for instance: {instance_id}")

    # Get attached volumes
    response = ec2.describe_instances(InstanceIds=[instance_id])
    if not response["Reservations"]:
      print(f"Instance {instance_id} not found")
      return complete_lifecycle(asg_name, lifecycle_hook, instance_id, "CONTINUE")

    instance = response["Reservations"][0]["Instances"][0]
    volumes = []

    for device in instance.get("BlockDeviceMappings", []):
      # Skip only the root volume (xvda), but include data volumes (xvdf, sdf, nvme devices)
      if device["DeviceName"] not in ["/dev/xvda", "/dev/sda1"]:  # Skip root volumes
        volumes.append(
          {"VolumeId": device["Ebs"]["VolumeId"], "Device": device["DeviceName"]}
        )
        print(
          f"Found data volume: {device['Ebs']['VolumeId']} at {device['DeviceName']}"
        )

    if volumes and instance.get("State", {}).get("Name") == "running":
      stop_and_unmount(instance_id)

    # Call Volume Manager to detach volumes and update registry.
    # Track per-volume success: if any detach fails or the volume doesn't reach
    # `available` state, signal ABANDON. A half-detached volume is the worst
    # outcome — the replacement instance cannot claim it, yet the registry still
    # advertises the graph as served by an instance that is going away.
    all_detached = True
    for volume in volumes:
      volume_id = volume["VolumeId"]
      try:
        print(f"Calling Volume Manager to detach volume {volume_id}")
        response = lambda_client.invoke(
          FunctionName=os.environ["VOLUME_MANAGER_FUNCTION_ARN"],
          InvocationType="RequestResponse",
          Payload=json.dumps(
            {
              "action": "detach_volume",
              "volume_id": volume_id,
              "force": False,  # Don't force detach, let it fail gracefully
            }
          ),
        )

        # Log the response from Volume Manager
        response_payload = json.loads(response["Payload"].read())
        print(f"Volume Manager response for {volume_id}: {response_payload}")
        if response_payload.get("statusCode") != 200:
          all_detached = False
          continue

        # The detach_volume API call returns as soon as EBS accepts the request
        # — the volume is still in `detaching` state for 15-30s afterward. Wait
        # for actual completion before signalling lifecycle CONTINUE; otherwise
        # the replacement instance's volume-manager will race and hit VolumeInUse.
        try:
          waiter = ec2.get_waiter("volume_available")
          waiter.wait(
            VolumeIds=[volume_id], WaiterConfig={"Delay": 5, "MaxAttempts": 36}
          )
          print(f"Volume {volume_id} reached available state")
        except Exception as e:
          print(
            f"Volume {volume_id} did not reach available in 3 min: {e}; "
            f"will signal ABANDON so the next launch doesn't race"
          )
          all_detached = False
        else:
          request_park(volume_id)

      except Exception as e:
        print(f"Failed to detach volume {volume_id}: {e}")
        all_detached = False

    # Clean the stale instance-registry entry for the terminating instance.
    # Without this the registry accumulates dead instance IDs and the routing
    # layer keeps resolving graphs to IPs that no longer exist.
    try:
      registry_table = (
        f"robosystems-graph-{os.environ['ENVIRONMENT']}-instance-registry"
      )
      dynamodb.delete_item(
        TableName=registry_table,
        Key={"instance_id": {"S": instance_id}},
      )
      print(f"Removed {instance_id} from instance-registry")
    except Exception as e:
      print(f"Failed to clean instance-registry for {instance_id}: {e}")

    return complete_lifecycle(
      asg_name,
      lifecycle_hook,
      instance_id,
      "CONTINUE" if all_detached else "ABANDON",
    )

  except Exception as e:
    print(f"Error processing termination: {e}")
    return complete_lifecycle(asg_name, lifecycle_hook, instance_id, "ABANDON")


def request_park(volume_id: str) -> None:
  """Ask the volume manager to lower the detached volume to its idle spec.

  Asynchronous, so the lifecycle hook never waits on it; the volume manager
  decides whether the volume's tier parks at all.
  """
  try:
    lambda_client.invoke(
      FunctionName=os.environ["VOLUME_MANAGER_FUNCTION_ARN"],
      InvocationType="Event",
      Payload=json.dumps({"action": "park_volume", "volume_id": volume_id}),
    )
  except Exception as e:
    print(f"Failed to request park for {volume_id}: {e}")


def stop_and_unmount(instance_id: str) -> str:
  """Stop the graph container and unmount the data volume, waiting for both.

  Returns the SSM command status, or why there is none. A failure is logged
  and the detach goes ahead regardless: the instance is terminating either way.
  """
  try:
    command_id = ssm.send_command(
      InstanceIds=[instance_id],
      DocumentName="AWS-RunShellScript",
      Parameters={
        "commands": STOP_AND_UNMOUNT_COMMANDS,
        "executionTimeout": [str(STOP_AND_UNMOUNT_EXECUTION_TIMEOUT_SECONDS)],
      },
      TimeoutSeconds=STOP_AND_UNMOUNT_BUDGET_SECONDS,
    )["Command"]["CommandId"]
  except Exception as e:
    print(f"Failed to send stop-and-unmount to {instance_id}: {e}")
    return "NotSent"

  deadline = time.monotonic() + STOP_AND_UNMOUNT_BUDGET_SECONDS
  while True:
    try:
      invocation = ssm.get_command_invocation(
        CommandId=command_id, InstanceId=instance_id
      )
      status = invocation["Status"]
      if status not in ("Pending", "InProgress", "Delayed"):
        print(f"Stop-and-unmount on {instance_id}: {status}")
        if status != "Success":
          print(f"stderr: {invocation.get('StandardErrorContent', '')[:1000]}")
        return status
    except ssm.exceptions.InvocationDoesNotExist:
      pass
    except Exception as e:
      print(f"Failed to read stop-and-unmount on {instance_id}: {e}")
      return "Unknown"
    if time.monotonic() >= deadline:
      print(
        f"Stop-and-unmount on {instance_id} not done after "
        f"{STOP_AND_UNMOUNT_BUDGET_SECONDS}s; detaching anyway"
      )
      return "TimedOut"
    time.sleep(STOP_AND_UNMOUNT_POLL_SECONDS)


def complete_lifecycle(asg_name, hook_name, instance_id, result):
  """Release the Auto Scaling lifecycle hook with CONTINUE or ABANDON."""
  asg.complete_lifecycle_action(
    LifecycleHookName=hook_name,
    AutoScalingGroupName=asg_name,
    LifecycleActionResult=result,
    InstanceId=instance_id,
  )
  return {"statusCode": 200, "body": json.dumps(f"Lifecycle completed: {result}")}


# Alias for Lambda container deployment (CloudFormation ImageConfig.Command)
lambda_handler = handler
