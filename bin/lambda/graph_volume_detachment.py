"""
Detach LadybugDB EBS data volumes when an EC2 instance terminates.

Triggered by an Auto Scaling terminate lifecycle hook (via SNS). Unmounts the
data volume over SSM, asks the volume manager to detach it, and waits for the
volume to reach `available` before releasing the hook. Signals CONTINUE only
when every volume detached cleanly; otherwise ABANDON, so the ASG does not
launch a replacement that races a half-detached volume.
"""

import json
import os

import boto3

# Initialize AWS clients
ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")
asg = boto3.client("autoscaling")
lambda_client = boto3.client("lambda")
dynamodb = boto3.client("dynamodb")


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

    # Unmount volumes via SSM (only if instance is still running)
    if instance.get("State", {}).get("Name") == "running":
      for volume in volumes:
        try:
          print(f"Unmounting volume {volume['VolumeId']} from {volume['Device']}")
          ssm.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={
              "commands": [
                "# Unmount LadybugDB data volume",
                "sync",  # Flush any pending writes
                "umount /mnt/ladybug-data || true",
                "umount /data || true",  # Legacy mount point
              ]
            },
            TimeoutSeconds=30,
          )
        except Exception as e:
          print(f"Failed to unmount (may be expected if instance is terminating): {e}")

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
