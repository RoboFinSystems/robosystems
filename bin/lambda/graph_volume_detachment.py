"""
Graph Volume Detachment Lambda Function

Handles the safe detachment of EBS volumes when EC2 instances are terminated.
This function is triggered by Auto Scaling lifecycle hooks to ensure volumes
are properly unmounted and detached before instance termination.
Supports both LadybugDB and Neo4j backends.
"""

import json
import boto3
import os

# Initialize AWS clients
ec2 = boto3.client("ec2")
ssm = boto3.client("ssm")
asg = boto3.client("autoscaling")
lambda_client = boto3.client("lambda")


def format_missing_field_error(field_name: str, available_keys: list) -> dict:
  """Format consistent error response for missing fields."""
  return {
    "statusCode": 400,
    "body": f"Missing {field_name}",
    "error": f"No {field_name} found. Available keys: {available_keys}",
  }


def handler(event, context):
  """
  Main Lambda handler for processing instance termination events

  Args:
      event: SNS event containing lifecycle hook details
      context: Lambda execution context

  Returns:
      dict: Response with status code and completion message
  """
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
    return {"statusCode": 500, "body": f"Message parsing error: {str(e)}"}

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
                "umount /mnt/lbug-data || true",
                "umount /data || true",  # Legacy mount point
              ]
            },
            TimeoutSeconds=30,
          )
        except Exception as e:
          print(f"Failed to unmount (may be expected if instance is terminating): {e}")

    # Call Volume Manager to detach volumes and update registry
    for volume in volumes:
      try:
        print(f"Calling Volume Manager to detach volume {volume['VolumeId']}")
        response = lambda_client.invoke(
          FunctionName=os.environ["VOLUME_MANAGER_FUNCTION_ARN"],
          InvocationType="RequestResponse",
          Payload=json.dumps(
            {
              "action": "detach_volume",
              "volume_id": volume["VolumeId"],
              "force": False,  # Don't force detach, let it fail gracefully
            }
          ),
        )

        # Log the response from Volume Manager
        response_payload = json.loads(response["Payload"].read())
        print(f"Volume Manager response for {volume['VolumeId']}: {response_payload}")

      except Exception as e:
        print(f"Failed to detach volume {volume['VolumeId']}: {e}")

    # Complete lifecycle action
    return complete_lifecycle(asg_name, lifecycle_hook, instance_id, "CONTINUE")

  except Exception as e:
    print(f"Error processing termination: {e}")
    return complete_lifecycle(asg_name, lifecycle_hook, instance_id, "ABANDON")


def complete_lifecycle(asg_name, hook_name, instance_id, result):
  """
  Complete the Auto Scaling lifecycle action

  Args:
      asg_name: Auto Scaling Group name
      hook_name: Lifecycle hook name
      instance_id: EC2 instance ID
      result: Action result (CONTINUE or ABANDON)

  Returns:
      dict: Response with status code and completion message
  """
  asg.complete_lifecycle_action(
    LifecycleHookName=hook_name,
    AutoScalingGroupName=asg_name,
    LifecycleActionResult=result,
    InstanceId=instance_id,
  )
  return {"statusCode": 200, "body": json.dumps(f"Lifecycle completed: {result}")}
