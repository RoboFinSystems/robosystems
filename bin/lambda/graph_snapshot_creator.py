"""
Lambda function to create daily snapshots of Graph shared master volumes.

This function:
1. Finds the shared master writer instance (WriterTier=shared)
2. Creates a snapshot of its data volume
3. Tags it with WriterTier=shared and AllRepositories=true
4. Cleans up old snapshots (keeps last 7 days)
"""

import json
import os
import boto3
from datetime import datetime, timezone, timedelta

ec2 = boto3.client("ec2")

ENVIRONMENT = os.environ.get("ENVIRONMENT", "prod")
SHARED_REPOSITORIES = os.environ.get("SHARED_REPOSITORIES", "sec")
RETENTION_DAYS = int(os.environ.get("RETENTION_DAYS", "7"))
STACK_NAME = os.environ.get("STACK_NAME", "")


def lambda_handler(event, context):
  try:
    print(
      f"Starting snapshot creation for {ENVIRONMENT} shared master with repositories: {SHARED_REPOSITORIES}"
    )

    # Find shared master writer instance using stack name
    filters = [
      {"Name": "instance-state-name", "Values": ["running"]},
      {"Name": "tag:aws:cloudformation:stack-name", "Values": [STACK_NAME]},
      {"Name": "tag:WriterTier", "Values": ["shared"]},
    ]

    response = ec2.describe_instances(Filters=filters)
    if not response["Reservations"]:
      return {"statusCode": 404, "body": "No shared master writer instance found"}

    instance = response["Reservations"][0]["Instances"][0]
    instance_id = instance["InstanceId"]
    print(f"Found shared master writer instance: {instance_id}")

    # Find data volume
    volume_id = None
    for bdm in instance.get("BlockDeviceMappings", []):
      if bdm["DeviceName"] == "/dev/xvdf":
        volume_id = bdm["Ebs"]["VolumeId"]
        break

    if not volume_id:
      return {"statusCode": 404, "body": "No data volume found"}

    # Create snapshot
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    response = ec2.create_snapshot(
      VolumeId=volume_id,
      Description=f"Automated snapshot of shared master ({SHARED_REPOSITORIES}) {timestamp}",
      TagSpecifications=[
        {
          "ResourceType": "snapshot",
          "Tags": [
            {"Key": "Name", "Value": f"{ENVIRONMENT}-kuzu-shared-{timestamp}"},
            {"Key": "Environment", "Value": ENVIRONMENT},
            {"Key": "Service", "Value": "RoboSystems"},
            {"Key": "Component", "Value": "Kuzu"},
            {"Key": "NodeType", "Value": "shared_writer"},
            {"Key": "RepositoryTypes", "Value": SHARED_REPOSITORIES},
            {"Key": "VolumeType", "Value": "data"},
            {"Key": "SourceInstance", "Value": instance_id},
            {"Key": "SnapshotType", "Value": "scheduled"},
            {"Key": "WriterTier", "Value": "shared"},
            {
              "Key": "AllRepositories",
              "Value": "true",
            },
          ],
        }
      ],
    )

    snapshot_id = response["SnapshotId"]
    print(f"Created snapshot: {snapshot_id}")

    # Clean up old snapshots
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    old_filters = [
      {"Name": "status", "Values": ["completed"]},
      {"Name": "tag:Environment", "Values": [ENVIRONMENT]},
      {"Name": "tag:SnapshotType", "Values": ["scheduled"]},
      {"Name": "tag:AllRepositories", "Values": ["true"]},
    ]

    snapshots = ec2.describe_snapshots(OwnerIds=["self"], Filters=old_filters)
    deleted = 0

    for snapshot in snapshots["Snapshots"]:
      if snapshot["StartTime"].replace(tzinfo=timezone.utc) < cutoff:
        try:
          ec2.delete_snapshot(SnapshotId=snapshot["SnapshotId"])
          deleted += 1
        except Exception as e:
          print(f"Failed to delete {snapshot['SnapshotId']}: {e}")

    return {
      "statusCode": 200,
      "body": json.dumps(
        {
          "snapshot_id": snapshot_id,
          "volume_id": volume_id,
          "deleted_snapshots": deleted,
        }
      ),
    }

  except Exception as e:
    print(f"Error: {str(e)}")
    return {"statusCode": 500, "body": str(e)}
