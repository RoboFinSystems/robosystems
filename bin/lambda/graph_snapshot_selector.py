"""
Lambda function to update replica launch template with latest snapshot.

This function:
1. Finds the latest snapshot from shared master writer instances (WriterTier=shared)
2. Updates the replica launch template with the new snapshot ID
3. Triggers instance refresh for the ASG if instances exist
"""

import json
import os
import boto3

ec2 = boto3.client("ec2")
autoscaling = boto3.client("autoscaling")

ENVIRONMENT = os.environ.get("ENVIRONMENT", "prod")


def lambda_handler(event, context):
  try:
    # Get ASG name from event or environment
    ASG_NAME = event.get("AUTO_SCALING_GROUP_NAME") or os.environ.get(
      "AUTO_SCALING_GROUP_NAME"
    )

    if not ASG_NAME:
      return {"statusCode": 400, "body": "AUTO_SCALING_GROUP_NAME not provided"}

    print(f"Finding latest snapshot for {ENVIRONMENT} replicas, ASG: {ASG_NAME}")

    # Find latest snapshot from shared master writer instances
    # These snapshots will have WriterTier=shared tag
    filters = [
      {"Name": "status", "Values": ["completed"]},
      {"Name": "tag:Environment", "Values": [ENVIRONMENT]},
      {"Name": "tag:WriterTier", "Values": ["shared"]},
      {"Name": "tag:SnapshotType", "Values": ["scheduled"]},
    ]

    snapshots = ec2.describe_snapshots(OwnerIds=["self"], Filters=filters)["Snapshots"]
    if not snapshots:
      return {"statusCode": 404, "body": "No snapshots found"}

    # Sort by start time to get latest
    snapshots.sort(key=lambda x: x["StartTime"], reverse=True)
    latest = snapshots[0]
    snapshot_id = latest["SnapshotId"]
    print(f"Latest snapshot: {snapshot_id} from {latest['StartTime']}")

    # Get ASG info
    asg = autoscaling.describe_auto_scaling_groups(AutoScalingGroupNames=[ASG_NAME])[
      "AutoScalingGroups"
    ][0]

    lt_id = asg["LaunchTemplate"]["LaunchTemplateId"]
    current_version = asg["LaunchTemplate"].get("Version", "$Latest")

    # Get current launch template
    lt_version = ec2.describe_launch_template_versions(
      LaunchTemplateId=lt_id, Versions=[current_version]
    )["LaunchTemplateVersions"][0]

    lt_data = lt_version["LaunchTemplateData"]

    # Update snapshot ID for data volume
    updated = False
    for bdm in lt_data.get("BlockDeviceMappings", []):
      if bdm["DeviceName"] == "/dev/xvdf":
        if "Ebs" not in bdm:
          bdm["Ebs"] = {}
        current_snapshot = bdm["Ebs"].get("SnapshotId")
        if current_snapshot != snapshot_id:
          bdm["Ebs"]["SnapshotId"] = snapshot_id
          updated = True
          print(f"Updating snapshot from {current_snapshot} to {snapshot_id}")
        break

    if not updated:
      print("Snapshot already up to date")
      return {"statusCode": 200, "body": "Already using latest snapshot"}

    # Create new version
    new_lt = ec2.create_launch_template_version(
      LaunchTemplateId=lt_id,
      SourceVersion=str(lt_version["VersionNumber"]),
      LaunchTemplateData=lt_data,
      VersionDescription=f"Updated with snapshot {snapshot_id}",
    )

    new_version = new_lt["LaunchTemplateVersion"]["VersionNumber"]

    # Set as default
    ec2.modify_launch_template(LaunchTemplateId=lt_id, DefaultVersion=str(new_version))

    # Update ASG
    autoscaling.update_auto_scaling_group(
      AutoScalingGroupName=ASG_NAME,
      LaunchTemplate={"LaunchTemplateId": lt_id, "Version": str(new_version)},
    )

    print(f"Updated launch template to version {new_version}")

    # Start instance refresh if instances exist
    if asg["DesiredCapacity"] > 0:
      refresh = autoscaling.start_instance_refresh(
        AutoScalingGroupName=ASG_NAME,
        Strategy="Rolling",
        DesiredConfiguration={"LaunchTemplate": {"Version": "$Latest"}},
        Preferences={
          "MinHealthyPercentage": 90,
          "InstanceWarmup": 300,
          "CheckpointPercentages": [50],
          "CheckpointDelay": 600,
        },
      )
      print(f"Started instance refresh: {refresh['InstanceRefreshId']}")

    return {
      "statusCode": 200,
      "body": json.dumps(
        {
          "snapshot_id": snapshot_id,
          "launch_template_version": new_version,
          "instance_refresh": asg["DesiredCapacity"] > 0,
        }
      ),
    }

  except Exception as e:
    print(f"Error: {str(e)}")
    return {"statusCode": 500, "body": str(e)}
