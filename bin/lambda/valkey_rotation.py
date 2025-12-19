#!/usr/bin/env python3
"""
Valkey Auth Token Rotation Lambda Function.

This Lambda function rotates Valkey auth tokens stored in AWS Secrets Manager.
It follows the standard AWS Secrets Manager rotation pattern with four steps:

1. createSecret - Generate new auth token
2. setSecret - Update Valkey with new auth token
3. testSecret - Verify new auth token works
4. finishSecret - Mark new secret as current

The function is designed to work with ElastiCache Valkey replication groups
and handles both single-node and multi-node configurations.
"""

import json
import logging
import os
import time
from typing import Any

import boto3
import redis

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS clients
secrets_client = boto3.client("secretsmanager")
elasticache_client = boto3.client("elasticache")


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
  """
  Main Lambda handler for Valkey auth token rotation.

  Args:
      event: Lambda event containing SecretId and Step
      context: Lambda context (unused)

  Returns:
      Success response or raises exception on failure
  """
  try:
    secret_arn = event["SecretId"]
    step = event["Step"]
    token = event.get("Token", "AWSCURRENT")

    logger.info(f"Starting {step} for secret {secret_arn}")

    # Route to appropriate step handler
    if step == "createSecret":
      create_secret(secret_arn, token)
    elif step == "setSecret":
      set_secret(secret_arn, token)
    elif step == "testSecret":
      test_secret(secret_arn, token)
    elif step == "finishSecret":
      finish_secret(secret_arn, token)
    else:
      raise ValueError(f"Invalid step: {step}")

    logger.info(f"Successfully completed {step} for secret {secret_arn}")
    return {"statusCode": 200, "body": f"Successfully completed {step}"}

  except Exception as e:
    logger.error(f"Error in {step}: {e!s}")
    raise


def create_secret(secret_arn: str, token: str) -> None:
  """
  Step 1: Create a new auth token.

  Generates a new random auth token and stores it in the AWSPENDING version
  of the secret without modifying the current AWSCURRENT version.
  """
  try:
    # Check if AWSPENDING version already exists
    try:
      secrets_client.get_secret_value(SecretId=secret_arn, VersionStage="AWSPENDING")
      logger.info("AWSPENDING version already exists, skipping creation")
      return
    except secrets_client.exceptions.ResourceNotFoundException:
      pass  # Expected - no pending version exists yet

    # Get current secret to understand structure
    current_secret = secrets_client.get_secret_value(
      SecretId=secret_arn, VersionStage="AWSCURRENT"
    )
    current_data = json.loads(current_secret["SecretString"])

    # Generate new auth token (64 character random string)
    import secrets as py_secrets
    import string

    # Use URL-safe characters (no quotes, backslashes, etc.)
    chars = string.ascii_letters + string.digits + "-_"
    new_token = "".join(py_secrets.choice(chars) for _ in range(64))

    # Create new secret data with same structure
    new_secret_data = current_data.copy()
    new_secret_data["VALKEY_AUTH_TOKEN"] = new_token

    # Store as AWSPENDING version
    secrets_client.put_secret_value(
      SecretId=secret_arn,
      SecretString=json.dumps(new_secret_data),
      VersionStages=["AWSPENDING"],
    )

    logger.info("Successfully created new auth token in AWSPENDING version")

  except Exception as e:
    logger.error(f"Failed to create secret: {e!s}")
    raise


def set_secret(secret_arn: str, token: str) -> None:
  """
  Step 2: Update Valkey with the new auth token.

  Updates the ElastiCache Valkey replication group to use the new auth token
  from the AWSPENDING version of the secret.
  """
  try:
    # Get new auth token from AWSPENDING version
    pending_secret = secrets_client.get_secret_value(
      SecretId=secret_arn, VersionStage="AWSPENDING"
    )
    pending_data = json.loads(pending_secret["SecretString"])
    new_auth_token = pending_data["VALKEY_AUTH_TOKEN"]

    # Get Valkey replication group ID from environment
    replication_group_id = os.environ.get("VALKEY_REPLICATION_GROUP_ID")
    if not replication_group_id:
      raise ValueError("VALKEY_REPLICATION_GROUP_ID environment variable not set")

    logger.info(f"Updating auth token for replication group: {replication_group_id}")

    # Update the replication group with new auth token
    elasticache_client.modify_replication_group(
      ReplicationGroupId=replication_group_id,
      AuthToken=new_auth_token,
      AuthTokenUpdateStrategy="ROTATE",  # Rotate without downtime
    )

    logger.info(f"Initiated auth token rotation for {replication_group_id}")

    # Wait for the modification to complete
    waiter = elasticache_client.get_waiter("replication_group_available")
    waiter.wait(
      ReplicationGroupId=replication_group_id,
      WaiterConfig={
        "Delay": 30,
        "MaxAttempts": 20,  # Wait up to 10 minutes
      },
    )

    logger.info("Auth token rotation completed successfully")

  except Exception as e:
    logger.error(f"Failed to set secret in Valkey: {e!s}")
    raise


def test_secret(secret_arn: str, token: str) -> None:
  """
  Step 3: Test the new auth token.

  Connects to Valkey using the new auth token from AWSPENDING version
  to verify it works correctly.
  """
  try:
    # Get new auth token from AWSPENDING version
    pending_secret = secrets_client.get_secret_value(
      SecretId=secret_arn, VersionStage="AWSPENDING"
    )
    pending_data = json.loads(pending_secret["SecretString"])
    new_auth_token = pending_data["VALKEY_AUTH_TOKEN"]

    # Get Valkey endpoint information
    replication_group_id = os.environ.get("VALKEY_REPLICATION_GROUP_ID")
    if not replication_group_id:
      raise ValueError("VALKEY_REPLICATION_GROUP_ID environment variable not set")

    # Get replication group details
    response = elasticache_client.describe_replication_groups(
      ReplicationGroupId=replication_group_id
    )

    if not response["ReplicationGroups"]:
      raise ValueError(f"Replication group {replication_group_id} not found")

    replication_group = response["ReplicationGroups"][0]

    # Get primary endpoint
    if "PrimaryEndpoint" in replication_group:
      endpoint = replication_group["PrimaryEndpoint"]["Address"]
      port = replication_group["PrimaryEndpoint"]["Port"]
    else:
      # Fall back to first node endpoint for single-node setups
      if not replication_group["NodeGroups"]:
        raise ValueError("No node groups found in replication group")
      node_group = replication_group["NodeGroups"][0]
      if not node_group["NodeGroupMembers"]:
        raise ValueError("No nodes found in node group")
      primary_node = next(
        (
          node
          for node in node_group["NodeGroupMembers"]
          if node["CurrentRole"] == "primary"
        ),
        node_group["NodeGroupMembers"][0],
      )
      endpoint = primary_node["ReadEndpoint"]["Address"]
      port = primary_node["ReadEndpoint"]["Port"]

    logger.info(f"Testing connection to {endpoint}:{port}")

    # Test connection with new auth token
    redis_client = redis.Redis(
      host=endpoint,
      port=port,
      password=new_auth_token,
      ssl=True,  # Use TLS for encrypted connections
      ssl_cert_reqs=None,  # Don't verify certificates for ElastiCache
      decode_responses=True,
      socket_connect_timeout=10,
      socket_timeout=10,
    )

    # Test basic Redis operations
    test_key = f"rotation_test_{int(time.time())}"
    redis_client.set(test_key, "test_value", ex=60)  # Expires in 1 minute
    value = redis_client.get(test_key)
    redis_client.delete(test_key)

    if value != "test_value":
      raise ValueError("Failed to read back test value from Valkey")

    # Test additional operations
    redis_client.ping()
    info = redis_client.info()

    logger.info(
      f"Successfully tested new auth token. Valkey version: {info.get('redis_version', 'unknown')}"
    )

  except Exception as e:
    logger.error(f"Failed to test secret: {e!s}")
    raise


def finish_secret(secret_arn: str, token: str) -> None:
  """
  Step 4: Finalize the rotation.

  Moves the AWSPENDING version to AWSCURRENT and removes the old version.
  This completes the rotation process.
  """
  try:
    # Get current and pending versions
    current_secret = secrets_client.get_secret_value(
      SecretId=secret_arn, VersionStage="AWSCURRENT"
    )
    pending_secret = secrets_client.get_secret_value(
      SecretId=secret_arn, VersionStage="AWSPENDING"
    )

    # Update version stages
    secrets_client.update_secret_version_stage(
      SecretId=secret_arn,
      VersionStage="AWSCURRENT",
      MoveToVersionId=pending_secret["VersionId"],
      RemoveFromVersionId=current_secret["VersionId"],
    )

    logger.info("Successfully moved AWSPENDING to AWSCURRENT")

    # Clean up old version
    secrets_client.update_secret_version_stage(
      SecretId=secret_arn,
      VersionStage="AWSPREVIOUS",
      MoveToVersionId=current_secret["VersionId"],
    )

    logger.info("Successfully moved old AWSCURRENT to AWSPREVIOUS")

    # Remove AWSPENDING stage to complete rotation
    secrets_client.update_secret_version_stage(
      SecretId=secret_arn,
      VersionStage="AWSPENDING",
      RemoveFromVersionId=pending_secret["VersionId"],
    )

    logger.info("Successfully removed AWSPENDING stage")

  except Exception as e:
    logger.error(f"Failed to finish secret rotation: {e!s}")
    raise


if __name__ == "__main__":
  # Test the rotation locally (requires AWS credentials)
  import sys

  if len(sys.argv) != 3:
    print("Usage: python valkey_rotation.py <secret_arn> <step>")
    sys.exit(1)

  secret_arn = sys.argv[1]
  step = sys.argv[2]

  # Set environment variable for testing
  if not os.environ.get("VALKEY_REPLICATION_GROUP_ID"):
    os.environ["VALKEY_REPLICATION_GROUP_ID"] = "robosystems-prod-valkey"

  event = {"SecretId": secret_arn, "Step": step, "Token": "AWSCURRENT"}

  result = lambda_handler(event, None)
  print(f"Result: {result}")
