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
  # Pre-initialize so the ``except`` block can log a meaningful value
  # even if the ``event`` lookups below raise. Without this, a missing
  # ``event['Step']`` raises KeyError → except clause references unbound
  # ``step`` → UnboundLocalError masks the real failure.
  step = "<unknown>"
  secret_arn = "<unknown>"
  try:
    secret_arn = event["SecretId"]
    step = event["Step"]
    token = event["ClientRequestToken"]

    logger.info(f"Starting {step} for secret {secret_arn}")

    # Validate rotation is enabled and version stages are correct
    metadata = secrets_client.describe_secret(SecretId=secret_arn)
    if not metadata["RotationEnabled"]:
      logger.error("Secret is not enabled for rotation")
      raise ValueError(f"Secret {secret_arn} is not enabled for rotation")

    versions = metadata["VersionIdsToStages"]
    if token not in versions:
      logger.error("Secret version has no stage for rotation")
      raise ValueError(
        f"Secret version {token} has no stage for rotation of secret {secret_arn}"
      )

    if "AWSCURRENT" in versions[token]:
      logger.info("Secret version already set as AWSCURRENT")
      return {"statusCode": 200, "body": "Secret version already current"}
    elif "AWSPENDING" not in versions[token]:
      logger.error("Secret version not set as AWSPENDING for rotation")
      raise ValueError(
        f"Secret version {token} not set as AWSPENDING for rotation of secret {secret_arn}"
      )

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
    # Log only error type to avoid exposing sensitive data in exceptions
    error_type = type(e).__name__
    logger.error(f"Error in {step}: {error_type}")
    raise


def create_secret(secret_arn: str, token: str) -> None:
  """
  Step 1: Create a new auth token.

  Generates a new random auth token and stores it in the AWSPENDING version
  of the secret without modifying the current AWSCURRENT version.
  """
  try:
    # Check if AWSPENDING version already exists for THIS rotation token
    try:
      secrets_client.get_secret_value(
        SecretId=secret_arn, VersionStage="AWSPENDING", VersionId=token
      )
      logger.info(
        f"AWSPENDING version already exists for token {token}, skipping creation"
      )
      return
    except secrets_client.exceptions.ResourceNotFoundException:
      pass  # Expected - no pending version exists yet for this token

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

    # Store as AWSPENDING version, associated with the rotation token
    secrets_client.put_secret_value(
      SecretId=secret_arn,
      ClientRequestToken=token,
      SecretString=json.dumps(new_secret_data),
      VersionStages=["AWSPENDING"],
    )

    logger.info(f"Successfully created new auth token in AWSPENDING version {token}")

  except Exception as e:
    error_type = type(e).__name__
    logger.error(f"Failed to create secret: {error_type}")
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
      ApplyImmediately=True,
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
    error_type = type(e).__name__
    logger.error(f"Failed to set secret in Valkey: {error_type}")
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
    error_type = type(e).__name__
    logger.error(f"Failed to test secret: {error_type}")
    raise


def finish_secret(secret_arn: str, token: str) -> None:
  """
  Step 4: Finalize the rotation.

  Moves the AWSPENDING version to AWSCURRENT and removes the old version.
  This completes the rotation process. Explicitly cleans up the AWSPENDING
  label as a safety net (known AWS quirk where it doesn't always get removed).
  """
  metadata = secrets_client.describe_secret(SecretId=secret_arn)
  current_version = None
  for version in metadata["VersionIdsToStages"]:
    if "AWSCURRENT" in metadata["VersionIdsToStages"][version]:
      current_version = version
      break

  # If token already has AWSCURRENT, rotation was already completed
  if current_version == token:
    logger.info(f"finishSecret: Version {token} already has AWSCURRENT")
    _remove_pending_stage(secret_arn, token)
    return

  if current_version is None:
    # Calling update_secret_version_stage with RemoveFromVersionId=None would
    # raise a boto3 error AFTER the new token has already been applied to
    # ElastiCache, leaving the secret in a half-rotated state. Fail loudly
    # instead so the rotation can be retried cleanly by Secrets Manager.
    raise ValueError(
      f"finishSecret: No AWSCURRENT version found for secret {secret_arn}"
    )

  # Move AWSCURRENT to the new version
  secrets_client.update_secret_version_stage(
    SecretId=secret_arn,
    VersionStage="AWSCURRENT",
    MoveToVersionId=token,
    RemoveFromVersionId=current_version,
  )
  logger.info("finishSecret: Successfully set AWSCURRENT stage to new version")

  # Explicitly remove AWSPENDING as a safety net
  _remove_pending_stage(secret_arn, token)


def _remove_pending_stage(secret_arn: str, version_id: str) -> None:
  """Remove AWSPENDING label from a version. Non-fatal if already removed."""
  try:
    secrets_client.update_secret_version_stage(
      SecretId=secret_arn,
      VersionStage="AWSPENDING",
      RemoveFromVersionId=version_id,
    )
    logger.info("finishSecret: Explicitly removed AWSPENDING label")
  except Exception as e:
    logger.warning(
      f"finishSecret: Could not remove AWSPENDING label (non-fatal): {e!s}"
    )


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

  event = {
    "SecretId": secret_arn,
    "Step": step,
    "ClientRequestToken": "test-token-00000000-0000-0000-0000-000000000000",
  }

  result = lambda_handler(event, None)
  print(f"Result: {result}")
