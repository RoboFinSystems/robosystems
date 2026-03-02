"""
API Key Rotation Lambda Function

Generic Secrets Manager rotation for API keys. Handles:
- Graph API keys (JSON with GRAPH_API_KEY, ENVIRONMENT fields)
- Admin API keys (plain string)
- Neo4j credentials (JSON with NEO4J_PASSWORD, TIER fields)

This function handles the 4-step rotation process:
1. createSecret - Generate new credentials
2. setSecret - No action needed (keys are validated at runtime)
3. testSecret - Verify the new credentials are valid
4. finishSecret - Complete the rotation
"""

import json
import logging
import secrets
import string
from datetime import UTC, datetime
from typing import Any

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
secrets_client = boto3.client("secretsmanager")


def generate_api_key(prefix: str = "lbug", length: int = 32) -> str:
  """
  Generate a secure API key with a prefix.

  Args:
      prefix: Prefix for the API key
      length: Length of the random portion

  Returns:
      A secure API key string
  """
  alphabet = string.ascii_letters + string.digits
  random_part = "".join(secrets.choice(alphabet) for _ in range(length))
  return f"{prefix}_{random_part}"


def generate_plain_key(length: int = 64) -> str:
  """
  Generate a secure plain API key (no prefix, alphanumeric only).

  Args:
      length: Length of the key

  Returns:
      A secure alphanumeric string
  """
  alphabet = string.ascii_letters + string.digits
  return "".join(secrets.choice(alphabet) for _ in range(length))


def generate_password(length: int = 32) -> str:
  """
  Generate a secure password for Neo4j.

  Args:
      length: Length of the password

  Returns:
      A secure password string
  """
  alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
  password = "".join(secrets.choice(alphabet) for _ in range(length))
  return password


def _detect_secret_type(secret_string: str) -> str:
  """
  Detect the type of secret from its current value.

  Returns one of: 'neo4j', 'graph_api', 'plain'
  """
  try:
    secret_dict = json.loads(secret_string)
    if isinstance(secret_dict, dict):
      if "NEO4J_PASSWORD" in secret_dict or "TIER" in secret_dict:
        return "neo4j"
      if "GRAPH_API_KEY" in secret_dict:
        return "graph_api"
  except (json.JSONDecodeError, TypeError):
    pass
  return "plain"


def lambda_handler(event: dict[str, Any], context: Any) -> None:
  """
  AWS Lambda handler for Secrets Manager rotation.

  Args:
      event: The Lambda event containing SecretId, ClientRequestToken, and Step
      context: The Lambda context
  """
  arn = event["SecretId"]
  token = event["ClientRequestToken"]
  step = event["Step"]

  # Setup the client
  metadata = secrets_client.describe_secret(SecretId=arn)
  if not metadata["RotationEnabled"]:
    logger.error("Secret is not enabled for rotation")
    raise ValueError(f"Secret {arn} is not enabled for rotation")

  versions = metadata["VersionIdsToStages"]
  if token not in versions:
    logger.error("Secret version has no stage for rotation")
    raise ValueError(
      f"Secret version {token} has no stage for rotation of secret {arn}"
    )

  if "AWSCURRENT" in versions[token]:
    logger.info("Secret version already set as AWSCURRENT")
    return
  elif "AWSPENDING" not in versions[token]:
    logger.error("Secret version not set as AWSPENDING for rotation")
    raise ValueError(
      f"Secret version {token} not set as AWSPENDING for rotation of secret {arn}"
    )

  # Call the appropriate step function
  if step == "createSecret":
    create_secret(arn, token)
  elif step == "setSecret":
    set_secret(arn, token)
  elif step == "testSecret":
    test_secret(arn, token)
  elif step == "finishSecret":
    finish_secret(arn, token)
  else:
    raise ValueError(f"Invalid step parameter: {step}")


def create_secret(arn: str, token: str) -> None:
  """
  Generate new credentials based on the current secret type.

  This step generates new credentials and stores them as the AWSPENDING version.
  """
  # Get the current secret to determine type and preserve structure
  try:
    current_secret = secrets_client.get_secret_value(
      SecretId=arn, VersionStage="AWSCURRENT"
    )
    current_string = current_secret["SecretString"]
  except Exception:
    current_string = ""

  secret_type = _detect_secret_type(current_string)

  if secret_type == "neo4j":
    current_dict = json.loads(current_string)
    tier = current_dict.get("TIER", "unknown")
    environment = current_dict.get("ENVIRONMENT", "unknown")
    new_secret_string = json.dumps(
      {
        "NEO4J_PASSWORD": generate_password(),
        "TIER": tier,
        "ENVIRONMENT": environment,
        "GENERATED_AT": datetime.now(UTC).isoformat(),
      }
    )
    logger.info(f"createSecret: Generating new Neo4j password for tier {tier}")

  elif secret_type == "graph_api":
    current_dict = json.loads(current_string)
    environment = current_dict.get("ENVIRONMENT", "unknown")
    new_secret_string = json.dumps(
      {
        "GRAPH_API_KEY": generate_api_key(f"graph_{environment}", 64),
        "ENVIRONMENT": environment,
        "GENERATED_AT": datetime.now(UTC).isoformat(),
        "rotation_version": token,
      }
    )
    logger.info("createSecret: Generating new Graph API key")

  else:
    # Plain string API key (admin keys, etc.)
    new_secret_string = generate_plain_key(64)
    logger.info("createSecret: Generating new plain API key")

  # Put the secret
  secrets_client.put_secret_value(
    SecretId=arn,
    ClientRequestToken=token,
    SecretString=new_secret_string,
    VersionStages=["AWSPENDING"],
  )
  logger.info("createSecret: Successfully generated new credentials")


def set_secret(arn: str, token: str) -> None:
  """
  Set the pending secret in the service.

  For API keys, this step is a no-op because the keys are validated at runtime
  by the services. The actual key validation happens during the testSecret step.
  """
  logger.info(
    "setSecret: No action needed for API keys - validation happens at runtime"
  )


def test_secret(arn: str, token: str) -> None:
  """
  Test the pending secret.

  This step verifies that the new credentials are properly formatted and valid.
  """
  # Get the pending secret
  pending_secret = secrets_client.get_secret_value(
    SecretId=arn, VersionStage="AWSPENDING", VersionId=token
  )
  pending_string = pending_secret["SecretString"]
  secret_type = _detect_secret_type(pending_string)

  if secret_type == "neo4j":
    pending_dict = json.loads(pending_string)
    _validate_neo4j(pending_dict)
    logger.info(
      f"testSecret: Successfully validated new Neo4j password for tier {pending_dict['TIER']}"
    )

  elif secret_type == "graph_api":
    pending_dict = json.loads(pending_string)
    _validate_graph_api_key(pending_dict)
    logger.info(
      f"testSecret: Successfully validated new Graph API key for {pending_dict.get('ENVIRONMENT', 'unknown')}"
    )

  else:
    # Plain string API key
    _validate_plain_key(pending_string)
    logger.info("testSecret: Successfully validated new plain API key")


def _validate_neo4j(pending_dict: dict[str, Any]) -> None:
  """Validate a Neo4j password secret."""
  if "NEO4J_PASSWORD" not in pending_dict:
    raise ValueError("Missing required key: NEO4J_PASSWORD")

  password = pending_dict["NEO4J_PASSWORD"]
  if not password or not isinstance(password, str):
    raise ValueError("Invalid password format for NEO4J_PASSWORD")

  if len(password) < 16:
    raise ValueError("Password must be at least 16 characters")

  if "TIER" not in pending_dict:
    raise ValueError("Missing TIER field for Neo4j secret")
  if "ENVIRONMENT" not in pending_dict:
    raise ValueError("Missing ENVIRONMENT field")
  if "GENERATED_AT" not in pending_dict:
    raise ValueError("Missing GENERATED_AT timestamp")


def _validate_graph_api_key(pending_dict: dict[str, Any]) -> None:
  """Validate a Graph API key secret."""
  if "GRAPH_API_KEY" not in pending_dict:
    raise ValueError("Missing required key: GRAPH_API_KEY")

  api_key = pending_dict["GRAPH_API_KEY"]
  if not api_key or not isinstance(api_key, str):
    raise ValueError("Invalid API key format for GRAPH_API_KEY")

  # Validate minimum total key length (should be at least 70 chars for graph_env_64chars)
  if len(api_key) < 70:
    raise ValueError(f"API key too short: {len(api_key)} chars, expected at least 70")

  # Validate key format (graph_environment_randomstring)
  if "_" not in api_key:
    raise ValueError("Invalid API key format: missing underscore")

  parts = api_key.split("_", 2)  # Split into max 3 parts: graph, environment, random
  if len(parts) != 3:
    raise ValueError("Invalid API key format: expected graph_environment_randomstring")

  if parts[0] != "graph":
    raise ValueError(f"Invalid API key prefix: expected 'graph', got '{parts[0]}'")

  valid_environments = ["prod", "staging"]
  if parts[1] not in valid_environments:
    raise ValueError(
      f"Invalid environment: expected one of {valid_environments}, got '{parts[1]}'"
    )

  random_part = parts[2]
  if len(random_part) < 64:
    raise ValueError(
      f"Random part too short: {len(random_part)} chars, expected at least 64"
    )

  if not all(c.isalnum() or c in "-_" for c in random_part):
    raise ValueError(
      "Invalid API key format: random part must be alphanumeric with - or _"
    )

  if "GENERATED_AT" not in pending_dict:
    raise ValueError("Missing GENERATED_AT timestamp")


def _validate_plain_key(key: str) -> None:
  """Validate a plain string API key."""
  if not key or not isinstance(key, str):
    raise ValueError("Invalid plain API key: empty or not a string")

  if len(key) < 32:
    raise ValueError(f"Plain API key too short: {len(key)} chars, expected at least 32")

  if not all(c.isalnum() for c in key):
    raise ValueError("Plain API key must be alphanumeric only")


def finish_secret(arn: str, token: str) -> None:
  """
  Finish the rotation by updating version stages.

  This step promotes the pending secret to current and explicitly cleans up
  the AWSPENDING label. Secrets Manager should remove AWSPENDING automatically
  when AWSCURRENT is moved, but this does not always happen (known AWS quirk).
  """
  metadata = secrets_client.describe_secret(SecretId=arn)
  current_version = None
  for version in metadata["VersionIdsToStages"]:
    if "AWSCURRENT" in metadata["VersionIdsToStages"][version]:
      current_version = version
      break

  # If token already has AWSCURRENT, rotation was already completed
  if current_version == token:
    logger.info(f"finishSecret: Version {token} already has AWSCURRENT")
    _remove_pending_stage(arn, token)
    return

  if current_version is None:
    logger.warning(f"finishSecret: No version with AWSCURRENT found for secret {arn}")

  # Move AWSCURRENT to the new version
  secrets_client.update_secret_version_stage(
    SecretId=arn,
    VersionStage="AWSCURRENT",
    MoveToVersionId=token,
    RemoveFromVersionId=current_version,
  )
  logger.info("finishSecret: Successfully set AWSCURRENT stage to new version")

  # Explicitly remove AWSPENDING as a safety net
  _remove_pending_stage(arn, token)

  # Log rotation completion
  try:
    new_secret = secrets_client.get_secret_value(
      SecretId=arn, VersionStage="AWSCURRENT"
    )
    secret_string = new_secret["SecretString"]
    secret_type = _detect_secret_type(secret_string)
    type_label = {
      "neo4j": "Neo4j password",
      "graph_api": "Graph API key",
      "plain": "API key",
    }
    logger.info(
      f"{type_label.get(secret_type, 'Secret')} rotation completed successfully"
    )
  except Exception as e:
    logger.warning(f"Could not log rotation completion details: {e!s}")


def _remove_pending_stage(arn: str, version_id: str) -> None:
  """Remove AWSPENDING label from a version. Non-fatal if already removed."""
  try:
    secrets_client.update_secret_version_stage(
      SecretId=arn,
      VersionStage="AWSPENDING",
      RemoveFromVersionId=version_id,
    )
    logger.info("finishSecret: Explicitly removed AWSPENDING label")
  except Exception as e:
    logger.warning(
      f"finishSecret: Could not remove AWSPENDING label (non-fatal): {e!s}"
    )
