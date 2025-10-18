"""
Graph API Key Rotation Lambda Function

Implements AWS Secrets Manager rotation for Graph API keys (Kuzu and Neo4j).
Generates new API keys and stores them securely.

This function handles the 4-step rotation process:
1. createSecret - Generate new API keys
2. setSecret - No action needed (keys are validated at runtime)
3. testSecret - Verify the new keys are valid
4. finishSecret - Complete the rotation

Supports both:
- Kuzu API keys (KUZU_API_KEY)
- Neo4j credentials (NEO4J_PASSWORD for neo4j-writers)
"""

import boto3
import json
import logging
import secrets
import string
from datetime import datetime, timezone
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
secrets_client = boto3.client("secretsmanager")


def generate_api_key(prefix: str = "kuzu", length: int = 32) -> str:
  """
  Generate a secure API key.

  Args:
      prefix: Prefix for the API key
      length: Length of the random portion

  Returns:
      A secure API key string
  """
  # Use a secure random generator
  alphabet = string.ascii_letters + string.digits
  random_part = "".join(secrets.choice(alphabet) for _ in range(length))
  return f"{prefix}_{random_part}"


def generate_password(length: int = 32) -> str:
  """
  Generate a secure password for Neo4j.

  Args:
      length: Length of the password

  Returns:
      A secure password string
  """
  # Include special characters for stronger passwords
  alphabet = string.ascii_letters + string.digits + "!@#$%^&*()-_=+[]{}|;:,.<>?"
  password = "".join(secrets.choice(alphabet) for _ in range(length))
  return password


def is_neo4j_secret(secret_dict: Dict[str, Any]) -> bool:
  """
  Determine if this is a Neo4j secret based on its structure.

  Args:
      secret_dict: The secret dictionary

  Returns:
      True if this is a Neo4j secret, False if it's a Kuzu API key secret
  """
  # Neo4j secrets have NEO4J_PASSWORD and TIER fields
  # Kuzu secrets have KUZU_API_KEY
  return "NEO4J_PASSWORD" in secret_dict or "TIER" in secret_dict


def lambda_handler(event: Dict[str, Any], context: Any) -> None:
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
    logger.error(f"Secret {arn} is not enabled for rotation")
    raise ValueError(f"Secret {arn} is not enabled for rotation")

  versions = metadata["VersionIdsToStages"]
  if token not in versions:
    logger.error(f"Secret version {token} has no stage for rotation of secret {arn}")
    raise ValueError(
      f"Secret version {token} has no stage for rotation of secret {arn}"
    )

  if "AWSCURRENT" in versions[token]:
    logger.info(f"Secret version {token} already set as AWSCURRENT for secret {arn}")
    return
  elif "AWSPENDING" not in versions[token]:
    logger.error(
      f"Secret version {token} not set as AWSPENDING for rotation of secret {arn}"
    )
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
    raise ValueError(f"Invalid step parameter {step} for secret {arn}")


def create_secret(arn: str, token: str) -> None:
  """
  Generate new credentials (API keys for Kuzu or password for Neo4j).

  This step generates new credentials and stores them as the AWSPENDING version.
  """
  # Get the current secret to preserve the structure
  try:
    current_secret = secrets_client.get_secret_value(
      SecretId=arn, VersionStage="AWSCURRENT"
    )
    current_dict = json.loads(current_secret["SecretString"])
  except Exception:
    # If no current secret exists, create a new structure
    current_dict = {}

  # Determine secret type and generate appropriate credentials
  if is_neo4j_secret(current_dict):
    # Neo4j secret - rotate password
    tier = current_dict.get("TIER", "unknown")
    environment = current_dict.get("ENVIRONMENT", "unknown")
    new_secret = {
      "NEO4J_PASSWORD": generate_password(),
      "TIER": tier,
      "ENVIRONMENT": environment,
      "GENERATED_AT": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(f"createSecret: Generating new Neo4j password for tier {tier}")
  else:
    # Kuzu API key secret
    environment = current_dict.get("ENVIRONMENT", "unknown")
    new_secret = {
      "KUZU_API_KEY": generate_api_key(f"kuzu_{environment}", 64),
      "ENVIRONMENT": environment,
      "GENERATED_AT": datetime.now(timezone.utc).isoformat(),
      "rotation_version": token,
    }
    logger.info("createSecret: Generating new Kuzu API key")

  # Put the secret
  secrets_client.put_secret_value(
    SecretId=arn,
    ClientRequestToken=token,
    SecretString=json.dumps(new_secret),
    VersionStages=["AWSPENDING"],
  )
  logger.info(
    f"createSecret: Successfully generated new credentials for ARN {arn} and version {token}"
  )


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
  In a production environment, this could make test API calls to verify the credentials work.
  """
  # Get the pending secret
  pending_secret = secrets_client.get_secret_value(
    SecretId=arn, VersionStage="AWSPENDING", VersionId=token
  )
  pending_dict = json.loads(pending_secret["SecretString"])

  # Determine secret type and validate accordingly
  if is_neo4j_secret(pending_dict):
    # Validate Neo4j password
    if "NEO4J_PASSWORD" not in pending_dict:
      raise ValueError("Missing required key: NEO4J_PASSWORD")

    password = pending_dict["NEO4J_PASSWORD"]
    if not password or not isinstance(password, str):
      raise ValueError("Invalid password format for NEO4J_PASSWORD")

    # Validate password strength (at least 16 characters)
    if len(password) < 16:
      raise ValueError("Password must be at least 16 characters")

    # Validate required metadata
    if "TIER" not in pending_dict:
      raise ValueError("Missing TIER field for Neo4j secret")
    if "ENVIRONMENT" not in pending_dict:
      raise ValueError("Missing ENVIRONMENT field")

    logger.info(f"testSecret: Successfully validated new Neo4j password for tier {pending_dict['TIER']}")
  else:
    # Validate Kuzu API key
    if "KUZU_API_KEY" not in pending_dict:
      raise ValueError("Missing required key: KUZU_API_KEY")

    api_key = pending_dict["KUZU_API_KEY"]
    if not api_key or not isinstance(api_key, str):
      raise ValueError("Invalid API key format for KUZU_API_KEY")

    # Validate key format (prefix_randomstring)
    if "_" not in api_key:
      raise ValueError("Invalid API key format: missing underscore")

    parts = api_key.split("_")
    if len(parts) < 3:  # Should be kuzu_environment_randomstring
      raise ValueError("Invalid API key format: expected kuzu_environment_randomstring")

    # Validate suffix contains only alphanumeric characters and hyphens/underscores
    random_part = "_".join(parts[2:])
    if not all(c.isalnum() or c in "-_" for c in random_part):
      raise ValueError(
        "Invalid API key format: random part must be alphanumeric with - or _"
      )

    logger.info("testSecret: Successfully validated new Kuzu API key")

  # Validate metadata
  if "GENERATED_AT" not in pending_dict:
    raise ValueError("Missing GENERATED_AT timestamp")

  # In production, you could make test API calls here to verify the credentials work
  # For now, we just validate the format


def finish_secret(arn: str, token: str) -> None:
  """
  Finish the rotation by updating version stages.

  This step promotes the pending secret to current.
  """
  metadata = secrets_client.describe_secret(SecretId=arn)
  current_version = None
  for version in metadata["VersionIdsToStages"]:
    if "AWSCURRENT" in metadata["VersionIdsToStages"][version]:
      current_version = version
      break

  # Update version stages
  secrets_client.update_secret_version_stage(
    SecretId=arn,
    VersionStage="AWSCURRENT",
    MoveToVersionId=token,
    RemoveFromVersionId=current_version,
  )
  logger.info(
    f"finishSecret: Successfully set AWSCURRENT stage to version {token} for secret {arn}"
  )

  # Log rotation completion
  try:
    # Get the new current secret to log the rotation
    new_secret = secrets_client.get_secret_value(
      SecretId=arn, VersionStage="AWSCURRENT"
    )
    secret_dict = json.loads(new_secret["SecretString"])
    secret_type = "Neo4j password" if is_neo4j_secret(secret_dict) else "Kuzu API key"
    logger.info(
      f"{secret_type} rotation completed successfully. Generated at: {secret_dict.get('GENERATED_AT', 'unknown')}"
    )
  except Exception as e:
    logger.warning(f"Could not log rotation completion details: {str(e)}")
