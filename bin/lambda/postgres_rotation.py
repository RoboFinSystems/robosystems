"""
PostgreSQL Password Rotation Lambda Function

Implements AWS Secrets Manager rotation for RDS PostgreSQL.

This function handles the 4-step rotation process:
1. createSecret - Generate a new password
2. setSecret - Set the password in the database
3. testSecret - Test the new password
4. finishSecret - Complete the rotation

On each rotation, the secret is written with four keys:
  POSTGRES_USER / POSTGRES_PASSWORD (legacy keys consumed by api/worker/dagster)
  username      / password          (standard keys consumed by RDS Proxy SECRETS auth)

Both key sets hold the same value. The proxy-compatible keys are populated on
every rotation so that EnableRDSProxy can be flipped to true at any time after
at least one rotation has occurred against a freshly deployed stack.
"""

import json
import logging
import os
from typing import Any

import boto3
import psycopg2
from psycopg2 import sql

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
secrets_client = boto3.client("secretsmanager")
rds_client = boto3.client("rds")


def get_database_connection_info(secret_arn: str, environment: str) -> dict[str, Any]:
  """
  Get database connection information based on the secret ARN and environment.

  Args:
      secret_arn: The ARN of the secret being rotated
      environment: The environment (prod, staging, dev)

  Returns:
      Dictionary with host, port, and database name
  """
  # Parse the secret name from ARN to determine the database
  # Format: arn:aws:secretsmanager:region:account:secret:robosystems/env/postgres-xxxxx
  secret_name = secret_arn.split(":")[-1].rsplit("-", 1)[0]
  env_from_secret = secret_name.split("/")[1]

  # Find the database instance
  db_info = {
    "host": None,
    "port": None,
    "database": "robosystems",  # Default database name
    "instance_id": None,
  }

  # Find the RDS instance. Use a paginator so accounts with >100 RDS instances
  # aren't silently truncated to the first page of describe_db_instances.
  try:
    paginator = rds_client.get_paginator("describe_db_instances")
    for page in paginator.paginate():
      for instance in page["DBInstances"]:
        if env_from_secret in instance["DBInstanceIdentifier"]:
          db_info["host"] = instance["Endpoint"]["Address"]
          db_info["port"] = instance["Endpoint"]["Port"]
          db_info["instance_id"] = instance["DBInstanceIdentifier"]
          db_info["engine"] = "postgres"
          db_info["database"] = instance.get("DBName", "robosystems")
          logger.info(f"Found RDS instance: {instance['DBInstanceIdentifier']}")
          return db_info
  except Exception as e:
    logger.warning(f"Error checking RDS instances: {e!s}")

  raise ValueError(
    f"Could not find database instance for environment: {env_from_secret}"
  )


def create_new_password() -> str:
  """Generate a new secure password."""
  response = secrets_client.get_random_password(
    PasswordLength=32,
    ExcludePunctuation=True,
    ExcludeCharacters="\"'\\/@",  # Exclude problematic characters for PostgreSQL
  )
  return response["RandomPassword"]


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

  # Get the environment from Lambda environment variables
  environment = os.environ.get("ENVIRONMENT", "prod")

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
    set_secret(arn, token, environment)
  elif step == "testSecret":
    test_secret(arn, token, environment)
  elif step == "finishSecret":
    finish_secret(arn, token)
  else:
    raise ValueError(f"Invalid step parameter {step} for secret {arn}")


def create_secret(arn: str, token: str) -> None:
  """
  Generate a new secret password.

  This step generates a new password and stores it as the AWSPENDING version.
  The pending secret is written with both legacy keys (POSTGRES_USER /
  POSTGRES_PASSWORD) and RDS Proxy-compatible keys (username / password),
  both holding the same value. This allows EnableRDSProxy to be flipped on
  at any time after at least one rotation has occurred.
  """
  # Get the current secret
  current_secret = secrets_client.get_secret_value(
    SecretId=arn, VersionStage="AWSCURRENT"
  )
  current_dict = json.loads(current_secret["SecretString"])

  # Generate new password
  new_password = create_new_password()

  # Preserve username across both key conventions. The canonical source of
  # truth is POSTGRES_USER (set by the CloudFormation SecretStringTemplate);
  # fall back to an existing "username" key, then to "postgres".
  username = current_dict.get("POSTGRES_USER") or current_dict.get(
    "username", "postgres"
  )

  # Write all four keys so the secret simultaneously satisfies legacy
  # consumers (POSTGRES_USER/POSTGRES_PASSWORD) and RDS Proxy SECRETS auth
  # (username/password).
  current_dict["POSTGRES_USER"] = username
  current_dict["POSTGRES_PASSWORD"] = new_password
  current_dict["username"] = username
  current_dict["password"] = new_password

  # Put the secret
  secrets_client.put_secret_value(
    SecretId=arn,
    ClientRequestToken=token,
    SecretString=json.dumps(current_dict),
    VersionStages=["AWSPENDING"],
  )
  logger.info(
    f"createSecret: Successfully put secret for ARN {arn} and version {token}"
  )


def set_secret(arn: str, token: str, environment: str) -> None:
  """
  Set the pending secret in the database.

  This step changes the password in PostgreSQL using the ALTER USER command.
  """
  # Get the pending secret
  pending_secret = secrets_client.get_secret_value(
    SecretId=arn, VersionStage="AWSPENDING", VersionId=token
  )
  pending_dict = json.loads(pending_secret["SecretString"])

  # Get the current secret for connection
  current_secret = secrets_client.get_secret_value(
    SecretId=arn, VersionStage="AWSCURRENT"
  )
  current_dict = json.loads(current_secret["SecretString"])

  # Get database connection info
  db_info = get_database_connection_info(arn, environment)

  # Connect and change password
  conn = None
  try:
    conn = psycopg2.connect(
      host=db_info["host"],
      port=db_info["port"],
      database=db_info["database"],
      user=current_dict.get("POSTGRES_USER", "postgres"),
      password=current_dict["POSTGRES_PASSWORD"],
      sslmode="require",
      connect_timeout=30,
    )
    conn.autocommit = True

    with conn.cursor() as cursor:
      username = pending_dict.get("POSTGRES_USER", "postgres")
      new_password = pending_dict["POSTGRES_PASSWORD"]

      # Quote the identifier (user name) safely via psycopg2.sql.Identifier
      # so a hostile username in the secret can't inject SQL.
      cursor.execute(
        sql.SQL("ALTER USER {username} WITH PASSWORD %s").format(
          username=sql.Identifier(username)
        ),
        (new_password,),
      )

    logger.info(
      f"setSecret: Successfully set password for user {username} in PostgreSQL"
    )

  except Exception as e:
    error_type = type(e).__name__
    logger.error(f"setSecret: Unable to set password: {error_type}: {e}")
    raise
  finally:
    if conn:
      conn.close()


def test_secret(arn: str, token: str, environment: str) -> None:
  """
  Test the pending secret.

  This step verifies that the new password works by attempting to connect.
  """
  # Get the pending secret
  pending_secret = secrets_client.get_secret_value(
    SecretId=arn, VersionStage="AWSPENDING", VersionId=token
  )
  pending_dict = json.loads(pending_secret["SecretString"])

  # Get database connection info
  db_info = get_database_connection_info(arn, environment)

  # Test connection with new password
  conn = None
  try:
    conn = psycopg2.connect(
      host=db_info["host"],
      port=db_info["port"],
      database=db_info["database"],
      user=pending_dict.get("POSTGRES_USER", "postgres"),
      password=pending_dict["POSTGRES_PASSWORD"],
      sslmode="require",
      connect_timeout=30,
    )

    # Run a simple query to verify the connection
    with conn.cursor() as cursor:
      cursor.execute("SELECT 1")
      cursor.fetchone()

    logger.info(
      f"testSecret: Successfully tested secret for user {pending_dict.get('POSTGRES_USER', 'postgres')}"
    )

  except Exception as e:
    error_type = type(e).__name__
    logger.error(
      f"testSecret: Unable to connect with pending secret: {error_type}: {e}"
    )
    raise
  finally:
    if conn:
      conn.close()


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
    logger.info(
      f"finishSecret: Version {token} already has AWSCURRENT for secret {arn}"
    )
    _remove_pending_stage(arn, token)
    return

  if current_version is None:
    # Calling update_secret_version_stage with RemoveFromVersionId=None would
    # raise a boto3 error AFTER the new password has already been set in the DB,
    # leaving credentials half-rotated. Fail loudly instead so the rotation can
    # be retried cleanly by Secrets Manager.
    raise ValueError(f"finishSecret: No AWSCURRENT version found for secret {arn}")

  # Move AWSCURRENT to the new version
  secrets_client.update_secret_version_stage(
    SecretId=arn,
    VersionStage="AWSCURRENT",
    MoveToVersionId=token,
    RemoveFromVersionId=current_version,
  )
  logger.info(
    f"finishSecret: Successfully set AWSCURRENT stage to version {token} for secret {arn}"
  )

  # Explicitly remove AWSPENDING as a safety net
  _remove_pending_stage(arn, token)


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
