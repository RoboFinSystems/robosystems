"""
PostgreSQL Password Rotation Lambda Function

Implements AWS Secrets Manager rotation for PostgreSQL databases.
Supports both RDS PostgreSQL and Aurora PostgreSQL engines.

This function handles the 4-step rotation process:
1. createSecret - Generate a new password
2. setSecret - Set the password in the database
3. testSecret - Test the new password
4. finishSecret - Complete the rotation
"""

import json
import logging
import os
from typing import Any

import boto3
import psycopg2

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
  # Format: arn:aws:secretsmanager:region:account:secret:robosystems/env/postgres/password-xxxxx
  secret_name = secret_arn.split(":")[-1].rsplit("-", 1)[0]
  env_from_secret = secret_name.split("/")[1]

  # Find the database instance
  db_info = {
    "host": None,
    "port": None,
    "database": "robosystems",  # Default database name
    "instance_id": None,
  }

  # Try Aurora clusters first
  try:
    response = rds_client.describe_db_clusters()
    for cluster in response["DBClusters"]:
      if env_from_secret in cluster["DBClusterIdentifier"]:
        db_info["host"] = cluster["Endpoint"]
        db_info["port"] = cluster["Port"]
        db_info["instance_id"] = cluster["DBClusterIdentifier"]
        db_info["engine"] = "aurora-postgresql"
        logger.info(f"Found Aurora cluster: {cluster['DBClusterIdentifier']}")
        return db_info
  except Exception as e:
    logger.warning(f"Error checking Aurora clusters: {e!s}")

  # Try RDS instances
  try:
    response = rds_client.describe_db_instances()
    for instance in response["DBInstances"]:
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
  """
  # Get the current secret
  current_secret = secrets_client.get_secret_value(
    SecretId=arn, VersionStage="AWSCURRENT"
  )
  current_dict = json.loads(current_secret["SecretString"])

  # Generate new password
  new_password = create_new_password()
  current_dict["password"] = new_password

  # Also update DATABASE_URL if it exists
  if "DATABASE_URL" in current_dict:
    # Parse and update the DATABASE_URL
    url_parts = current_dict["DATABASE_URL"].split("@")
    if len(url_parts) == 2:
      # Extract username from the URL
      proto_user = url_parts[0].split("://")
      if len(proto_user) == 2:
        username = proto_user[1].split(":")[0]
        new_url = f"{proto_user[0]}://{username}:{new_password}@{url_parts[1]}"
        current_dict["DATABASE_URL"] = new_url

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
      user=current_dict.get("username", "postgres"),
      password=current_dict["password"],
      sslmode="require",
      connect_timeout=30,
    )
    conn.autocommit = True

    with conn.cursor() as cursor:
      # Use quote_ident equivalent for username and password
      username = pending_dict.get("username", "postgres")
      new_password = pending_dict["password"]

      # PostgreSQL requires special handling for password changes
      cursor.execute(
        "ALTER USER %s WITH PASSWORD %s",
        (psycopg2.extensions.AsIs(username), new_password),
      )

    logger.info(
      f"setSecret: Successfully set password for user {username} in PostgreSQL"
    )

  except Exception as e:
    logger.error(f"setSecret: Unable to set password: {e!s}")
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
      user=pending_dict.get("username", "postgres"),
      password=pending_dict["password"],
      sslmode="require",
      connect_timeout=30,
    )

    # Run a simple query to verify the connection
    with conn.cursor() as cursor:
      cursor.execute("SELECT 1")
      cursor.fetchone()

    logger.info(
      f"testSecret: Successfully tested secret for user {pending_dict.get('username', 'postgres')}"
    )

  except Exception as e:
    logger.error(f"testSecret: Unable to connect with pending secret: {e!s}")
    raise
  finally:
    if conn:
      conn.close()


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
