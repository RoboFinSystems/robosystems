"""
PostgreSQL Secrets Update Lambda Function

Updates AWS Secrets Manager with PostgreSQL connection details when RDS instance
state changes. Also creates additional databases (like 'dagster') if they don't exist.
This ensures applications always have the correct database endpoint.

Note: This Lambda requires VPC configuration to connect to RDS for database creation.
"""

import json
import os

import boto3

# psycopg2 is included via Lambda layer or packaged with the deployment
try:
  import psycopg2

  PSYCOPG2_AVAILABLE = True
except ImportError:
  PSYCOPG2_AVAILABLE = False
  print("Warning: psycopg2 not available - database creation will be skipped")


def update_or_create_secret(secret_name, secret_value, description=None, tags=None):
  """Create or update a secret in AWS Secrets Manager"""
  secrets_client = boto3.client("secretsmanager")

  # Convert dict to JSON string if needed
  if isinstance(secret_value, dict):
    secret_value = json.dumps(secret_value)

  try:
    # Check if the secret already exists
    try:
      secrets_client.describe_secret(SecretId=secret_name)
      # Secret exists, update it
      response = secrets_client.update_secret(
        SecretId=secret_name, SecretString=secret_value
      )
      if tags:
        secrets_client.tag_resource(SecretId=secret_name, Tags=tags)
      return {"status": "updated", "arn": response["ARN"]}
    except secrets_client.exceptions.ResourceNotFoundException:
      # Secret doesn't exist, create it
      create_args = {
        "Name": secret_name,
        "SecretString": secret_value,
      }
      if description:
        create_args["Description"] = description
      if tags:
        create_args["Tags"] = tags

      response = secrets_client.create_secret(**create_args)
      return {"status": "created", "arn": response["ARN"]}
  except Exception as e:
    # Avoid logging full exception which may contain sensitive data
    error_type = type(e).__name__
    print(f"Error managing secret {secret_name}: {error_type}")
    raise


def create_database_if_not_exists(host, port, username, password, db_name):
  """Create a database if it doesn't already exist"""
  if not PSYCOPG2_AVAILABLE:
    print(f"Skipping database creation for '{db_name}' - psycopg2 not available")
    return {"status": "skipped", "reason": "psycopg2 not available"}

  try:
    # Connect to the default 'postgres' database to create new databases
    conn = psycopg2.connect(
      host=host,
      port=port,
      user=username,
      password=password,
      database="postgres",
      connect_timeout=10,
    )
    conn.autocommit = True  # Required for CREATE DATABASE

    with conn.cursor() as cur:
      # Check if database exists
      cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
      exists = cur.fetchone()

      if not exists:
        # Create the database
        # Use quote_ident equivalent for safety
        cur.execute(f'CREATE DATABASE "{db_name}"')
        print(f"Created database '{db_name}'")
        return {"status": "created", "database": db_name}
      else:
        print(f"Database '{db_name}' already exists")
        return {"status": "exists", "database": db_name}

  except psycopg2.Error as e:
    # Log only error type to avoid exposing connection details
    error_type = type(e).__name__
    print(f"PostgreSQL error creating database '{db_name}': {error_type}")
    return {"status": "error", "database": db_name, "error": error_type}
  except Exception as e:
    error_type = type(e).__name__
    print(f"Error creating database '{db_name}': {error_type}")
    return {"status": "error", "database": db_name, "error": error_type}
  finally:
    if "conn" in locals():
      conn.close()


def lambda_handler(event, context):
  try:
    # Get RDS instance details
    rds_client = boto3.client("rds")
    db_instance_id = os.environ["POSTGRES_DB_INSTANCE"]

    # Get database instance information
    response = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_id)

    if not response["DBInstances"]:
      return {"statusCode": 500, "body": "Failed to get RDS instance details"}

    db_instance = response["DBInstances"][0]

    # Check if the RDS instance has an endpoint yet (may not be available during startup)
    if "Endpoint" not in db_instance:
      print(f"RDS instance {db_instance_id} is not ready yet - no endpoint available")
      return {
        "statusCode": 200,
        "body": json.dumps({
          "message": "RDS instance not ready yet, skipping initialization",
          "status": db_instance.get("DBInstanceStatus", "unknown"),
        }),
      }

    db_endpoint = db_instance["Endpoint"]["Address"]
    db_port = db_instance["Endpoint"]["Port"]
    db_username = os.environ["POSTGRES_USERNAME"]
    db_password = os.environ["POSTGRES_PASSWORD"]
    db_name = os.environ["POSTGRES_DB_NAME"]
    environment = os.environ["ENVIRONMENT"]

    print(f"RDS Endpoint: {db_endpoint}")

    secrets_results = []
    database_results = []

    # Create Postgres specific environment variables
    # Include host/port/username for services that need individual components
    postgres_url = (
      f"postgresql://{db_username}:{db_password}@{db_endpoint}:{db_port}/{db_name}"
    )
    postgres_vars = {
      "DATABASE_URL": postgres_url,
      "POSTGRES_PASSWORD": db_password,
      "password": db_password,  # Alias for CloudFormation dynamic references
      "host": db_endpoint,
      "port": str(db_port),
      "username": db_username,
      "database": db_name,
    }

    # Upload Postgres connection info to Secrets Manager
    secret_name = f"robosystems/{environment}/postgres"
    try:
      secret_result = update_or_create_secret(
        secret_name=secret_name,
        secret_value=postgres_vars,
        description=f"PostgreSQL connection details for {environment} environment",
        tags=[
          {"Key": "Environment", "Value": environment},
          {"Key": "Service", "Value": "RoboSystems"},
          {"Key": "Component", "Value": "Postgres"},
        ],
      )
      secrets_results.append({"secret_name": secret_name, "result": secret_result})
    except Exception as e:
      error_type = type(e).__name__
      print(f"Error creating/updating PostgreSQL secret: {error_type}")
      secrets_results.append({"secret_name": secret_name, "error": error_type})

    # Create additional databases if configured
    # These are databases that share the same RDS instance but need separate schemas
    additional_databases = os.environ.get("ADDITIONAL_DATABASES", "dagster").split(",")
    additional_databases = [db.strip() for db in additional_databases if db.strip()]

    for additional_db in additional_databases:
      print(f"Ensuring database '{additional_db}' exists...")
      db_result = create_database_if_not_exists(
        host=db_endpoint,
        port=db_port,
        username=db_username,
        password=db_password,
        db_name=additional_db,
      )
      database_results.append(db_result)

    return {
      "statusCode": 200,
      "body": json.dumps(
        {
          "message": "PostgreSQL secrets updated successfully",
          "secrets": secrets_results,
          "databases": database_results,
        }
      ),
    }

  except Exception as e:
    error_type = type(e).__name__
    print(f"Error: {error_type}")
    return {
      "statusCode": 500,
      "body": json.dumps(
        {"message": "Error updating PostgreSQL configs", "error": error_type}
      ),
    }
