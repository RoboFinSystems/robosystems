"""
PostgreSQL Secrets Update Lambda Function

Updates AWS Secrets Manager with PostgreSQL connection details when RDS instance
state changes. This ensures applications always have the correct database endpoint.
"""

import json
import os
import boto3


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
    print(f"Error managing secret {secret_name}: {str(e)}")
    raise


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
    db_endpoint = db_instance["Endpoint"]["Address"]
    db_port = db_instance["Endpoint"]["Port"]

    print(f"RDS Endpoint: {db_endpoint}")

    secrets_results = []

    # Create Postgres specific environment variables
    postgres_url = f"postgresql://{os.environ['POSTGRES_USERNAME']}:{os.environ['POSTGRES_PASSWORD']}@{db_endpoint}:{db_port}/{os.environ['POSTGRES_DB_NAME']}"
    postgres_vars = {
      "DATABASE_URL": postgres_url,
      "POSTGRES_PASSWORD": os.environ["POSTGRES_PASSWORD"],
    }

    # Upload Postgres connection info to Secrets Manager
    secret_name = f"robosystems/{os.environ['ENVIRONMENT']}/postgres"
    try:
      secret_result = update_or_create_secret(
        secret_name=secret_name,
        secret_value=postgres_vars,
        description=f"PostgreSQL connection details for {os.environ['ENVIRONMENT']} environment",
        tags=[
          {"Key": "Environment", "Value": os.environ["ENVIRONMENT"]},
          {"Key": "Service", "Value": "RoboSystems"},
          {"Key": "Component", "Value": "Postgres"},
        ],
      )
      secrets_results.append({"secret_name": secret_name, "result": secret_result})
    except Exception as e:
      print(f"Error creating/updating PostgreSQL secret: {str(e)}")
      secrets_results.append({"secret_name": secret_name, "error": str(e)})

    return {
      "statusCode": 200,
      "body": json.dumps(
        {
          "message": "PostgreSQL secrets updated successfully",
          "secrets": secrets_results,
        }
      ),
    }

  except Exception as e:
    print(f"Error: {str(e)}")
    return {
      "statusCode": 500,
      "body": json.dumps(
        {"message": "Error updating PostgreSQL configs", "error": str(e)}
      ),
    }
