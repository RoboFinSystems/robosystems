"""
SEC XBRL Pipeline Maintenance Tasks

Provides maintenance operations for the SEC pipeline including:
- Resetting/recreating the Kuzu SEC database
- Clearing S3 processed data for a specific year
- Future: data integrity checks, etc.
"""

import asyncio
from typing import Dict
from datetime import datetime

from robosystems.celery import celery_app
from robosystems.config import env
from robosystems.logger import logger


@celery_app.task(
  queue=env.QUEUE_SHARED_INGESTION,
  name="sec_xbrl.reset_sec_database",
  max_retries=1,
)
def reset_sec_database(confirm: bool = False) -> Dict:
  """
  Completely reset the SEC database by deleting and recreating it with schema.

  This is the nuclear option - it will:
  1. Delete the entire SEC database
  2. Recreate it with proper schema
  3. Return it to a clean, empty state

  Args:
      confirm: Must be True to actually perform the reset

  Returns:
      Status of the reset operation
  """
  if not confirm:
    return {
      "status": "cancelled",
      "message": "Reset not confirmed. Set confirm=True to proceed.",
    }

  start_time = datetime.now()

  logger.warning("🚨 RESETTING SEC DATABASE - This will delete ALL data!")

  # The SEC database details (matching ingestion.py)
  db_name = "sec"
  graph_id = "sec"
  schema_type = "shared"
  repository_name = "sec"

  async def reset_database():
    """Async function to reset the database."""
    from robosystems.graph_api.client.factory import KuzuClientFactory

    try:
      # Get a client for the SEC database
      client = await KuzuClientFactory.create_client(
        graph_id=graph_id, operation_type="write"
      )

      logger.info(f"Checking if database {db_name} exists...")

      # Check if database exists
      database_exists = False
      try:
        db_info = await client.get_database_info(graph_id)
        if db_info:
          database_exists = True
          logger.info("Database exists, will delete it...")
      except Exception as e:
        logger.info(f"Database doesn't exist or can't be accessed: {e}")

      # Delete the database if it exists
      if database_exists:
        try:
          logger.info(f"Deleting database {graph_id}...")
          await client.delete_database(graph_id)
          logger.info("✅ Database deleted successfully")
        except Exception as del_err:
          logger.warning(f"Could not delete database (may already be gone): {del_err}")

      # Now recreate the database with proper schema
      logger.info(f"Creating new SEC database with {schema_type} schema...")

      result = await client.create_database(
        graph_id=graph_id, schema_type=schema_type, repository_name=repository_name
      )

      if not result or result.get("status") != "success":
        raise Exception(f"Failed to create database: {result}")

      logger.info(f"✅ Database recreated successfully: {result}")

      # Verify the schema was applied
      node_count = 0
      rel_count = 0
      try:
        schema = await client.get_schema()
        # Handle both dict and list responses
        if isinstance(schema, dict):
          node_count = len(schema.get("node_tables", []))
          rel_count = len(schema.get("rel_tables", []))
        elif isinstance(schema, list):
          # If it's a list of tables, count node vs rel types
          node_count = sum(1 for t in schema if t.get("type") == "NODE")
          rel_count = sum(1 for t in schema if t.get("type") == "REL")

        if node_count > 0 or rel_count > 0:
          logger.info(
            f"✅ Schema verified: {node_count} node types, {rel_count} relationship types"
          )
      except Exception as e:
        logger.warning(f"Could not verify schema: {e}")

      return {
        "status": "success",
        "result": result,
        "node_types": node_count,
        "relationship_types": rel_count,
      }

    except Exception as e:
      logger.error(f"Failed to reset SEC database: {e}")
      raise

  # Run the async function
  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)

  try:
    reset_result = loop.run_until_complete(reset_database())

    duration = (datetime.now() - start_time).total_seconds()

    logger.info(f"🎉 SEC database reset completed in {duration:.1f}s")

    return {
      "status": "success",
      "message": "SEC database has been completely reset",
      "database": db_name,
      "graph_id": graph_id,
      "duration_seconds": duration,
      "node_types": reset_result.get("node_types", 0),
      "relationship_types": reset_result.get("relationship_types", 0),
      "details": reset_result,
    }

  except Exception as e:
    logger.error(f"Failed to reset SEC database: {e}")
    return {
      "status": "failed",
      "error": str(e),
      "duration_seconds": (datetime.now() - start_time).total_seconds(),
    }
  finally:
    loop.close()


@celery_app.task(
  queue=env.QUEUE_SHARED_INGESTION,
  name="sec_xbrl.full_reset_for_year",
  max_retries=1,
)
def full_reset_for_year(year: int, confirm: bool = False) -> Dict:
  """
  Clear all processed S3 data for a specific year and reset the SEC database.

  This task will:
  1. Delete all processed data from S3 for the specified year (final processed parquet files)
  2. Reset the entire SEC database (delete and recreate with schema)

  Args:
      year: Year of data to clear
      confirm: Must be True to actually perform the reset

  Returns:
      Status of the reset operation
  """
  if not confirm:
    return {
      "status": "cancelled",
      "message": "Reset not confirmed. Set confirm=True to proceed.",
    }

  start_time = datetime.now()

  logger.warning(f"🚨 FULL RESET for year {year} - Clearing S3 and resetting database")

  # Track results
  s3_result = {"status": "skipped", "files_deleted": 0}
  kuzu_result = {"status": "skipped"}

  # Step 1: Clear S3 processed data
  try:
    import boto3

    # Get S3 client
    s3_client = boto3.client(
      "s3",
      endpoint_url=env.AWS_ENDPOINT_URL if env.AWS_ENDPOINT_URL else None,
      region_name=env.AWS_DEFAULT_REGION,
    )

    # List and delete all objects for the year
    bucket = env.SEC_PROCESSED_BUCKET
    prefix = f"processed/year={year}/"

    logger.info(f"Clearing S3 data from s3://{bucket}/{prefix}")

    # List all objects to delete
    objects_to_delete = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
      if "Contents" in page:
        for obj in page["Contents"]:
          objects_to_delete.append({"Key": obj["Key"]})

    if objects_to_delete:
      # Delete in batches of 1000 (S3 limit)
      for i in range(0, len(objects_to_delete), 1000):
        batch = objects_to_delete[i : i + 1000]
        s3_client.delete_objects(Bucket=bucket, Delete={"Objects": batch})

      logger.info(f"Deleted {len(objects_to_delete)} objects from S3")
      s3_result = {"status": "success", "files_deleted": len(objects_to_delete)}
    else:
      logger.info(f"No objects found in S3 for year {year}")
      s3_result = {"status": "success", "files_deleted": 0}

  except Exception as e:
    logger.error(f"Failed to clear S3 data: {e}")
    s3_result = {"status": "failed", "error": str(e)}

  # Step 2: Reset the SEC database
  try:
    logger.info("Resetting SEC database...")

    # Call the existing reset function
    db_result = reset_sec_database(confirm=True)

    if db_result["status"] == "success":
      kuzu_result = {"status": "success"}
      logger.info("SEC database reset successfully")
    else:
      kuzu_result = {
        "status": "failed",
        "error": db_result.get("error", "Unknown error"),
      }
      logger.error(f"Failed to reset SEC database: {kuzu_result['error']}")

  except Exception as e:
    logger.error(f"Failed to reset SEC database: {e}")
    kuzu_result = {"status": "failed", "error": str(e)}

  duration = (datetime.now() - start_time).total_seconds()

  # Determine overall status
  if s3_result["status"] == "success" and kuzu_result["status"] == "success":
    overall_status = "completed"
    logger.info(f"✅ Full reset for year {year} completed in {duration:.1f}s")
  else:
    overall_status = "partial_failure"
    logger.warning(
      f"⚠️ Full reset for year {year} partially failed after {duration:.1f}s"
    )

  return {
    "status": overall_status,
    "year": year,
    "duration_seconds": duration,
    "s3_clear": s3_result,
    "kuzu_reset": kuzu_result,
  }
