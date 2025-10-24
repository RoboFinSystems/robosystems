"""
SEC XBRL Pipeline Maintenance Tasks

Provides maintenance operations for the SEC pipeline including:
- Resetting/recreating the SEC graph database (supports Kuzu and Neo4j backends)
- Clearing S3 processed data for a specific year
- Future: data integrity checks, etc.
"""

import asyncio
from typing import Dict
from datetime import datetime

from robosystems.celery import celery_app
from robosystems.config import env
from robosystems.logger import logger
from robosystems.graph_api.client.factory import GraphClientFactory


@celery_app.task(
  queue=env.QUEUE_SHARED_INGESTION,
  name="sec_xbrl.reset_sec_database",
  max_retries=1,
)
def reset_sec_database(confirm: bool = False, backend: str = "kuzu") -> Dict:
  """
  Completely reset the SEC database by deleting and recreating it with schema.

  This is the nuclear option - it will:
  1. Delete the entire SEC database
  2. Recreate it with proper schema
  3. Return it to a clean, empty state

  Args:
      confirm: Must be True to actually perform the reset
      backend: Backend type for context/logging ("kuzu" or "neo4j")

  Returns:
      Status of the reset operation
  """
  if not confirm:
    return {
      "status": "cancelled",
      "message": "Reset not confirmed. Set confirm=True to proceed.",
    }

  start_time = datetime.now()

  logger.warning(
    f"🚨 RESETTING SEC DATABASE ({backend} backend) - This will delete ALL data!"
  )

  # The SEC database details (matching ingestion.py)
  db_name = "sec"
  graph_id = "sec"
  schema_type = "shared"
  repository_name = "sec"

  async def reset_database():
    """Async function to reset the database."""

    try:
      # Get a client for the SEC database
      client = await GraphClientFactory.create_client(
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
          logger.info("✅ Database deleted successfully (including DuckDB staging)")
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

      # Create Graph record in PostgreSQL for the SEC repository
      # This is required for ingestion and rebuild operations
      from robosystems.models.iam.graph import Graph
      from robosystems.models.iam.graph_credits import GraphTier
      from robosystems.database import get_db_session

      db_session = next(get_db_session())
      try:
        # Check if Graph record already exists
        existing_graph = Graph.get_by_id(graph_id, db_session)
        if not existing_graph:
          # Create Graph record for shared repository
          Graph.create(
            graph_id=graph_id,
            graph_name="SEC XBRL Repository",
            graph_type="generic",
            session=db_session,
            base_schema="base",
            schema_extensions=["roboledger"],
            graph_instance_id="shared-kuzu-writer",  # SEC uses shared writer cluster
            graph_tier=GraphTier.KUZU_XLARGE,  # SEC shared repository tier
            graph_metadata={
              "repository_type": "shared",
              "repository_name": repository_name,
              "description": "Shared SEC XBRL financial reporting data repository",
              "status": "available",
            },
          )
          logger.info(f"✅ Created PostgreSQL Graph record for {graph_id}")
        else:
          # Update existing graph to ensure it's marked as available
          graph_metadata = (
            {**existing_graph.graph_metadata} if existing_graph.graph_metadata else {}
          )
          graph_metadata["status"] = "available"
          existing_graph.graph_metadata = graph_metadata
          db_session.commit()
          logger.info(f"✅ Updated existing PostgreSQL Graph record for {graph_id}")
      finally:
        db_session.close()

      # Verify the schema was applied
      node_count = 0
      rel_count = 0
      schema_ddl = None
      try:
        schema = await client.get_schema()
        # Handle both dict and list responses
        if isinstance(schema, dict):
          node_count = len(schema.get("node_tables", []))
          rel_count = len(schema.get("rel_tables", []))
          schema_ddl = schema.get("ddl")
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

      # Generate schema DDL using SchemaManager (like entity graphs do)
      schema_ddl = None
      try:
        logger.info("Generating schema DDL from SchemaManager...")
        from robosystems.schemas.manager import SchemaManager

        manager = SchemaManager()
        config = manager.create_schema_configuration(
          name="SEC Database Schema",
          description="Complete financial reporting schema with XBRL taxonomy support",
          extensions=["roboledger"],  # SEC schema uses roboledger extension
        )

        schema = manager.load_and_compile_schema(config)
        schema_ddl = schema.to_cypher()
        logger.info(f"✅ Generated schema DDL: {len(schema_ddl)} characters")
        logger.debug(f"DDL preview: {schema_ddl[:500]}...")

        # Persist schema DDL to PostgreSQL for rebuild operations
        from robosystems.models.iam import GraphSchema

        db_session_for_schema = next(get_db_session())
        try:
          # Check if schema already exists
          existing_schema = GraphSchema.get_active_schema(
            graph_id, db_session_for_schema
          )
          if not existing_schema:
            GraphSchema.create(
              graph_id=graph_id,
              schema_type="shared",
              schema_ddl=schema_ddl,
              schema_json={
                "base": "base",
                "extensions": ["roboledger"],
              },
              session=db_session_for_schema,
            )
            logger.info(f"✅ Persisted schema DDL to PostgreSQL for {graph_id}")
          else:
            logger.info(f"Schema DDL already exists in PostgreSQL for {graph_id}")
        finally:
          db_session_for_schema.close()

      except Exception as e:
        logger.warning(f"Could not generate schema DDL: {e}")

      # Auto-create DuckDB staging tables from schema (like entity graphs do)
      duckdb_tables_created = 0
      if schema_ddl:
        try:
          logger.info("Creating DuckDB staging tables from schema...")
          from robosystems.operations.graph.table_service import TableService
          from robosystems.database import get_db_session

          db = next(get_db_session())
          try:
            table_service = TableService(db)
            created_tables = table_service.create_tables_from_schema(
              graph_id=graph_id,
              user_id="system",  # Shared repository, no specific user
              schema_ddl=schema_ddl,
            )
            duckdb_tables_created = len(created_tables)
            logger.info(
              f"✅ Auto-created {duckdb_tables_created} DuckDB staging tables for SEC graph"
            )
          finally:
            db.close()
        except Exception as e:
          logger.warning(f"Could not create DuckDB staging tables: {e}")

      return {
        "status": "success",
        "result": result,
        "node_types": node_count,
        "relationship_types": rel_count,
        "duckdb_tables_created": duckdb_tables_created,
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
      "message": f"SEC database ({backend} backend) has been completely reset",
      "database": db_name,
      "graph_id": graph_id,
      "backend": backend,
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
def full_reset_for_year(
  year: int, confirm: bool = False, backend: str = "kuzu"
) -> Dict:
  """
  Clear all processed S3 data for a specific year and reset the SEC database.

  This task will:
  1. Delete all processed data from S3 for the specified year (final processed parquet files)
  2. Reset the entire SEC database (delete and recreate with schema)

  Args:
      year: Year of data to clear
      confirm: Must be True to actually perform the reset
      backend: Backend type for context/logging ("kuzu" or "neo4j")

  Returns:
      Status of the reset operation
  """
  if not confirm:
    return {
      "status": "cancelled",
      "message": "Reset not confirmed. Set confirm=True to proceed.",
    }

  start_time = datetime.now()

  logger.warning(
    f"🚨 FULL RESET for year {year} ({backend} backend) - Clearing S3 and resetting database"
  )

  # Track results
  s3_result = {"status": "skipped", "files_deleted": 0}
  db_result = {"status": "skipped"}

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
    logger.info(f"Resetting SEC database ({backend} backend)...")

    # Call the existing reset function with backend parameter
    reset_result = reset_sec_database(confirm=True, backend=backend)

    if reset_result["status"] == "success":
      db_result = {"status": "success"}
      logger.info("SEC database reset successfully")
    else:
      db_result = {
        "status": "failed",
        "error": reset_result.get("error", "Unknown error"),
      }
      logger.error(f"Failed to reset SEC database: {db_result['error']}")

  except Exception as e:
    logger.error(f"Failed to reset SEC database: {e}")
    db_result = {"status": "failed", "error": str(e)}

  duration = (datetime.now() - start_time).total_seconds()

  # Determine overall status
  if s3_result["status"] == "success" and db_result["status"] == "success":
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
    "backend": backend,
    "duration_seconds": duration,
    "s3_clear": s3_result,
    "database_reset": db_result,
  }
