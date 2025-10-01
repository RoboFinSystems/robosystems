"""
SEC XBRL Kuzu Ingestion Task

Handles the final stage of the SEC pipeline - ingesting processed
parquet files directly from the processed folder using native S3 bulk loading.

Key features:
- Uses Kuzu's native S3 COPY FROM with glob patterns
- Processes nodes before relationships (dependency order)
- Single-threaded to prevent overwhelming Kuzu
- Supports incremental updates
- Reads directly from processed folder (no consolidation needed)
"""

import asyncio
from typing import Dict, Any, Optional, List, Set
from datetime import datetime

from celery import Task
from robosystems.celery import celery_app
from robosystems.config import env
from robosystems.logger import logger


# Queue configuration
QUEUE_SHARED_INGESTION = env.QUEUE_SHARED_INGESTION


def get_sec_schema_types():
  """
  Dynamically get SEC/XBRL schema types from the roboledger extension.
  Returns node types, relationship types, and global types for deduplication.

  NOTE: The SEC schema loader (ContextAwareSchemaLoader with "sec_repository" context)
  already filters out base nodes that don't have SEC data (User, GraphMetadata, Connection)
  and relationships that aren't populated by SEC filings.
  """
  from robosystems.schemas.loader import get_sec_schema_loader
  from robosystems.schemas.base import BASE_NODES, BASE_RELATIONSHIPS

  # Load schema specifically for SEC repository (filtered roboledger subset)
  loader = get_sec_schema_loader()

  # Get all node and relationship types (already filtered by schema loader)
  all_node_types = list(loader.nodes.keys())
  all_rel_types = list(loader.relationships.keys())

  # Determine which are global (from base schema)
  base_node_names = {node.name for node in BASE_NODES}
  base_rel_names = {rel.name for rel in BASE_RELATIONSHIPS}

  # Order nodes with base types first, then extension types
  ordered_nodes = []

  # Add base nodes first (in their original order)
  for node in BASE_NODES:
    if node.name in all_node_types:
      ordered_nodes.append(node.name)

  # Add extension nodes
  for node_name in all_node_types:
    if node_name not in ordered_nodes:
      ordered_nodes.append(node_name)

  # Order relationships similarly
  ordered_rels = []

  # Add base relationships first
  for rel in BASE_RELATIONSHIPS:
    if rel.name in all_rel_types:
      ordered_rels.append(rel.name)

  # Add extension relationships
  for rel_name in all_rel_types:
    if rel_name not in ordered_rels:
      ordered_rels.append(rel_name)

  return {
    "node_types": ordered_nodes,
    "relationship_types": ordered_rels,
    "global_node_types": base_node_names,
    "global_relationship_types": base_rel_names,
  }


# Lazy-load schema types only when needed
_schema_types: Optional[Dict[str, Any]] = None
NODE_TYPES: Optional[List[str]] = None
RELATIONSHIP_TYPES: Optional[List[str]] = None
GLOBAL_NODE_TYPES: Optional[Set[str]] = None
GLOBAL_RELATIONSHIP_TYPES: Optional[Set[str]] = None

# Note: We always use IGNORE_ERRORS=true for SEC data to handle:
# - Referential integrity issues (missing facts, multi-entity filings)
# - Duplicate entries across filings
# - Data inconsistencies from upstream processing
# This ensures a single bad row doesn't fail an entire batch ingestion


def _ensure_schema_loaded():
  """Ensure schema is loaded before use."""
  global \
    _schema_types, \
    NODE_TYPES, \
    RELATIONSHIP_TYPES, \
    GLOBAL_NODE_TYPES, \
    GLOBAL_RELATIONSHIP_TYPES

  if _schema_types is None:
    _schema_types = get_sec_schema_types()
    NODE_TYPES = _schema_types["node_types"]
    RELATIONSHIP_TYPES = _schema_types["relationship_types"]
    GLOBAL_NODE_TYPES = _schema_types["global_node_types"]
    GLOBAL_RELATIONSHIP_TYPES = _schema_types["global_relationship_types"]

    logger.info(
      f"Loaded SEC schema: {len(NODE_TYPES)} node types, "
      f"{len(RELATIONSHIP_TYPES)} relationship types"
    )


@celery_app.task(
  bind=True,
  queue=QUEUE_SHARED_INGESTION,
  name="sec_xbrl.ingest_to_kuzu",  # More descriptive name
  max_retries=3,
  default_retry_delay=60,
)
def ingest_sec_data(
  self: Task,
  pipeline_run_id: str,
  year: int = None,
  bucket: str = None,
  prefix: str = "processed/",
  db_name: str = "sec",
  graph_id: str = "sec",
  schema_type: str = "shared",  # Parameterized for testability
  repository_name: str = "sec",  # Parameterized for testability
  batch_mode: bool = True,  # Use new batch structure
  incremental: bool = False,  # Only load new batches
  timestamp_after: Optional[str] = None,  # Load batches after this timestamp
  use_consolidated: bool = False,  # Use consolidated files instead of raw processed
) -> Dict[str, Any]:
  """
  Ingest SEC data from S3 to Kuzu using native bulk loading.

  Expects data organized as either:
  - processed/year={year}/nodes/{type}/*.parquet (raw processed files)
  - processed/year={year}/relationships/{type}/*.parquet

  Or when use_consolidated=True:
  - consolidated/nodes/{type}/batch_*.parquet (consolidated files, all years)
  - consolidated/relationships/{type}/batch_*.parquet

  Args:
      pipeline_run_id: Pipeline identifier for tracking
      year: Year to ingest (optional, can be extracted from prefix)
      bucket: S3 bucket (defaults to SEC_PROCESSED_BUCKET)
      prefix: S3 prefix for data (default: "processed/")
      db_name: Target database name
      graph_id: Graph ID for routing
      schema_type: Schema type for database creation (default: "shared")
      repository_name: Repository name for shared schemas (default: "sec")
      batch_mode: Use new batch structure (vs legacy structure)
      incremental: Only load new batches since last run
      timestamp_after: Only load batches with timestamp after this
      use_consolidated: Use consolidated files for better performance (default: False)

  Returns:
      Dict with ingestion results
  """
  start_time = datetime.now()

  # Set environment variable for large tables requiring cleanup in Kuzu ingestion
  # This is used by the generic Kuzu ingestion API to handle memory management
  import os

  os.environ["KUZU_LARGE_TABLES_REQUIRING_CLEANUP"] = env.XBRL_GRAPH_LARGE_NODES

  # Ensure schema is loaded (only happens once, on first SEC task execution)
  _ensure_schema_loaded()

  # Type assertions for type checker - these are guaranteed to be set after _ensure_schema_loaded()
  assert NODE_TYPES is not None
  assert RELATIONSHIP_TYPES is not None
  assert GLOBAL_NODE_TYPES is not None
  assert GLOBAL_RELATIONSHIP_TYPES is not None

  # First, verify the SEC database exists with proper schema
  # IMPORTANT: We do NOT create or recreate databases here - only verify
  logger.info("Verifying SEC database exists with proper schema...")
  try:
    from robosystems.kuzu_api.client.factory import KuzuClientFactory
    import asyncio

    async def verify_database():
      """
      Verify SEC database exists with proper schema.

      This will NOT create or recreate the database.
      If database doesn't exist or lacks schema, it will raise an error.
      Use 'just sec-reset' to create/reset the database.
      """
      try:
        # Get a client for the SEC database - this will route correctly
        # In dev: goes to local Kuzu instance
        # In prod: goes to shared master for writes
        client = await KuzuClientFactory.create_client(
          graph_id=graph_id, operation_type="write"
        )

        # Check if database exists and has schema
        database_exists = False
        has_schema = False

        try:
          db_info = await client.get_database_info(graph_id)
          if db_info:
            database_exists = True
            logger.info("SEC database exists, checking schema...")

            # Check if it has tables (schema applied)
            try:
              schema = await client.get_schema()
              # Handle different schema response formats
              if isinstance(schema, dict):
                # Full schema response with node_tables and rel_tables
                node_count = len(schema.get("node_tables", schema.get("nodes", [])))
                rel_count = len(
                  schema.get("rel_tables", schema.get("relationships", []))
                )
              elif isinstance(schema, list):
                # KuzuClient returns tables array - count NODE and REL types
                node_count = len([s for s in schema if s.get("type") == "NODE"])
                rel_count = len([s for s in schema if s.get("type") == "REL"])
              else:
                # Unknown format, assume no schema
                logger.warning(f"Unknown schema format: {type(schema)}")
                node_count = 0
                rel_count = 0

              # SEC database should have at least some core nodes and relationships
              # Don't recreate if we have a reasonable schema
              if node_count > 5 and rel_count > 5:
                has_schema = True
                logger.info(
                  f"SEC database has schema: {node_count} nodes, {rel_count} relationships"
                )
              else:
                logger.warning(
                  f"SEC database has minimal schema ({node_count} nodes, {rel_count} rels) - checking if it needs recreation"
                )
                # Only recreate if truly empty (less than expected minimum)
                if node_count == 0 and rel_count == 0:
                  logger.warning("SEC database is completely empty - will recreate")
                  has_schema = False
                else:
                  logger.info(
                    "SEC database has some schema - keeping existing database"
                  )
                  has_schema = True
            except Exception as schema_err:
              logger.warning(f"Could not check schema: {schema_err}")
              has_schema = False
        except Exception:
          # Database doesn't exist at all
          logger.info("SEC database doesn't exist")
          database_exists = False

        # IMPORTANT: We no longer create or recreate databases during ingestion
        # Only verify that the database exists with proper schema
        if database_exists and has_schema:
          logger.info("SEC database exists with proper schema")
          return True
        else:
          # Build clear error message
          error_parts = []
          if not database_exists:
            error_parts.append("SEC database does not exist")
          elif not has_schema:
            error_parts.append("SEC database exists but lacks proper RoboLedger schema")

          error_parts.append(
            "Please run 'just sec-reset' to create/reset the SEC database"
          )
          error_msg = ". ".join(error_parts)
          logger.error(error_msg)

          # Don't try to create or recreate - just fail with clear message
          raise RuntimeError(error_msg)

      except Exception as e:
        logger.error(f"Failed to ensure SEC database: {e}")
        raise

    # Run the async function
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
      success = loop.run_until_complete(verify_database())
      if not success:
        raise RuntimeError(
          "SEC database verification failed. "
          "Please run 'just sec-reset' to create/reset the database."
        )
    finally:
      loop.close()

  except RuntimeError as e:
    # Re-raise RuntimeError with our specific message
    logger.error(f"SEC database verification failed: {e}")
    raise
  except Exception as e:
    logger.error(f"Unexpected error during SEC database verification: {e}")
    raise RuntimeError(f"Failed to verify SEC database: {e}")

  # Extract year from prefix if not provided
  if not year and "year=" in prefix:
    # Extract year from prefix like "processed/year=2025/"
    import re

    match = re.search(r"year=(\d{4})", prefix)
    if match:
      year = int(match.group(1))

  # Default to current year if still not set
  if not year:
    year = datetime.now().year

  # Default bucket if not provided
  if not bucket:
    bucket = env.SEC_PROCESSED_BUCKET

  # Initialize tracking
  self.update_state(
    state="PROGRESS",
    meta={
      "step": "initializing",
      "progress_percent": 0,
      "pipeline_run_id": pipeline_run_id,
      "year": year,
    },
  )

  logger.info(f"Starting SEC data ingestion v2 for year {year} from {bucket}")
  logger.info(f"Batch mode: {batch_mode}, Incremental: {incremental}")

  # Build S3 base path
  if use_consolidated:
    # Use consolidated files WITH year partitioning for better memory control
    s3_base_path = f"s3://{bucket}/consolidated/year={year}/"
    logger.info(f"Using consolidated files from: {s3_base_path} (year {year})")
  elif batch_mode:
    s3_base_path = f"s3://{bucket}/processed/year={year}/"
  else:
    # Legacy path structure (still points to processed for direct ingestion)
    s3_base_path = f"s3://{bucket}/processed/year={year}/"

  # Configure S3 credentials - Prefer IAM roles over access keys
  s3_config = {
    "region": env.AWS_REGION,
  }

  # Use IAM roles in production/staging, fallback to access keys in development
  if env.ENVIRONMENT in ["prod", "staging"]:
    # Use IAM role automatically - boto3 will detect ECS task role
    logger.debug("Using IAM role for S3 access in SEC ingestion")
  elif env.AWS_S3_ACCESS_KEY_ID:
    # Development fallback: use access keys if provided
    logger.debug("Using access keys for S3 access in SEC ingestion")
    s3_config["aws_access_key_id"] = env.AWS_S3_ACCESS_KEY_ID
    if env.AWS_S3_SECRET_ACCESS_KEY:
      s3_config["aws_secret_access_key"] = env.AWS_S3_SECRET_ACCESS_KEY

  # For LocalStack/MinIO testing
  if env.AWS_ENDPOINT_URL:
    s3_config["endpoint_url"] = env.AWS_ENDPOINT_URL

  # Track what we've loaded
  nodes_loaded = []
  relationships_loaded = []
  total_records = 0
  total_files = 0

  # Calculate total operations for progress tracking
  total_operations = len(NODE_TYPES) + len(RELATIONSHIP_TYPES)
  operations_completed = 0

  try:
    # Phase 1: Load all node types
    logger.info("=" * 60)
    logger.info("PHASE 1: Loading NODE types")
    logger.info("=" * 60)

    for node_type in NODE_TYPES:
      operations_completed += 1
      progress_percent = int((operations_completed / total_operations) * 90)

      self.update_state(
        state="PROGRESS",
        meta={
          "step": "loading_nodes",
          "progress_percent": progress_percent,
          "current_operation": f"Loading {node_type} nodes",
          "nodes_loaded": len(nodes_loaded),
          "relationships_loaded": 0,
        },
      )

      # Build glob pattern for structure
      if use_consolidated:
        # Consolidated structure: consolidated/year={year}/nodes/{NodeType}/batch_*.parquet
        glob_pattern = f"{s3_base_path}nodes/{node_type}/batch_*.parquet"
      elif batch_mode:
        # New structure: processed/year={year}/nodes/{NodeType}/*.parquet
        glob_pattern = f"{s3_base_path}nodes/{node_type}/*.parquet"

        # Note: incremental loading would need to track which files have been loaded
        # This could be done via a manifest or by querying the database for existing data
        if incremental and timestamp_after:
          logger.warning(
            f"Incremental loading not supported with new structure for {node_type}"
          )
      else:
        # Legacy: glob across all company/filing directories
        glob_pattern = f"{s3_base_path}**/{node_type}.parquet"

      # ALWAYS use IGNORE_ERRORS for SEC pipeline to prevent single bad row from failing entire batch
      # SEC data can have inconsistencies and missing references that shouldn't stop ingestion
      ignore_errors = True

      logger.info(f"Loading {node_type} from: {glob_pattern}")
      logger.info("  Using IGNORE_ERRORS=true to handle data inconsistencies")

      try:
        result = _bulk_load_node_type_sync(
          db_name=db_name,
          node_type=node_type,
          s3_pattern=glob_pattern,
          s3_config=s3_config,
          ignore_errors=ignore_errors,
        )

        nodes_loaded.append(
          {
            "type": node_type,
            "records": result.get("records_loaded") or 0,
            "files": result.get("files_processed") or 0,
            "duration_ms": result.get("duration_ms") or 0,
            "warnings": result.get("warning_count", 0),
          }
        )

        # Log warnings if present (now always using ignore_errors, so they're handled gracefully)
        if result.get("warning_count", 0) > 0:
          logger.debug(
            f"Handled {result.get('warning_count')} data inconsistencies for {node_type}"
          )

        total_records += result.get("records_loaded") or 0
        total_files += result.get("files_processed") or 0

        records = result.get("records_loaded")
        if records is None:
          # Wildcard loading doesn't return counts
          logger.info(f"✅ Loaded {node_type} in {result.get('duration_ms') or 0}ms")
        elif records > 0:
          logger.info(
            f"✅ Loaded {node_type}: {records} records in {result.get('duration_ms') or 0}ms"
          )
        else:
          # With IGNORE_ERRORS or when table already populated, count is 0
          logger.debug(
            f"Loaded {node_type} in {result.get('duration_ms') or 0}ms (no new records)"
          )

      except Exception as e:
        logger.error(f"Failed to load {node_type}: {e}")
        nodes_loaded.append(
          {
            "type": node_type,
            "error": str(e),
          }
        )

    # Phase 2: Load all relationship types
    logger.info("=" * 60)
    logger.info("PHASE 2: Loading RELATIONSHIP types")
    logger.info("=" * 60)

    for rel_type in RELATIONSHIP_TYPES:
      operations_completed += 1
      progress_percent = int((operations_completed / total_operations) * 90)

      self.update_state(
        state="PROGRESS",
        meta={
          "step": "loading_relationships",
          "progress_percent": progress_percent,
          "current_operation": f"Loading {rel_type} relationships",
          "nodes_loaded": len(nodes_loaded),
          "relationships_loaded": len(relationships_loaded),
        },
      )

      # Build glob pattern for structure
      if use_consolidated:
        # Consolidated structure: consolidated/year={year}/relationships/{RelType}/batch_*.parquet
        glob_pattern = f"{s3_base_path}relationships/{rel_type}/batch_*.parquet"
      elif batch_mode:
        # New structure: processed/year={year}/relationships/{RelType}/*.parquet
        glob_pattern = f"{s3_base_path}relationships/{rel_type}/*.parquet"

        # Note: incremental loading would need to track which files have been loaded
        # This could be done via a manifest or by querying the database for existing data
        if incremental and timestamp_after:
          logger.warning(
            f"Incremental loading not supported with new structure for {rel_type}"
          )
      else:
        # Legacy: glob across all company/filing directories
        glob_pattern = f"{s3_base_path}**/{rel_type}.parquet"

      # ALWAYS use IGNORE_ERRORS for SEC pipeline to prevent single bad row from failing entire batch
      # SEC data can have inconsistencies and missing references that shouldn't stop ingestion
      ignore_errors = True

      logger.info(f"Loading {rel_type} from: {glob_pattern}")
      logger.info("  Using IGNORE_ERRORS=true to handle data inconsistencies")

      try:
        result = _bulk_load_relationship_type_sync(
          db_name=db_name,
          rel_type=rel_type,
          s3_pattern=glob_pattern,
          s3_config=s3_config,
          ignore_errors=ignore_errors,  # Use based on whether it's a base schema relationship
        )

        relationships_loaded.append(
          {
            "type": rel_type,
            "records": result.get("records_loaded") or 0,
            "files": result.get("files_processed") or 0,
            "duration_ms": result.get("duration_ms") or 0,
            "warnings": result.get("warning_count", 0),
          }
        )

        # Log warnings if present (now always using ignore_errors, so they're handled gracefully)
        if result.get("warning_count", 0) > 0:
          logger.debug(
            f"Handled {result.get('warning_count')} data inconsistencies for {rel_type}"
          )

        total_records += result.get("records_loaded") or 0
        total_files += result.get("files_processed") or 0

        records = result.get("records_loaded")
        if records is None:
          # Wildcard loading doesn't return counts
          logger.info(f"✅ Loaded {rel_type} in {result.get('duration_ms') or 0}ms")
        elif records > 0:
          logger.info(
            f"✅ Loaded {rel_type}: {records} records in {result.get('duration_ms') or 0}ms"
          )
        else:
          # With IGNORE_ERRORS or when table already populated, count is 0
          logger.debug(
            f"Loaded {rel_type} in {result.get('duration_ms') or 0}ms (no new records)"
          )

      except Exception as e:
        logger.error(f"Failed to load {rel_type}: {e}")
        relationships_loaded.append(
          {
            "type": rel_type,
            "error": str(e),
          }
        )

    # Phase 3: Final validation
    logger.info("=" * 60)
    logger.info("PHASE 3: Validation and Summary")
    logger.info("=" * 60)

    duration_seconds = (datetime.now() - start_time).total_seconds()

    # Count successes and failures
    nodes_success = sum(1 for n in nodes_loaded if "error" not in n)
    nodes_failed = len(nodes_loaded) - nodes_success
    rels_success = sum(1 for r in relationships_loaded if "error" not in r)
    rels_failed = len(relationships_loaded) - rels_success

    logger.info(f"Ingestion complete in {duration_seconds:.2f} seconds")
    logger.info(f"  Nodes: {nodes_success} succeeded, {nodes_failed} failed")
    logger.info(f"  Relationships: {rels_success} succeeded, {rels_failed} failed")
    # Note: Wildcard S3 loading doesn't provide record counts
    if total_records > 0:
      logger.info(f"  Total records: {total_records:,}")
    if total_files > 0:
      logger.info(f"  Total files: {total_files:,}")

    # Calculate warning summary
    total_warnings = sum(n.get("warnings", 0) for n in nodes_loaded)
    total_warnings += sum(r.get("warnings", 0) for r in relationships_loaded)

    # Check for critical data rejection issues
    critical_nodes = [
      n
      for n in nodes_loaded
      if n.get("warnings", 0) > 0 and n["type"] not in GLOBAL_NODE_TYPES
    ]
    critical_rels = [
      r
      for r in relationships_loaded
      if r.get("warnings", 0) > 0
      and r["type"] not in ["ELEMENT_HAS_PERIOD", "ELEMENT_HAS_UNIT"]
    ]

    if critical_nodes or critical_rels:
      logger.error("=" * 80)
      logger.error("🔴 CRITICAL DATA REJECTION DETECTED!")
      logger.error("The following non-global types had warnings (data rejected):")
      for node in critical_nodes:
        logger.error(f"  - {node['type']}: {node['warnings']} warnings")
      for rel in critical_rels:
        logger.error(f"  - {rel['type']}: {rel['warnings']} warnings")
      logger.error("Check parquet file data types match Kuzu schema!")
      logger.error("=" * 80)

    # Mark as complete
    self.update_state(
      state="SUCCESS",
      meta={
        "step": "completed",
        "progress_percent": 100,
        "duration_seconds": duration_seconds,
        "nodes_loaded": nodes_loaded,
        "relationships_loaded": relationships_loaded,
        "total_records": total_records,
        "total_files": total_files,
        "total_warnings": total_warnings,
      },
    )

    return {
      "status": "completed" if total_warnings == 0 else "completed_with_warnings",
      "pipeline_run_id": pipeline_run_id,
      "year": year,
      "batch_mode": batch_mode,
      "incremental": incremental,
      "duration_seconds": duration_seconds,
      "nodes": {
        "types_loaded": nodes_success,
        "types_failed": nodes_failed,
        "details": nodes_loaded,
      },
      "relationships": {
        "types_loaded": rels_success,
        "types_failed": rels_failed,
        "details": relationships_loaded,
      },
      "totals": {
        "records": total_records,
        "files": total_files,
        "warnings": total_warnings,
      },
      "critical_issues": {
        "nodes_with_rejected_data": [n["type"] for n in critical_nodes],
        "relationships_with_rejected_data": [r["type"] for r in critical_rels],
      }
      if critical_nodes or critical_rels
      else None,
    }

  except Exception as e:
    logger.error(f"Ingestion failed: {e}")
    self.update_state(
      state="FAILURE",
      meta={
        "step": "failed",
        "error": str(e),
        "nodes_loaded": nodes_loaded,
        "relationships_loaded": relationships_loaded,
      },
    )
    raise


async def _bulk_load_node_type(
  db_name: str,
  node_type: str,
  s3_pattern: str,
  s3_config: Dict[str, Any],
  ignore_errors: bool,
) -> Dict[str, Any]:
  """Bulk load node type using S3 glob pattern via SYNC mode with direct S3 COPY."""
  from robosystems.kuzu_api.client.factory import KuzuClientFactory
  import boto3
  from urllib.parse import urlparse

  start_time = datetime.now()

  # Get client for Kuzu operations
  client = await KuzuClientFactory.create_client(
    graph_id=db_name, operation_type="write"
  )

  try:
    # First, discover files that match the pattern for better logging
    parsed = urlparse(s3_pattern)
    bucket_name = parsed.netloc or parsed.path.split("/")[2]
    path_pattern = (
      "/".join(parsed.path.split("/")[3:])
      if parsed.netloc
      else "/".join(parsed.path.split("/")[3:])
    )

    # Create S3 client for discovery
    s3_discovery = boto3.client(
      "s3",
      endpoint_url=s3_config.get("endpoint_url"),
      aws_access_key_id=s3_config.get("aws_access_key_id"),
      aws_secret_access_key=s3_config.get("aws_secret_access_key"),
      region_name=s3_config.get("region", "us-east-1"),
    )

    # List files matching the pattern (simplified glob matching)
    prefix = path_pattern.replace("*.parquet", "").replace("batch_*.parquet", "")
    matching_files = []
    total_size = 0

    try:
      paginator = s3_discovery.get_paginator("list_objects_v2")
      pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

      for page in pages:
        if "Contents" in page:
          for obj in page["Contents"]:
            if obj["Key"].endswith(".parquet"):
              matching_files.append(obj["Key"])
              total_size += obj.get("Size", 0)
    except Exception as e:
      logger.debug(f"Could not pre-discover S3 files: {e}")
      # Continue anyway, the COPY command will handle it

    # Estimate records based on file count and consolidation patterns
    # We know consolidated files use PARQUET_ROW_GROUP_SIZE = 50,000 rows
    estimated_records = 0
    if len(matching_files) == 1:
      # Single file means it had fewer than 50k rows (wasn't worth batching)
      estimated_records = 50000  # Upper bound for single file
      logger.info(
        f"📊 Loading {node_type}: Found 1 file ({total_size / (1024 * 1024):.1f}MB) from {s3_pattern}"
      )
      logger.info("  Estimated records: <50,000 (single file, not batched)")
    elif len(matching_files) > 0:
      # Multiple files or consolidated batches
      # For consolidated files, estimate based on typical compression
      # Our XBRL data typically compresses to ~10-20 bytes per record in parquet
      if "consolidated" in s3_pattern:
        # Consolidated files are efficiently packed
        avg_bytes_per_record = 12  # Better compression in larger files
      else:
        # Raw files from processing phase
        avg_bytes_per_record = 20  # Less efficient smaller files

      estimated_records = int(total_size / avg_bytes_per_record)

      logger.info(
        f"📊 Loading {node_type}: Found {len(matching_files)} files "
        f"({total_size / (1024 * 1024):.1f}MB) from {s3_pattern}"
      )
      logger.info(
        f"  Estimated records: ~{estimated_records:,} (based on typical compression ratio)"
      )
    else:
      logger.info(f"📊 Loading {node_type}: No files found matching {s3_pattern}")

    logger.info(f"Starting S3 bulk load for {node_type} from pattern: {s3_pattern}")
    logger.info("Using SSE-based monitoring for multi-hour ingestion support")

    # Use the new SSE-based ingestion for long-running operations
    # Only include credentials that are actually set (filter out None values)
    s3_credentials = {}
    if s3_config.get("aws_access_key_id"):
      s3_credentials["aws_access_key_id"] = s3_config["aws_access_key_id"]
    if s3_config.get("aws_secret_access_key"):
      s3_credentials["aws_secret_access_key"] = s3_config["aws_secret_access_key"]
    if s3_config.get("endpoint_url"):
      s3_credentials["endpoint_url"] = s3_config["endpoint_url"]
    if s3_config.get("region"):
      s3_credentials["region"] = s3_config["region"]

    # Call the SSE-enabled ingestion with up to 4-hour timeout
    response = await client.ingest_with_sse(
      graph_id=db_name,
      table_name=node_type,
      s3_pattern=s3_pattern,
      s3_credentials=s3_credentials,
      ignore_errors=ignore_errors,
      timeout=14400,  # 4 hours for very large ingestions
    )

    # Parse the SSE response
    result = response  # SSE client returns a dict directly

    duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

    # Extract metrics from SSE response
    if isinstance(result, dict):
      # SSE response has different structure
      if result.get("status") == "completed":
        records_loaded = result.get("records_loaded", 0)
        # Try to get from nested result if not at top level
        if records_loaded == 0 and "result" in result:
          records_loaded = result["result"].get("records_loaded", 0)
        files_processed = len(matching_files)  # We already counted these
      else:
        # Ingestion failed
        error_msg = result.get("error", "Unknown error")
        logger.error(f"❌ SSE ingestion failed for {node_type}: {error_msg}")
        records_loaded = 0
        files_processed = 0

      # Check for warnings in the response metadata
      warnings = []
      if "metadata" in result and isinstance(result["metadata"], dict):
        warnings = result["metadata"].get("warnings", [])

      # Log warnings if present
      if warnings:
        logger.error(
          f"⚠️ S3 bulk load for {node_type} completed with {len(warnings)} WARNINGS! "
          f"Data may be rejected due to type mismatches or schema violations."
        )
        # Log first few warnings for immediate visibility
        for i, warning in enumerate(warnings[:3]):
          logger.error(
            f"  Warning {i + 1}: {warning.get('message', 'Unknown error')} "
            f"(File: {warning.get('file_path', 'unknown')})"
          )

        # This is critical - if we have warnings and not using IGNORE_ERRORS,
        # it means data is being rejected
        if not ignore_errors:
          logger.error(
            f"🔴 CRITICAL: {node_type} data is being REJECTED! "
            f"Check parquet file types match Kuzu schema."
          )
      else:
        # Enhanced success logging with throughput metrics
        if records_loaded > 0:
          throughput = records_loaded / (duration_ms / 1000) if duration_ms > 0 else 0
          logger.info(
            f"✅ Loaded {node_type}: {records_loaded:,} records in {duration_ms}ms "
            f"({throughput:.0f} records/sec)"
          )
        else:
          # Log at debug level when no records are loaded (might be already populated)
          logger.debug(
            f"S3 bulk load completed for {node_type} in {duration_ms}ms with no new records"
          )

        # Add file processing stats if available
        if len(matching_files) > 0:
          logger.info(
            f"  Processed {len(matching_files)} files ({total_size / (1024 * 1024):.1f}MB total)"
          )

      return {
        "records_loaded": records_loaded,
        "files_processed": files_processed or len(matching_files),
        "duration_ms": duration_ms,
        "total_size_mb": round(total_size / (1024 * 1024), 2)
        if total_size > 0
        else None,
        "warnings": warnings,
        "warning_count": len(warnings),
      }
    else:
      logger.warning(f"Unexpected response type: {type(result)}")
      return {
        "records_loaded": 0,
        "files_processed": 0,
        "duration_ms": duration_ms,
      }

  finally:
    # CRITICAL: Ensure connection is properly closed to avoid locking
    await client.close()
    logger.debug(f"Closed connection for {node_type} ingestion")


async def _bulk_load_relationship_type(
  db_name: str,
  rel_type: str,
  s3_pattern: str,
  s3_config: Dict[str, Any],
  ignore_errors: bool,
) -> Dict[str, Any]:
  """Bulk load relationship type using S3 glob pattern via SYNC mode with direct S3 COPY."""
  from robosystems.kuzu_api.client.factory import KuzuClientFactory
  import boto3
  from urllib.parse import urlparse

  start_time = datetime.now()

  # Get client for Kuzu operations
  client = await KuzuClientFactory.create_client(
    graph_id=db_name, operation_type="write"
  )

  try:
    # First, discover files that match the pattern for better logging
    parsed = urlparse(s3_pattern)
    bucket_name = parsed.netloc or parsed.path.split("/")[2]
    path_pattern = (
      "/".join(parsed.path.split("/")[3:])
      if parsed.netloc
      else "/".join(parsed.path.split("/")[3:])
    )

    # Create S3 client for discovery
    s3_discovery = boto3.client(
      "s3",
      endpoint_url=s3_config.get("endpoint_url"),
      aws_access_key_id=s3_config.get("aws_access_key_id"),
      aws_secret_access_key=s3_config.get("aws_secret_access_key"),
      region_name=s3_config.get("region", "us-east-1"),
    )

    # List files matching the pattern
    prefix = path_pattern.replace("*.parquet", "").replace("batch_*.parquet", "")
    matching_files = []
    total_size = 0

    try:
      paginator = s3_discovery.get_paginator("list_objects_v2")
      pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

      for page in pages:
        if "Contents" in page:
          for obj in page["Contents"]:
            if obj["Key"].endswith(".parquet"):
              matching_files.append(obj["Key"])
              total_size += obj.get("Size", 0)
    except Exception as e:
      logger.debug(f"Could not pre-discover S3 files: {e}")
      # Continue anyway, the COPY command will handle it

    # Estimate records for relationships
    estimated_records = 0
    if len(matching_files) == 1:
      # Single file means it had fewer than 50k rows
      estimated_records = 50000  # Upper bound
      logger.info(
        f"📊 Loading {rel_type}: Found 1 file ({total_size / (1024 * 1024):.1f}MB) from {s3_pattern}"
      )
      logger.info("  Estimated records: <50,000 (single file, not batched)")
    elif len(matching_files) > 0:
      # Multiple files or consolidated batches
      # Relationships typically have smaller records than nodes
      if "consolidated" in s3_pattern:
        avg_bytes_per_record = 8  # Relationships are compact
      else:
        avg_bytes_per_record = 15  # Less efficient in smaller files

      estimated_records = int(total_size / avg_bytes_per_record)

      logger.info(
        f"📊 Loading {rel_type}: Found {len(matching_files)} files "
        f"({total_size / (1024 * 1024):.1f}MB) from {s3_pattern}"
      )
      logger.info(
        f"  Estimated records: ~{estimated_records:,} (based on typical compression ratio)"
      )
    else:
      logger.info(f"📊 Loading {rel_type}: No files found matching {s3_pattern}")

    logger.info(f"Starting S3 bulk load for {rel_type} from pattern: {s3_pattern}")
    logger.info("Using SSE-based monitoring for multi-hour ingestion support")

    # Use the new SSE-based ingestion for long-running operations
    # Only include credentials that are actually set (filter out None values)
    s3_credentials = {}
    if s3_config.get("aws_access_key_id"):
      s3_credentials["aws_access_key_id"] = s3_config["aws_access_key_id"]
    if s3_config.get("aws_secret_access_key"):
      s3_credentials["aws_secret_access_key"] = s3_config["aws_secret_access_key"]
    if s3_config.get("endpoint_url"):
      s3_credentials["endpoint_url"] = s3_config["endpoint_url"]
    if s3_config.get("region"):
      s3_credentials["region"] = s3_config["region"]

    # Call the SSE-enabled ingestion with up to 4-hour timeout
    response = await client.ingest_with_sse(
      graph_id=db_name,
      table_name=rel_type,
      s3_pattern=s3_pattern,
      s3_credentials=s3_credentials,
      ignore_errors=ignore_errors,
      timeout=14400,  # 4 hours for very large ingestions
    )

    # Parse the SSE response
    result = response  # SSE client returns a dict directly

    duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)

    # Extract metrics from SSE response
    if isinstance(result, dict):
      # SSE response has different structure
      if result.get("status") == "completed":
        records_loaded = result.get("records_loaded", 0)
        # Try to get from nested result if not at top level
        if records_loaded == 0 and "result" in result:
          records_loaded = result["result"].get("records_loaded", 0)
        files_processed = len(matching_files)  # We already counted these
      else:
        # Ingestion failed
        error_msg = result.get("error", "Unknown error")
        logger.error(f"❌ SSE ingestion failed for {rel_type}: {error_msg}")
        records_loaded = 0
        files_processed = 0

      # Check for warnings in the response metadata
      warnings = []
      if "metadata" in result and isinstance(result["metadata"], dict):
        warnings = result["metadata"].get("warnings", [])

      # Log warnings if present
      if warnings:
        logger.error(
          f"⚠️ S3 bulk load for {rel_type} completed with {len(warnings)} WARNINGS! "
          f"Data may be rejected due to type mismatches or schema violations."
        )
        # Log first few warnings for immediate visibility
        for i, warning in enumerate(warnings[:3]):
          logger.error(
            f"  Warning {i + 1}: {warning.get('message', 'Unknown error')} "
            f"(File: {warning.get('file_path', 'unknown')})"
          )

        # This is critical - if we have warnings and not using IGNORE_ERRORS,
        # it means data is being rejected
        if not ignore_errors:
          logger.error(
            f"🔴 CRITICAL: {rel_type} data is being REJECTED! "
            f"Check parquet file types match Kuzu schema."
          )
      else:
        # Enhanced success logging with throughput metrics
        if records_loaded > 0:
          throughput = records_loaded / (duration_ms / 1000) if duration_ms > 0 else 0
          logger.info(
            f"✅ Loaded {rel_type}: {records_loaded:,} records in {duration_ms}ms "
            f"({throughput:.0f} records/sec)"
          )
        else:
          # Log at debug level when no records are loaded (might be already populated)
          logger.debug(
            f"S3 bulk load completed for {rel_type} in {duration_ms}ms with no new records"
          )

        # Add file processing stats if available
        if len(matching_files) > 0:
          logger.info(
            f"  Processed {len(matching_files)} files ({total_size / (1024 * 1024):.1f}MB total)"
          )

      return {
        "records_loaded": records_loaded,
        "files_processed": files_processed or len(matching_files),
        "duration_ms": duration_ms,
        "total_size_mb": round(total_size / (1024 * 1024), 2)
        if total_size > 0
        else None,
        "warnings": warnings,
        "warning_count": len(warnings),
      }
    else:
      logger.warning(f"Unexpected response type: {type(result)}")
      return {
        "records_loaded": 0,
        "files_processed": 0,
        "duration_ms": duration_ms,
      }

  finally:
    # CRITICAL: Ensure connection is properly closed to avoid locking
    await client.close()
    logger.debug(f"Closed connection for {rel_type} ingestion")


# Make the async functions work with Celery
def _bulk_load_node_type_sync(*args, **kwargs):
  """Synchronous wrapper for async function."""
  return asyncio.run(_bulk_load_node_type(*args, **kwargs))


def _bulk_load_relationship_type_sync(*args, **kwargs):
  """Synchronous wrapper for async function."""
  return asyncio.run(_bulk_load_relationship_type(*args, **kwargs))
