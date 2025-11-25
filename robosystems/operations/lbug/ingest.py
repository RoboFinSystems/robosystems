"""
Schema-Driven LadybugDB Database Ingestion Operations

This module provides core operations for ingesting processed SEC data into graph databases.
All mapping logic is now schema-driven - no hardcoded arrays.
These are reusable pipeline operations, not one-off scripts.
"""

import os
import tempfile
import re
from pathlib import Path
from typing import Optional, List, Tuple, Dict

from ...logger import logger
from ...security import SecurityAuditLogger
from ...processors.xbrl.schema_config_generator import (
  XBRLSchemaConfigGenerator,
  create_roboledger_ingestion_processor,
  IngestTableInfo,
)
from ...config import env


# Cache schema adapters to avoid recompilation
_schema_adapter_cache: Dict[str, XBRLSchemaConfigGenerator] = {}


def _get_cached_schema_adapter(
  schema_config: Optional[dict] = None,
) -> XBRLSchemaConfigGenerator:
  """
  Get a cached schema adapter to avoid recompilation.

  Args:
      schema_config: Schema configuration dict

  Returns:
      Cached or new schema adapter
  """
  # Create a cache key from the schema config
  if schema_config:
    cache_key = f"{schema_config.get('name', 'custom')}_{schema_config.get('base_schema', 'base')}_{'_'.join(schema_config.get('extensions', []))}"
  else:
    cache_key = "roboledger_default"

  # Check cache
  if cache_key in _schema_adapter_cache:
    logger.debug(f"Using cached schema adapter: {cache_key}")
    return _schema_adapter_cache[cache_key]

  # Create new adapter
  logger.info(f"Creating new schema adapter: {cache_key}")
  if schema_config:
    adapter = XBRLSchemaConfigGenerator(schema_config)
  else:
    adapter = create_roboledger_ingestion_processor()

  # Cache it
  _schema_adapter_cache[cache_key] = adapter

  return adapter


def ingest_from_s3(
  bucket: str,
  db_name: str,
  s3_prefix: str = "processed/",
  schema_config: Optional[dict] = None,
) -> bool:
  """
  Ingest processed parquet files from S3 into graph database.

  Args:
      bucket: S3 bucket containing processed files
      db_name: Name of the target graph database
      s3_prefix: S3 prefix for processed files
      schema_config: Schema configuration dict (defaults to base + roboledger)

  Returns:
      bool: True if ingestion successful, False otherwise
  """
  try:
    import boto3

    logger.info(
      f"Starting schema-driven LadybugDB ingestion from S3: {bucket}/{s3_prefix} -> {db_name}"
    )

    # Setup S3 client with S3-specific credentials
    s3_config = env.get_s3_config()
    endpoint_url = s3_config.get("endpoint_url")

    if endpoint_url:
      # LocalStack test environment
      s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
      )
    else:
      # Production with S3-specific credentials
      s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_config.get("aws_access_key_id"),
        aws_secret_access_key=s3_config.get("aws_secret_access_key"),
        region_name=s3_config.get("region_name"),
      )

    # Download processed files to temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
      # List all processed parquet files
      response = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

      if "Contents" not in response:
        logger.warning(f"No processed files found in {bucket}/{s3_prefix}")
        return True

      downloaded_files = []
      for obj in response["Contents"]:
        if obj["Key"].endswith(".parquet"):
          # Extract just the filename, not the full path
          # This preserves the original file name for proper table mapping
          filename = os.path.basename(obj["Key"])
          local_path = Path(temp_dir) / filename

          s3_client.download_file(bucket, obj["Key"], str(local_path))
          downloaded_files.append(str(local_path))

      logger.info(f"Downloaded {len(downloaded_files)} parquet files")

      if not downloaded_files:
        logger.warning("No parquet files to ingest")
        return True

      # Perform local ingestion with schema config
      return ingest_from_local_files(downloaded_files, db_name, schema_config)

  except Exception as e:
    logger.error(f"S3 ingestion failed: {e}")
    return False


def ingest_from_local_files(
  file_paths: List[str], db_name: str, schema_config: Optional[dict] = None
) -> bool:
  """
  Ingest parquet files from local filesystem into graph database.

  Uses schema-driven ingestion logic - no hardcoded mappings.

  Args:
      file_paths: List of local parquet file paths
      db_name: Name of the target graph database
      schema_config: Schema configuration dict (defaults to base + roboledger)

  Returns:
      bool: True if ingestion successful, False otherwise
  """
  try:
    from robosystems.graph_api.core.ladybug import Engine
    from .schema_setup import ensure_schema

    logger.info(f"Starting LadybugDB ingestion: {len(file_paths)} files -> {db_name}")

    # Initialize LadybugDB engine
    from .path_utils import (
      get_lbug_database_path,
      ensure_lbug_directory,
    )

    # Get the correct database path using the utility
    db_path = get_lbug_database_path(db_name)

    # Ensure database directory exists
    ensure_lbug_directory(db_path)

    # Ensure schema exists (this will only create it if needed)
    logger.info("Checking if schema initialization is needed")
    schema_created = ensure_schema(db_name, schema_config)
    if schema_created:
      logger.info("Schema was created for the first time")
    else:
      logger.info("Schema already exists, skipping creation")

    logger.info(f"Opening graph database at: {db_path}")
    engine = Engine(str(db_path))

    # Get cached schema adapter for file pattern matching
    # This avoids recompiling the schema on every ingestion
    schema_adapter = _get_cached_schema_adapter(schema_config)

    # Categorize files using schema-driven logic
    node_files, relationship_files = _categorize_files_schema_driven(
      file_paths, schema_adapter
    )

    logger.info(
      f"File categorization: {len(node_files)} nodes, {len(relationship_files)} relationships"
    )

    # Process nodes first
    ingested_count = 0

    for file_path in node_files:
      table_info = _parse_filename_schema_driven(file_path, schema_adapter)
      if table_info and _ingest_node_schema_driven(
        engine, file_path, table_info, schema_adapter
      ):
        ingested_count += 1

    # Process relationships second
    for file_path in relationship_files:
      table_info = _parse_filename_schema_driven(file_path, schema_adapter)
      if table_info and _ingest_relationship_schema_driven(
        engine, file_path, table_info, schema_adapter
      ):
        ingested_count += 1

    logger.info(
      f"Schema-driven ingestion completed: {ingested_count}/{len(file_paths)} files"
    )
    return ingested_count > 0

  except Exception as e:
    logger.error(f"Local ingestion failed: {e}")
    return False


def _categorize_files_schema_driven(
  file_paths: List[str], schema_adapter: XBRLSchemaConfigGenerator
) -> Tuple[List[str], List[str]]:
  """
  Categorize files into nodes and relationships using schema-driven logic.
  No hardcoded arrays - everything derived from schema.

  Returns:
      Tuple of (node_files, relationship_files)
  """
  node_files = []
  relationship_files = []

  for file_path in file_paths:
    # Use full path for schema-driven detection to leverage directory structure
    # Pass the full path, not just the filename
    if schema_adapter.is_relationship_file(file_path):
      relationship_files.append(file_path)
    else:
      node_files.append(file_path)

  return node_files, relationship_files


def _parse_filename_schema_driven(
  file_path: str, schema_adapter: XBRLSchemaConfigGenerator
) -> Optional[dict]:
  """
  Parse filename using schema-driven logic to extract table information.
  No hardcoded mappings - everything derived from schema.

  Returns:
      Dict with table_name, is_relationship, etc.
  """
  # Pass the full path to preserve directory context for table mapping
  # The directory name often contains the table name
  table_name = schema_adapter.get_table_name_from_file(file_path)
  if not table_name:
    logger.warning(f"No table mapping found for file: {file_path}")
    return None

  # Get table info from schema
  table_info = schema_adapter.get_table_info(table_name)
  if not table_info:
    logger.warning(f"No table info found for: {table_name}")
    return None

  table_type = "relationship" if table_info.is_relationship else "node"
  logger.debug(f"File {file_path} -> Table {table_name} (type: {table_type})")

  return {
    "table_name": table_name,
    "is_relationship": table_info.is_relationship,
    "table_info": table_info,
    "file_path": file_path,
  }


def _ingest_node_schema_driven(
  engine, file_path: str, table_info: dict, schema_adapter: XBRLSchemaConfigGenerator
) -> bool:
  """
  Schema-driven node ingestion - create table from schema, then copy with appropriate settings.
  """
  table_name = table_info["table_name"]  # Extract early for exception handling
  try:
    schema_table_info = table_info["table_info"]

    # Get IngestTableInfo directly from schema table info
    ingest_info = schema_table_info

    # Create table using schema definition
    if not _create_table_from_schema(engine, table_name, ingest_info, file_path):
      return False

    # Always use COPY with IGNORE_ERRORS for global entities, standard COPY for others
    # This is much faster than UPSERT operations
    return _copy_node_data_schema_driven(engine, file_path, table_name, ingest_info)

  except Exception as e:
    logger.error(f"Schema-driven node ingestion failed for {table_name}: {e}")
    return False


def _ingest_relationship_schema_driven(
  engine, file_path: str, table_info: dict, schema_adapter: XBRLSchemaConfigGenerator
) -> bool:
  """
  Schema-driven relationship ingestion - create rel table from schema, then COPY with IGNORE_ERRORS.
  """
  table_name = table_info["table_name"]  # Extract early for exception handling
  try:
    schema_table_info = table_info["table_info"]

    # Get IngestTableInfo directly from schema table info
    ingest_info = schema_table_info

    # Create relationship table using schema definition
    if not _create_relationship_table_from_schema(
      engine, table_name, ingest_info, file_path
    ):
      return False

    # Use COPY for relationships (all use report-specific identifiers)
    return _copy_relationship_data_schema_driven(
      engine, file_path, table_name, ingest_info, schema_adapter
    )

  except Exception as e:
    logger.error(f"Schema-driven relationship ingestion failed for {table_name}: {e}")
    return False


def _create_table_from_schema(
  engine, table_name: str, ingest_info: IngestTableInfo, file_path: str
) -> bool:
  """Create node table using schema definition with parquet data validation."""
  try:
    import pyarrow.parquet as pq

    # Read just the parquet schema without loading data
    parquet_file = pq.ParquetFile(file_path)
    arrow_schema = parquet_file.schema_arrow

    # Debug: Log what we found in the parquet file
    parquet_columns = set(field.name for field in arrow_schema)
    logger.debug(
      f"Parquet file {file_path} has {len(parquet_columns)} columns: {sorted(parquet_columns)}"
    )

    # Build column definitions from schema
    columns = []

    # Get primary keys from schema
    primary_keys = ingest_info.primary_keys
    if not primary_keys:
      # For relationship tables, we should not create them as node tables
      # Check if this is actually a relationship being incorrectly treated as a node
      if ingest_info.is_relationship:
        logger.error(
          f"Relationship {table_name} being treated as node table - skipping"
        )
        return False
      # Fallback to first column if no primary keys defined
      first_col = arrow_schema[0].name
      logger.warning(
        f"No primary keys defined in schema for {table_name}, falling back to first column: {first_col}"
      )
      primary_keys = [first_col]
    else:
      logger.debug(
        f"Using schema-defined primary key for {table_name}: {primary_keys[0]}"
      )

    # Add columns from schema definition (prefer schema over parquet)
    schema_columns = set(ingest_info.columns)
    logger.debug(
      f"Schema for {table_name} expects {len(schema_columns)} columns: {sorted(schema_columns)}"
    )

    # Use intersection of schema and parquet columns
    available_columns = schema_columns.intersection(parquet_columns)
    logger.debug(
      f"Intersection for {table_name}: {len(available_columns)} columns: {sorted(available_columns)}"
    )

    if not available_columns:
      logger.warning(f"No matching columns between schema and parquet for {table_name}")
      logger.warning(f"  Schema columns: {sorted(schema_columns)}")
      logger.warning(f"  Parquet columns: {sorted(parquet_columns)}")
      return False

    # Build column definitions
    for field in arrow_schema:
      if field.name in available_columns:
        lbug_type = _map_arrow_to_lbug_type(str(field.type))
        # Handle SQL reserved words by quoting them
        column_name = field.name
        if column_name.lower() in [
          "order",
          "group",
          "select",
          "from",
          "where",
          "having",
        ]:
          column_name = f"`{column_name}`"

        columns.append(f"{column_name} {lbug_type}")

    if not columns:
      logger.error(f"No valid columns found for table {table_name}")
      return False

    columns_str = ",\n        ".join(columns)

    # Use schema-defined primary key
    primary_key = primary_keys[0] if primary_keys else arrow_schema[0].name

    # Quote the primary key if it's a reserved word
    if primary_key.lower() in [
      "order",
      "group",
      "select",
      "from",
      "where",
      "having",
      "to",
    ]:
      primary_key = f"`{primary_key}`"

    create_sql = f"""
      CREATE NODE TABLE IF NOT EXISTS {table_name} (
          {columns_str},
          PRIMARY KEY ({primary_key})
      )"""

    engine.execute_query(create_sql)
    logger.debug(f"Created table {table_name} with {len(columns)} columns from schema")
    return True

  except Exception as e:
    if "already exists" in str(e).lower():
      return True
    logger.error(f"Failed to create table {table_name}: {e}")
    return False


def _create_relationship_table_from_schema(
  engine, table_name: str, ingest_info: IngestTableInfo, file_path: str
) -> bool:
  """Create relationship table using schema definition."""
  try:
    import pyarrow.parquet as pq

    # Read parquet schema to validate columns exist
    parquet_file = pq.ParquetFile(file_path)
    arrow_schema = parquet_file.schema_arrow
    parquet_columns = set(field.name for field in arrow_schema)

    # Get from/to nodes from schema
    from_node = ingest_info.from_node
    to_node = ingest_info.to_node

    if not from_node or not to_node:
      logger.error(f"Missing from_node or to_node in schema for {table_name}")
      return False

    # Check if parquet has 'from' and 'to' columns
    has_from_to = "from" in parquet_columns and "to" in parquet_columns
    if not has_from_to:
      logger.warning(
        f"Parquet file missing 'from'/'to' columns for relationship {table_name}"
      )
      return False

    # Get property columns from schema
    property_columns = []
    if ingest_info.properties:
      for prop_name in ingest_info.properties:
        if prop_name in parquet_columns:
          # Map parquet column to appropriate type
          for field in arrow_schema:
            if field.name == prop_name:
              lbug_type = _map_arrow_to_lbug_type(str(field.type))
              # Handle SQL reserved words by quoting them
              column_name = prop_name
              if column_name.lower() in [
                "order",
                "group",
                "select",
                "from",
                "where",
                "having",
                "to",
              ]:
                column_name = f"`{column_name}`"
              property_columns.append(f"{column_name} {lbug_type}")
              break

    # Create relationship table
    if property_columns:
      props_str = ",\n            ".join(property_columns)
      create_sql = f"""
        CREATE REL TABLE IF NOT EXISTS {table_name} (
            FROM {from_node} TO {to_node},
            {props_str}
        )"""
    else:
      create_sql = f"""
        CREATE REL TABLE IF NOT EXISTS {table_name} (
            FROM {from_node} TO {to_node}
        )"""

    engine.execute_query(create_sql)
    logger.debug(
      f"Created relationship table {table_name} from schema: {from_node} -> {to_node}"
    )
    return True

  except Exception as e:
    if "already exists" in str(e).lower():
      return True
    logger.error(f"Failed to create relationship {table_name}: {e}")
    return False


# Valid identifier pattern for table/column names
VALID_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _is_valid_identifier(identifier: str) -> bool:
  """
  Validate that an identifier is safe to use in queries.

  Args:
      identifier: The identifier to validate

  Returns:
      bool: True if valid, False otherwise
  """
  if not identifier or not isinstance(identifier, str):
    SecurityAuditLogger.log_input_validation_failure(
      field_name="identifier",
      invalid_value=str(identifier),
      validation_error="Invalid identifier type or empty",
    )
    return False

  is_valid = bool(VALID_IDENTIFIER_PATTERN.match(identifier))
  if not is_valid:
    SecurityAuditLogger.log_injection_attempt(
      payload=identifier, injection_type="cypher"
    )

  return is_valid


def _sanitize_parameter_name(name: str) -> str:
  """
  Sanitize parameter name for use in queries.

  Args:
      name: The parameter name to sanitize

  Returns:
      str: Sanitized parameter name
  """
  # Replace invalid characters with underscores
  sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
  # Ensure it starts with a letter or underscore
  if sanitized and not sanitized[0].isalpha() and sanitized[0] != "_":
    sanitized = "_" + sanitized
  return sanitized or "param"


# NOTE: UPSERT/MERGE functionality has been removed in favor of COPY with IGNORE_ERRORS
# If you need to perform actual MERGE operations, use the query endpoint directly.
# Performance comparison:
# - COPY with IGNORE_ERRORS: ~5ms per operation (handles duplicates gracefully)
# - Row-by-row MERGE/UPSERT: ~200ms per row (40x slower!)
# The overhead of MERGE operations makes them unsuitable for bulk ingestion.


def _is_global_relationship_schema_driven(relationship_name: str) -> bool:
  """
  Determine if a relationship involves global entities based on schema analysis.

  Global relationships connect to global entities (defined in base schema) and may
  have duplicates across reports, requiring IGNORE_ERRORS for efficient bulk loading.

  Args:
      relationship_name: Name of the relationship to check

  Returns:
      bool: True if this relationship involves global entities requiring IGNORE_ERRORS
  """
  try:
    from ...schemas.base import BASE_RELATIONSHIPS

    # Check if the relationship is defined in the base schema
    base_relationship_names = {rel.name for rel in BASE_RELATIONSHIPS}

    is_global = relationship_name in base_relationship_names

    logger.debug(
      f"Relationship {relationship_name}: global={is_global} (base_schema_relationship={is_global})"
    )
    return is_global

  except Exception as e:
    logger.warning(
      f"Failed to determine if relationship {relationship_name} is global, defaulting to False: {e}"
    )
    # Fail safe: default to standard COPY without IGNORE_ERRORS
    return False


def _is_global_entity_schema_driven(table_name: str) -> bool:
  """
  Determine if a table represents a global entity based on schema analysis.

  Global entities are shared across multiple reports/filings and require
  IGNORE_ERRORS to handle duplicates efficiently during bulk ingestion.

  Report-specific entities are generated uniquely per filing and should never
  have duplicates, so they use standard COPY without IGNORE_ERRORS.

  Args:
      table_name: Name of the table to check

  Returns:
      bool: True if this is a global entity requiring IGNORE_ERRORS
  """
  try:
    from ...schemas.base import BASE_NODES

    # Base schema nodes are always global
    base_node_names = {node.name for node in BASE_NODES}

    # All global entities are now properly defined in the base schema
    # This includes: Entity, Unit, Period, Element, Label, Reference, Taxonomy, etc.
    is_global = table_name in base_node_names

    logger.debug(
      f"Entity {table_name}: global={is_global} (base_schema_node={is_global})"
    )
    return is_global

  except Exception as e:
    logger.warning(
      f"Failed to determine if {table_name} is global, defaulting to False: {e}"
    )
    # Fail safe: default to standard COPY without IGNORE_ERRORS
    return False


def _copy_node_data_schema_driven(
  engine, file_path: str, table_name: str, ingest_info: IngestTableInfo
) -> bool:
  """
  Copy node data using schema-driven approach with LadybugDB's COPY FROM functionality.

  Performance characteristics:
  - COPY with IGNORE_ERRORS: ~5ms per operation (handles duplicates gracefully)
  - COPY without IGNORE_ERRORS: ~3ms per operation (fails on duplicates)
  - Row-by-row MERGE/UPSERT: ~200ms per row (40x slower!)

  Strategy:
  - Global entities (Entity, Unit, Element, etc.): Use COPY with IGNORE_ERRORS
    These are shared across reports and may have duplicates
  - Report-specific nodes: Use standard COPY (no duplicates expected)
    Each report generates unique identifiers for its nodes

  The IGNORE_ERRORS flag adds minimal overhead (~2ms) while providing
  robust duplicate handling, making it ideal for bulk ingestion.
  """
  try:
    # Validate table name to prevent injection
    if not _is_valid_identifier(table_name):
      raise ValueError(f"Invalid table name: {table_name}")

    # Use COPY FROM for efficient bulk loading
    # The parquet file should have columns matching the table schema

    # Determine if this is a global entity based on schema analysis
    # Global entities are defined in the base schema and are shared across reports
    is_global_entity = _is_global_entity_schema_driven(table_name)

    # The parquet files are generated to match the table schema exactly.
    # We can use simple COPY without specifying columns.

    if is_global_entity:
      # Use IGNORE_ERRORS for global entities to handle duplicates efficiently
      copy_query = f"COPY {table_name} FROM '{file_path}' (IGNORE_ERRORS=true)"
      logger.info(
        f"Copying data from {file_path} into {table_name} using COPY FROM with IGNORE_ERRORS"
      )
    else:
      # Standard COPY for report-specific nodes
      copy_query = f"COPY {table_name} FROM '{file_path}'"
      logger.info(f"Copying data from {file_path} into {table_name} using COPY FROM")

    # Track performance metrics
    import time
    import pyarrow.parquet as pq

    # Get row count for performance tracking
    try:
      parquet_file = pq.ParquetFile(file_path)
      row_count = parquet_file.metadata.num_rows
    except Exception:
      row_count = 0

    start_time = time.time()

    # Set timeout to 30 minutes for large COPY operations
    timeout_set = False
    if hasattr(engine, "set_query_timeout"):
      engine.set_query_timeout(1800000)  # 30 minutes
      timeout_set = True
    else:
      logger.debug("Engine does not support set_query_timeout method")

    try:
      engine.execute_query(copy_query)
      execution_time = time.time() - start_time

      # Log performance metrics
      if row_count > 0:
        ms_per_row = (execution_time * 1000) / row_count
        logger.info(
          f"Successfully copied {row_count:,} rows into {table_name} "
          f"in {execution_time:.2f}s ({ms_per_row:.3f}ms/row)"
        )

        # Warn if performance is poor
        if ms_per_row > 0.5:  # More than 0.5ms per row is slow
          logger.warning(
            f"SLOW NODE COPY: {table_name} - {ms_per_row:.3f}ms/row "
            f"({row_count} rows in {execution_time:.2f}s)"
          )
      else:
        logger.info(
          f"Successfully copied data into {table_name} in {execution_time:.2f}s"
        )

      return True

    except Exception as copy_err:
      # If COPY fails due to column mismatch, log helpful error
      if "column" in str(copy_err).lower():
        import pyarrow.parquet as pq

        # Just read schema, not data
        parquet_file = pq.ParquetFile(file_path)
        parquet_columns = [field.name for field in parquet_file.schema_arrow]
        logger.error(f"COPY failed - parquet columns: {parquet_columns}")
        logger.error(f"Expected columns for {table_name}: {ingest_info.columns}")
      raise
    finally:
      # Always reset timeout to default after COPY operation
      if timeout_set and hasattr(engine, "set_query_timeout"):
        engine.set_query_timeout(120000)  # Back to 2 minutes

  except Exception as e:
    logger.error(f"Failed to copy node data for {table_name}: {e}")
    return False


def _copy_relationship_data_schema_driven(
  engine,
  file_path: str,
  table_name: str,
  ingest_info: IngestTableInfo,
  schema_adapter: XBRLSchemaConfigGenerator,
) -> bool:
  """
  Copy relationship data using schema-driven approach with LadybugDB's COPY FROM functionality.

  NOTE: With report-specific identifiers, relationships are also unique per report:
  - Each relationship references report-specific node identifiers
  - No duplicates within a single report's relationship data
  - Can use fast COPY FROM operations
  - Special handling needed for entity relationships
  """
  try:
    # Validate table name to prevent injection
    if not _is_valid_identifier(table_name):
      raise ValueError(f"Invalid table name: {table_name}")

    # Use COPY FROM for efficient bulk loading of relationships
    # Determine if this relationship involves global entities based on schema analysis
    is_global_relationship = _is_global_relationship_schema_driven(table_name)

    # For relationships in LadybugDB, the parquet file must match the table schema exactly.
    # The parquet file should have:
    # - "from" column: source node ID
    # - "to" column: target node ID
    # - Additional columns: any relationship properties defined in schema

    if is_global_relationship:
      # Use IGNORE_ERRORS for relationships with global entities
      copy_query = f"COPY {table_name} FROM '{file_path}' (IGNORE_ERRORS=true)"
      logger.info(
        f"Copying relationship data from {file_path} into {table_name} using COPY FROM with IGNORE_ERRORS"
      )
    else:
      # Standard COPY for report-specific relationships
      copy_query = f"COPY {table_name} FROM '{file_path}'"
      logger.info(
        f"Copying relationship data from {file_path} into {table_name} using COPY FROM"
      )

    # Track performance metrics
    import time
    import pyarrow.parquet as pq

    # Get row count for performance tracking
    try:
      parquet_file = pq.ParquetFile(file_path)
      row_count = parquet_file.metadata.num_rows
    except Exception:
      row_count = 0

    start_time = time.time()

    # Set timeout to 30 minutes for large COPY operations
    timeout_set = False
    if hasattr(engine, "set_query_timeout"):
      engine.set_query_timeout(1800000)  # 30 minutes
      timeout_set = True
    else:
      logger.debug("Engine does not support set_query_timeout method")

    try:
      engine.execute_query(copy_query)
      execution_time = time.time() - start_time

      # Log performance metrics
      if row_count > 0:
        ms_per_row = (execution_time * 1000) / row_count
        logger.info(
          f"Successfully copied {row_count:,} relationships into {table_name} "
          f"in {execution_time:.2f}s ({ms_per_row:.3f}ms/row)"
        )

        # Warn if performance is poor - relationships are slower due to FK validation
        if ms_per_row > 1.0:  # More than 1ms per row is slow for relationships
          logger.warning(
            f"SLOW RELATIONSHIP COPY: {table_name} - {ms_per_row:.3f}ms/row "
            f"({row_count} rows in {execution_time:.2f}s)"
          )
      else:
        logger.info(
          f"Successfully copied relationships into {table_name} in {execution_time:.2f}s"
        )

      return True

    except Exception as copy_err:
      # If COPY fails, log helpful error
      if "column" in str(copy_err).lower() or "foreign key" in str(copy_err).lower():
        import pyarrow.parquet as pq

        # Just read schema, not data
        parquet_file = pq.ParquetFile(file_path)
        parquet_columns = [field.name for field in parquet_file.schema_arrow]
        logger.error(f"COPY failed - parquet columns: {parquet_columns}")
        logger.error(
          f"Expected from_node: {ingest_info.from_node}, to_node: {ingest_info.to_node}"
        )
        if ingest_info.properties:
          logger.error(f"Expected properties: {ingest_info.properties}")
      raise
    finally:
      # Always reset timeout to default after COPY operation
      if timeout_set and hasattr(engine, "set_query_timeout"):
        engine.set_query_timeout(120000)  # Back to 2 minutes

  except Exception as e:
    logger.error(f"Failed to copy relationship data for {table_name}: {e}")
    return False


def _map_arrow_to_lbug_type(arrow_type: str) -> str:
  """Map Arrow types to LadybugDB types."""
  arrow_type_lower = arrow_type.lower()

  # Handle string types
  if any(x in arrow_type_lower for x in ["string", "utf8", "large_string"]):
    return "STRING"
  elif "int64" in arrow_type_lower:
    return "INT64"
  elif (
    "int32" in arrow_type_lower
    or "int16" in arrow_type_lower
    or "int8" in arrow_type_lower
  ):
    return "INT32"
  elif "double" in arrow_type_lower or "float64" in arrow_type_lower:
    return "DOUBLE"
  elif "float" in arrow_type_lower or "float32" in arrow_type_lower:
    return "FLOAT"
  elif "bool" in arrow_type_lower:
    return "BOOLEAN"
  elif "timestamp" in arrow_type_lower or "datetime" in arrow_type_lower:
    return "TIMESTAMP"
  elif "date" in arrow_type_lower:
    return "DATE"
  elif "decimal" in arrow_type_lower:
    return "DOUBLE"  # Map decimal to double for simplicity
  else:
    # Default to STRING for unknown types
    logger.debug(f"Unknown Arrow type '{arrow_type}', defaulting to STRING")
    return "STRING"
