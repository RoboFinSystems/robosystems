"""Bulk ingestion of processed parquet into a LadybugDB database.

File-to-table mapping, primary keys, and relationship endpoints are all read
from the compiled schema, so adding a node or relationship type needs no change
here. Nodes are always ingested before relationships — a relationship COPY
fails on an endpoint whose node row does not yet exist.
"""

import os
import re
import tempfile
from pathlib import Path

from robosystems.adapters.sec.processors.schema import (
  IngestTableInfo,
  XBRLSchemaConfigGenerator,
  create_roboledger_ingestion_processor,
)

from ....config import env
from ....logger import logger
from ....security import SecurityAuditLogger

# Cache schema adapters to avoid recompilation
_schema_adapter_cache: dict[str, XBRLSchemaConfigGenerator] = {}


def _get_cached_schema_adapter(
  schema_config: dict | None = None,
) -> XBRLSchemaConfigGenerator:
  """Return a schema adapter, compiling and caching it on first use.

  Compilation is expensive enough that recompiling per file dominates ingest
  time; the cache is process-local and never invalidated, so a schema change
  needs a restart.
  """
  if schema_config:
    cache_key = f"{schema_config.get('name', 'custom')}_{schema_config.get('base_schema', 'base')}_{'_'.join(schema_config.get('extensions', []))}"
  else:
    cache_key = "roboledger_default"

  if cache_key in _schema_adapter_cache:
    logger.debug(f"Using cached schema adapter: {cache_key}")
    return _schema_adapter_cache[cache_key]

  logger.info(f"Creating new schema adapter: {cache_key}")
  if schema_config:
    adapter = XBRLSchemaConfigGenerator(schema_config)
  else:
    adapter = create_roboledger_ingestion_processor()

  _schema_adapter_cache[cache_key] = adapter

  return adapter


def ingest_from_s3(
  bucket: str,
  db_name: str,
  s3_prefix: str = "processed/",
  schema_config: dict | None = None,
) -> bool:
  """Download parquet under ``s3_prefix`` and ingest it into ``db_name``.

  An empty prefix is success, not failure. ``schema_config`` defaults to base +
  roboledger.
  """
  try:
    import boto3

    logger.info(
      f"Starting schema-driven LadybugDB ingestion from S3: {bucket}/{s3_prefix} -> {db_name}"
    )

    s3_config = env.get_s3_config()
    endpoint_url = s3_config.get("endpoint_url")

    if endpoint_url:
      # LocalStack (dev/test) takes fixed dummy credentials.
      s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
      )
    else:
      s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_config.get("aws_access_key_id"),
        aws_secret_access_key=s3_config.get("aws_secret_access_key"),
        region_name=s3_config.get("region_name"),
      )

    with tempfile.TemporaryDirectory() as temp_dir:
      response = s3_client.list_objects_v2(Bucket=bucket, Prefix=s3_prefix)

      if "Contents" not in response:
        logger.warning(f"No processed files found in {bucket}/{s3_prefix}")
        return True

      downloaded_files = []
      for obj in response["Contents"]:
        if obj["Key"].endswith(".parquet"):
          # Keep the bare filename: table mapping keys off it.
          filename = os.path.basename(obj["Key"])
          local_path = Path(temp_dir) / filename

          s3_client.download_file(bucket, obj["Key"], str(local_path))
          downloaded_files.append(str(local_path))

      logger.info(f"Downloaded {len(downloaded_files)} parquet files")

      if not downloaded_files:
        logger.warning("No parquet files to ingest")
        return True

      return ingest_from_local_files(downloaded_files, db_name, schema_config)

  except Exception as e:
    logger.error(f"S3 ingestion failed: {e}")
    return False


def ingest_from_local_files(
  file_paths: list[str], db_name: str, schema_config: dict | None = None
) -> bool:
  """Ingest local parquet files into ``db_name``, nodes before relationships.

  Creates the schema if the database has none. Returns True when at least one
  file loaded — a partial load is reported as success, so check the logged
  ``ingested/total`` counts when completeness matters.
  """
  try:
    from robosystems.graph_api.core.ladybug import Engine

    from .schema_setup import ensure_schema

    logger.info(f"Starting LadybugDB ingestion: {len(file_paths)} files -> {db_name}")

    from .path_utils import (
      ensure_lbug_directory,
      get_lbug_database_path,
    )

    db_path = get_lbug_database_path(db_name)

    ensure_lbug_directory(db_path)

    logger.info("Checking if schema initialization is needed")
    schema_created = ensure_schema(db_name, schema_config)
    if schema_created:
      logger.info("Schema was created for the first time")
    else:
      logger.info("Schema already exists, skipping creation")

    logger.info(f"Opening graph database at: {db_path}")
    engine = Engine(str(db_path))

    schema_adapter = _get_cached_schema_adapter(schema_config)

    node_files, relationship_files = _categorize_files_schema_driven(
      file_paths, schema_adapter
    )

    logger.info(
      f"File categorization: {len(node_files)} nodes, {len(relationship_files)} relationships"
    )

    # Nodes first: a relationship COPY fails on a missing endpoint row.
    ingested_count = 0

    for file_path in node_files:
      table_info = _parse_filename_schema_driven(file_path, schema_adapter)
      if table_info and _ingest_node_schema_driven(
        engine, file_path, table_info, schema_adapter
      ):
        ingested_count += 1

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
  file_paths: list[str], schema_adapter: XBRLSchemaConfigGenerator
) -> tuple[list[str], list[str]]:
  """Split files into ``(node_files, relationship_files)`` using the schema.

  The full path is passed to the adapter, not the basename — the directory
  often carries the table name.
  """
  node_files = []
  relationship_files = []

  for file_path in file_paths:
    if schema_adapter.is_relationship_file(file_path):
      relationship_files.append(file_path)
    else:
      node_files.append(file_path)

  return node_files, relationship_files


def _parse_filename_schema_driven(
  file_path: str, schema_adapter: XBRLSchemaConfigGenerator
) -> dict | None:
  """Resolve a file to its table, or None when the schema has no mapping.

  Takes the full path so the directory can contribute the table name.
  """
  table_name = schema_adapter.get_table_name_from_file(file_path)
  if not table_name:
    logger.warning(f"No table mapping found for file: {file_path}")
    return None

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
  """Create the node table from the schema, then COPY the file into it."""
  table_name = table_info["table_name"]  # Bound early for the except branch
  try:
    schema_table_info = table_info["table_info"]

    ingest_info = schema_table_info

    if not _create_table_from_schema(engine, table_name, ingest_info, file_path):
      return False

    return _copy_node_data_schema_driven(engine, file_path, table_name, ingest_info)

  except Exception as e:
    logger.error(f"Schema-driven node ingestion failed for {table_name}: {e}")
    return False


def _ingest_relationship_schema_driven(
  engine, file_path: str, table_info: dict, schema_adapter: XBRLSchemaConfigGenerator
) -> bool:
  """Create the relationship table from the schema, then COPY into it."""
  table_name = table_info["table_name"]  # Bound early for the except branch
  try:
    schema_table_info = table_info["table_info"]

    ingest_info = schema_table_info

    if not _create_relationship_table_from_schema(
      engine, table_name, ingest_info, file_path
    ):
      return False

    return _copy_relationship_data_schema_driven(
      engine, file_path, table_name, ingest_info, schema_adapter
    )

  except Exception as e:
    logger.error(f"Schema-driven relationship ingestion failed for {table_name}: {e}")
    return False


def _create_table_from_schema(
  engine, table_name: str, ingest_info: IngestTableInfo, file_path: str
) -> bool:
  """Create the node table for the columns the schema and parquet share.

  Only the parquet metadata is read, never the data. An existing table is
  treated as success.
  """
  try:
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(file_path)
    arrow_schema = parquet_file.schema_arrow

    parquet_columns = {field.name for field in arrow_schema}
    logger.debug(
      f"Parquet file {file_path} has {len(parquet_columns)} columns: {sorted(parquet_columns)}"
    )

    columns = []

    primary_keys = ingest_info.primary_keys
    if not primary_keys:
      if ingest_info.is_relationship:
        logger.error(
          f"Relationship {table_name} being treated as node table - skipping"
        )
        return False
      first_col = arrow_schema[0].name
      logger.warning(
        f"No primary keys defined in schema for {table_name}, falling back to first column: {first_col}"
      )
      primary_keys = [first_col]
    else:
      logger.debug(
        f"Using schema-defined primary key for {table_name}: {primary_keys[0]}"
      )

    schema_columns = set(ingest_info.columns)
    logger.debug(
      f"Schema for {table_name} expects {len(schema_columns)} columns: {sorted(schema_columns)}"
    )

    available_columns = schema_columns.intersection(parquet_columns)
    logger.debug(
      f"Intersection for {table_name}: {len(available_columns)} columns: {sorted(available_columns)}"
    )

    if not available_columns:
      logger.warning(f"No matching columns between schema and parquet for {table_name}")
      logger.warning(f"  Schema columns: {sorted(schema_columns)}")
      logger.warning(f"  Parquet columns: {sorted(parquet_columns)}")
      return False

    for field in arrow_schema:
      if field.name in available_columns:
        lbug_type = _map_arrow_to_lbug_type(str(field.type))
        # Reserved words have to be backtick-quoted to survive DDL.
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

    primary_key = primary_keys[0] if primary_keys else arrow_schema[0].name

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
  """Create the relationship table. The parquet must carry `from` and `to`."""
  try:
    import pyarrow.parquet as pq

    parquet_file = pq.ParquetFile(file_path)
    arrow_schema = parquet_file.schema_arrow
    parquet_columns = {field.name for field in arrow_schema}

    from_node = ingest_info.from_node
    to_node = ingest_info.to_node

    if not from_node or not to_node:
      logger.error(f"Missing from_node or to_node in schema for {table_name}")
      return False

    has_from_to = "from" in parquet_columns and "to" in parquet_columns
    if not has_from_to:
      logger.warning(
        f"Parquet file missing 'from'/'to' columns for relationship {table_name}"
      )
      return False

    property_columns = []
    if ingest_info.properties:
      for prop_name in ingest_info.properties:
        if prop_name in parquet_columns:
          for field in arrow_schema:
            if field.name == prop_name:
              lbug_type = _map_arrow_to_lbug_type(str(field.type))
              # Reserved words have to be backtick-quoted to survive DDL.
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
  """True when the identifier is safe to interpolate into a query.

  Table and column names reach the query string unparameterised, so a rejection
  is logged to the security audit trail as an injection attempt.
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
  """Coerce a name into a valid query parameter identifier."""
  sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", name)
  if sanitized and not sanitized[0].isalpha() and sanitized[0] != "_":
    sanitized = "_" + sanitized
  return sanitized or "param"


# Bulk loading here is COPY only. Row-by-row MERGE/UPSERT costs ~200ms/row
# against ~5ms for COPY with IGNORE_ERRORS, which rules it out at ingest
# volumes. Genuine MERGE semantics belong on the query endpoint.


def _is_global_relationship_schema_driven(relationship_name: str) -> bool:
  """True for base-schema relationships, which may repeat across reports.

  Those need ``IGNORE_ERRORS`` on COPY; report-specific ones are unique by
  construction and must not swallow errors.
  """
  try:
    from ....schemas.base import BASE_RELATIONSHIPS

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
    # Fail safe: a strict COPY surfaces duplicates rather than hiding them.
    return False


def _is_global_entity_schema_driven(table_name: str) -> bool:
  """True for base-schema nodes, which are shared across filings.

  Those repeat across reports and need ``IGNORE_ERRORS`` on COPY;
  report-specific nodes carry per-filing identifiers and must not.
  """
  try:
    from ....schemas.base import BASE_NODES

    base_node_names = {node.name for node in BASE_NODES}

    is_global = table_name in base_node_names

    logger.debug(
      f"Entity {table_name}: global={is_global} (base_schema_node={is_global})"
    )
    return is_global

  except Exception as e:
    logger.warning(
      f"Failed to determine if {table_name} is global, defaulting to False: {e}"
    )
    # Fail safe: a strict COPY surfaces duplicates rather than hiding them.
    return False


def _copy_node_data_schema_driven(
  engine, file_path: str, table_name: str, ingest_info: IngestTableInfo
) -> bool:
  """COPY a node parquet file into its table.

  Global (base-schema) entities load with ``IGNORE_ERRORS`` because the same
  row legitimately arrives from several reports; ~5ms/op against ~3ms for a
  strict COPY. Report-specific nodes use the strict form so a duplicate is a
  real error. Column lists are omitted — the parquet is generated to match the
  table exactly.
  """
  try:
    if not _is_valid_identifier(table_name):
      raise ValueError(f"Invalid table name: {table_name}")

    is_global_entity = _is_global_entity_schema_driven(table_name)

    if is_global_entity:
      copy_query = f"COPY {table_name} FROM '{file_path}' (IGNORE_ERRORS=true)"
      logger.info(
        f"Copying data from {file_path} into {table_name} using COPY FROM with IGNORE_ERRORS"
      )
    else:
      copy_query = f"COPY {table_name} FROM '{file_path}'"
      logger.info(f"Copying data from {file_path} into {table_name} using COPY FROM")

    import time

    import pyarrow.parquet as pq

    try:
      parquet_file = pq.ParquetFile(file_path)
      row_count = parquet_file.metadata.num_rows
    except Exception:
      row_count = 0

    start_time = time.time()

    # A large COPY easily exceeds the default query timeout.
    timeout_set = False
    if hasattr(engine, "set_query_timeout"):
      engine.set_query_timeout(1800000)  # 30 minutes
      timeout_set = True
    else:
      logger.debug("Engine does not support set_query_timeout method")

    try:
      engine.execute_query(copy_query)
      execution_time = time.time() - start_time

      if row_count > 0:
        ms_per_row = (execution_time * 1000) / row_count
        logger.info(
          f"Successfully copied {row_count:,} rows into {table_name} "
          f"in {execution_time:.2f}s ({ms_per_row:.3f}ms/row)"
        )

        if ms_per_row > 0.5:  # More than 0.5ms/row is slow for nodes
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
      # Column mismatch is the common cause; dump both sides to compare.
      if "column" in str(copy_err).lower():
        import pyarrow.parquet as pq

        parquet_file = pq.ParquetFile(file_path)
        parquet_columns = [field.name for field in parquet_file.schema_arrow]
        logger.error(f"COPY failed - parquet columns: {parquet_columns}")
        logger.error(f"Expected columns for {table_name}: {ingest_info.columns}")
      raise
    finally:
      if timeout_set and hasattr(engine, "set_query_timeout"):
        engine.set_query_timeout(120000)  # Back to the 2-minute default

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
  """COPY a relationship parquet file into its table.

  The parquet must expose ``from`` and ``to`` plus any schema-declared
  properties, in the table's order. Base-schema relationships load with
  ``IGNORE_ERRORS`` (the same edge can arrive from several reports);
  report-specific ones carry per-filing identifiers and use the strict form.
  """
  try:
    if not _is_valid_identifier(table_name):
      raise ValueError(f"Invalid table name: {table_name}")

    is_global_relationship = _is_global_relationship_schema_driven(table_name)

    if is_global_relationship:
      copy_query = f"COPY {table_name} FROM '{file_path}' (IGNORE_ERRORS=true)"
      logger.info(
        f"Copying relationship data from {file_path} into {table_name} using COPY FROM with IGNORE_ERRORS"
      )
    else:
      copy_query = f"COPY {table_name} FROM '{file_path}'"
      logger.info(
        f"Copying relationship data from {file_path} into {table_name} using COPY FROM"
      )

    import time

    import pyarrow.parquet as pq

    try:
      parquet_file = pq.ParquetFile(file_path)
      row_count = parquet_file.metadata.num_rows
    except Exception:
      row_count = 0

    start_time = time.time()

    # A large COPY easily exceeds the default query timeout.
    timeout_set = False
    if hasattr(engine, "set_query_timeout"):
      engine.set_query_timeout(1800000)  # 30 minutes
      timeout_set = True
    else:
      logger.debug("Engine does not support set_query_timeout method")

    try:
      engine.execute_query(copy_query)
      execution_time = time.time() - start_time

      if row_count > 0:
        ms_per_row = (execution_time * 1000) / row_count
        logger.info(
          f"Successfully copied {row_count:,} relationships into {table_name} "
          f"in {execution_time:.2f}s ({ms_per_row:.3f}ms/row)"
        )

        # Relationships are slower than nodes: endpoints are validated.
        if ms_per_row > 1.0:
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
      # Column or endpoint mismatch is the common cause; dump both sides.
      if "column" in str(copy_err).lower() or "foreign key" in str(copy_err).lower():
        import pyarrow.parquet as pq

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
      if timeout_set and hasattr(engine, "set_query_timeout"):
        engine.set_query_timeout(120000)  # Back to the 2-minute default

  except Exception as e:
    logger.error(f"Failed to copy relationship data for {table_name}: {e}")
    return False


def _map_arrow_to_lbug_type(arrow_type: str) -> str:
  """Map an Arrow type name to a LadybugDB type, defaulting to STRING."""
  arrow_type_lower = arrow_type.lower()

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
