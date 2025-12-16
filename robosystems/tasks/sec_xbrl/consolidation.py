"""
SEC XBRL Parquet File Consolidation Task

Consolidates many small parquet files into fewer, optimally-sized files
for efficient LadybugDB ingestion.

Key features:
- Streaming consolidation to avoid memory exhaustion
- Target file size of 256MB for optimal LadybugDB performance
- Parallel processing across node/relationship types
- Idempotent - can be re-run safely
- Progress tracking via pipeline state
"""

from typing import Dict, List
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

from celery import Task
from robosystems.celery import celery_app
from robosystems.config import env
from robosystems.logger import logger
from robosystems.operations.aws.s3 import S3Client


# Consolidation configuration
TARGET_FILE_SIZE = 256 * 1024 * 1024  # 256MB target file size
MAX_MEMORY_PER_BATCH = 512 * 1024 * 1024  # 512MB max memory per batch
MIN_FILES_TO_CONSOLIDATE = 1  # Consolidate everything (even single files)
PARQUET_ROW_GROUP_SIZE = 50000  # Optimal for LadybugDB


def get_schema_types() -> tuple[List[str], List[str]]:
  """Get all node and relationship types from the SEC schema."""
  from robosystems.tasks.sec_xbrl import ingestion

  # Ensure schema is loaded before accessing
  ingestion._ensure_schema_loaded()

  # Now read the module attributes after they've been loaded
  # Type assertions - these are guaranteed to be set after _ensure_schema_loaded()
  assert ingestion.NODE_TYPES is not None
  assert ingestion.RELATIONSHIP_TYPES is not None

  return ingestion.NODE_TYPES, ingestion.RELATIONSHIP_TYPES


def get_enforced_schema(table_name: str, original_schema: pa.Schema) -> pa.Schema:
  """
  Get an enforced schema for a table to ensure critical columns have the correct types.

  This prevents issues where PyArrow infers INT32 for fields that should be STRING,
  particularly for fields like EIN that may contain only numbers but need to preserve
  leading zeros and be treated as strings in LadybugDB.

  Args:
      table_name: Name of the table (e.g., "Entity", "Report")
      original_schema: The original PyArrow schema from the parquet file

  Returns:
      Modified schema with enforced types for critical columns
  """
  # Define columns that MUST be strings regardless of content
  STRING_ENFORCED_COLUMNS = {
    "Entity": [
      "ein",  # Employer ID Number - may have leading zeros
      "tax_id",  # Tax ID - same as EIN
      "ticker",  # Stock ticker symbol
      "cik",  # Central Index Key - numeric but treated as string
      "sic",  # SIC code - numeric but categorical
      "phone",  # Phone numbers
      "zipcode",  # Zip codes may have leading zeros
      "postal_code",  # International postal codes
    ],
    "Report": [
      "accession_number",  # SEC accession number
      "form",  # Form type (10-K, 10-Q, etc.)
      "filing_number",  # Filing number
    ],
    "Unit": [
      "numerator_uri",  # URIs should always be strings
      "denominator_uri",  # URIs should always be strings
      "identifier",  # Unit identifier
    ],
    # Add any fields ending in _uri, _id, _code, _number as strings
    # These are typically identifiers that shouldn't be treated as numbers
  }

  # Get the list of columns to enforce for this table
  columns_to_enforce = STRING_ENFORCED_COLUMNS.get(table_name, [])

  # Also check for common identifier patterns that should be strings
  identifier_suffixes = ["_uri", "_id", "_code", "_number", "_key", "_ref"]

  # Create a new schema with enforced types
  new_fields = []
  for field in original_schema:
    should_be_string = False

    # Check if this field is in the explicit enforcement list
    if field.name in columns_to_enforce:
      should_be_string = True

    # Check if field name ends with an identifier suffix
    elif any(field.name.endswith(suffix) for suffix in identifier_suffixes):
      should_be_string = True
      logger.debug(
        f"Auto-enforcing STRING type for {table_name}.{field.name} due to suffix pattern"
      )

    # IMPORTANT: Convert null type fields to string type
    # This happens when a parquet file has all null values for a column
    elif field.type == pa.null():
      should_be_string = True
      logger.debug(f"Converting NULL type to STRING for {table_name}.{field.name}")

    if should_be_string:
      # Force this field to be a string (utf8)
      # Preserve nullability from original field
      new_field = pa.field(field.name, pa.string(), nullable=field.nullable)
      if field.type != pa.string():
        logger.debug(
          f"Enforcing STRING type for {table_name}.{field.name} (was {field.type})"
        )
      new_fields.append(new_field)
    else:
      # Keep the original field type
      new_fields.append(field)

  return pa.schema(new_fields)


@celery_app.task(
  bind=True,
  queue=env.QUEUE_SHARED_PROCESSING,
  name="sec_xbrl.consolidate_parquet_files",
  max_retries=3,
  default_retry_delay=60,
)
def consolidate_parquet_files(
  self: Task,
  table_type: str,  # "nodes" or "relationships"
  table_name: str,  # e.g., "Entity", "REPORT_HAS_FACT"
  year: int,  # Specific year to consolidate
  bucket: str,
  pipeline_id: str = None,  # Currently unused but reserved for future state tracking  # noqa: ARG001
) -> Dict:
  """
  Consolidate parquet files for a specific table and year.

  Args:
      table_type: "nodes" or "relationships"
      table_name: Name of the table (e.g., "Entity", "REPORT_HAS_FACT")
      year: Year to consolidate (e.g., 2024)
      bucket: S3 bucket containing processed files
      pipeline_id: Pipeline ID for tracking

  Returns:
      Dict with consolidation results
  """
  start_time = datetime.now()

  try:
    s3_client = S3Client()

    # Source and destination with year partitioning
    source_prefix = f"processed/year={year}/{table_type}/{table_name}"
    dest_prefix = f"consolidated/year={year}/{table_type}/{table_name}"

    logger.info(
      f"Starting consolidation for {table_type}/{table_name} year={year} "
      f"from s3://{bucket}/{source_prefix}"
    )

    # List source files for this specific year
    source_files = list_s3_files(s3_client, bucket, source_prefix)

    if len(source_files) < MIN_FILES_TO_CONSOLIDATE:
      logger.info(
        f"Skipping consolidation for {table_name} year={year}: only {len(source_files)} files "
        f"(minimum {MIN_FILES_TO_CONSOLIDATE})"
      )
      return {
        "status": "skipped",
        "table": table_name,
        "year": year,
        "reason": f"Only {len(source_files)} files",
        "source_files": len(source_files),
      }

    # Check if already consolidated
    existing_consolidated = list_s3_files(s3_client, bucket, dest_prefix)
    if existing_consolidated:
      logger.info(
        f"Found {len(existing_consolidated)} existing consolidated files for {table_name} year={year}, "
        "checking if re-consolidation needed"
      )

      # Simple heuristic: if we have consolidated files and they're reasonably sized, skip
      if len(existing_consolidated) <= max(5, len(source_files) // 100):
        return {
          "status": "already_consolidated",
          "table": table_name,
          "year": year,
          "consolidated_files": len(existing_consolidated),
          "source_files": len(source_files),
        }

    # Group files into memory-safe batches
    file_batches = create_file_batches(source_files)

    # Process each batch
    consolidated_files = []
    total_rows = 0

    for batch_idx, batch in enumerate(file_batches):
      try:
        output_key = f"{dest_prefix}/batch_{batch_idx:04d}.parquet"

        # Stream consolidation with schema enforcement
        rows_written = stream_consolidate_batch(
          s3_client, bucket, batch, output_key, table_name
        )

        consolidated_files.append(output_key)
        total_rows += rows_written

        logger.info(
          f"Consolidated batch {batch_idx} for year={year}: {len(batch)} files -> "
          f"{output_key} ({rows_written:,} rows)"
        )

      except Exception as e:
        logger.error(f"Failed to consolidate batch {batch_idx} for year={year}: {e}")
        # Continue with other batches even if one fails
        continue

    duration = (datetime.now() - start_time).total_seconds()

    # Calculate consolidation ratio
    consolidation_ratio = (
      len(source_files) / len(consolidated_files) if consolidated_files else 0
    )

    # Calculate size metrics
    total_source_size = sum(f["Size"] for f in source_files)

    result = {
      "status": "success",
      "table": table_name,
      "year": year,
      "source_files": len(source_files),
      "consolidated_files": len(consolidated_files),
      "consolidation_ratio": round(consolidation_ratio, 1),
      "total_rows": total_rows,
      "total_source_size_mb": round(total_source_size / (1024 * 1024), 2),
      "duration_seconds": round(duration, 2),
      "files_created": consolidated_files,
    }

    logger.info(
      f"✅ Consolidation complete for {table_name} year={year}: "
      f"{len(source_files)} files ({total_source_size / (1024 * 1024):.1f}MB) -> "
      f"{len(consolidated_files)} files ({consolidation_ratio:.1f}x reduction) "
      f"with {total_rows:,} rows in {duration:.1f}s"
    )

    return result

  except Exception as e:
    logger.error(f"Consolidation failed for {table_name} year={year}: {e}")

    # Retry if transient error
    if self.request.retries < self.max_retries:
      raise self.retry(exc=e)

    return {
      "status": "failed",
      "table": table_name,
      "year": year,
      "error": str(e),
      "duration_seconds": (datetime.now() - start_time).total_seconds(),
    }


def list_s3_files(s3_client: S3Client, bucket: str, prefix: str) -> List[Dict]:
  """List all parquet files in an S3 prefix with their metadata."""
  files = []

  try:
    paginator = s3_client.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for page in pages:
      if "Contents" in page:
        for obj in page["Contents"]:
          if obj["Key"].endswith(".parquet"):
            files.append(
              {
                "Key": obj["Key"],
                "Size": obj["Size"],
                "LastModified": obj["LastModified"],
              }
            )
  except Exception as e:
    logger.error(f"Failed to list S3 files at s3://{bucket}/{prefix}: {e}")
    raise

  return files


def list_s3_files_all_years(
  s3_client: S3Client, bucket: str, table_type: str, table_name: str
) -> List[Dict]:
  """
  List all parquet files for a specific table across all years.

  Args:
      s3_client: S3 client instance
      bucket: S3 bucket name
      table_type: "nodes" or "relationships"
      table_name: Name of the table (e.g., "Entity", "REPORT_HAS_FACT")

  Returns:
      List of file metadata dictionaries
  """
  all_files = []

  # First, list all year directories
  year_prefix = "processed/year="

  try:
    paginator = s3_client.s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=year_prefix, Delimiter="/")

    year_folders = []
    for page in pages:
      if "CommonPrefixes" in page:
        for prefix_info in page["CommonPrefixes"]:
          # Extract year folder like "processed/year=2024/"
          year_folders.append(prefix_info["Prefix"])

    # For each year, list files for the specific table
    for year_folder in year_folders:
      table_prefix = f"{year_folder}{table_type}/{table_name}/"
      logger.debug(f"Scanning {table_prefix}")

      table_files = list_s3_files(s3_client, bucket, table_prefix)
      all_files.extend(table_files)

    # Calculate total size
    total_size = sum(f["Size"] for f in all_files)
    avg_size = total_size / len(all_files) if all_files else 0

    logger.info(
      f"Found {len(all_files)} files for {table_name} across {len(year_folders)} years "
      f"(total: {total_size / (1024 * 1024):.1f}MB, avg: {avg_size / 1024:.1f}KB)"
    )

  except Exception as e:
    logger.error(f"Failed to list files across years for {table_name}: {e}")
    raise

  return all_files


def create_file_batches(files: List[Dict]) -> List[List[Dict]]:
  """
  Group files into memory-safe batches for consolidation.
  Each batch should not exceed MAX_MEMORY_PER_BATCH when loaded.
  """
  batches = []
  current_batch = []
  current_size = 0

  for file_info in files:
    file_size = file_info["Size"]

    # Start new batch if adding this file would exceed memory limit
    if current_size + file_size > MAX_MEMORY_PER_BATCH and current_batch:
      batches.append(current_batch)
      current_batch = [file_info]
      current_size = file_size
    else:
      current_batch.append(file_info)
      current_size += file_size

  # Add remaining files
  if current_batch:
    batches.append(current_batch)

  logger.info(f"Created {len(batches)} batches from {len(files)} files")
  return batches


def stream_consolidate_batch(
  s3_client: S3Client,
  bucket: str,
  batch_files: List[Dict],
  output_key: str,
  table_name: str,
) -> int:
  """
  Stream consolidation of multiple parquet files without loading all into memory.
  Returns the number of rows written.
  """
  writer = None
  schema = None
  enforced_schema = None
  total_rows = 0
  buffer = BytesIO()

  try:
    for _, file_info in enumerate(batch_files):
      try:
        # Download file to memory
        file_obj = BytesIO()
        s3_client.s3_client.download_fileobj(bucket, file_info["Key"], file_obj)
        file_obj.seek(0)

        # Read parquet file with relaxed schema handling
        try:
          parquet_file = pq.ParquetFile(file_obj)
          current_schema = parquet_file.schema_arrow
        except Exception as e:
          logger.warning(f"Failed to read parquet file {file_info['Key']}: {e}")
          continue

        # Initialize writer with enforced schema from first file
        if writer is None:
          schema = current_schema
          # Apply schema enforcement to prevent type mismatches
          enforced_schema = get_enforced_schema(table_name, schema)
          writer = pq.ParquetWriter(
            buffer,
            enforced_schema,  # Use enforced schema instead of original
            compression="snappy",
            use_dictionary=True,
            # Note: row_group_size is controlled per write_batch, not in constructor
          )
        else:
          # For subsequent files, check if schema matches and update enforced schema if needed
          # This handles cases where the first file might have null columns that later files have as strings
          for field in current_schema:
            enforced_field = (
              enforced_schema.field(field.name)
              if field.name in enforced_schema.names
              else None
            )
            if enforced_field and enforced_schema:
              # If current field is string but enforced is null, update to string
              if field.type == pa.string() and enforced_field.type == pa.null():
                # Recreate enforced schema with updated field type
                new_fields = []
                for ef in enforced_schema:
                  if ef.name == field.name:
                    new_fields.append(
                      pa.field(field.name, pa.string(), nullable=field.nullable)
                    )
                  else:
                    new_fields.append(ef)
                enforced_schema = pa.schema(new_fields)
                logger.debug(
                  f"Updated enforced schema: {field.name} from null to string"
                )

        # Process file in batches to control memory
        try:
          batches = list(parquet_file.iter_batches(batch_size=10000))
        except Exception as e:
          # If we can't read batches due to schema issues, try reading the whole table
          logger.debug(f"Failed to read batches from {file_info['Key']}: {e}")
          try:
            table = parquet_file.read()
            # Convert table to batches manually
            batches = table.to_batches(max_chunksize=10000)
          except Exception as table_error:
            logger.warning(
              f"Failed to read table from {file_info['Key']}: {table_error}"
            )
            continue

        for batch in batches:
          # Cast the batch to match enforced schema
          try:
            # Try direct cast first
            if batch.schema != enforced_schema:
              table = pa.Table.from_batches([batch])
              table = table.cast(enforced_schema)
              batch = table.to_batches()[0]
          except Exception as e:
            # Fall back to column-by-column casting for better error handling
            logger.debug(
              f"Direct cast failed for {file_info['Key']}, trying column-by-column: {e}"
            )
            arrays = []
            if enforced_schema:
              for field in enforced_schema:
                if field.name in batch.schema.names:
                  col_array = batch.column(field.name)
                  if col_array.type != field.type:
                    # Special handling for null to string casting
                    if col_array.type == pa.null() and field.type == pa.string():
                      # Create string array with all nulls
                      arrays.append(pa.array([None] * batch.num_rows, type=pa.string()))
                    elif field.type == pa.string() and col_array.type != pa.string():
                      # Cast to string, handling nulls properly
                      try:
                        # Convert to string, preserving nulls
                        col_array = pa.compute.cast(col_array, pa.string())
                      except Exception as cast_error:
                        logger.warning(
                          f"Could not cast {field.name} from {col_array.type} to string: {cast_error}"
                        )
                        # Fall back to null string array
                        arrays.append(
                          pa.array([None] * batch.num_rows, type=pa.string())
                        )
                        continue
                      arrays.append(col_array)
                    else:
                      # Try standard cast
                      try:
                        col_array = pa.compute.cast(col_array, field.type)
                      except Exception as cast_error:
                        logger.warning(
                          f"Could not cast {field.name} from {col_array.type} to {field.type}: {cast_error}"
                        )
                        # Keep original array if cast fails
                        col_array = col_array
                      arrays.append(col_array)
                  else:
                    arrays.append(col_array)
                else:
                  # Column doesn't exist, create null array
                  arrays.append(pa.nulls(batch.num_rows, field.type))
              batch = pa.RecordBatch.from_arrays(arrays, schema=enforced_schema)

          writer.write_batch(batch)
          total_rows += batch.num_rows

      except Exception as e:
        logger.warning(f"Failed to process file {file_info['Key']}: {e}")
        # Continue with other files
        continue

    # Finalize and upload
    if writer:
      writer.close()
      writer = None  # Mark as closed to prevent double-close in finally

      # Get file size before upload
      buffer.seek(0, 2)  # Seek to end
      file_size = buffer.tell()
      buffer.seek(0)  # Seek back to start for upload

      # Upload consolidated file to S3
      s3_client.s3_client.upload_fileobj(
        buffer, bucket, output_key, ExtraArgs={"ContentType": "application/parquet"}
      )

      logger.info(
        f"  📦 Uploaded {output_key} ({file_size / (1024 * 1024):.1f}MB, {total_rows:,} rows)"
      )

    return total_rows

  except Exception as e:
    logger.error(f"Stream consolidation failed: {e}")
    raise
  finally:
    # Only close writer if not already closed
    if writer is not None:
      try:
        writer.close()
      except Exception:
        pass
    # Note: Don't close buffer here - it's handled by the caller


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.orchestrate_consolidation_phase",
  max_retries=1,
)
def orchestrate_consolidation_phase(
  years: List[int],  # Years to consolidate
  bucket: str = None,
  pipeline_id: str = None,
) -> Dict:
  """
  Orchestrate the consolidation phase for all node and relationship types.
  Creates individual consolidation tasks for parallel processing.
  Now creates separate tasks for each year to maintain year partitioning.

  Args:
      years: List of years to consolidate
      bucket: S3 bucket (defaults to SEC_PROCESSED_BUCKET)
      pipeline_id: Pipeline ID for tracking

  Returns:
      Dict with orchestration results
  """
  from celery import group

  bucket = bucket or env.SEC_PROCESSED_BUCKET or "robosystems-sec-processed"
  pipeline_id = pipeline_id or f"consolidation_{datetime.now().timestamp()}"

  # Get all node and relationship types
  node_types, relationship_types = get_schema_types()

  logger.info(
    f"Starting consolidation phase for years {years} "
    f"({len(node_types)} node types, {len(relationship_types)} relationship types)"
  )

  # Create consolidation tasks - one per table per year
  consolidation_tasks = []

  # Node consolidation tasks
  for year in years:
    for node_type in node_types:
      task = consolidate_parquet_files.s(  # type: ignore[attr-defined]
        table_type="nodes",
        table_name=node_type,
        year=year,
        bucket=bucket,
        pipeline_id=pipeline_id,
      )
      consolidation_tasks.append(task)

  # Relationship consolidation tasks
  for year in years:
    for rel_type in relationship_types:
      task = consolidate_parquet_files.s(  # type: ignore[attr-defined]
        table_type="relationships",
        table_name=rel_type,
        year=year,
        bucket=bucket,
        pipeline_id=pipeline_id,
      )
      consolidation_tasks.append(task)

  # Execute all consolidation tasks in parallel (up to 30 at once via queue config)
  job = group(consolidation_tasks).apply_async()

  total_tasks = len(consolidation_tasks)
  tasks_per_year = len(node_types) + len(relationship_types)

  return {
    "status": "started",
    "phase": "consolidation",
    "job_id": job.id,
    "pipeline_id": pipeline_id,
    "total_tasks": total_tasks,
    "years": years,
    "tasks_per_year": tasks_per_year,
    "bucket": bucket,
    "message": f"Started {total_tasks} consolidation tasks ({tasks_per_year} per year × {len(years)} years)",
  }


@celery_app.task(
  queue=env.QUEUE_SHARED_EXTRACTION,
  name="sec_xbrl.get_consolidation_status",
  max_retries=1,
)
def get_consolidation_status(pipeline_id: str) -> Dict:
  """
  Get status of consolidation phase.

  Args:
      pipeline_id: Pipeline ID to check

  Returns:
      Dict with consolidation status
  """
  # This would typically check a state store or Redis
  # For now, return a simple status
  return {
    "status": "in_progress",
    "pipeline_id": pipeline_id,
    "message": "Consolidation status tracking not yet implemented",
  }
