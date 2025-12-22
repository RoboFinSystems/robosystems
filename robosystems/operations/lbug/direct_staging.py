"""Direct file staging for small files.

This module provides a fast path for staging small files directly to DuckDB,
bypassing Dagster job overhead while still reporting AssetMaterializations
for observability in the Dagster UI.

For small files (< SMALL_FILE_STAGING_THRESHOLD_MB), this approach:
- Stages files immediately during the HTTP request
- Reports an AssetMaterialization to Dagster for observability
- Returns control to the caller in milliseconds instead of seconds

For large files, the regular Dagster job should be used for:
- Async processing that won't timeout
- Progress streaming for long operations
- Retry logic for unreliable operations
"""

import asyncio
import time
from typing import Any

from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.logger import logger

# Timeout for Dagster materialization reporting (seconds)
# This prevents the API from hanging if Dagster is unreachable
DAGSTER_REPORT_TIMEOUT = 5.0


async def stage_file_directly(
  db: Session,
  file_id: str,
  graph_id: str,
  table_id: str,
  s3_key: str,
  file_size_bytes: int,
  row_count: int | None = None,
) -> dict[str, Any]:
  """
  Stage a file directly to DuckDB without Dagster orchestration.

  This is the fast path for small files. It:
  1. Gets all uploaded files for the table
  2. Calls the Graph API to create/update the DuckDB staging table
  3. Marks the file as staged in the database
  4. Reports an AssetMaterialization to Dagster for observability

  Args:
      db: SQLAlchemy database session (from FastAPI dependency injection)
      file_id: The file ID to stage
      graph_id: The graph database ID
      table_id: The table ID
      s3_key: The S3 key of the file
      file_size_bytes: Size of the file in bytes
      row_count: Optional row count for the file

  Returns:
      Dict with staging result including status, duration, etc.
  """
  # Lazy imports to avoid circular dependencies
  from robosystems.graph_api.client.factory import GraphClientFactory
  from robosystems.models.iam import GraphFile, GraphTable

  start_time = time.time()

  logger.info(
    f"Direct staging file {file_id} to graph {graph_id} (fast path for small file)"
  )

  try:
    # Get the file and table
    graph_file = GraphFile.get_by_id(file_id, db)
    if not graph_file:
      return {
        "status": "error",
        "message": f"File {file_id} not found",
        "file_id": file_id,
      }

    table = GraphTable.get_by_id(table_id, db)
    if not table:
      return {
        "status": "error",
        "message": f"Table {table_id} not found",
        "file_id": file_id,
      }

    # Get all uploaded files for this table (for proper staging)
    all_files = GraphFile.get_all_for_table(table_id, db)
    uploaded_files = [f for f in all_files if f.upload_status == "uploaded"]

    if not uploaded_files:
      return {
        "status": "skipped",
        "message": "No uploaded files to stage",
        "file_id": file_id,
      }

    # Build file list with S3 URIs
    bucket = env.AWS_S3_BUCKET
    s3_files = [f"s3://{bucket}/{f.s3_key}" for f in uploaded_files]
    file_id_map = {f"s3://{bucket}/{f.s3_key}": f.id for f in uploaded_files}

    logger.info(
      f"Direct staging {len(s3_files)} files to DuckDB table {table.table_name}"
    )

    # Stage via Graph API
    client = await GraphClientFactory.create_client(
      graph_id=graph_id, operation_type="write"
    )

    try:
      staging_result = await client.create_table(
        graph_id=graph_id,
        table_name=table.table_name,
        s3_pattern=s3_files,
        file_id_map=file_id_map,
      )
      logger.debug(f"DuckDB staging result: {staging_result}")
    finally:
      await client.close()

    # Mark file as staged
    graph_file.mark_duckdb_staged(session=db, row_count=row_count or 0)

    duration_ms = (time.time() - start_time) * 1000

    logger.info(f"Direct staging completed for file {file_id} in {duration_ms:.2f}ms")

    # Report AssetMaterialization to Dagster for observability (fire-and-forget with timeout)
    await _report_staging_materialization(
      file_id=file_id,
      graph_id=graph_id,
      table_name=table.table_name,
      file_size_bytes=file_size_bytes,
      row_count=row_count,
      duration_ms=duration_ms,
      files_staged=len(s3_files),
    )

    return {
      "status": "success",
      "file_id": file_id,
      "graph_id": graph_id,
      "table_name": table.table_name,
      "files_staged": len(s3_files),
      "duckdb_status": "staged",
      "duration_ms": duration_ms,
      "method": "direct",
    }

  except Exception as e:
    duration_ms = (time.time() - start_time) * 1000
    logger.error(f"Direct staging failed for file {file_id}: {e}")
    return {
      "status": "error",
      "file_id": file_id,
      "message": str(e),
      "duration_ms": duration_ms,
      "method": "direct",
    }


async def _report_staging_materialization(
  file_id: str,
  graph_id: str,
  table_name: str,
  file_size_bytes: int,
  row_count: int | None,
  duration_ms: float,
  files_staged: int,
) -> None:
  """
  Report an AssetMaterialization to Dagster for observability.

  This allows direct-staged files to appear in the Dagster UI alongside
  files staged via Dagster jobs, providing a unified view of all staging events.

  Uses a timeout to prevent blocking if Dagster is unreachable.
  """
  try:
    # Run the blocking Dagster operations in a thread with timeout
    await asyncio.wait_for(
      asyncio.to_thread(
        _report_staging_materialization_sync,
        file_id,
        graph_id,
        table_name,
        file_size_bytes,
        row_count,
        duration_ms,
        files_staged,
      ),
      timeout=DAGSTER_REPORT_TIMEOUT,
    )
    logger.info(f"Reported AssetMaterialization for file {file_id} to Dagster")

  except TimeoutError:
    logger.warning(
      f"Dagster materialization reporting timed out for file {file_id} after {DAGSTER_REPORT_TIMEOUT}s. "
      "Staging succeeded but won't appear in Dagster UI."
    )
  except Exception as e:
    # Don't fail staging if Dagster reporting fails - it's just observability
    logger.warning(
      f"Failed to report AssetMaterialization to Dagster for file {file_id}: {e}. "
      "Staging succeeded but won't appear in Dagster UI."
    )


def _report_staging_materialization_sync(
  file_id: str,
  graph_id: str,
  table_name: str,
  file_size_bytes: int,
  row_count: int | None,
  duration_ms: float,
  files_staged: int,
) -> None:
  """Synchronous version of materialization reporting (runs in thread)."""
  from dagster import AssetKey, AssetMaterialization, DagsterInstance, MetadataValue

  # Get the Dagster instance - this may block if config is unavailable
  instance = DagsterInstance.get()

  # Create the materialization event
  # Use static asset key so it appears in Dagster UI (graph_id in metadata)
  materialization = AssetMaterialization(
    asset_key=AssetKey("staged_files"),
    description=f"Direct staging of {files_staged} file(s) to table {table_name}",
    metadata={
      "file_id": MetadataValue.text(file_id),
      "graph_id": MetadataValue.text(graph_id),
      "table_name": MetadataValue.text(table_name),
      "file_size_bytes": MetadataValue.int(file_size_bytes),
      "row_count": MetadataValue.int(row_count or 0),
      "duration_ms": MetadataValue.float(duration_ms),
      "files_staged": MetadataValue.int(files_staged),
      "method": MetadataValue.text("direct"),
    },
  )

  # Report the materialization
  instance.report_runless_asset_event(materialization)
