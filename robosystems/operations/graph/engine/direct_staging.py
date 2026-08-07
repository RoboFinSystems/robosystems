"""In-request staging of small files into DuckDB.

Files under ``SMALL_FILE_STAGING_THRESHOLD_MB`` are staged inside the HTTP
request — milliseconds instead of seconds — while still reporting an
AssetMaterialization so the run appears in the Dagster UI.

Larger files belong on the Dagster job, which gets async execution beyond the
request timeout, streamed progress, and retries.
"""

import time
from typing import Any

from sqlalchemy.orm import Session

from robosystems.config import env
from robosystems.logger import logger


async def stage_file_directly(
  db: Session,
  file_id: str,
  graph_id: str,
  table_id: str,
  s3_key: str,
  file_size_bytes: int,
  row_count: int | None = None,
) -> dict[str, Any]:
  """Stage one uploaded file into its DuckDB table, in this process.

  The staging call covers *every* uploaded file for the table, not just this
  one — the DuckDB table is rebuilt from the full set each time. Errors are
  returned as ``{"status": "error", ...}`` rather than raised.
  """
  from robosystems.graph_api.client.factory import GraphClientFactory
  from robosystems.models.core import GraphFile, GraphTable

  start_time = time.time()

  logger.info(
    f"Direct staging file {file_id} to graph {graph_id} (fast path for small file)"
  )

  try:
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

    all_files = GraphFile.get_all_for_table(table_id, db)
    uploaded_files = [f for f in all_files if f.upload_status == "uploaded"]

    if not uploaded_files:
      return {
        "status": "skipped",
        "message": "No uploaded files to stage",
        "file_id": file_id,
      }

    # Build file list with S3 URIs
    bucket = env.USER_DATA_BUCKET
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
      # Check if the DuckDB table already exists to use incremental INSERT INTO
      existing_tables = await client.list_tables(graph_id)
      existing_table_names = [t["table_name"] for t in existing_tables]

      if table.table_name in existing_table_names:
        # Table exists - use INSERT INTO for just the new file (incremental)
        new_file_s3 = f"s3://{bucket}/{s3_key}"
        logger.info(
          f"Table {table.table_name} exists, using INSERT INTO for file {s3_key}"
        )
        staging_result = await client.insert_into_table(
          graph_id=graph_id,
          table_name=table.table_name,
          s3_pattern=[new_file_s3],
          deduplicate=True,
          file_id_map={new_file_s3: file_id},
        )
      else:
        # Table does not exist - create with all files (first-file path)
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
    from robosystems.dagster.reporting import report_asset_materialization

    await report_asset_materialization(
      asset_key="user_graph_file_staging",
      description=f"Direct staging of {len(s3_files)} file(s) to table {table.table_name}",
      metadata={
        "file_id": file_id,
        "graph_id": graph_id,
        "table_name": table.table_name,
        "file_size_bytes": file_size_bytes,
        "row_count": row_count or 0,
        "duration_ms": duration_ms,
        "files_staged": len(s3_files),
        "staging_method": "direct",
      },
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
