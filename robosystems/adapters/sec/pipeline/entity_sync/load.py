"""SEC Entity Sync Load Asset.

Registers parquet files as GraphFiles, uploads to the user's S3 bucket,
stages to DuckDB on the graph instance, and materializes to LadybugDB.

Follows the same pattern as the QuickBooks load asset.
"""

import asyncio
import json
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import SECEntitySyncConfig
from .utils import get_pipeline_work_dir


@asset(
  group_name="sec_entity_pipeline",
  description="Load SEC parquet files to Graph API DuckDB staging and materialize to LadybugDB",
  deps=["sec_entity_transform"],
  kinds={"s3", "duckdb", "ladybug"},
  metadata={
    "pipeline": "sec_entity",
    "stage": "load",
  },
)
def sec_entity_load(
  context: AssetExecutionContext,
  config: SECEntitySyncConfig,
) -> MaterializeResult:
  """Load SEC parquet files into the user's graph database.

  For each table:
  1. Get or create GraphTable record
  2. Create GraphFile record
  3. Upload parquet to USER_DATA_BUCKET
  4. Stage to DuckDB on graph instance via Graph API
  5. Materialize from DuckDB to LadybugDB

  Returns:
      MaterializeResult with load statistics
  """
  work_dir = get_pipeline_work_dir(config.graph_id)
  output_dir = work_dir / "output"
  meta_path = work_dir / "table_metadata.json"

  if not meta_path.exists():
    raise ValueError(
      f"Table metadata not found at {meta_path}. Run sec_entity_transform first."
    )

  table_meta = json.loads(meta_path.read_text())

  context.log.info(
    f"Loading SEC data for graph={config.graph_id}, tables={len(table_meta)}"
  )

  result = asyncio.run(_load_tables(context, config, output_dir, table_meta))

  return MaterializeResult(metadata=result)


async def _load_tables(
  context: AssetExecutionContext,
  config: SECEntitySyncConfig,
  output_dir: Path,
  table_meta: dict[str, dict[str, str]],
) -> dict:
  """Load all SEC tables: register, upload, stage, materialize."""
  from robosystems.config import env
  from robosystems.config.storage.graph import get_staging_key
  from robosystems.database import SessionFactory
  from robosystems.graph_api.client.factory import get_graph_client
  from robosystems.models.core.graph.graph_file import GraphFile
  from robosystems.models.core.graph.graph_table import GraphTable
  from robosystems.operations.aws.s3 import S3Client
  from robosystems.operations.connection_service import ConnectionService

  s3_client = S3Client()
  bucket = env.USER_DATA_BUCKET
  tables_staged = 0
  tables_materialized = 0
  total_rows = 0

  # Build ordered table list: all nodes first, then all relationships
  # This ensures foreign key constraints are satisfied during materialization
  node_tables = [
    name for name, meta in table_meta.items() if meta["entity_type"] == "nodes"
  ]
  rel_tables = [
    name for name, meta in table_meta.items() if meta["entity_type"] == "relationships"
  ]
  ordered_tables = node_tables + rel_tables

  with SessionFactory() as session:
    # Stage all tables (upload to S3, create DuckDB tables)
    for table_name in ordered_tables:
      meta = table_meta[table_name]
      entity_type = meta["entity_type"]
      table_type = "node" if entity_type == "nodes" else "relationship"
      parquet_file = output_dir / f"sec_{table_name}.parquet"

      if not parquet_file.exists():
        context.log.info(f"Skipping {table_name}: no parquet file")
        continue

      file_size = parquet_file.stat().st_size
      row_count = _count_parquet_rows(parquet_file)

      context.log.info(f"Loading {table_name}: {row_count} rows, {file_size:,} bytes")

      # 1. Get or create GraphTable
      graph_table = GraphTable.get_by_name(config.graph_id, table_name, session)
      if not graph_table:
        graph_table = GraphTable.create(
          graph_id=config.graph_id,
          table_name=table_name,
          table_type=table_type,
          schema_json={},
          session=session,
        )
        context.log.info(f"Created GraphTable: {table_name}")

      # 2. Create GraphFile record
      file_name = f"sec_{table_name}.parquet"
      graph_file = GraphFile.create(
        graph_id=config.graph_id,
        table_id=graph_table.id,
        file_name=file_name,
        s3_key="",  # Set after computing key
        file_format="parquet",
        file_size_bytes=file_size,
        upload_method="pipeline",
        session=session,
        row_count=row_count,
        upload_status="pending",
      )

      # 3. Upload to S3
      s3_key = get_staging_key(
        user_id=config.user_id,
        graph_id=config.graph_id,
        table_name=table_name,
        file_id=graph_file.id,
        filename=file_name,
      )
      graph_file.s3_key = s3_key
      session.commit()

      s3_client.upload_file(
        file_path=str(parquet_file),
        bucket=bucket,
        key=s3_key,
        content_type="application/octet-stream",
      )
      graph_file.mark_uploaded(session=session)

      # Update table-level statistics (aligned with API upload flow)
      graph_table.update_stats(
        session=session,
        row_count=(graph_table.row_count or 0) + row_count,
        file_count=(graph_table.file_count or 0) + 1,
        total_size_bytes=(graph_table.total_size_bytes or 0) + file_size,
      )

      context.log.info(f"Uploaded {file_name} → s3://{bucket}/{s3_key}")

      # 4. Stage to DuckDB on graph instance
      s3_uri = f"s3://{bucket}/{s3_key}"
      file_id_map = {s3_uri: graph_file.id}

      client = await get_graph_client(config.graph_id, operation_type="write")

      try:
        await client.create_table(
          graph_id=config.graph_id,
          table_name=table_name,
          s3_pattern=[s3_uri],
          file_id_map=file_id_map,
        )
        graph_file.mark_duckdb_staged(session=session, row_count=row_count)
        tables_staged += 1
        context.log.info(f"Staged {table_name} to DuckDB")
      except Exception as e:
        context.log.error(f"Failed to stage {table_name}: {e}")
        raise

    # 5. Materialize all staged tables to LadybugDB
    context.log.info("Materializing all tables to LadybugDB...")
    client = await get_graph_client(config.graph_id, operation_type="write")

    for table_name in ordered_tables:
      parquet_file = output_dir / f"sec_{table_name}.parquet"
      if not parquet_file.exists():
        continue

      try:
        mat_result = await client.materialize_table(
          graph_id=config.graph_id,
          table_name=table_name,
        )
        rows = mat_result.get("rows_ingested", 0) if mat_result else 0
        total_rows += rows
        tables_materialized += 1
        context.log.info(f"Materialized {table_name}: {rows} rows")

        # Mark files as ingested
        graph_table = GraphTable.get_by_name(config.graph_id, table_name, session)
        if graph_table:
          files = GraphFile.get_all_for_table(graph_table.id, session)
          for f in files:
            if f.duckdb_status == "staged" and f.graph_status != "ingested":
              f.mark_graph_ingested(session=session)
      except Exception as e:
        context.log.error(f"Failed to materialize {table_name}: {e}")
        raise

    # 6. Update last sync timestamp
    try:
      await ConnectionService.update_last_sync(config.connection_id, config.graph_id)
      context.log.info("Updated connection last_sync timestamp")
    except Exception as e:
      context.log.warning(f"Failed to update last_sync (non-fatal): {e}")

  context.log.info(
    f"Load complete: {tables_staged} staged, "
    f"{tables_materialized} materialized, {total_rows} total rows"
  )

  return {
    "graph_id": config.graph_id,
    "cik": config.cik,
    "tables_staged": tables_staged,
    "tables_materialized": tables_materialized,
    "total_rows": total_rows,
  }


def _count_parquet_rows(path: Path) -> int:
  """Count rows in a parquet file without loading it fully."""
  import pyarrow.parquet as pq

  try:
    metadata = pq.read_metadata(str(path))
    return metadata.num_rows
  except Exception:
    return 0
