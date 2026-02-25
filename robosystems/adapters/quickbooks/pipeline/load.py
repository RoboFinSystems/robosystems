"""QuickBooks Load Asset.

Registers parquet files as GraphFiles, uploads to S3, stages to
DuckDB on the graph instance, and materializes to LadybugDB.
"""

import asyncio
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import QBSyncConfig
from .utils import QB_ALL_TABLES, get_pipeline_work_dir

# Map dbt output table names to schema table names (PascalCase for nodes)
TABLE_NAME_MAP = {
  "entity": "Entity",
  "element": "Element",
  "dimension": "Dimension",
  "transaction": "Transaction",
  "line_item": "LineItem",
  "entity_has_transaction": "ENTITY_HAS_TRANSACTION",
  "transaction_has_line_item": "TRANSACTION_HAS_LINE_ITEM",
  "line_item_relates_to_element": "LINE_ITEM_RELATES_TO_ELEMENT",
  "line_item_has_dimension": "LINE_ITEM_HAS_DIMENSION",
}

# Determine table type for GraphTable registration
TABLE_TYPES = {
  "Entity": "node",
  "Element": "node",
  "Dimension": "node",
  "Transaction": "node",
  "LineItem": "node",
  "ENTITY_HAS_TRANSACTION": "relationship",
  "TRANSACTION_HAS_LINE_ITEM": "relationship",
  "LINE_ITEM_RELATES_TO_ELEMENT": "relationship",
  "LINE_ITEM_HAS_DIMENSION": "relationship",
}


@asset(
  group_name="qb_pipeline",
  description="Load QB parquet files to Graph API DuckDB staging and materialize to LadybugDB",
  deps=["qb_transform"],
  kinds={"s3", "duckdb", "ladybug"},
  metadata={
    "pipeline": "quickbooks",
    "stage": "load",
  },
)
def qb_load(
  context: AssetExecutionContext,
  config: QBSyncConfig,
) -> MaterializeResult:
  """Load QB parquet files into the graph database.

  For each table:
  1. Get or create GraphTable record
  2. Create GraphFile record
  3. Upload parquet to S3
  4. Stage to DuckDB on graph instance via Graph API
  5. Materialize from DuckDB to LadybugDB

  Returns:
      MaterializeResult with load statistics
  """
  # Get output path from shared pipeline directory
  work_dir = get_pipeline_work_dir(config.graph_id)
  output_dir = work_dir / "output"

  context.log.info(
    f"Loading QB data for graph={config.graph_id}, output_dir={output_dir}"
  )

  result = asyncio.run(_load_tables(context, config, output_dir))

  return MaterializeResult(metadata=result)


async def _load_tables(
  context: AssetExecutionContext,
  config: QBSyncConfig,
  output_dir: Path,
) -> dict:
  """Load all QB tables: register, upload, stage, materialize."""
  from robosystems.config import env
  from robosystems.config.storage.graph import get_staging_key
  from robosystems.database import SessionFactory
  from robosystems.graph_api.client.factory import get_graph_client
  from robosystems.models.iam.graph_file import GraphFile
  from robosystems.models.iam.graph_table import GraphTable
  from robosystems.operations.aws.s3 import S3Client
  from robosystems.operations.connection_service import ConnectionService

  s3_client = S3Client()
  bucket = env.USER_DATA_BUCKET
  tables_staged = 0
  tables_materialized = 0
  total_rows = 0

  with SessionFactory() as session:
    # Process tables in dependency order (nodes first, then relationships)
    for dbt_table in QB_ALL_TABLES:
      schema_table = TABLE_NAME_MAP[dbt_table]
      table_type = TABLE_TYPES[schema_table]
      parquet_file = output_dir / f"qb_{dbt_table}.parquet"

      if not parquet_file.exists():
        context.log.info(f"Skipping {schema_table}: no parquet file")
        continue

      file_size = parquet_file.stat().st_size
      row_count = _count_parquet_rows(parquet_file)

      context.log.info(f"Loading {schema_table}: {row_count} rows, {file_size:,} bytes")

      # 1. Get or create GraphTable
      graph_table = GraphTable.get_by_name(config.graph_id, schema_table, session)
      if not graph_table:
        graph_table = GraphTable.create(
          graph_id=config.graph_id,
          table_name=schema_table,
          table_type=table_type,
          schema_json={},
          session=session,
        )
        context.log.info(f"Created GraphTable: {schema_table}")

      # 2. Create GraphFile record
      file_name = f"qb_{schema_table}.parquet"
      graph_file = GraphFile.create(
        graph_id=config.graph_id,
        table_id=graph_table.id,
        file_name=file_name,
        s3_key="",  # Will be set after computing key
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
        table_name=schema_table,
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
      graph_file.upload_status = "uploaded"
      session.commit()

      context.log.info(f"Uploaded {file_name} → s3://{bucket}/{s3_key}")

      # 4. Stage to DuckDB on graph instance
      s3_uri = f"s3://{bucket}/{s3_key}"
      file_id_map = {s3_uri: graph_file.id}

      client = await get_graph_client(config.graph_id, operation_type="write")

      try:
        await client.create_table(
          graph_id=config.graph_id,
          table_name=schema_table,
          s3_pattern=[s3_uri],
          file_id_map=file_id_map,
        )
        graph_file.mark_duckdb_staged(session=session, row_count=row_count)
        tables_staged += 1
        context.log.info(f"Staged {schema_table} to DuckDB")
      except Exception as e:
        context.log.error(f"Failed to stage {schema_table}: {e}")
        raise

    # 5. Materialize all staged tables to LadybugDB
    context.log.info("Materializing all tables to LadybugDB...")
    client = await get_graph_client(config.graph_id, operation_type="write")

    for dbt_table in QB_ALL_TABLES:
      schema_table = TABLE_NAME_MAP[dbt_table]
      parquet_file = output_dir / f"qb_{dbt_table}.parquet"
      if not parquet_file.exists():
        continue

      try:
        mat_result = await client.materialize_table(
          graph_id=config.graph_id,
          table_name=schema_table,
          ignore_errors=True,
        )
        rows = mat_result.get("rows_ingested", 0) if mat_result else 0
        total_rows += rows
        tables_materialized += 1
        context.log.info(f"Materialized {schema_table}: {rows} rows")

        # Mark files as ingested
        graph_table = GraphTable.get_by_name(config.graph_id, schema_table, session)
        if graph_table:
          files = GraphFile.get_all_for_table(graph_table.id, session)
          for f in files:
            if f.duckdb_status == "staged" and f.graph_status != "ingested":
              f.mark_graph_ingested(session=session)
      except Exception as e:
        context.log.error(f"Failed to materialize {schema_table}: {e}")
        raise

    # 6. Update last sync timestamp
    try:
      await ConnectionService.update_last_sync(config.connection_id, config.graph_id)
      context.log.info("Updated connection last_sync timestamp")
    except Exception as e:
      context.log.warning(f"Failed to update last_sync (non-fatal): {e}")

  context.log.info(
    f"Load complete: {tables_staged} staged, {tables_materialized} materialized, "
    f"{total_rows} total rows"
  )

  return {
    "graph_id": config.graph_id,
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
