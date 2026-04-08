"""Direct graph materialization without Dagster orchestration.

This module provides a fast path for materializing DuckDB staging tables
to the graph database, bypassing Dagster job overhead while still reporting
AssetMaterializations for observability in the Dagster UI.

For user-triggered materializations, this approach:
- Executes materialization immediately in the API worker process
- Emits SSE events for real-time progress tracking
- Reports an AssetMaterialization to Dagster for observability
- Eliminates Dagster cold start latency (5-15s+)

For large or long-running materializations, the regular Dagster job can be
used as a fallback by disabling DIRECT_GRAPH_MATERIALIZATION_ENABLED.

Pattern follows: robosystems/middleware/sse/direct_monitor.py
"""

import time
from typing import Any

from sqlalchemy.orm import Session

from robosystems.logger import logger


async def materialize_graph_directly(
  db: Session,
  graph_id: str,
  force: bool = False,
  rebuild: bool = False,
  ignore_errors: bool = True,
  materialize_embeddings: bool = False,
  operation_id: str | None = None,
) -> dict[str, Any]:
  """
  Materialize all DuckDB staging tables to the graph database directly.

  This is the fast path equivalent of the Dagster materialize_graph_tables op.

  Args:
      db: SQLAlchemy database session
      graph_id: Graph database identifier
      force: Force materialization even if graph is not stale
      rebuild: Delete and recreate graph database before materialization
      ignore_errors: Continue ingestion on row errors
      materialize_embeddings: Include embedding columns and build HNSW vector indexes
      operation_id: Optional SSE operation ID for progress tracking

  Returns:
      Dict with materialization result including status, tables, rows, etc.
  """
  from robosystems.graph_api.client.factory import GraphClientFactory
  from robosystems.middleware.sse.operation_manager import get_operation_manager
  from robosystems.models.core import Graph, GraphFile, GraphSchema, GraphTable

  from .chunked_materialization import materialize_table_chunked

  start_time = time.time()
  manager = get_operation_manager()

  logger.info(
    f"Direct materialization starting for {graph_id} "
    f"(force={force}, rebuild={rebuild}, ignore_errors={ignore_errors})"
  )

  if operation_id:
    await manager.emit_progress(operation_id, "Starting materialization...", 0)

  try:
    # Verify graph exists
    graph_record = Graph.get_by_id(graph_id, db)
    if not graph_record:
      error_result = {
        "status": "error",
        "graph_id": graph_id,
        "message": f"Graph {graph_id} not found",
      }
      if operation_id:
        await manager.fail_operation(operation_id, error=error_result["message"])
      return error_result

    # Recover stale files before proceeding
    recovered_ids = GraphFile.recover_stale_files(graph_id, db)
    if recovered_ids:
      logger.info(
        f"Recovered {len(recovered_ids)} stale files for graph {graph_id}: {recovered_ids}"
      )
      # Re-stage recovered files
      await _restage_stale_files(db, graph_id, recovered_ids)

    # Check staleness
    was_stale = graph_record.graph_stale or False
    stale_reason = graph_record.graph_stale_reason

    if not was_stale and not force and not rebuild:
      # Check if there are any tables with staged data in DuckDB
      staged_tables_count = (
        db.query(GraphTable)
        .join(GraphFile, GraphTable.id == GraphFile.table_id)
        .filter(GraphTable.graph_id == graph_id)
        .filter(GraphFile.duckdb_status == "staged")
        .distinct()
        .count()
      )
      if staged_tables_count > 0:
        was_stale = True
        stale_reason = "new_data_staged"
        logger.info(
          f"Graph {graph_id} has {staged_tables_count} tables with staged data"
        )

    if not was_stale and not force and not rebuild:
      logger.info(
        f"Graph {graph_id} is not stale and force=false, rebuild=false - skipping"
      )
      result = {
        "status": "skipped",
        "graph_id": graph_id,
        "was_stale": False,
        "tables_materialized": [],
        "total_rows": 0,
        "execution_time_ms": (time.time() - start_time) * 1000,
        "message": "Graph is fresh - no materialization needed",
      }
      if operation_id:
        await manager.complete_operation(
          operation_id, result=result, message=result["message"]
        )
      return result

    # Get graph client
    client = await GraphClientFactory.create_client(
      graph_id=graph_id, operation_type="write"
    )

    try:
      # Handle rebuild if requested
      if rebuild:
        if operation_id:
          await manager.emit_progress(
            operation_id, "Rebuild requested - regenerating graph database", 10
          )

        graph_metadata = (
          {**graph_record.graph_metadata} if graph_record.graph_metadata else {}
        )
        graph_metadata["status"] = "rebuilding"
        graph_metadata["rebuild_started_at"] = time.time()
        graph_record.graph_metadata = graph_metadata
        db.commit()

        try:
          logger.info(f"[20%] Deleting graph database for {graph_id}")
          if operation_id:
            await manager.emit_progress(operation_id, "Deleting graph database...", 20)
          await client.delete_database(graph_id, preserve_duckdb=True)

          schema = GraphSchema.get_active_schema(graph_id, db)
          if not schema:
            raise ValueError(f"No schema found for graph {graph_id}")

          schema_type_for_rebuild = "custom" if schema.schema_ddl else "entity"
          logger.info(
            f"[30%] Recreating graph database with schema type: {schema_type_for_rebuild}"
          )
          if operation_id:
            await manager.emit_progress(
              operation_id,
              f"Recreating graph with schema type: {schema_type_for_rebuild}",
              30,
            )
          await client.create_database(
            graph_id=graph_id,
            schema_type=schema_type_for_rebuild,
            custom_schema_ddl=schema.schema_ddl,
          )
          logger.info("[40%] Graph database recreated successfully")

        except Exception as e:
          graph_metadata["status"] = "rebuild_failed"
          graph_metadata["rebuild_failed_at"] = time.time()
          graph_metadata["rebuild_error"] = str(e)
          graph_record.graph_metadata = graph_metadata
          db.commit()
          raise

      # Get tables with staged data
      if operation_id:
        await manager.emit_progress(operation_id, "Discovering staged tables...", 45)

      tables_with_staged_data = (
        db.query(GraphTable)
        .join(GraphFile, GraphTable.id == GraphFile.table_id)
        .filter(GraphTable.graph_id == graph_id)
        .filter(GraphFile.duckdb_status == "staged")
        .distinct()
        .all()
      )

      if not tables_with_staged_data:
        logger.info(f"No tables with staged data found for graph {graph_id}")
        result = {
          "status": "success",
          "graph_id": graph_id,
          "was_stale": was_stale,
          "stale_reason": stale_reason,
          "tables_materialized": [],
          "total_rows": 0,
          "execution_time_ms": (time.time() - start_time) * 1000,
          "message": "No tables with staged data to materialize",
        }
        if operation_id:
          await manager.complete_operation(
            operation_id, result=result, message=result["message"]
          )
        return result

      # Sort tables: nodes before relationships (using table_type field)
      node_tables = [
        t.table_name for t in tables_with_staged_data if t.table_type == "node"
      ]
      rel_tables = [
        t.table_name for t in tables_with_staged_data if t.table_type == "relationship"
      ]
      ordered_tables = node_tables + rel_tables

      logger.info(
        f"[50%] Materializing {len(ordered_tables)} tables: "
        f"{len(node_tables)} nodes, {len(rel_tables)} relationships"
      )

      if operation_id:
        await manager.emit_progress(
          operation_id,
          f"Materializing {len(ordered_tables)} tables ({len(node_tables)} nodes, {len(rel_tables)} relationships)",
          50,
        )

      # Materialize each table
      tables_materialized = []
      total_rows = 0
      base_progress = 50
      progress_per_table = 40 / max(len(ordered_tables), 1)

      for i, table_name in enumerate(ordered_tables):
        progress = int(base_progress + (i * progress_per_table))

        try:
          logger.info(f"[{progress}%] Materializing table {table_name}")
          if operation_id:
            await manager.emit_progress(
              operation_id,
              f"Materializing table {table_name} ({i + 1}/{len(ordered_tables)})",
              progress,
            )

          mat_result = await materialize_table_chunked(
            client=client,
            graph_id=graph_id,
            table_name=table_name,
            tier=graph_record.graph_tier or "ladybug-standard",
            ignore_errors=ignore_errors,
            materialize_embeddings=materialize_embeddings,
            file_ids=None,
          )

          rows_ingested = mat_result.get("rows_ingested", 0)
          total_rows += rows_ingested
          tables_materialized.append(table_name)

          logger.info(f"Materialized {table_name}: {rows_ingested:,} rows")

        except Exception as e:
          logger.error(f"Failed to materialize table {table_name}: {e}")
          if not ignore_errors:
            raise

      # Mark graph as fresh
      logger.info("[95%] Marking graph as fresh")
      if operation_id:
        await manager.emit_progress(operation_id, "Marking graph as fresh...", 95)
      graph_record.mark_fresh(session=db)

      # Update graph metadata if rebuild was performed
      if rebuild:
        graph_metadata = (
          {**graph_record.graph_metadata} if graph_record.graph_metadata else {}
        )
        graph_metadata["status"] = "available"
        graph_metadata["rebuild_completed_at"] = time.time()
        if "rebuild_started_at" in graph_metadata:
          rebuild_duration = (
            graph_metadata["rebuild_completed_at"]
            - graph_metadata["rebuild_started_at"]
          )
          graph_metadata["last_rebuild_duration_seconds"] = rebuild_duration
        graph_record.graph_metadata = graph_metadata
        db.commit()

      execution_time_ms = (time.time() - start_time) * 1000

      logger.info(
        f"[100%] Direct materialization complete: {len(tables_materialized)} tables, "
        f"{total_rows:,} rows in {execution_time_ms:.2f}ms"
      )

      result = {
        "status": "success",
        "graph_id": graph_id,
        "was_stale": was_stale,
        "stale_reason": stale_reason,
        "tables_materialized": tables_materialized,
        "total_rows": total_rows,
        "execution_time_ms": execution_time_ms,
        "rebuild": rebuild,
        "message": f"Graph materialized successfully from {len(tables_materialized)} tables",
        "method": "direct",
      }

      if operation_id:
        await manager.complete_operation(
          operation_id, result=result, message=result["message"]
        )

      # Report AssetMaterialization to Dagster (fire-and-forget)
      from robosystems.dagster.reporting import report_asset_materialization

      await report_asset_materialization(
        asset_key="user_graph_materialized",
        description=f"Direct materialization of {len(tables_materialized)} tables to graph {graph_id}",
        metadata={
          "graph_id": graph_id,
          "tables_materialized": len(tables_materialized),
          "total_rows": total_rows,
          "duration_ms": execution_time_ms,
          "rebuild": rebuild,
          "materialization_method": "direct",
        },
      )

      return result

    finally:
      await client.close()

  except Exception as e:
    duration_ms = (time.time() - start_time) * 1000
    logger.error(f"Direct materialization failed for {graph_id}: {e}")

    error_result = {
      "status": "error",
      "graph_id": graph_id,
      "message": str(e),
      "duration_ms": duration_ms,
      "method": "direct",
    }

    if operation_id:
      await manager.fail_operation(
        operation_id,
        error=str(e),
        error_details={"error_type": type(e).__name__},
      )

    return error_result


async def _restage_stale_files(
  db: Session,
  graph_id: str,
  file_ids: list[str],
) -> None:
  """
  Re-stage stale files by running direct staging for each.

  Args:
      db: SQLAlchemy session
      graph_id: Graph database identifier
      file_ids: List of file IDs to re-stage
  """
  from robosystems.models.core import GraphFile

  from .direct_staging import stage_file_directly

  for file_id in file_ids:
    graph_file = GraphFile.get_by_id(file_id, db)
    if not graph_file:
      logger.warning(f"Stale file {file_id} not found during recovery, skipping")
      continue

    logger.info(f"Re-staging stale file {file_id} ({graph_file.file_name})")
    try:
      result = await stage_file_directly(
        db=db,
        file_id=file_id,
        graph_id=graph_id,
        table_id=graph_file.table_id,
        s3_key=graph_file.s3_key,
        file_size_bytes=graph_file.file_size_bytes,
        row_count=graph_file.row_count,
      )
      if result.get("status") == "success":
        logger.info(f"Successfully re-staged stale file {file_id}")
      else:
        logger.warning(
          f"Re-staging stale file {file_id} returned: {result.get('status')}"
        )
    except Exception as e:
      logger.error(f"Failed to re-stage stale file {file_id}: {e}")
