"""
Graph Ingestion Task for v2 Incremental Ingestion

Handles selective ingestion from DuckDB staging tables to graph database
via Graph API. Works with any graph backend (Kuzu, Neo4j, etc.).
Filters by file_id to enable surgical updates without reprocessing all data.

Key features:
- Selective ingestion by file_id (WHERE file_id = ...)
- Async processing via Celery
- Uses Graph API client factory for ECS→EC2 routing
- Backend-agnostic (works with any graph database)
- Tracks graph ingestion status in PostgreSQL
"""

import asyncio
from typing import Dict, Any
from datetime import datetime, timezone

from celery import Task

from robosystems.celery import celery_app, QUEUE_DEFAULT
from robosystems.logger import logger
from robosystems.database import session
from robosystems.models.iam import GraphFile
from robosystems.graph_api.client.factory import GraphClientFactory


@celery_app.task(
  bind=True,
  queue=QUEUE_DEFAULT,
  name="table_operations.ingest_file_to_graph",
  max_retries=3,
  default_retry_delay=120,
)
def ingest_file_to_graph(
  self: Task,
  file_id: str,
  graph_id: str,
  table_name: str,
) -> Dict[str, Any]:
  """
  Ingest a single file from DuckDB staging to graph database (selective ingestion).

  This task runs after DuckDB staging completes. It ingests ONLY the rows
  for the specified file_id, enabling incremental updates without full rebuilds.

  NOTE: This is for ADDITIONS only. Deletions require marking graph stale
  and triggering a full materialization later.

  Args:
      file_id: GraphFile.id of the file to ingest
      graph_id: Graph database identifier
      table_name: DuckDB table name to ingest from

  Returns:
      Dict with status, rows_ingested, and timing info
  """
  start_time = datetime.now(timezone.utc)

  logger.info(
    f"Starting selective graph ingestion for file {file_id} in graph {graph_id}, table {table_name}"
  )

  try:
    graph_file = GraphFile.get_by_id(file_id, session)
    if not graph_file:
      raise ValueError(f"File {file_id} not found")

    if graph_file.duckdb_status != "staged":
      logger.warning(
        f"File {file_id} not staged in DuckDB (status: {graph_file.duckdb_status}). Skipping graph ingestion."
      )
      return {
        "status": "skipped",
        "message": f"File not staged in DuckDB (status: {graph_file.duckdb_status})",
        "file_id": file_id,
      }

    logger.info(
      f"Ingesting file {file_id} to graph using Graph API (selective ingestion)"
    )

    client = asyncio.run(
      GraphClientFactory.create_client(graph_id=graph_id, operation_type="write")
    )

    # Selective ingestion: only ingest this specific file_id
    result = asyncio.run(
      client.ingest_table_to_graph(
        graph_id=graph_id,
        table_name=table_name,
        ignore_errors=True,
        file_ids=[file_id],  # Filter by this specific file
      )
    )

    rows_ingested = result.get("rows_ingested", 0)
    execution_time_ms = result.get("execution_time_ms", 0)

    logger.info(
      f"Successfully ingested {rows_ingested} rows for file {file_id} to graph in {execution_time_ms:.2f}ms"
    )

    graph_file.mark_graph_ingested(session=session)

    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

    logger.info(f"File {file_id} marked as graph ingested in {execution_time:.2f}s")

    return {
      "status": "success",
      "file_id": file_id,
      "graph_id": graph_id,
      "table_name": table_name,
      "rows_ingested": rows_ingested,
      "execution_time_seconds": execution_time,
      "graph_status": "ingested",
    }

  except Exception as e:
    execution_time = (datetime.now(timezone.utc) - start_time).total_seconds()

    logger.error(
      f"Graph ingestion failed for file {file_id} after {execution_time:.2f}s: {e}"
    )

    raise self.retry(exc=e, countdown=self.default_retry_delay)
