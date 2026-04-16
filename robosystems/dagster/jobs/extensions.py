"""Dagster extensions materialization job.

Materializes extensions OLTP data (PostgreSQL) to the LadybugDB graph database.
Connector-agnostic — works regardless of which connector (QB, Xero, Plaid, native)
populated the OLTP tables.

Can be triggered:
- After any connector sync (e.g., chained after qb_load)
- On-demand via API (POST /v1/graphs/{graph_id}/materialize with source=extensions)
- On-demand via Dagster UI
- On a schedule (future)
"""

from typing import Any

from dagster import (
  AssetKey,
  AssetMaterialization,
  Config,
  Failure,
  MetadataValue,
  OpExecutionContext,
  Out,
  job,
  op,
)

from robosystems.dagster.resources import DatabaseResource, GraphResource


class ExtensionsMaterializeConfig(Config):
  """Configuration for extensions materialization."""

  graph_id: str
  entity_id: str = ""
  rebuild: bool = True


@op(out={"materialize_result": Out(dict)})
def materialize_extensions_to_graph(
  context: OpExecutionContext,
  db: DatabaseResource,
  graph: GraphResource,
  config: ExtensionsMaterializeConfig,
) -> dict[str, Any]:
  """Materialize extensions OLTP data to LadybugDB graph.

  Uses postgres_scanner to read from the extensions tenant schema,
  stages into DuckDB, then materializes to LadybugDB via the existing
  ATTACH + COPY FROM pipeline.
  """
  import asyncio

  from robosystems.operations.extensions.materialize import ExtensionsMaterializer

  graph_id = config.graph_id
  entity_id = config.entity_id or None
  context.log.info(
    f"Starting extensions materialization for {graph_id} (rebuild={config.rebuild})"
  )

  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)

  try:
    materializer = ExtensionsMaterializer()
    result = loop.run_until_complete(
      materializer.materialize(
        graph_id=graph_id,
        entity_id=entity_id,
        rebuild=config.rebuild,
      )
    )
  finally:
    loop.close()

  if result.status == "error":
    context.log.error(f"Extensions materialization failed: {result.errors}")
    raise Failure(
      description=f"Extensions materialization failed for {graph_id}",
      metadata={
        "graph_id": MetadataValue.text(graph_id),
        "errors": MetadataValue.text("; ".join(result.errors)),
        "duration_ms": MetadataValue.float(result.duration_ms),
      },
    )

  # Clear staleness so the Dagster sensor does not re-submit for this event
  from robosystems.database import get_db_session
  from robosystems.models.core.graph.graph import Graph

  db_gen = get_db_session()
  db_session = next(db_gen)
  try:
    graph_row = db_session.query(Graph).filter(Graph.graph_id == graph_id).first()
    if graph_row:
      graph_row.mark_fresh(session=db_session)
  finally:
    try:
      next(db_gen)
    except StopIteration:
      pass

  context.log.info(
    f"Extensions materialization complete: "
    f"{len(result.tables_materialized)} tables, "
    f"{result.total_rows} rows, "
    f"{result.duration_ms:.0f}ms"
  )

  context.log_event(
    AssetMaterialization(
      asset_key=AssetKey("user_graph_extensions_materialized"),
      description=f"Materialized {len(result.tables_materialized)} extension tables to graph {graph_id}",
      metadata={
        "graph_id": MetadataValue.text(graph_id),
        "tables_materialized": MetadataValue.int(len(result.tables_materialized)),
        "total_rows": MetadataValue.int(result.total_rows),
        "duration_ms": MetadataValue.float(result.duration_ms),
        "rebuild": MetadataValue.bool(config.rebuild),
      },
    )
  )

  return {
    "graph_id": graph_id,
    "status": result.status,
    "tables_staged": result.tables_staged,
    "tables_materialized": result.tables_materialized,
    "total_rows": result.total_rows,
    "duration_ms": result.duration_ms,
    "errors": result.errors,
  }


@job(
  tags={"dagster/priority": "1", "pipeline": "extensions"},
  description="Materialize extensions OLTP data to LadybugDB graph",
)
def extensions_materialize_job():
  """Materialize all extension data from PostgreSQL OLTP to LadybugDB graph."""
  materialize_extensions_to_graph()
