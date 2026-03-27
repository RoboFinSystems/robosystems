"""Dagster extensions materialization job.

Materializes extensions OLTP data (PostgreSQL) to the LadybugDB graph database.
Connector-agnostic — works regardless of which connector (QB, Xero, Plaid, native)
populated the OLTP tables.

Can be triggered:
- After any connector sync (e.g., chained after qb_load)
- On-demand via API or Dagster UI
- On a schedule (future)
"""

from typing import Any

from dagster import (
  Config,
  Failure,
  MetadataValue,
  OpExecutionContext,
  Out,
  job,
  op,
)

from robosystems.dagster.resources import DatabaseResource, GraphResource


class LedgerMaterializeConfig(Config):
  """Configuration for ledger materialization."""

  graph_id: str
  entity_id: str = ""
  rebuild: bool = True


@op(out={"materialize_result": Out(dict)})
def materialize_ledger_to_graph(
  context: OpExecutionContext,
  db: DatabaseResource,
  graph: GraphResource,
  config: LedgerMaterializeConfig,
) -> dict[str, Any]:
  """Materialize extensions OLTP data to LadybugDB graph.

  Uses postgres_scanner to read from the extensions tenant schema,
  stages into DuckDB, then materializes to LadybugDB via the existing
  ATTACH + COPY FROM pipeline.
  """
  import asyncio

  from robosystems.operations.extensions.materialize import LedgerMaterializer

  graph_id = config.graph_id
  entity_id = config.entity_id or None
  context.log.info(
    f"Starting ledger materialization for {graph_id} (rebuild={config.rebuild})"
  )

  loop = asyncio.new_event_loop()
  asyncio.set_event_loop(loop)

  try:
    materializer = LedgerMaterializer()
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
    context.log.error(f"Ledger materialization failed: {result.errors}")
    raise Failure(
      description=f"Ledger materialization failed for {graph_id}",
      metadata={
        "graph_id": MetadataValue.text(graph_id),
        "errors": MetadataValue.text("; ".join(result.errors)),
        "duration_ms": MetadataValue.float(result.duration_ms),
      },
    )

  context.log.info(
    f"Ledger materialization complete: "
    f"{len(result.tables_materialized)} tables, "
    f"{result.total_rows} rows, "
    f"{result.duration_ms:.0f}ms"
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
  tags={"dagster/priority": "1", "pipeline": "ledger"},
  description="Materialize extensions OLTP data to LadybugDB graph",
)
def ledger_materialize_job():
  """Standalone job for ledger materialization."""
  materialize_ledger_to_graph()
