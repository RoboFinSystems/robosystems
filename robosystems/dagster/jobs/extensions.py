"""Dagster jobs for extensions: OLTP -> LadybugDB materialization, obligation promotion.

Materialization is connector-agnostic; it reads whatever populated the tenant's
OLTP tables.
"""

from datetime import UTC, datetime
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
  """Materialize extensions OLTP data to the LadybugDB graph.

  postgres_scanner reads the tenant schema into DuckDB, which streams Arrow
  record batches into LadybugDB (no intermediate file).
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
    # The staleness sensor reaches this op without the HTTP command's storage
    # cap check, so enforce it here. Unknown usage (Graph API unreachable)
    # fails the run rather than proceeding unverified.
    from robosystems.database import get_db_session
    from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker
    from robosystems.models.core.graph.graph import Graph

    db_gen = get_db_session()
    db_session = next(db_gen)
    try:
      graph_row = db_session.query(Graph).filter(Graph.graph_id == graph_id).first()
      graph_tier = (
        str(graph_row.graph_tier)
        if graph_row and graph_row.graph_tier
        else "ladybug-standard"
      )
      storage_check = loop.run_until_complete(
        IngestionLimitChecker.check_instance_storage(db_session, graph_id, graph_tier)
      )
    finally:
      try:
        next(db_gen)
      except StopIteration:
        pass

    if not storage_check["allowed"]:
      context.log.error(
        f"Storage cap blocked materialization for {graph_id}: "
        f"{'; '.join(storage_check['errors'])}"
      )
      raise Failure(
        description=f"Storage cap blocked materialization for {graph_id}",
        metadata={
          "graph_id": MetadataValue.text(graph_id),
          "errors": MetadataValue.text("; ".join(storage_check["errors"])),
          "storage_status": MetadataValue.text(storage_check["status"]),
        },
      )

    # Compare-and-clear anchor for mark_fresh: a write stamped after this
    # point is not in the snapshot and must keep the graph stale.
    started_at = datetime.now(UTC)
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

  if result.paused_until is not None:
    # A planned writer roll, not a failure: no RunFailure alert. graph_stale
    # stays set, so the sensor picks the graph up again after the pause.
    context.log.warning(
      f"Extensions materialization for {graph_id} deferred: graph writes are "
      f"paused until {result.paused_until.isoformat()}"
    )
    return {"graph_id": graph_id, "status": "deferred"}

  if result.status != "success":
    # 'partial' fails too: a missing relationship table renders empty
    # statements. graph_stale stays set; the sensor retries only once a later
    # write restamps graph_stale_at (its run_key) and its 2h in-progress
    # cursor entry has expired.
    context.log.error(f"Extensions materialization {result.status}: {result.errors}")
    raise Failure(
      description=(f"Extensions materialization {result.status} for {graph_id}"),
      metadata={
        "graph_id": MetadataValue.text(graph_id),
        "status": MetadataValue.text(result.status),
        "errors": MetadataValue.text("; ".join(result.errors)),
        "duration_ms": MetadataValue.float(result.duration_ms),
      },
    )

  from robosystems.database import get_db_session
  from robosystems.models.core.graph.graph import Graph

  db_gen = get_db_session()
  db_session = next(db_gen)
  try:
    graph_row = db_session.query(Graph).filter(Graph.graph_id == graph_id).first()
    if graph_row:
      cleared = graph_row.mark_fresh(session=db_session, started_at=started_at)
      if not cleared:
        context.log.info(
          f"{graph_id} was written during the materialization; leaving it stale "
          "for the next sweep"
        )
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
  """Materialize all extension data from PostgreSQL OLTP to the LadybugDB graph.

  Launchers must tag the run ``materialize_db=<graph_id>`` (the graph is run
  config, so it can't be a job tag); manual UI launches should too. The tag
  queues a would-be lock refusal; the materializer's Valkey lock is the backstop.
  """
  materialize_extensions_to_graph()


# Period-boundary obligation promotion


class PromoteObligationsConfig(Config):
  """Config for promoting matured pending obligations on one graph."""

  graph_id: str
  # Events with `occurred_at <= as_of_iso` are eligible. The sensor stamps it
  # per RunRequest and it is part of the run_key.
  as_of_iso: str
  dispatch_handlers: bool = False


@op(out={"promotion_result": Out(dict)})
def promote_obligations_for_graph(
  context: OpExecutionContext,
  config: PromoteObligationsConfig,
) -> dict[str, Any]:
  """Run ``promote_pending_obligations`` in a tenant-scoped session."""
  from datetime import datetime

  from robosystems.db.extensions import extensions_session
  from robosystems.operations.event_block.promotion import (
    promote_pending_obligations,
  )

  graph_id = config.graph_id
  as_of = datetime.fromisoformat(config.as_of_iso)
  context.log.info(
    f"Promoting obligations for {graph_id} (as_of={as_of.isoformat()}, "
    f"dispatch={config.dispatch_handlers})"
  )

  with extensions_session(graph_id, statement_timeout_ms=None) as session:
    result = promote_pending_obligations(
      session,
      graph_id,
      as_of=as_of,
      dispatch_handlers=config.dispatch_handlers,
    )

  context.log.info(
    f"Promotion complete for {graph_id}: "
    f"classified={result.classified_count} "
    f"dispatched={result.dispatched_count} "
    f"errors={result.error_count}"
  )

  if result.errors:
    # Non-fatal: status flips already committed.
    for evt_id, msg in result.errors:
      context.log.warning(f"Promotion error for event {evt_id}: {msg}")

  return {
    "graph_id": graph_id,
    "classified_count": result.classified_count,
    "dispatched_count": result.dispatched_count,
    "error_count": result.error_count,
    "classified_event_ids": result.classified_event_ids,
    "dispatched_event_ids": result.dispatched_event_ids,
  }


@job(
  tags={"dagster/priority": "1", "pipeline": "extensions"},
  description="Promote matured pending schedule obligations for one graph",
)
def extensions_promote_obligations_job():
  """Promotion sweep for one graph; the sensor fans out one run per graph with work."""
  promote_obligations_for_graph()
