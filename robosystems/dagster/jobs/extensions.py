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
  """Materialize extensions OLTP data to LadybugDB graph.

  Uses postgres_scanner to read from the extensions tenant schema,
  stages into DuckDB, then materializes to LadybugDB via the Arrow
  record-batch streaming handoff (no intermediate file).
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
    # Enforce the tier storage cap on this path too. The HTTP materialize
    # command runs the same check, but this op is also reached directly by
    # the staleness sensor, which would otherwise rebuild over-cap graphs on
    # every sync. Unknown usage (Graph API unreachable) fails the run rather
    # than proceeding unverified; the sensor resubmits after its cursor
    # expiry.
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

    # Stamped before the source is read: a write that marks the graph stale
    # after this point is not in the snapshot, and mark_fresh must leave the
    # flag set for it (compare-and-clear).
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

  if result.status != "success":
    # 'partial' matters as much as 'error': a graph missing a relationship
    # table renders empty statements. Failing here leaves graph_stale set,
    # so the next OLTP write triggers another rebuild attempt.
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

  # Clear staleness so the Dagster sensor does not re-submit for this event
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
  """Materialize all extension data from PostgreSQL OLTP to LadybugDB graph."""
  materialize_extensions_to_graph()


# ─────────────────────────────────────────────────────────────────────────
# Period-boundary obligation promotion
# ─────────────────────────────────────────────────────────────────────────


class PromoteObligationsConfig(Config):
  """Config for promoting matured pending obligations on one graph."""

  graph_id: str
  # ISO timestamp cutoff. Events with `occurred_at <= as_of_iso` are
  # eligible. The sensor stamps `datetime.now(UTC)` per RunRequest so
  # each run captures its own wall clock — Dagster's run dedup keys off
  # the timestamp + graph_id.
  as_of_iso: str
  dispatch_handlers: bool = False


@op(out={"promotion_result": Out(dict)})
def promote_obligations_for_graph(
  context: OpExecutionContext,
  config: PromoteObligationsConfig,
) -> dict[str, Any]:
  """Open a tenant-scoped session and run the promotion sweep.

  Defers all real logic to ``promote_pending_obligations`` so the same
  function can be called from an admin CLI / REPL during incidents.
  """
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

  with extensions_session(graph_id) as session:
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
    # Non-fatal: status flips already committed. Surface as warnings so
    # the run is yellow not red — operators investigate via logs.
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
  """One-graph wrapper that runs the promotion sweep.

  The companion sensor fires one RunRequest per graph that has work to
  do — same fan-out shape as ``stale_graph_materialization_sensor`` so
  Dagster's per-run logging, dedup, and retry semantics apply uniformly.
  """
  promote_obligations_for_graph()
