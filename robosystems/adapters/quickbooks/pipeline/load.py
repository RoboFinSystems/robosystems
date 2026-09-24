"""QuickBooks load asset: dbt DuckDB output → extensions PostgreSQL via OLTPLoader."""

from datetime import UTC, datetime

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import QBSyncConfig
from .utils import get_pipeline_work_dir


@asset(
  group_name="qb_pipeline",
  description="Load QB OLTP tables from DuckDB into extensions PostgreSQL",
  deps=["qb_transform"],
  kinds={"postgres"},
  metadata={
    "pipeline": "quickbooks",
    "stage": "load",
  },
)
def qb_load(
  context: AssetExecutionContext,
  config: QBSyncConfig,
) -> MaterializeResult:
  """Always releases the per-connection sync lock, so the next sync need not
  wait out its TTL."""

  try:
    return _run_qb_load(context, config)
  finally:
    _release_sync_lock(context, config)


def _run_qb_load(
  context: AssetExecutionContext,
  config: QBSyncConfig,
) -> MaterializeResult:
  from robosystems.operations.extensions.loader import OLTPLoader

  work_dir = get_pipeline_work_dir(config.graph_id)
  duckdb_path = work_dir / "quickbooks.duckdb"

  context.log.info(f"Loading QB data for graph={config.graph_id}, duckdb={duckdb_path}")

  # CDC watermark candidate, taken after extract: the next sync re-fetches
  # anything touched since, and the SyncToken gate dedups the overlap.
  # Advanced only on success, full rebuilds included.
  sync_started_at = datetime.now(UTC)

  loader = OLTPLoader()
  try:
    result = loader.load(
      graph_id=config.graph_id,
      source="quickbooks",
      connection_id=config.connection_id,
      duckdb_path=duckdb_path,
      created_by=config.user_id,
      full_rebuild=config.full_rebuild,
      since_date=config.since_date or None,
    )
  except Exception as exc:
    # last_sync is not advanced: the close gate reads it.
    _record_failed_sync_result(context, config, exc)
    raise

  _update_last_sync(context, config, _sync_result_summary(config, result))

  _advance_cdc_watermark(context, config, sync_started_at)

  _bootstrap_fiscal_calendar_if_needed(context, config)

  _trigger_auto_map_if_needed(context, config)

  try:
    from robosystems.operations.extensions.staleness import mark_graph_stale

    mark_graph_stale(config.graph_id, "connector_sync")
    context.log.info("Marked graph stale after QB sync")
  except Exception as e:
    context.log.warning(f"Failed to mark graph stale (non-fatal): {e}")

  if result.errors:
    for error in result.errors[:10]:
      context.log.warning(f"Load warning: {error}")

  context.log.info(
    f"Load complete: {result.elements} elements, {result.dimensions} dimensions, "
    f"{result.agents_inserted} agents inserted, {result.agents_updated} agents updated, "
    f"{result.events_captured} events captured, {result.events_updated} events updated, "
    f"{result.events_drift_detected} drift flagged "
    f"({result.total_rows} total rows). "
    f"Dropped {result.dropped_unbalanced_entries} unbalanced entries, "
    f"{result.dropped_empty_transactions} empty transactions."
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "elements": result.elements,
      "dimensions": result.dimensions,
      "agents_inserted": result.agents_inserted,
      "agents_updated": result.agents_updated,
      "events_captured": result.events_captured,
      "events_updated": result.events_updated,
      "events_drift_detected": result.events_drift_detected,
      "dropped_unbalanced_entries": result.dropped_unbalanced_entries,
      "dropped_empty_transactions": result.dropped_empty_transactions,
      "total_rows": result.total_rows,
      "errors": len(result.errors),
    }
  )


def _release_sync_lock(context: AssetExecutionContext, config: QBSyncConfig) -> None:
  """Best-effort; the lock's TTL is the fallback. An empty ``sync_lock_id``
  means Valkey was unavailable at acquire time."""
  if not config.sync_lock_id:
    return
  try:
    from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
    from robosystems.middleware.auth.distributed_lock import release_lock_by_id

    redis_client = create_redis_client(ValkeyDatabase.LOCKS)
    released = release_lock_by_id(
      redis_client,
      lock_key=f"qb_sync:{config.connection_id}",
      lock_id=config.sync_lock_id,
    )
    if released:
      context.log.info(
        f"Released the per-connection sync lock for connection {config.connection_id}"
      )
    else:
      context.log.info(
        f"Sync lock release no-op for connection {config.connection_id} "
        f"(lock already expired or held by someone else)"
      )
  except Exception as e:
    context.log.warning(
      f"Failed to release the sync lock for connection {config.connection_id} "
      f"(non-fatal — TTL is the fallback): {e}"
    )


def _sync_result_summary(config: QBSyncConfig, result) -> dict:
  """Shape a LoadResult into the Connection.last_sync_result payload."""
  return {
    "status": "succeeded",
    "synced_at": datetime.now(UTC).isoformat(),
    "window": {
      "since_date": config.since_date or None,
      "full_rebuild": bool(config.full_rebuild),
    },
    "counts": {
      "events_captured": result.events_captured,
      "events_updated": result.events_updated,
      "reconciling_items": result.events_drift_detected,
      "dispatch_failed": result.events_dispatch_failed,
      "cross_source_matched": result.events_cross_source_matched,
      "skipped_same_sync_token": result.events_skipped_same_sync_token,
      "skipped_stale_sync_token": result.events_skipped_stale_sync_token,
      "dropped_unbalanced_entries": result.dropped_unbalanced_entries,
      "dropped_empty_transactions": result.dropped_empty_transactions,
      "elements": result.elements,
      "agents_inserted": result.agents_inserted,
      "agents_updated": result.agents_updated,
    },
    "errors": list(result.errors[:10]),
  }


def _record_failed_sync_result(
  context: AssetExecutionContext, config: QBSyncConfig, exc: Exception
) -> None:
  """Persist a failed-attempt outcome without advancing last_sync."""
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection import Connection

  try:
    session = SessionFactory()
    try:
      conn = Connection.get_by_id(config.connection_id, session)
      if conn:
        conn.record_sync_result(
          session,
          {
            "status": "failed",
            "synced_at": datetime.now(UTC).isoformat(),
            "window": {
              "since_date": config.since_date or None,
              "full_rebuild": bool(config.full_rebuild),
            },
            "error": {"code": type(exc).__name__, "message": str(exc)[:500]},
          },
        )
        context.log.info("Recorded failed sync result on connection")
    finally:
      session.close()
  except Exception as e:
    context.log.warning(f"Failed to record sync failure (non-fatal): {e}")


def _update_last_sync(
  context: AssetExecutionContext,
  config: QBSyncConfig,
  result_summary: dict | None = None,
) -> None:
  """Sync DB access: a Dagster worker may already be running an event loop."""
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection import Connection

  try:
    session = SessionFactory()
    try:
      conn = Connection.get_by_id(config.connection_id, session)
      if conn:
        conn.update_last_sync(session, result_summary)
        context.log.info("Updated connection last_sync timestamp + result")
      else:
        context.log.warning(
          "Connection %s not found for last_sync update", config.connection_id
        )
    finally:
      session.close()
  except Exception as e:
    context.log.warning(f"Failed to update last_sync (non-fatal): {e}")


def _advance_cdc_watermark(
  context: AssetExecutionContext, config: QBSyncConfig, watermark
) -> None:
  """Best-effort: a failure only means the next sync re-fetches the window."""
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection import Connection

  try:
    session = SessionFactory()
    try:
      conn = Connection.get_by_id(config.connection_id, session)
      if conn:
        conn.advance_cdc_watermark(watermark, session)
        context.log.info(
          "Advanced CDC watermark to %s for connection %s",
          watermark.isoformat(),
          config.connection_id,
        )
      else:
        context.log.warning(
          "Connection %s not found for CDC watermark advance",
          config.connection_id,
        )
    finally:
      session.close()
  except Exception as e:
    context.log.warning(
      "Failed to advance CDC watermark (non-fatal; next sync will replay "
      "from prior watermark + SyncToken gate dedups): %s",
      e,
    )


def _bootstrap_fiscal_calendar_if_needed(
  context: AssetExecutionContext, config: QBSyncConfig
) -> None:
  """Initialize the fiscal calendar on the first QB sync, closed through the
  month before last: QB history is already closed upstream, so exactly one
  period is queued for the first close. Later syncs only backfill missing
  FiscalPeriod rows.
  """
  from robosystems.db.extensions import extensions_session
  from robosystems.operations.roboledger.fiscal_calendar import (
    FiscalCalendarService,
    add_months,
    current_month_period,
    previous_period,
  )
  from robosystems.operations.roboledger.fiscal_calendar.service import (
    CalendarAlreadyInitializedError,
  )

  try:
    service = FiscalCalendarService()
    with extensions_session(config.graph_id, statement_timeout_ms=None) as session:
      existing = service.get(session, config.graph_id)
      if existing is not None and existing.initialized_at is not None:
        current = current_month_period()
        start = add_months(current, -23)
        if existing.closed_through_period and existing.closed_through_period < start:
          start = existing.closed_through_period
        inserted = service.ensure_fiscal_periods(
          session,
          config.graph_id,
          start_period=start,
          end_period=current,
          closed_through=existing.closed_through_period,
        )
        if inserted:
          session.commit()
          context.log.info(
            f"Backfilled {inserted} FiscalPeriod rows "
            f"({start} → {current}, closed_through={existing.closed_through_period})"
          )
        else:
          context.log.info(
            "Fiscal calendar already initialized "
            f"(closed_through={existing.closed_through_period}); skipping bootstrap"
          )
        return

      closed_through = previous_period(previous_period(current_month_period()))
      service.initialize(
        session,
        config.graph_id,
        closed_through=closed_through,
        actor_id=config.user_id,
        actor_type="system",
        note="Auto-initialized on first QB sync",
      )
      current = current_month_period()
      start = add_months(current, -23)
      if closed_through < start:
        start = closed_through
      inserted = service.ensure_fiscal_periods(
        session,
        config.graph_id,
        start_period=start,
        end_period=current,
        closed_through=closed_through,
      )
      session.commit()
      context.log.info(
        f"Initialized fiscal calendar on first sync: closed_through={closed_through} "
        f"({inserted} FiscalPeriod rows seeded {start} → {current})"
      )
  except CalendarAlreadyInitializedError:
    context.log.info("Fiscal calendar already initialized (race); skipping bootstrap")
  except Exception as e:
    context.log.warning(f"Failed to bootstrap fiscal calendar (non-fatal): {e}")


def _trigger_auto_map_if_needed(
  context: AssetExecutionContext, config: QBSyncConfig
) -> None:
  """Enqueue the MappingOperator when the coa_mapping structure has no
  associations yet; the worker queue dedups a repeat enqueue.

  ``asyncio.run`` is safe: ``qb_load`` is a sync asset with no running loop.
  """
  import asyncio

  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions import Association, Structure

  mapping_id: str | None = None
  try:
    with extensions_session(config.graph_id, statement_timeout_ms=None) as session:
      structure = (
        session.query(Structure)
        .filter(
          Structure.block_type == "coa_mapping",
          Structure.is_active.is_(True),
        )
        .order_by(Structure.created_at.asc())
        .first()
      )
      if structure is None:
        context.log.info(
          "No coa_mapping structure found on graph; skipping auto-map trigger"
        )
        return
      existing_assoc_count = (
        session.query(Association)
        .filter(
          Association.structure_id == structure.id,
          Association.association_type == "mapping",
        )
        .count()
      )
      if existing_assoc_count > 0:
        context.log.info(
          f"Mapping structure has {existing_assoc_count} existing associations; "
          "skipping auto-map trigger"
        )
        return
      mapping_id = str(structure.id)
  except Exception as e:
    context.log.warning(f"Failed to inspect mapping structure (non-fatal): {e}")
    return

  try:
    from robosystems.worker.client import enqueue_task

    result = asyncio.run(
      enqueue_task(
        task_type="operator",
        graph_id=config.graph_id,
        user_id=config.user_id,
        params={"operator_type": "mapping", "mapping_id": mapping_id},
      )
    )
    if result.get("deduplicated"):
      context.log.info(
        f"Auto-map task already in-flight (operation_id={result.get('operation_id')})"
      )
    else:
      context.log.info(
        f"Enqueued auto-map task: operation_id={result.get('operation_id')}, "
        f"mapping_id={mapping_id}"
      )
  except Exception as e:
    context.log.warning(f"Failed to enqueue auto-map task (non-fatal): {e}")
