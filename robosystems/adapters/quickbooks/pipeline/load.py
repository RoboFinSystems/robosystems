"""QuickBooks Load Asset.

Reads OLTP-shaped tables from the dbt DuckDB output and inserts
them into the extensions PostgreSQL database via OLTPLoader.
"""

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
  """Load QB data from dbt DuckDB into extensions OLTP tables.

  Ingest steps:
  1. Provisions the tenant schema if needed
  2. Inserts/updates structural rows (elements, dimensions)
  3. Captures each QB transaction as an event_block row with
     ``status='captured'`` — no GL writes happen here. Handlers fire
     when the user approves the event in the inbox, which is when
     transactions/entries/line_items rows actually get created.

  Releases the per-connection sync lock on completion (or on exception)
  so the next sync can fire immediately without waiting for the 30-min TTL.

  Returns:
      MaterializeResult with element/dimension/event counts and
      data-quality drop counters.
  """

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

  # CDC watermark candidate. Captured at the START of
  # load, NOT at extract start, by design: extract has already run by
  # the time we get here, so any QB-side row touched between extract's
  # finish and this moment isn't in this sync. Setting watermark =
  # load-start means the next sync's `changedSince` will re-fetch
  # anything that landed in that window — which the SyncToken gate
  # then dedups as a no-op. The alternative (extract-start) would
  # potentially skip late-landing rows; with the gate, slightly
  # over-fetching is the safer trade. Advanced only after a successful
  # load below — a raised exception or fatal `result.errors` leaves
  # the prior watermark in place so the next sync replays from there.
  #
  # NOTE on `full_rebuild`: this advance fires unconditionally on a
  # successful load, including full-rebuild syncs. That's intentional —
  # a full rebuild definitively covers all history, so the next
  # incremental should pick up from the rebuild's start, not from
  # whatever stale watermark existed before. If you ever need the
  # opposite (full rebuild preserves prior watermark), gate this call
  # on `config.full_rebuild` here.
  sync_started_at = datetime.now(UTC)

  loader = OLTPLoader()
  result = loader.load(
    graph_id=config.graph_id,
    source="quickbooks",
    connection_id=config.connection_id,
    duckdb_path=duckdb_path,
    created_by=config.user_id,
    # Only operator-explicit full rebuilds trigger the
    # pre-sync wipe. Incremental window syncs (since_date set) and
    # default lookback (neither set) skip the wipe entirely and rely
    # on the UPSERT path.
    full_rebuild=config.full_rebuild,
    since_date=config.since_date or None,
  )

  # Update last sync timestamp
  _update_last_sync(context, config)

  # Advance CDC watermark only after load success.
  # `result.errors` carries non-fatal load warnings already; the truly
  # fatal path raises before reaching here (and the watermark stays
  # unchanged, so the next sync re-attempts from the prior watermark).
  _advance_cdc_watermark(context, config, sync_started_at)

  # Bootstrap fiscal calendar on first sync. Idempotent: skips when the
  # calendar is already initialized (re-sync). Without this, a fresh QB
  # tenant lands with `calendar_not_initialized` and the entire close-period
  # UX is unreachable until the user manually calls `initialize-ledger`.
  _bootstrap_fiscal_calendar_if_needed(context, config)

  # Trigger auto-map of CoA → reporting concepts on first sync. Idempotent:
  # skips when the mapping structure already has associations (re-sync, or
  # user has hand-curated). Without this, a fresh QB tenant has 0% mapping
  # coverage and `live-financial-statement` returns empty facts even though
  # GL data is fully loaded.
  _trigger_auto_map_if_needed(context, config)

  # Mark graph stale so materialization pipeline knows OLTP data changed
  try:
    from robosystems.operations.extensions.staleness import mark_graph_stale

    mark_graph_stale(config.graph_id, "connector_sync")
    context.log.info("Marked graph stale after QB sync")
  except Exception as e:
    context.log.warning(f"Failed to mark graph stale (non-fatal): {e}")

  if result.errors:
    for error in result.errors[:10]:
      context.log.warning(f"Load warning: {error}")

  # Transactions/entries/line_items are produced by handlers
  # post-approval, not by sync — they're always 0 here. Surface the actual
  # sync outputs (events captured/updated) and data-quality drop counters.
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
  """Release the per-connection sync lock on qb_load
  completion (success or failure).

  Best-effort: any error here is logged and swallowed — the lock will
  expire via its 30-min TTL if the explicit release fails. The lock
  token (`config.sync_lock_id`) is empty when Valkey was unavailable
  at acquire time; that path is a no-op too.
  """
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
      context.log.info(f"Released B7 sync lock for connection {config.connection_id}")
    else:
      context.log.info(
        f"B7 sync lock release no-op for connection {config.connection_id} "
        f"(lock already expired or held by someone else)"
      )
  except Exception as e:
    context.log.warning(
      f"Failed to release B7 sync lock for connection {config.connection_id} "
      f"(non-fatal — TTL is the fallback): {e}"
    )


def _update_last_sync(context: AssetExecutionContext, config: QBSyncConfig) -> None:
  """Update the connection's last_sync timestamp.

  Uses sync DB access directly to avoid asyncio.run() issues in Dagster workers
  where an event loop may already be running.
  """
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection import Connection

  try:
    session = SessionFactory()
    try:
      conn = Connection.get_by_id(config.connection_id, session)
      if conn:
        conn.update_last_sync(session)
        context.log.info("Updated connection last_sync timestamp")
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
  """Advance the Connection's CDC watermark.

  Called after a successful load. The watermark is the timestamp at the
  START of this load run, so the next CDC fetch picks up from there. If
  this helper itself fails (e.g., DB transient), we log and swallow:
  worst case the next sync re-fetches the same window, and the SyncToken
  gate skips already-ingested rows.
  """
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
  """Initialize the fiscal calendar on the first QB sync.

  Uses the same `closed_through = month_before_last` pattern the synthetic
  demo uses: customers migrating from QB typically already closed historical
  periods in QB, so RoboLedger starts from a sealed-history boundary with
  exactly one period (`close_target = last_completed_month`) queued for the
  first close.

  Idempotent: re-syncs that find a calendar with `initialized_at` already
  set skip silently. The exception type is caught explicitly so the load
  asset doesn't fail on a benign re-init attempt.
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
    with extensions_session(config.graph_id) as session:
      existing = service.get(session, config.graph_id)
      if existing is not None and existing.initialized_at is not None:
        # Self-heal: earlier bootstrap revisions seeded the calendar pointer
        # state but skipped FiscalPeriod row creation, leaving close-period
        # to fail with `period_not_found`. Run ensure_fiscal_periods over the
        # canonical window so existing tenants converge on re-sync.
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
    # Non-fatal: a failed calendar bootstrap leaves the rest of the sync
    # intact. Operator can manually initialize via the regular endpoint.
    context.log.warning(f"Failed to bootstrap fiscal calendar (non-fatal): {e}")


def _trigger_auto_map_if_needed(
  context: AssetExecutionContext, config: QBSyncConfig
) -> None:
  """Enqueue the MappingOperator on the first sync if mapping coverage is 0.

  Looks up the tenant's `coa_mapping` Structure, checks current association
  count, and (only if zero) enqueues a background `agent_mapping` task. The
  agent walks unmapped CoA elements, proposes associations, and auto-approves
  high-confidence matches — the same path users invoke via the
  `auto-map-elements` operation.

  Idempotent on two levels:
  - Skip when the mapping structure already has ≥1 association (user has
    started curating, or auto-map already ran).
  - The worker queue's per-task dedup window catches a second enqueue
    of the same (graph, mapping_id) within DEDUP_TTL.

  Runs the async `enqueue_task` via `asyncio.run`. `qb_load` is a sync
  Dagster asset and doesn't have an enclosing event loop, so this is safe.
  """
  import asyncio

  from robosystems.db.extensions import extensions_session
  from robosystems.models.extensions import Association, Structure

  mapping_id: str | None = None
  try:
    with extensions_session(config.graph_id) as session:
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
