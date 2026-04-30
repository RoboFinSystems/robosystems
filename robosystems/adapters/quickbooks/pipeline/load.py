"""QuickBooks Load Asset.

Reads OLTP-shaped tables from the dbt DuckDB output and inserts
them into the extensions PostgreSQL database via OLTPLoader.
"""

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

  Phase 2 ingest:
  1. Provisions the tenant schema if needed
  2. Inserts/updates structural rows (elements, dimensions)
  3. Captures each QB transaction as an event_block row with
     ``status='captured'`` — no GL writes happen here. Handlers fire
     when the user approves the event in the inbox, which is when
     transactions/entries/line_items rows actually get created.

  Returns:
      MaterializeResult with element/dimension/event counts and
      data-quality drop counters.
  """
  from robosystems.operations.extensions.loader import OLTPLoader

  work_dir = get_pipeline_work_dir(config.graph_id)
  duckdb_path = work_dir / "quickbooks.duckdb"

  context.log.info(f"Loading QB data for graph={config.graph_id}, duckdb={duckdb_path}")

  loader = OLTPLoader()
  result = loader.load(
    graph_id=config.graph_id,
    source="quickbooks",
    connection_id=config.connection_id,
    duckdb_path=duckdb_path,
    created_by=config.user_id,
  )

  # Update last sync timestamp
  _update_last_sync(context, config)

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

  # Phase 2 ingest: transactions/entries/line_items are produced by handlers
  # post-approval, not by sync — they're always 0 here. Surface the actual
  # sync outputs (events captured/updated) and data-quality drop counters.
  context.log.info(
    f"Load complete: {result.elements} elements, {result.dimensions} dimensions, "
    f"{result.agents_inserted} agents inserted, {result.agents_updated} agents updated, "
    f"{result.events_captured} events captured, {result.events_updated} events updated "
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
      "dropped_unbalanced_entries": result.dropped_unbalanced_entries,
      "dropped_empty_transactions": result.dropped_empty_transactions,
      "total_rows": result.total_rows,
      "errors": len(result.errors),
    }
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
