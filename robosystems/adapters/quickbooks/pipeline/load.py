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

  Uses the generic OLTPLoader which:
  1. Provisions the tenant schema if needed
  2. Deletes existing data for this source + connection
  3. Inserts accounts, transactions, entries, line_items, dimensions
  4. Resolves all foreign keys using external_id lookups

  Returns:
      MaterializeResult with load statistics
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

  if result.errors:
    for error in result.errors[:10]:
      context.log.warning(f"Load warning: {error}")

  context.log.info(
    f"Load complete: {result.accounts} accounts, {result.transactions} transactions, "
    f"{result.entries} entries, {result.line_items} line items, "
    f"{result.dimensions} dimensions ({result.total_rows} total)"
  )

  return MaterializeResult(
    metadata={
      "graph_id": config.graph_id,
      "accounts": result.accounts,
      "transactions": result.transactions,
      "entries": result.entries,
      "line_items": result.line_items,
      "dimensions": result.dimensions,
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
  from robosystems.models.iam.connection import Connection

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
