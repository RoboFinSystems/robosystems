"""QuickBooks Extract Asset.

Fetches data from QuickBooks API and writes raw parquet files
to a temp directory for dbt transformation.
"""

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import QBSyncConfig
from .utils import (
  flatten_company_info,
  get_pipeline_work_dir,
  parse_journal_report,
  write_extract_parquet,
)


@asset(
  group_name="qb_pipeline",
  description="Extract data from QuickBooks API to parquet files",
  kinds={"quickbooks"},
  metadata={
    "pipeline": "quickbooks",
    "stage": "extract",
  },
)
def qb_extract(
  context: AssetExecutionContext,
  config: QBSyncConfig,
) -> MaterializeResult:
  """Extract QuickBooks data to parquet files.

  Fetches accounts, company info, and the JournalReport (all transaction
  types in double-entry format) from the QuickBooks API, then writes
  raw parquet files for dbt transformation.

  The JournalReport includes invoices, bills, payments, purchases,
  deposits, and manual journal entries — everything needed for a
  complete accounting graph.

  Returns:
      MaterializeResult with extract_path metadata
  """
  from datetime import datetime, timedelta

  from robosystems.adapters.quickbooks.client import QBClient
  from robosystems.database import SessionFactory
  from robosystems.models.core.connection.connection_credentials import (
    ConnectionCredentials,
  )

  context.log.info(
    f"Extracting QB data for graph={config.graph_id}, "
    f"connection={config.connection_id}, realm={config.realm_id}, "
    f"full_rebuild={config.full_rebuild}"
  )

  # Get credentials from PostgreSQL
  with SessionFactory() as session:
    creds = ConnectionCredentials.get_by_connection_id(config.connection_id, session)
    if not creds:
      raise ValueError(f"No credentials found for connection {config.connection_id}")
    credentials = creds.get_credentials()

  # Initialize QB client
  realm_id = config.realm_id
  if not realm_id:
    raise ValueError("realm_id is required for QuickBooks extraction")

  client = QBClient(realm_id=realm_id, qb_credentials=credentials)
  context.log.info("QBClient initialized, fetching data...")

  # Fetch company info (always full, 1 API call)
  raw_company_info = client.get_entity_info()
  company_info = flatten_company_info(raw_company_info)
  context.log.info(f"Fetched company info: {len(company_info)} entities")

  # Fetch accounts (always full, small dataset)
  accounts = client.get_accounts()
  context.log.info(f"Fetched {len(accounts)} accounts")

  # Fetch JournalReport (all transaction types in double-entry format)
  # QB API requires explicit dates to return data
  end_date = datetime.now().strftime("%Y-%m-%d")
  if config.full_rebuild:
    start_date = "2000-01-01"  # Far enough back to catch all history
    context.log.info(f"Full rebuild: fetching transactions from {start_date}")
  else:
    start_date = (datetime.now() - timedelta(days=config.lookback_days)).strftime(
      "%Y-%m-%d"
    )
    context.log.info(f"Incremental: fetching transactions from {start_date}")

  report = client.get_transactions(start_date=start_date, end_date=end_date)
  journal_entries, journal_lines = parse_journal_report(report)

  context.log.info(
    f"Parsed: {len(journal_entries)} transactions, {len(journal_lines)} lines"
  )

  # Write parquet to shared pipeline directory
  extract_dir = get_pipeline_work_dir(config.graph_id) / "extract"
  write_extract_parquet(
    extract_dir, accounts, journal_entries, journal_lines, company_info
  )

  context.log.info(f"Extract complete → {extract_dir}")

  return MaterializeResult(
    metadata={
      "extract_path": str(extract_dir),
      "graph_id": config.graph_id,
      "realm_id": realm_id,
      "accounts": len(accounts),
      "journal_entries": len(journal_entries),
      "journal_lines": len(journal_lines),
      "full_rebuild": config.full_rebuild,
    }
  )
