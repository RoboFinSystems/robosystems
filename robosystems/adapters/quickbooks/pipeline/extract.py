"""QuickBooks Extract Asset.

Fetches data from QuickBooks API and writes raw parquet files
to a temp directory for dbt transformation.
"""

import tempfile
from pathlib import Path

from dagster import AssetExecutionContext, MaterializeResult, asset

from .configs import QBSyncConfig
from .utils import (
  filter_entries_by_date,
  flatten_company_info,
  flatten_journal_entries,
  flatten_journal_lines,
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

  Fetches accounts, journal entries, and company info from the
  QuickBooks API, then writes raw parquet files for dbt transformation.

  For incremental syncs, journal entries are filtered to the lookback
  window (default 60 days) to reduce API costs.

  Returns:
      MaterializeResult with extract_path metadata
  """
  from robosystems.adapters.quickbooks.client import QBClient
  from robosystems.database import SessionFactory
  from robosystems.models.iam.connection_credentials import ConnectionCredentials

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

  # Fetch journal entries
  raw_entries = client.get_journal_entries()
  context.log.info(f"Fetched {len(raw_entries)} total journal entries from QB API")

  # Filter for incremental sync
  if not config.full_rebuild:
    raw_entries = filter_entries_by_date(raw_entries, config.lookback_days)

  # Flatten into tabular format
  journal_entries = flatten_journal_entries(raw_entries)
  journal_lines = flatten_journal_lines(raw_entries)
  context.log.info(
    f"Flattened: {len(journal_entries)} entries, {len(journal_lines)} lines"
  )

  # Write parquet to temp directory
  extract_dir = Path(tempfile.mkdtemp(prefix=f"qb_extract_{config.graph_id}_"))
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
