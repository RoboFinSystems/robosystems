"""Dagster assets for data pipelines.

Assets represent data artifacts that are produced and consumed:
- SEC pipeline assets (extraction, processing, staging, materialization)
- QuickBooks pipeline assets (sync, transform, materialize)
- Plaid pipeline assets (sync, transform, materialize)
"""

from robosystems.dagster.assets.sec import (
  # Assets
  sec_companies_list,
  sec_raw_filings,
  sec_processed_filings,
  sec_duckdb_staging,
  sec_graph_materialized,
  # Partitions
  sec_year_partitions,
  # Config classes
  SECCompaniesConfig,
  SECDownloadConfig,
  SECProcessConfig,
  SECDuckDBConfig,
  SECMaterializeConfig,
)
from robosystems.dagster.assets.quickbooks import (
  qb_accounts,
  qb_transactions,
  qb_graph_data,
)
from robosystems.dagster.assets.plaid import (
  plaid_accounts,
  plaid_transactions,
  plaid_graph_data,
)

__all__ = [
  # SEC assets
  "sec_companies_list",
  "sec_raw_filings",
  "sec_processed_filings",
  "sec_duckdb_staging",
  "sec_graph_materialized",
  "sec_year_partitions",
  # SEC config
  "SECCompaniesConfig",
  "SECDownloadConfig",
  "SECProcessConfig",
  "SECDuckDBConfig",
  "SECMaterializeConfig",
  # QuickBooks assets
  "qb_accounts",
  "qb_transactions",
  "qb_graph_data",
  # Plaid assets
  "plaid_accounts",
  "plaid_transactions",
  "plaid_graph_data",
]
