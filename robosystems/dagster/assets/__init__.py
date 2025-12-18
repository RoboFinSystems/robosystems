"""Dagster assets for data pipelines.

Assets represent data artifacts that are produced and consumed:
- SEC pipeline assets (extraction, processing, staging, materialization)
- QuickBooks pipeline assets (sync, transform, materialize)
- Plaid pipeline assets (sync, transform, materialize)
"""

from robosystems.dagster.assets.sec import (
  # Assets - download phase
  sec_companies_list,
  sec_raw_filings,
  # Assets - batch processing (year-partitioned, for CLI workflows)
  sec_batch_process,
  # Assets - dynamic partition processing (for Dagster UI visibility)
  sec_filings_to_process,
  sec_process_filing,
  # Assets - staging and materialization
  sec_duckdb_staging,
  sec_graph_materialized,
  # Partitions
  sec_year_partitions,
  sec_filing_partitions,
  # Config classes
  SECCompaniesConfig,
  SECDownloadConfig,
  SECBatchProcessConfig,
  SECFilingDiscoveryConfig,
  SECSingleFilingConfig,
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
  # SEC assets - download phase
  "sec_companies_list",
  "sec_raw_filings",
  # SEC assets - batch processing (for CLI workflows)
  "sec_batch_process",
  # SEC assets - dynamic partition processing (for Dagster UI)
  "sec_filings_to_process",
  "sec_process_filing",
  # SEC assets - staging and materialization
  "sec_duckdb_staging",
  "sec_graph_materialized",
  # SEC partitions
  "sec_year_partitions",
  "sec_filing_partitions",
  # SEC config
  "SECCompaniesConfig",
  "SECDownloadConfig",
  "SECBatchProcessConfig",
  "SECFilingDiscoveryConfig",
  "SECSingleFilingConfig",
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
