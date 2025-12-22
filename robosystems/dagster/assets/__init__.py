"""Dagster assets for data pipelines.

Assets represent data artifacts that are produced and consumed:
- SEC pipeline assets (extraction, processing, staging, materialization)
- QuickBooks pipeline assets (sync, transform, materialize)
- Plaid pipeline assets (sync, transform, materialize)
- Staged files (observable source for direct API staging)
"""

from robosystems.dagster.assets.plaid import (
  plaid_accounts,
  plaid_graph_data,
  plaid_transactions,
)
from robosystems.dagster.assets.quickbooks import (
  qb_accounts,
  qb_graph_data,
  qb_transactions,
)
from robosystems.dagster.assets.sec import (
  SECBatchProcessConfig,
  # Config classes
  SECCompaniesConfig,
  SECDownloadConfig,
  SECDuckDBConfig,
  SECFilingDiscoveryConfig,
  SECMaterializeConfig,
  SECSingleFilingConfig,
  # Assets - batch processing (year-partitioned, for CLI workflows)
  sec_batch_process,
  # Assets - download phase
  sec_companies_list,
  # Assets - staging and materialization
  sec_duckdb_staging,
  sec_filing_partitions,
  # Assets - dynamic partition processing (for Dagster UI visibility)
  sec_filings_to_process,
  sec_graph_materialized,
  sec_process_filing,
  sec_raw_filings,
  # Partitions
  sec_year_partitions,
)
from robosystems.dagster.assets.staged_files import staged_files_source

__all__ = [
  "SECBatchProcessConfig",
  # SEC config
  "SECCompaniesConfig",
  "SECDownloadConfig",
  "SECDuckDBConfig",
  "SECFilingDiscoveryConfig",
  "SECMaterializeConfig",
  "SECSingleFilingConfig",
  # Plaid assets
  "plaid_accounts",
  "plaid_graph_data",
  "plaid_transactions",
  # QuickBooks assets
  "qb_accounts",
  "qb_graph_data",
  "qb_transactions",
  # SEC assets - batch processing (for CLI workflows)
  "sec_batch_process",
  # SEC assets - download phase
  "sec_companies_list",
  # SEC assets - staging and materialization
  "sec_duckdb_staging",
  "sec_filing_partitions",
  # SEC assets - dynamic partition processing (for Dagster UI)
  "sec_filings_to_process",
  "sec_graph_materialized",
  "sec_process_filing",
  "sec_raw_filings",
  # SEC partitions
  "sec_year_partitions",
  # Direct staging observable source
  "staged_files_source",
]
