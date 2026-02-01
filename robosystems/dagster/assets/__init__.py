"""Dagster assets for data pipelines.

Assets represent data artifacts that are produced and consumed:
- User graph assets (creation, staging, materialization)
- SEC pipeline assets (extraction, processing, materialization)
- QuickBooks pipeline assets (sync, transform, materialize)
- Plaid pipeline assets (sync, transform, materialize)
"""

from robosystems.dagster.assets.graphs import (
  user_graph_creation_source,
  user_graph_file_staging_source,
  user_graph_materialized_source,
  user_repository_provisioning_source,
  user_subgraph_creation_source,
)
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
  # Config classes
  SECDirectCopyConfig,
  SECDownloadConfig,
  SECIncrementalStageConfig,
  SECMaterializeConfig,
  SECProcessConfig,
  SECStageConfig,
  # Assets - two-stage materialization
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
  # Assets - direct S3 → LadybugDB copy (alternative to DuckDB staging)
  sec_graph_direct_copy,
  sec_graph_materialized,
  # Assets - quarterly batch processing with consolidated output
  sec_processed_filings,
  # Partitions (quarterly to stay under EFTS 10k limit)
  sec_quarter_partitions,
  sec_raw_filings,
)
from robosystems.dagster.assets.sec_backup import (
  SECBackupConfig,
  sec_backup,
)

__all__ = [
  "SECBackupConfig",
  "SECDirectCopyConfig",
  "SECDownloadConfig",
  "SECIncrementalStageConfig",
  "SECMaterializeConfig",
  "SECProcessConfig",
  "SECStageConfig",
  "plaid_accounts",
  "plaid_graph_data",
  "plaid_transactions",
  "qb_accounts",
  "qb_graph_data",
  "qb_transactions",
  "sec_backup",
  "sec_duckdb_incremental_staged",
  "sec_duckdb_staged",
  "sec_graph_direct_copy",
  "sec_graph_materialized",
  "sec_processed_filings",
  "sec_quarter_partitions",
  "sec_raw_filings",
  "user_graph_creation_source",
  "user_graph_file_staging_source",
  "user_graph_materialized_source",
  "user_repository_provisioning_source",
  "user_subgraph_creation_source",
]
