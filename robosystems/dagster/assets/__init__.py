"""Dagster assets for data pipelines.

Assets represent data artifacts that are produced and consumed:
- User graph assets (creation, staging, materialization)
- SEC pipeline assets (extraction, processing, materialization)
- Shared repository assets (S3 publish, replica refresh)
"""

from robosystems.dagster.assets.graphs import (
  user_graph_creation_source,
  user_graph_file_staging_source,
  user_graph_materialized_source,
  user_repository_provisioning_source,
  user_subgraph_creation_source,
)
from robosystems.dagster.assets.sec import (
  # Config classes
  SECBackupConfig,
  SECDirectCopyConfig,
  SECDownloadConfig,
  SECEntityUpdateConfig,
  SECIncrementalCopyConfig,
  SECIncrementalStageConfig,
  SECMaterializeConfig,
  SECProcessConfig,
  SECStageConfig,
  # Assets - backup
  sec_backup,
  # Assets - two-stage materialization
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
  # Assets - Entity update (mutable attributes)
  sec_entity_incremental_update,
  # Assets - direct S3 → LadybugDB copy (alternative to DuckDB staging)
  sec_graph_direct_copy,
  sec_graph_incremental_copy,
  sec_graph_materialized,
  # Assets - quarterly batch processing with consolidated output
  sec_processed_filings,
  # Partitions (quarterly to stay under EFTS 10k limit)
  sec_quarter_partitions,
  sec_raw_filings,
)
from robosystems.dagster.assets.shared_repositories import (
  SharedReplicaRefreshConfig,
  SharedRepositoryPublishConfig,
  shared_replicas_refreshed,
  shared_repository_s3_published,
)

__all__ = [
  "SECBackupConfig",
  "SECDirectCopyConfig",
  "SECDownloadConfig",
  "SECEntityUpdateConfig",
  "SECIncrementalCopyConfig",
  "SECIncrementalStageConfig",
  "SECMaterializeConfig",
  "SECProcessConfig",
  "SECStageConfig",
  "SharedReplicaRefreshConfig",
  "SharedRepositoryPublishConfig",
  "sec_backup",
  "sec_duckdb_incremental_staged",
  "sec_duckdb_staged",
  "sec_entity_incremental_update",
  "sec_graph_direct_copy",
  "sec_graph_incremental_copy",
  "sec_graph_materialized",
  "sec_processed_filings",
  "sec_quarter_partitions",
  "sec_raw_filings",
  "shared_replicas_refreshed",
  "shared_repository_s3_published",
  "user_graph_creation_source",
  "user_graph_file_staging_source",
  "user_graph_materialized_source",
  "user_repository_provisioning_source",
  "user_subgraph_creation_source",
]
