"""SEC Pipeline Dagster Assets.

This package contains all Dagster assets for the SEC XBRL pipeline:

Pipeline stages (run independently via separate jobs):

1. DOWNLOAD (sec_download job):
   - sec_raw_filings - Discover via EFTS, download XBRL ZIPs (quarterly partitions)
   - Creates SourceFile records in PostgreSQL for processing tracking

2. PROCESS (sec_process job, quarterly partitions):
   - sec_processed_filings - Process entire quarter's filings as batch
   - Outputs consolidated parquet files (one per table per quarter)

3. MATERIALIZE (two-stage pipeline):
   - sec_stage job: sec_duckdb_staged - Stage processed files to persistent DuckDB
   - sec_materialize job: sec_graph_materialized - Materialize from DuckDB to LadybugDB

4. INCREMENTAL (daily updates):
   - sec_graph_incremental_copy - Direct S3 → LadybugDB copy for current quarter
   - sec_entity_incremental_update - Update mutable Entity attributes

5. BACKUP:
   - sec_backup - Create downloadable backups for users
"""

from robosystems.dagster.assets.sec.backup import sec_backup
from robosystems.dagster.assets.sec.configs import (
  SEC_FORM_TYPE_BATCHES,
  SEC_QUARTERS,
  SEC_START_YEAR,
  SECBackupConfig,
  SECDirectCopyConfig,
  SECDownloadConfig,
  SECEntityUpdateConfig,
  SECIncrementalCopyConfig,
  SECIncrementalStageConfig,
  SECMaterializeConfig,
  SECProcessConfig,
  SECStageConfig,
  sec_quarter_partitions,
)
from robosystems.dagster.assets.sec.download import sec_raw_filings
from robosystems.dagster.assets.sec.entity_update import sec_entity_incremental_update
from robosystems.dagster.assets.sec.materialize import (
  sec_graph_direct_copy,
  sec_graph_incremental_copy,
  sec_graph_materialized,
)
from robosystems.dagster.assets.sec.process import sec_processed_filings
from robosystems.dagster.assets.sec.stage import (
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
)

__all__ = [
  # Constants
  "SEC_FORM_TYPE_BATCHES",
  "SEC_QUARTERS",
  "SEC_START_YEAR",
  # Config classes
  "SECBackupConfig",
  "SECDirectCopyConfig",
  "SECDownloadConfig",
  "SECEntityUpdateConfig",
  "SECIncrementalCopyConfig",
  "SECIncrementalStageConfig",
  "SECMaterializeConfig",
  "SECProcessConfig",
  "SECStageConfig",
  # Assets
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
]
