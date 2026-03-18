"""SEC Pipeline Dagster Components.

This package contains all Dagster orchestration for the SEC adapter:
assets, jobs, sensors, and schedules.

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

3b. HISTORICAL MATERIALIZE (two-stage pipeline for sec_historical):
   - sec_historical_stage job: sec_historical_duckdb_staged - Stage historical data to DuckDB
   - sec_historical_materialize job: sec_historical_materialized - Materialize to LadybugDB

4. INCREMENTAL (nightly updates, sec graph only):
   - sec_duckdb_incremental_staged - Stage current quarter to DuckDB (INSERT with dedup)
   - sec_graph_materialized - Full LadybugDB rebuild from DuckDB
   - sec_entity_incremental_update - Update mutable Entity attributes

5. PUBLISH (post-materialization):
   - sec_lbug_s3_published - Publish raw .lbug to S3 for replica cluster
   - sec_historical_lbug_s3_published - Publish historical .lbug to S3 for replica cluster
   - sec_duckdb_s3_published - Publish raw .duckdb to S3
   - sec_historical_duckdb_s3_published - Publish historical .duckdb to S3
   - sec_vector_s3_published - Export and publish vector index to S3 for replica cluster
   - sec_lbug_r2_published - Publish raw .lbug to R2 for zero-egress subscriber downloads

6. ARTIFACTS (graph-based confidence refinement):
   - sec_knowledge_artifacts - Generate element + structure knowledge artifacts

Nightly incremental chain (sensor-driven):
  download → process (250 batch loop) → stage (DuckDB INSERT + vector build)
  → materialize (full LadybugDB rebuild) → lbug S3 publish
  → duckdb S3 publish → vector S3 publish → replica refresh

Usage:
    from robosystems.adapters.sec.pipeline import get_dagster_components

    components = get_dagster_components()
    # components["assets"] - list of Dagster assets
    # components["jobs"] - list of Dagster jobs
    # components["sensors"] - list of Dagster sensors
    # components["schedules"] - list of Dagster schedules
"""

from robosystems.adapters.sec.pipeline.artifact import (
  SECArtifactConfig,
  sec_knowledge_artifacts,
)
from robosystems.adapters.sec.pipeline.configs import (
  SEC_FORM_TYPE_BATCHES,
  SEC_HISTORICAL_END_YEAR,
  SEC_HISTORICAL_FORM_TYPES,
  SEC_PRIMARY_START_YEAR,
  SEC_QUARTERS,
  SEC_START_YEAR,
  SECDownloadConfig,
  SECEntityUpdateConfig,
  SECHistoricalStageConfig,
  SECIncrementalStageConfig,
  SECMaterializeConfig,
  SECProcessConfig,
  SECStageConfig,
  sec_quarter_partitions,
)
from robosystems.adapters.sec.pipeline.download import sec_raw_filings
from robosystems.adapters.sec.pipeline.duckdb_s3_publish import (
  sec_duckdb_s3_published,
  sec_historical_duckdb_s3_published,
)
from robosystems.adapters.sec.pipeline.entity_update import (
  sec_entity_incremental_update,
)
from robosystems.adapters.sec.pipeline.jobs import (
  sec_artifact_generation_job,
  sec_download_job,
  sec_duckdb_s3_publish_job,
  sec_entity_update_job,
  sec_historical_duckdb_s3_publish_job,
  sec_historical_lbug_s3_publish_job,
  sec_historical_materialize_job,
  sec_historical_stage_job,
  sec_historical_staged_materialize_job,
  sec_incremental_stage_job,
  sec_lbug_r2_publish_job,
  sec_lbug_s3_publish_job,
  sec_materialize_job,
  sec_process_job,
  sec_stage_job,
  sec_staged_materialize_job,
  sec_vector_s3_publish_job,
)
from robosystems.adapters.sec.pipeline.materialize import (
  sec_graph_materialized,
  sec_historical_materialized,
)
from robosystems.adapters.sec.pipeline.process import sec_processed_filings
from robosystems.adapters.sec.pipeline.r2_publish import sec_lbug_r2_published
from robosystems.adapters.sec.pipeline.s3_publish import (
  sec_historical_lbug_s3_published,
  sec_lbug_s3_published,
)
from robosystems.adapters.sec.pipeline.sensors import (
  sec_incremental_download_schedule,
  sec_incremental_pipeline_sensor,
  sec_post_materialize_publish_sensor,
  sec_processing_sensor,
  sec_stage_to_materialize_sensor,
)
from robosystems.adapters.sec.pipeline.stage import (
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
  sec_historical_duckdb_staged,
)
from robosystems.adapters.sec.pipeline.vector_publish import sec_vector_s3_published


def get_dagster_components():
  """Return all Dagster components for the SEC adapter.

  Returns a dictionary with keys: assets, jobs, sensors, schedules.
  Used by dagster/definitions.py to collect SEC pipeline components.
  """
  return {
    "assets": [
      sec_raw_filings,
      sec_processed_filings,
      sec_duckdb_staged,
      sec_historical_duckdb_staged,
      sec_duckdb_incremental_staged,
      sec_entity_incremental_update,
      sec_graph_materialized,
      sec_historical_materialized,
      sec_lbug_s3_published,
      sec_historical_lbug_s3_published,
      sec_duckdb_s3_published,
      sec_historical_duckdb_s3_published,
      sec_lbug_r2_published,
      sec_vector_s3_published,
      sec_knowledge_artifacts,
    ],
    "jobs": [
      sec_download_job,
      sec_process_job,
      sec_stage_job,
      sec_materialize_job,
      sec_staged_materialize_job,
      sec_historical_stage_job,
      sec_historical_materialize_job,
      sec_historical_staged_materialize_job,
      sec_incremental_stage_job,
      sec_entity_update_job,
      sec_lbug_s3_publish_job,
      sec_duckdb_s3_publish_job,
      sec_historical_duckdb_s3_publish_job,
      sec_lbug_r2_publish_job,
      sec_vector_s3_publish_job,
      sec_artifact_generation_job,
      sec_historical_lbug_s3_publish_job,
    ],
    "sensors": [
      sec_processing_sensor,
      sec_incremental_pipeline_sensor,
      sec_stage_to_materialize_sensor,
      sec_post_materialize_publish_sensor,
    ],
    "schedules": [
      sec_incremental_download_schedule,
    ],
  }


__all__ = [
  "SEC_FORM_TYPE_BATCHES",
  "SEC_HISTORICAL_END_YEAR",
  "SEC_HISTORICAL_FORM_TYPES",
  "SEC_PRIMARY_START_YEAR",
  "SEC_QUARTERS",
  "SEC_START_YEAR",
  "SECArtifactConfig",
  "SECDownloadConfig",
  "SECEntityUpdateConfig",
  "SECHistoricalStageConfig",
  "SECIncrementalStageConfig",
  "SECMaterializeConfig",
  "SECProcessConfig",
  "SECStageConfig",
  "get_dagster_components",
  "sec_artifact_generation_job",
  "sec_download_job",
  "sec_duckdb_incremental_staged",
  "sec_duckdb_s3_publish_job",
  "sec_duckdb_s3_published",
  "sec_duckdb_staged",
  "sec_entity_incremental_update",
  "sec_entity_update_job",
  "sec_graph_materialized",
  "sec_historical_duckdb_s3_publish_job",
  "sec_historical_duckdb_s3_published",
  "sec_historical_duckdb_staged",
  "sec_historical_lbug_s3_publish_job",
  "sec_historical_lbug_s3_published",
  "sec_historical_materialize_job",
  "sec_historical_materialized",
  "sec_historical_stage_job",
  "sec_historical_staged_materialize_job",
  "sec_incremental_download_schedule",
  "sec_incremental_pipeline_sensor",
  "sec_incremental_stage_job",
  "sec_knowledge_artifacts",
  "sec_lbug_r2_publish_job",
  "sec_lbug_r2_published",
  "sec_lbug_s3_publish_job",
  "sec_lbug_s3_published",
  "sec_materialize_job",
  "sec_post_materialize_publish_sensor",
  "sec_process_job",
  "sec_processed_filings",
  "sec_processing_sensor",
  "sec_quarter_partitions",
  "sec_raw_filings",
  "sec_stage_job",
  "sec_stage_to_materialize_sensor",
  "sec_staged_materialize_job",
  "sec_vector_s3_publish_job",
  "sec_vector_s3_published",
]
