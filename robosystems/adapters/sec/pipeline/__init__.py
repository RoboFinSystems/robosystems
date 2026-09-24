"""Dagster assets, jobs, sensors and schedules for the SEC pipeline.

Stages: download → process → DuckDB stage → LadybugDB materialize → publish
(S3, R2, Hugging Face), plus text indexing, knowledge artifacts and the public
filer catalog. See README.md in this directory; ``get_dagster_components()``
is what dagster/definitions.py collects.
"""

from robosystems.adapters.sec.pipeline.artifact import (
  SECArtifactConfig,
  sec_knowledge_artifacts,
)
from robosystems.adapters.sec.pipeline.catalog import sec_filing_catalog
from robosystems.adapters.sec.pipeline.configs import (
  SEC_FORM_TYPE_BATCHES,
  SEC_HISTORICAL_END_YEAR,
  SEC_HISTORICAL_FORM_TYPES,
  SEC_PRIMARY_START_YEAR,
  SEC_QUARTERS,
  SEC_START_YEAR,
  SECDownloadConfig,
  SECFilingCatalogConfig,
  SECHFPublishConfig,
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
from robosystems.adapters.sec.pipeline.hf_publish import sec_lbug_hf_published
from robosystems.adapters.sec.pipeline.jobs import (
  sec_artifact_generation_job,
  sec_download_job,
  sec_duckdb_s3_publish_job,
  sec_filing_catalog_job,
  sec_historical_duckdb_s3_publish_job,
  sec_historical_lbug_s3_publish_job,
  sec_historical_materialize_job,
  sec_historical_stage_job,
  sec_historical_staged_materialize_job,
  sec_incremental_stage_job,
  sec_ixbrl_index_job,
  sec_lbug_hf_publish_job,
  sec_lbug_r2_publish_job,
  sec_lbug_s3_publish_job,
  sec_materialize_job,
  sec_narratives_index_job,
  sec_process_job,
  sec_stage_job,
  sec_staged_materialize_job,
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
  sec_master_sleep_on_failure_sensor,
  sec_post_materialize_publish_sensor,
  sec_post_stage_index_sensor,
  sec_processing_sensor,
  sec_stage_to_materialize_sensor,
  sec_wake_to_stage_sensor,
)
from robosystems.adapters.sec.pipeline.stage import (
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
  sec_historical_duckdb_staged,
)
from robosystems.adapters.sec.pipeline.text_index import (
  sec_ixbrl_disclosures_indexed,
  sec_narratives_indexed,
)


def get_dagster_components():
  """assets, jobs, sensors, schedules and shared_replica_deps, for
  dagster/definitions.py."""
  return {
    "shared_replica_deps": [
      "sec_lbug_s3_published",
      "sec_historical_lbug_s3_published",
    ],
    "assets": [
      sec_raw_filings,
      sec_processed_filings,
      sec_duckdb_staged,
      sec_historical_duckdb_staged,
      sec_duckdb_incremental_staged,
      sec_graph_materialized,
      sec_historical_materialized,
      sec_lbug_s3_published,
      sec_historical_lbug_s3_published,
      sec_duckdb_s3_published,
      sec_historical_duckdb_s3_published,
      sec_lbug_r2_published,
      sec_lbug_hf_published,
      sec_knowledge_artifacts,
      sec_narratives_indexed,
      sec_ixbrl_disclosures_indexed,
      sec_filing_catalog,
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
      sec_lbug_s3_publish_job,
      sec_duckdb_s3_publish_job,
      sec_historical_duckdb_s3_publish_job,
      sec_lbug_r2_publish_job,
      sec_lbug_hf_publish_job,
      sec_artifact_generation_job,
      sec_historical_lbug_s3_publish_job,
      sec_narratives_index_job,
      sec_ixbrl_index_job,
      sec_filing_catalog_job,
    ],
    "sensors": [
      sec_processing_sensor,
      sec_incremental_pipeline_sensor,
      sec_stage_to_materialize_sensor,
      sec_post_materialize_publish_sensor,
      sec_post_stage_index_sensor,
      sec_wake_to_stage_sensor,
      sec_master_sleep_on_failure_sensor,
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
  "SECFilingCatalogConfig",
  "SECHFPublishConfig",
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
  "sec_filing_catalog",
  "sec_filing_catalog_job",
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
  "sec_ixbrl_disclosures_indexed",
  "sec_ixbrl_index_job",
  "sec_knowledge_artifacts",
  "sec_lbug_hf_publish_job",
  "sec_lbug_hf_published",
  "sec_lbug_r2_publish_job",
  "sec_lbug_r2_published",
  "sec_lbug_s3_publish_job",
  "sec_lbug_s3_published",
  "sec_master_sleep_on_failure_sensor",
  "sec_materialize_job",
  "sec_narratives_index_job",
  "sec_narratives_indexed",
  "sec_post_materialize_publish_sensor",
  "sec_post_stage_index_sensor",
  "sec_process_job",
  "sec_processed_filings",
  "sec_processing_sensor",
  "sec_quarter_partitions",
  "sec_raw_filings",
  "sec_stage_job",
  "sec_stage_to_materialize_sensor",
  "sec_staged_materialize_job",
  "sec_wake_to_stage_sensor",
]
