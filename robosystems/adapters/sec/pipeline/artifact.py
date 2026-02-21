"""SEC Knowledge Artifact Generation Asset.

Generates precomputed Parquet artifacts from the full DuckDB staging database
for graph-based confidence refinement during XBRL enrichment.

Artifacts:
  - element_knowledge.parquet: Graph-structural signals per element qname
  - structure_profiles.parquet: Element frequency distributions per canonical_type
  - structure_consensus.parquet: Cross-filing majority-vote for identical structures

Dev: opens local DuckDB at {DUCKDB_STAGING_PATH}/{source}.duckdb
Prod: downloads from S3 first (same pattern as DuckDBAnalyticsContext)
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from pathlib import Path

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  asset,
)


class SECArtifactConfig(Config):
  """Configuration for SEC artifact generation."""

  duckdb_source: str = "sec"
  memory_limit: str = "10GB"


@asset(
  group_name="sec_pipeline",
  description="Generate element + structure knowledge artifacts from DuckDB staging",
  kinds={"duckdb", "analytics", "parquet"},
  deps=["sec_duckdb_staged"],
  metadata={"pipeline": "sec", "stage": "artifact"},
)
def sec_knowledge_artifacts(
  context: AssetExecutionContext,
  config: SECArtifactConfig,
) -> MaterializeResult:
  """Generate all knowledge artifacts for graph-based confidence refinement.

  Downloads the DuckDB staging file (in prod) or opens it locally (in dev),
  then runs both ElementKnowledgeBuilder and StructureKnowledgeBuilder.
  """
  from robosystems.adapters.sec.knowledge.artifact import (
    ElementKnowledgeBuilder,
    StructureKnowledgeBuilder,
  )
  from robosystems.adapters.sec.knowledge.framework import DuckDBAnalyticsContext
  from robosystems.config import env

  # Use DuckDBAnalyticsContext to handle S3 download in prod
  with DuckDBAnalyticsContext(
    duckdb_source=config.duckdb_source,
    memory_limit=config.memory_limit,
  ) as ctx:
    db_path = ctx.db_path
    context.log.info(f"Building artifacts from DuckDB at: {db_path}")

    # Build element knowledge artifact
    context.log.info("Building element knowledge artifact")
    element_builder = ElementKnowledgeBuilder(memory_limit=config.memory_limit)
    element_path = element_builder.build(db_path)
    context.log.info(f"Element knowledge artifact written to: {element_path}")

    # Build structure knowledge artifacts
    context.log.info("Building structure knowledge artifacts")
    structure_builder = StructureKnowledgeBuilder(memory_limit=config.memory_limit)
    profiles_path, consensus_path = structure_builder.build(db_path)
    context.log.info(f"Structure profiles artifact written to: {profiles_path}")
    context.log.info(f"Structure consensus artifact written to: {consensus_path}")

    # In prod, upload artifacts to S3
    if env.ENVIRONMENT != "dev":
      _upload_artifacts_to_s3(context, element_path, profiles_path, consensus_path)

  return MaterializeResult(
    metadata={
      "element_knowledge_path": str(element_path),
      "structure_profiles_path": str(profiles_path),
      "structure_consensus_path": str(consensus_path),
    }
  )


def _upload_artifacts_to_s3(
  context: AssetExecutionContext,
  element_path: "Path",
  profiles_path: "Path",
  consensus_path: "Path",
) -> None:
  """Upload generated artifacts to S3 for distribution."""
  from robosystems.config import env
  from robosystems.config.storage.shared import DataSourceType, get_processed_key
  from robosystems.operations.aws.s3 import S3Client

  s3 = S3Client()
  bucket = env.SHARED_PROCESSED_BUCKET

  artifact_files = [
    (element_path, "element_knowledge.parquet"),
    (profiles_path, "structure_profiles.parquet"),
    (consensus_path, "structure_consensus.parquet"),
  ]

  for local_path, filename in artifact_files:
    s3_key = get_processed_key(DataSourceType.SEC, "artifacts", filename)
    context.log.info(f"Uploading {filename} to s3://{bucket}/{s3_key}")
    s3.upload_file(str(local_path), bucket, s3_key)
