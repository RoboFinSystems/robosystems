"""SEC Knowledge Artifact Generation Asset.

Generates precomputed Parquet artifacts from the full DuckDB staging database
for graph-based confidence refinement during XBRL enrichment.

Artifacts:
  - element_knowledge.parquet: Graph-structural signals per element qname
  - structure_profiles.parquet: Element frequency distributions per canonical_type
  - structure_consensus.parquet: Cross-filing majority-vote for identical structures
  - disclosure_profiles.parquet: Element frequency distributions per disclosure type
  - disclosure_consensus.parquet: Cross-filing majority-vote for disclosure types

Dev: opens local DuckDB at {DUCKDB_STAGING_PATH}/{source}.duckdb
Prod: downloads from S3 first (same pattern as DuckDBAnalyticsContext)
"""

import gc
from typing import TYPE_CHECKING

if TYPE_CHECKING:
  from pathlib import Path

from dagster import (
  AssetExecutionContext,
  Config,
  MaterializeResult,
  asset,
)
from pydantic import Field


class SECArtifactConfig(Config):
  """Configuration for SEC artifact generation."""

  duckdb_source: str = "sec"
  # DuckDB memory budget per builder connection, bounded on both sides. Too
  # low and DuckDB itself OOMs on un-spillable block pins during the
  # structure-composition extraction (an 8GB budget died this way on the
  # Jul 2026 ~55GB corpus, regardless of task size). Too close to the ECS
  # task memory (24GB, see jobs.py) and the result set materialized in
  # Python on top of DuckDB's buffer starves the kernel into OOM-killing
  # the task (SIGKILL — a 14GB budget on the older 16GB task). 16GB leaves
  # ~8GB of Python headroom and cleared the Jul 2026 corpus.
  memory_limit: str = Field(
    default="16GB",
    description="DuckDB memory budget per builder; keep well below task memory.",
  )
  publish_r2: bool = False


@asset(
  group_name="sec_pipeline",
  description="Generate element + structure knowledge artifacts from DuckDB staging",
  kinds={"duckdb", "analytics", "parquet"},
  deps=["sec_duckdb_s3_published"],
  metadata={"pipeline": "sec", "stage": "artifact"},
)
def sec_knowledge_artifacts(
  context: AssetExecutionContext,
  config: SECArtifactConfig,
) -> MaterializeResult:
  """Generate all knowledge artifacts for graph-based confidence refinement.

  Downloads the DuckDB staging file (in prod) or opens it locally (in dev),
  then runs each builder sequentially. Each artifact is uploaded to S3
  immediately after generation (in non-dev), and the builder is freed before
  the next one starts to keep peak memory bounded.
  """
  from robosystems.adapters.sec.knowledge.artifact import (
    DisclosureProfileBuilder,
    ElementKnowledgeBuilder,
    StructureKnowledgeBuilder,
    _log_memory,
  )
  from robosystems.adapters.sec.knowledge.framework import DuckDBAnalyticsContext
  from robosystems.config import env

  is_prod = env.ENVIRONMENT != "dev"
  _log_memory("asset start")

  # Keep context open for temp dir lifetime (prod S3 download), but close the
  # idle DuckDB connection immediately — builders open their own connections.
  with DuckDBAnalyticsContext(
    duckdb_source=config.duckdb_source,
    memory_limit="256MB",
  ) as ctx:
    db_path = ctx.db_path
    db_size_gb = db_path.stat().st_size / (1024**3) if db_path.exists() else 0
    context.log.info(
      f"Building artifacts from DuckDB at: {db_path} ({db_size_gb:.1f} GB)"
    )

    # Log available disk space (important: 104GB file on 200GB ephemeral)
    try:
      import shutil

      disk = shutil.disk_usage(str(db_path.parent))
      context.log.info(
        f"Disk: {disk.used / (1024**3):.1f} GB used, "
        f"{disk.free / (1024**3):.1f} GB free of {disk.total / (1024**3):.1f} GB"
      )
    except Exception:
      pass  # Non-fatal: disk usage logging is best-effort

    # Close the idle DuckDB connection to free its memory buffer.
    # The context stays open to keep the temp directory alive (prod S3 download).
    ctx.close_connection()
    gc.collect()
    _log_memory("after DuckDB download + connection close")

    # --- Element knowledge (must run first — PageRank used by disclosure profiles) ---
    context.log.info("Building element knowledge artifact")
    element_builder = ElementKnowledgeBuilder(memory_limit=config.memory_limit)
    element_path = element_builder.build(db_path)
    del element_builder
    gc.collect()
    _log_memory("after element builder cleanup")
    context.log.info(f"Element knowledge artifact written to: {element_path}")

    if is_prod:
      _upload_artifact(
        context, element_path, "element_knowledge.parquet", config.publish_r2
      )

    # --- Structure knowledge ---
    context.log.info("Building structure knowledge artifacts")
    structure_builder = StructureKnowledgeBuilder(memory_limit=config.memory_limit)
    profiles_path, consensus_path = structure_builder.build(db_path)
    del structure_builder
    gc.collect()
    _log_memory("after structure builder cleanup")
    context.log.info(f"Structure profiles artifact written to: {profiles_path}")
    context.log.info(f"Structure consensus artifact written to: {consensus_path}")

    if is_prod:
      _upload_artifact(
        context, profiles_path, "structure_profiles.parquet", config.publish_r2
      )
      _upload_artifact(
        context, consensus_path, "structure_consensus.parquet", config.publish_r2
      )

    # --- Disclosure knowledge (uses PageRank from element_knowledge) ---
    context.log.info("Building disclosure knowledge artifacts")
    disclosure_builder = DisclosureProfileBuilder(memory_limit=config.memory_limit)
    disc_profiles_path, disc_consensus_path = disclosure_builder.build(db_path)
    del disclosure_builder
    gc.collect()
    _log_memory("after disclosure builder cleanup")
    context.log.info(f"Disclosure profiles artifact written to: {disc_profiles_path}")
    context.log.info(f"Disclosure consensus artifact written to: {disc_consensus_path}")

    if is_prod:
      _upload_artifact(
        context, disc_profiles_path, "disclosure_profiles.parquet", config.publish_r2
      )
      _upload_artifact(
        context, disc_consensus_path, "disclosure_consensus.parquet", config.publish_r2
      )

  return MaterializeResult(
    metadata={
      "element_knowledge_path": str(element_path),
      "structure_profiles_path": str(profiles_path),
      "structure_consensus_path": str(consensus_path),
      "disclosure_profiles_path": str(disc_profiles_path),
      "disclosure_consensus_path": str(disc_consensus_path),
    }
  )


def _upload_artifact(
  context: AssetExecutionContext,
  local_path: "Path",
  filename: str,
  publish_r2: bool = False,
) -> None:
  """Upload a single artifact to S3 and optionally R2.

  S3 is the primary store for prod/staging enrichment (private).
  R2 provides public downloads for local development (zero egress).
  R2 upload only runs when publish_r2=True (set via Dagster config).
  """
  from robosystems.config import env
  from robosystems.config.storage.shared import DataSourceType, get_processed_key
  from robosystems.operations.aws.s3 import S3Client

  # S3 upload (primary — used by prod/staging enrichment)
  s3 = S3Client()
  bucket = env.SHARED_PROCESSED_BUCKET
  s3_key = get_processed_key(DataSourceType.SEC, "artifacts", filename)
  context.log.info(f"Uploading {filename} to s3://{bucket}/{s3_key}")
  s3.upload_file(str(local_path), bucket, s3_key)

  # R2 upload (public — used by dev enrichment)
  if publish_r2:
    _upload_artifact_r2(context, local_path, filename)


def _upload_artifact_r2(
  context: AssetExecutionContext,
  local_path: "Path",
  filename: str,
) -> None:
  """Upload a single artifact to the public R2 bucket for dev downloads."""
  from robosystems.config import env
  from robosystems.config.storage.shared import get_artifact_r2_key

  r2_config = env.get_r2_config()
  if not r2_config or not env.R2_PUBLIC_BUCKET_NAME:
    return

  try:
    import boto3

    name = filename.removesuffix(".parquet")
    r2 = boto3.client("s3", **r2_config)
    r2_key = get_artifact_r2_key(name)
    bucket = env.R2_PUBLIC_BUCKET_NAME
    context.log.info(f"Uploading {filename} to r2://{bucket}/{r2_key}")
    r2.upload_file(str(local_path), bucket, r2_key)
  except Exception as e:
    context.log.warning(f"R2 upload failed for {filename} (non-fatal): {e}")
