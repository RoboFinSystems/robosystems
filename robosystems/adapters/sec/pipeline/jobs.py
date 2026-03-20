"""Dagster SEC pipeline jobs and schedules.

Pipeline Architecture (3 phases, run independently):

  Phase 1 - Download (EFTS-based, quarterly partitions):
    sec_download_job: sec_raw_filings
    Uses SEC EFTS API to discover and download XBRL ZIPs to S3.
    Quarterly partitions (e.g., 2024-Q1) to stay under EFTS 10k result limit.
    Creates SourceFile records in PostgreSQL for processing tracking.

  Phase 2 - Process (quarterly batch with consolidated output):
    sec_process_job: sec_processed_filings
    Each run processes an entire quarter's worth of filings.
    Outputs consolidated parquet files (one per table per quarter).
    Individual filing failures tracked in SourceFile; job continues processing.
    Parallel across quarters via DAGSTER_MAX_CONCURRENT_RUNS.

  Phase 3 - Materialize (two-stage pipeline):
    sec_stage_job: sec_duckdb_staged (DuckDB staging - full rebuild)
    sec_materialize_job: sec_graph_materialized (LadybugDB materialization)

    If LadybugDB materialization fails, just re-run sec_materialize_job.

Workflow:
  just sec-download 10 2024    # Download top 10 companies (all 4 quarters)
  # Enable sec_processing_sensor in Dagster UI to auto-process quarters
  just sec-materialize         # Stage to DuckDB + Materialize to LadybugDB

  # Decoupled (for checkpointing/retry):
  just sec-stage               # Stage 1: Stage to DuckDB only (full mode)
  just sec-materialize-graph   # Stage 2: Materialize to LadybugDB (retry-safe)

  # Manual quarter processing (via Dagster UI):
  # Launch sec_process job with partition_key: "2024-Q1"

  # Or all-in-one for demos:
  just sec-load NVDA 2024      # Chains all steps for single company
"""

from dagster import (
  AssetSelection,
  define_asset_job,
)

from .artifact import sec_knowledge_artifacts
from .configs import sec_quarter_partitions
from .download import sec_raw_filings
from .duckdb_s3_publish import (
  sec_duckdb_s3_published,
  sec_historical_duckdb_s3_published,
)
from .entity_update import sec_entity_incremental_update
from .materialize import (
  sec_graph_materialized,
  sec_historical_materialized,
)
from .process import sec_processed_filings
from .r2_publish import sec_lbug_r2_published
from .s3_publish import sec_historical_lbug_s3_published, sec_lbug_s3_published
from .stage import (
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
  sec_historical_duckdb_staged,
)
from .text_index import sec_narratives_indexed, sec_textblocks_indexed
from .vector_publish import sec_vector_s3_published

# ============================================================================
# SEC Pipeline Jobs
# ============================================================================


# Phase 1: Download (quarter-partitioned)
# Downloads raw XBRL ZIPs to S3 using EFTS discovery.
# Uses quarterly partitions to stay under EFTS 10k result limit.
# Use with sec_processing_sensor to trigger parallel processing.
sec_download_job = define_asset_job(
  name="sec_download",
  description="Download SEC XBRL filings from EFTS to S3.",
  selection=AssetSelection.assets(
    sec_raw_filings,
  ),
  tags={"pipeline": "sec", "phase": "download"},
  partitions_def=sec_quarter_partitions,
)


# Phase 2: Process (quarterly batch processing)
# Each run processes up to 250 filings, flushes to S3, then exits.
# Sensor re-triggers while pending files remain. Parallel execution across
# quarters is controlled by DAGSTER_MAX_CONCURRENT_RUNS.
#
# Uses Enhanced profile (4 vCPU, 16 GB) - embedding enrichment is memory-intensive.
# Spot-preferred: 250-filing batches keep runs to ~3-5 hrs, reducing Spot
# interruption risk. On reclaim, pending filings are re-triggered by sensor.
sec_process_job = define_asset_job(
  name="sec_process",
  description="Process SEC filings into parquet files.",
  selection=AssetSelection.assets(
    sec_processed_filings,
  ),
  partitions_def=sec_quarter_partitions,
  tags={
    "pipeline": "sec",
    "phase": "process",
    # Enhanced profile: 4 vCPU, 16 GB, 50 GB storage - embedding enrichment is memory-intensive
    "ecs/cpu": "4096",
    "ecs/memory": "16384",
    "ecs/ephemeral_storage": "50",
    # Spot-preferred: S3 cache makes runs fully resilient to Spot interruptions.
    # On reclaim, completed filings are restored from cache on next run.
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE_SPOT", "weight": 9, "base": 0},
        {"capacityProvider": "FARGATE", "weight": 1, "base": 0},
      ],
    },
  },
)


# ============================================================================
# Phase 3: Materialize (Decoupled Staging + Materialization)
# ============================================================================
# Decoupled design enables retry of materialization without re-staging:
# - sec_stage_job: Stage to persistent DuckDB (2+ hours for full SEC)
# - sec_materialize_job: Materialize from DuckDB to LadybugDB (retry-safe)
#
# If LadybugDB materialization fails, just re-run sec_materialize_job.

# Stage 1: DuckDB Staging
# Discovers processed files from S3 and stages to persistent DuckDB.
# Actual DuckDB work happens on LadybugDB instance via Graph API.
sec_stage_job = define_asset_job(
  name="sec_stage",
  description="Stage SEC parquet files to DuckDB (full rebuild).",
  selection=AssetSelection.assets(sec_duckdb_staged),
  tags={
    "pipeline": "sec",
    "phase": "stage",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions (long-running orchestration)
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)

# Stage 2: LadybugDB Materialization
# Materializes to LadybugDB from existing DuckDB staging.
# Retry-safe: if this fails, just re-run it - DuckDB staging is preserved.
# Actual materialization happens on LadybugDB instance via Graph API.
sec_materialize_job = define_asset_job(
  name="sec_materialize",
  description="Materialize SEC graph from DuckDB to LadybugDB.",
  selection=AssetSelection.assets(sec_graph_materialized),
  tags={
    "pipeline": "sec",
    "phase": "materialize",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions (long-running orchestration)
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)

# Combined: Run both stages in sequence
# Useful for full rebuilds with checkpointing between stages.
# Actual work happens on LadybugDB instance via Graph API.
sec_staged_materialize_job = define_asset_job(
  name="sec_staged_materialize",
  description="Stage and materialize SEC graph (DuckDB + LadybugDB).",
  selection=AssetSelection.assets(sec_duckdb_staged, sec_graph_materialized),
  tags={
    "pipeline": "sec",
    "phase": "full",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions (long-running orchestration)
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 3b: Incremental DuckDB Staging (Keep DuckDB in Sync)
# ============================================================================
# Incremental staging to DuckDB for nightly SEC updates.
# INSERT new quarter files with dedup - only net new rows added.
#
# After staging, sec_stage_to_materialize_sensor triggers a full
# LadybugDB rebuild from DuckDB (feasible because sec graph is 2024+ only).
#
# Chain: process → stage (this) → materialize → entity update → S3 sync

sec_incremental_stage_job = define_asset_job(
  name="sec_incremental_stage",
  description="Stage current quarter to SEC DuckDB (incremental).",
  selection=AssetSelection.assets(sec_duckdb_incremental_staged),
  tags={
    "pipeline": "sec",
    "mode": "incremental",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions (long-running orchestration)
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 3c: Historical DuckDB Stage + Materialize (for sec_historical)
# ============================================================================
# Two-stage pipeline for sec_historical: DuckDB staging then LadybugDB materialization.
# Uses the same decoupled pattern as the primary sec graph.

sec_historical_stage_job = define_asset_job(
  name="sec_historical_stage",
  description="Stage SEC historical parquet files to DuckDB (full rebuild).",
  selection=AssetSelection.assets(sec_historical_duckdb_staged),
  tags={
    "pipeline": "sec",
    "phase": "historical_stage",
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)

sec_historical_materialize_job = define_asset_job(
  name="sec_historical_materialize",
  description="Materialize SEC historical graph from DuckDB to LadybugDB.",
  selection=AssetSelection.assets(sec_historical_materialized),
  tags={
    "pipeline": "sec",
    "phase": "historical_materialize",
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)

sec_historical_staged_materialize_job = define_asset_job(
  name="sec_historical_staged_materialize",
  description="Stage and materialize SEC historical graph (DuckDB + LadybugDB).",
  selection=AssetSelection.assets(
    sec_historical_duckdb_staged, sec_historical_materialized
  ),
  tags={
    "pipeline": "sec",
    "phase": "historical_full",
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 3d: Entity Update (Update Mutable Entity Attributes)
# ============================================================================
# Updates existing Entity nodes with latest attribute values.
# Entity nodes are unique in being mutable - company names, tickers,
# filer categories can change over time.
#
# Materialization rebuilds the graph from DuckDB but cannot update existing
# Entity nodes with changed attributes. This job uses Cypher MERGE to update
# existing Entity nodes with latest values.
#
# Typically 50-200 entities change per quarter. MERGE is 40x slower than COPY,
# but acceptable for small update volumes.
#
# Chain: process → stage → materialize → entity_update → S3 sync

sec_entity_update_job = define_asset_job(
  name="sec_entity_update",
  description="Update mutable Entity attributes via Cypher MERGE.",
  selection=AssetSelection.assets(sec_entity_incremental_update),
  tags={
    "pipeline": "sec",
    "mode": "entity_update",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions (long-running orchestration)
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 4: S3 Publish (LadybugDB + DuckDB)
# ============================================================================
# Post-materialization publish chain (sequential to avoid overloading instance):
#   materialize → lbug S3 publish → duckdb S3 publish
# Orchestrated by sec_post_materialize_publish_sensor.

sec_lbug_s3_publish_job = define_asset_job(
  name="sec_lbug_s3_publish",
  description="Publish SEC LadybugDB database to S3 for replica cluster.",
  selection=AssetSelection.assets(sec_lbug_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "lbug_s3_publish",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions during large uploads
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)

sec_duckdb_s3_publish_job = define_asset_job(
  name="sec_duckdb_s3_publish",
  description="Publish SEC DuckDB staging to S3 (raw .duckdb).",
  selection=AssetSelection.assets(sec_duckdb_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "duckdb_s3_publish",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions during large uploads
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)

sec_historical_duckdb_s3_publish_job = define_asset_job(
  name="sec_historical_duckdb_s3_publish",
  description="Publish SEC historical DuckDB staging to S3 (raw .duckdb).",
  selection=AssetSelection.assets(sec_historical_duckdb_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "duckdb_s3_publish",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions during large uploads
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# Historical LadybugDB publish - run ad-hoc after historical graph rebuild.

sec_historical_lbug_s3_publish_job = define_asset_job(
  name="sec_historical_lbug_s3_publish",
  description="Publish SEC historical database to S3 for replica cluster.",
  selection=AssetSelection.assets(sec_historical_lbug_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "s3_publish",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions during large uploads
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 4b: Vector Index S3 Publish
# ============================================================================
# Exports LanceDB vector index from graph instance and uploads to S3.
# Runs after DuckDB S3 publish in the nightly chain.
# The index was built during DuckDB staging (sec_duckdb_staged).

sec_vector_s3_publish_job = define_asset_job(
  name="sec_vector_s3_publish",
  description="Export and publish SEC vector index to S3 for replica cluster.",
  selection=AssetSelection.assets(sec_vector_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "vector_s3_publish",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions during uploads
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 5b: R2 Publish (Zero-egress subscriber downloads)
# ============================================================================
# Publishes raw .lbug to Cloudflare R2 for subscriber downloads.
# Same on-instance backup pattern as S3 publish, but R2 has zero egress fees.
# Creates/updates GraphBackup record so file appears in download list.

sec_lbug_r2_publish_job = define_asset_job(
  name="sec_lbug_r2_publish",
  description="Publish SEC database to R2 for zero-egress subscriber downloads.",
  selection=AssetSelection.assets(sec_lbug_r2_published),
  tags={
    "pipeline": "sec",
    "phase": "r2_publish",
    # Light profile: HTTP orchestration to Graph API
    "ecs/cpu": "512",
    "ecs/memory": "2048",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions during large uploads
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 6: Artifact Generation (Knowledge artifacts for enrichment refinement)
# ============================================================================
# Generates precomputed Parquet artifacts from DuckDB staging for graph-based
# confidence refinement. Compute-heavy: runs graph algorithms locally.

# ============================================================================
# Phase 5c: Text Search Indexing (OpenSearch)
# ============================================================================
# Index filing text content into OpenSearch for full-text search.
# Two assets: XBRL text blocks (already externalized) + narrative sections (extracted from raw HTML).

sec_textblocks_index_job = define_asset_job(
  name="sec_textblocks_index",
  description="Index XBRL text blocks into OpenSearch.",
  selection=AssetSelection.assets(sec_textblocks_indexed),
  tags={
    "pipeline": "sec",
    "phase": "text_index",
    "ecs/cpu": "1024",
    "ecs/memory": "4096",
    "ecs/ephemeral_storage": "21",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE_SPOT", "weight": 9, "base": 0},
        {"capacityProvider": "FARGATE", "weight": 1, "base": 0},
      ],
    },
  },
)

sec_narratives_index_job = define_asset_job(
  name="sec_narratives_index",
  description="Extract and index narrative sections from SEC filings into OpenSearch.",
  selection=AssetSelection.assets(sec_narratives_indexed),
  tags={
    "pipeline": "sec",
    "phase": "text_index",
    "ecs/cpu": "1024",
    "ecs/memory": "4096",
    "ecs/ephemeral_storage": "50",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE_SPOT", "weight": 9, "base": 0},
        {"capacityProvider": "FARGATE", "weight": 1, "base": 0},
      ],
    },
  },
)


sec_artifact_generation_job = define_asset_job(
  name="sec_artifact_generation",
  description="Generate element + structure knowledge artifacts.",
  selection=AssetSelection.assets(sec_knowledge_artifacts),
  tags={
    "pipeline": "sec",
    "phase": "artifact",
    # Downloads full DuckDB staging file from S3 then runs graph algorithms.
    # DuckDB uses threads=1 + spill-to-disk for large joins.
    # Observed peak: ~14.4 GB (Mar 2026, 104 GB corpus). 16 GB to test lower bound.
    # Ephemeral: 200GB covers 104GB DuckDB + spill (structure membership query) + artifacts.
    "ecs/cpu": "2048",
    "ecs/memory": "16384",
    "ecs/ephemeral_storage": "200",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)
