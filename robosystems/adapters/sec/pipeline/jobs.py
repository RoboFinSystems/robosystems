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
from .backup import sec_backup
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
from .s3_publish import sec_historical_lbug_s3_published
from .stage import (
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
  sec_historical_duckdb_staged,
)

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
# Each run processes an entire quarter's worth of filings (up to 10,000),
# outputting consolidated parquet files. Parallel execution across quarters
# is controlled by DAGSTER_MAX_CONCURRENT_RUNS.
#
# Uses Standard profile (2 vCPU, 8 GB) - processing is disk-buffered, not memory-intensive.
# Each filing is processed independently to disk, then uploaded to S3.
# EFTS returns max 10k filings per quarterly partition, but batch_limit caps per run.
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
    # Standard profile: 2 vCPU, 8 GB, 50 GB storage - disk-buffered processing
    "ecs/cpu": "2048",
    "ecs/memory": "8192",
    "ecs/ephemeral_storage": "50",
    # Long-running job (hours) - use on-demand to avoid Spot interruptions
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
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
    # Minimal profile: just orchestrating Graph API calls, no local compute
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
    # Minimal profile: just orchestrating Graph API calls, no local compute
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
    # Minimal profile: just orchestrating Graph API calls, no local compute
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
    # Minimal profile: just orchestrating Graph API calls, no local compute
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
    # Minimal profile: just orchestrating Graph API calls, no local compute
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
# Phase 4: Backup (Downloadable .lbug for Users)
# ============================================================================
# Creates downloadable backups of the SEC database for users with
# repository subscriptions. Run after materialization completes.

sec_backup_job = define_asset_job(
  name="sec_create_backup",
  description="Create downloadable SEC database backup.",
  selection=AssetSelection.assets(sec_backup),
  tags={
    "pipeline": "sec",
    "phase": "backup",
    # Minimal profile: just orchestrating Graph API calls, no local compute
    # Backup runs entirely on-instance (CHECKPOINT + tar.gz + S3 upload)
    "ecs/cpu": "256",
    "ecs/memory": "512",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions during backup monitoring
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 5: S3 Publish (DuckDB + Historical LadybugDB)
# ============================================================================
# Primary LadybugDB publish (sec_lbug_s3_published) has no job here - it's
# triggered by asset lineage from sec_graph_materialized.
# This section covers: DuckDB publishes + historical LadybugDB (ad-hoc).

sec_duckdb_s3_publish_job = define_asset_job(
  name="sec_duckdb_s3_publish",
  description="Publish SEC DuckDB staging to S3 (raw .duckdb).",
  selection=AssetSelection.assets(sec_duckdb_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "duckdb_s3_publish",
    # Minimal profile: just orchestrating Graph API calls, no local compute
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
    # Minimal profile: just orchestrating Graph API calls, no local compute
    "ecs/cpu": "256",
    "ecs/memory": "512",
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
    # Minimal profile: just orchestrating Graph API calls, no local compute
    "ecs/cpu": "256",
    "ecs/memory": "512",
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

sec_artifact_generation_job = define_asset_job(
  name="sec_artifact_generation",
  description="Generate element + structure knowledge artifacts.",
  selection=AssetSelection.assets(sec_knowledge_artifacts),
  tags={
    "pipeline": "sec",
    "phase": "artifact",
    # Same profile as analytics — local DuckDB + Python compute
    "ecs/cpu": "4096",
    "ecs/memory": "16384",
    "ecs/ephemeral_storage": "100",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)
