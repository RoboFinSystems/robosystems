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

from robosystems.dagster.assets import (
  sec_backup,
  sec_duckdb_incremental_staged,
  sec_duckdb_staged,
  sec_entity_incremental_update,
  sec_graph_direct_copy,
  sec_graph_incremental_copy,
  sec_graph_materialized,
  sec_processed_filings,
  sec_quarter_partitions,
  sec_raw_filings,
  sec_replicas_refreshed,
  sec_s3_published,
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
  description="Download SEC XBRL filings to S3 via EFTS (quarterly partitions).",
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
  description="Process an entire quarter's SEC filings with disk-buffered output.",
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
  description="Stage SEC files to persistent DuckDB (no graph ingestion).",
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
  description="Materialize SEC graph from DuckDB staging (retry-safe).",
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
  description="Full SEC pipeline: stage to DuckDB then materialize to LadybugDB.",
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
# Incremental staging to DuckDB for daily SEC updates.
# INSERT new quarter files with dedup - only net new rows added.
#
# This keeps DuckDB ready for a full rebuild if needed, but we don't
# run the expensive sec_graph_materialized (full rebuild) daily.
# Instead, after staging, we run sec_incremental_copy for fast updates.
#
# Chain: process → stage (this) → copy → S3 sync

sec_incremental_stage_job = define_asset_job(
  name="sec_incremental_stage",
  description="Incremental DuckDB staging (keeps DuckDB in sync for potential rebuilds).",
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
# Phase 3c: Direct S3 → LadybugDB Copy (Bypasses DuckDB Staging)
# ============================================================================
# Alternative to DuckDB staging for large-scale loads where DuckDB
# merge operations would exceed memory limits.
#
# Benefits:
# - No memory pressure from DuckDB merge/dedupe operations
# - Proven to work at scale (200M+ rows)
# - Uses LadybugDB's httpfs extension with spill_to_disk=true
# - Simpler pipeline with fewer moving parts
#
# Trade-offs:
# - Relies on LadybugDB constraints for deduplication (not pre-deduped)
# - Must rebuild graph to avoid duplicates (rebuild_graph=true recommended)

sec_direct_copy_job = define_asset_job(
  name="sec_direct_copy",
  description="Direct S3 → LadybugDB copy (bypasses DuckDB staging).",
  selection=AssetSelection.assets(sec_graph_direct_copy),
  tags={
    "pipeline": "sec",
    "phase": "direct_copy",
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
# Phase 3d: Incremental S3 → LadybugDB Copy (Preferred for Daily Updates)
# ============================================================================
# Preferred approach for daily incremental updates because:
# - Copies directly from S3 parquet to LadybugDB (bypasses DuckDB)
# - Uses ignore_errors=true to skip duplicates (constraint violations)
# - No need to diff what's new in DuckDB vs LadybugDB
# - Simpler and faster than DuckDB incremental staging
#
# Only scans current quarter + previous quarter during 5-day overlap.
# Safe to run daily - duplicates are rejected by LadybugDB constraints.

sec_incremental_copy_job = define_asset_job(
  name="sec_incremental_copy",
  description="Incremental S3 → LadybugDB copy (preferred for daily updates).",
  selection=AssetSelection.assets(sec_graph_incremental_copy),
  tags={
    "pipeline": "sec",
    "mode": "incremental_copy",
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
# Phase 3d: Entity Update (Update Mutable Entity Attributes)
# ============================================================================
# Updates existing Entity nodes with latest attribute values.
# Entity nodes are unique in being mutable - company names, tickers,
# filer categories can change over time.
#
# The incremental COPY only INSERTs new records - it cannot update existing
# ones due to primary key constraints. This job uses Cypher MERGE to update
# existing Entity nodes.
#
# Typically 50-200 entities change per quarter. MERGE is 40x slower than COPY,
# but acceptable for small update volumes.
#
# Chain: process → stage → copy → entity_update → S3 sync

sec_entity_update_job = define_asset_job(
  name="sec_entity_update",
  description="Update existing Entity nodes with latest data (handles mutable attributes).",
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
  description="Create downloadable backup of SEC database for user downloads.",
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
# Phase 5: Publish (S3 for Replica Cluster)
# ============================================================================
# Publishes the raw .lbug database to S3 for replica cluster consumption.
# Replicas use LadybugDB S3 ATTACH to connect directly to the published database.
# This is distinct from sec_backup which creates compressed, downloadable backups.

sec_s3_publish_job = define_asset_job(
  name="sec_s3_publish",
  description="Publish SEC database to S3 for replica cluster (S3 ATTACH source).",
  selection=AssetSelection.assets(sec_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "publish",
    # Minimal profile: runs CHECKPOINT via HTTP, then SSM for S3 upload
    # Actual upload happens on the shared master instance via SSM
    "ecs/cpu": "256",
    "ecs/memory": "512",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions during publish
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 5b: Replica Fleet Refresh (After S3 Publish)
# ============================================================================
# Triggers rolling refresh of replica fleet to pick up new S3 database.
# Monitors progress as each instance is replaced.
# At scale (100+ replicas), this can take hours.

sec_replica_refresh_job = define_asset_job(
  name="sec_replica_refresh",
  description="Rolling refresh of replica fleet to pick up new S3 database.",
  selection=AssetSelection.assets(sec_replicas_refreshed),
  tags={
    "pipeline": "sec",
    "phase": "replica_refresh",
    # Minimal profile: just monitoring ASG refresh via boto3
    "ecs/cpu": "256",
    "ecs/memory": "512",
    "ecs/ephemeral_storage": "21",
    # On-demand to avoid interruptions during long-running refresh monitoring
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)
