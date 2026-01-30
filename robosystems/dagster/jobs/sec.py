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

from robosystems.config import env
from robosystems.dagster.assets import (
  sec_duckdb_incremental_stage,
  sec_duckdb_staged,
  sec_graph_materialized,
  sec_processed_filings,
  sec_quarter_partitions,
  sec_raw_filings,
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
# Uses Heavy profile (16 vCPU, 64 GB) - sized to handle full quarterly batches.
# EFTS returns max 10k filings per quarterly partition, so batch_limit=10000
# matches the theoretical maximum. Uses On-Demand Fargate for reliability.
sec_process_job = define_asset_job(
  name="sec_process",
  description="Process an entire quarter's SEC filings (up to 10k) with heavy resources.",
  selection=AssetSelection.assets(
    sec_processed_filings,
  ),
  partitions_def=sec_quarter_partitions,
  tags={
    "pipeline": "sec",
    "phase": "process",
    # Heavy profile: 16 vCPU, 64 GB - sized for full quarterly batches (10k filings max)
    "ecs/task_definition": f"robosystems-dagster-run-heavy-{env.ENVIRONMENT}",
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
# Uses Standard profile (2 vCPU, 8 GB) - staging is I/O bound, not CPU intensive.
sec_stage_job = define_asset_job(
  name="sec_stage",
  description="Stage SEC files to persistent DuckDB (no graph ingestion).",
  selection=AssetSelection.assets(sec_duckdb_staged),
  tags={
    "pipeline": "sec",
    "phase": "stage",
    # Standard profile: 2 vCPU, 8 GB - staging is mostly I/O bound
    "ecs/task_definition": f"robosystems-dagster-run-standard-{env.ENVIRONMENT}",
    # Long-running job (2+ hours) - use on-demand to avoid Spot interruptions
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
# Uses Standard profile (2 vCPU, 8 GB) - materialization is network/API bound.
sec_materialize_job = define_asset_job(
  name="sec_materialize",
  description="Materialize SEC graph from DuckDB staging (retry-safe).",
  selection=AssetSelection.assets(sec_graph_materialized),
  tags={
    "pipeline": "sec",
    "phase": "materialize",
    # Standard profile: 2 vCPU, 8 GB - materialization is network/API bound
    "ecs/task_definition": f"robosystems-dagster-run-standard-{env.ENVIRONMENT}",
    # Long-running job - use on-demand to avoid Spot interruptions
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)

# Combined: Run both stages in sequence
# Useful for full rebuilds with checkpointing between stages.
# Uses Standard profile (2 vCPU, 8 GB) - combined stage+materialize is I/O bound.
sec_staged_materialize_job = define_asset_job(
  name="sec_staged_materialize",
  description="Full SEC pipeline: stage to DuckDB then materialize to LadybugDB.",
  selection=AssetSelection.assets(sec_duckdb_staged, sec_graph_materialized),
  tags={
    "pipeline": "sec",
    "phase": "full",
    # Standard profile: 2 vCPU, 8 GB - stage+materialize is I/O/network bound
    "ecs/task_definition": f"robosystems-dagster-run-standard-{env.ENVIRONMENT}",
    # Long-running job (2+ hours) - use on-demand to avoid Spot interruptions
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)


# ============================================================================
# Phase 3b: Incremental Ingest (Daily Updates)
# ============================================================================
# Incremental pipeline for daily SEC updates:
# 1. sec_duckdb_incremental_stage: INSERT new quarter files with dedup
# 2. sec_graph_materialized: Full rebuild from updated DuckDB tables
#
# Unlike full rebuild, incremental staging only adds net new rows.
# Materialization still does full rebuild since LadybugDB doesn't support
# incremental updates efficiently.

sec_incremental_ingest_job = define_asset_job(
  name="sec_incremental_ingest",
  description="Incremental SEC ingest: stage new dates → materialize → snapshot.",
  selection=AssetSelection.assets(
    sec_duckdb_incremental_stage,
    sec_graph_materialized,
  ),
  tags={
    "pipeline": "sec",
    "mode": "incremental",
    # Standard profile: 2 vCPU, 8 GB - incremental staging is I/O bound
    "ecs/task_definition": f"robosystems-dagster-run-standard-{env.ENVIRONMENT}",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)
