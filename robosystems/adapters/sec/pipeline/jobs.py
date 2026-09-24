"""Dagster SEC pipeline jobs.

Phases: download (EFTS, quarter-partitioned) → process (one batch of filings
to parquet per run, re-triggered by the sensor) → stage to DuckDB →
materialize to LadybugDB → publish. Staging and materialization are separate
jobs so a failed materialize re-runs without re-staging. Local recipes:
``just sec-download``, ``just sec-materialize``, ``just sec-load``.
"""

from dagster import (
  AssetSelection,
  define_asset_job,
)

from .artifact import sec_knowledge_artifacts
from .catalog import sec_filing_catalog
from .configs import sec_quarter_partitions
from .download import sec_raw_filings
from .duckdb_s3_publish import (
  sec_duckdb_s3_published,
  sec_historical_duckdb_s3_published,
)
from .hf_publish import sec_lbug_hf_published
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
from .text_index import (
  sec_ixbrl_disclosures_indexed,
  sec_narratives_indexed,
)

# Stage/materialize/publish jobs run on a light on-demand task: the work
# happens on the LadybugDB instance via the Graph API, and a long
# orchestration run should not be Spot-interrupted.

sec_download_job = define_asset_job(
  name="sec_download",
  description="Download SEC XBRL filings from EFTS to S3.",
  selection=AssetSelection.assets(
    sec_raw_filings,
  ),
  tags={"pipeline": "sec", "phase": "download"},
  partitions_def=sec_quarter_partitions,
)


# One batch per run; the sensor re-triggers while pending files remain.
# 4 vCPU / 16 GB for embedding enrichment. Spot is safe: completed filings
# are restored from the S3 cache on the next run.
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
    # No dagster/max_retries on purpose: the processing sensor is the single
    # recovery path, and Dagster's auto-retry races its active-run guard
    # (which sees only STARTED/QUEUED), producing a second run for the quarter
    # and duplicate part files. The `quarter` tag_concurrency_limit in
    # dagster_prod.yaml is the backstop.
    "ecs/cpu": "4096",
    "ecs/memory": "16384",
    "ecs/ephemeral_storage": "50",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE_SPOT", "weight": 9, "base": 0},
        {"capacityProvider": "FARGATE", "weight": 1, "base": 0},
      ],
    },
  },
)


# Full staging takes 2+ hours; a failed materialize re-runs from the
# preserved staging.
sec_stage_job = define_asset_job(
  name="sec_stage",
  description="Stage SEC parquet files to DuckDB (full rebuild).",
  selection=AssetSelection.assets(sec_duckdb_staged),
  tags={
    "pipeline": "sec",
    "phase": "stage",
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

sec_materialize_job = define_asset_job(
  name="sec_materialize",
  description="Materialize SEC graph from DuckDB to LadybugDB.",
  selection=AssetSelection.assets(sec_graph_materialized),
  tags={
    "pipeline": "sec",
    "phase": "materialize",
    # One writer per database (tag_concurrency_limits in dagster_prod.yaml):
    # concurrent COPYs silently duplicate rel-table edges, which have no
    # primary key. Job-level so manual launches carry it too.
    "materialize_db": "sec",
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

sec_staged_materialize_job = define_asset_job(
  name="sec_staged_materialize",
  description="Stage and materialize SEC graph (DuckDB + LadybugDB).",
  selection=AssetSelection.assets(sec_duckdb_staged, sec_graph_materialized),
  tags={
    "pipeline": "sec",
    "phase": "full",
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


# Nightly: net-new rows only. The follow-on materialize is a full LadybugDB
# rebuild, feasible because the sec graph is 2024+ only.
sec_incremental_stage_job = define_asset_job(
  name="sec_incremental_stage",
  description="Stage current quarter to SEC DuckDB (incremental).",
  selection=AssetSelection.assets(sec_duckdb_incremental_staged),
  tags={
    "pipeline": "sec",
    "mode": "incremental",
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


# Publishes run sequentially after materialize (lbug, then duckdb) so the
# instance is not overloaded; sec_post_materialize_publish_sensor chains them.
sec_lbug_s3_publish_job = define_asset_job(
  name="sec_lbug_s3_publish",
  description="Publish SEC LadybugDB database to S3 for replica cluster.",
  selection=AssetSelection.assets(sec_lbug_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "lbug_s3_publish",
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

sec_duckdb_s3_publish_job = define_asset_job(
  name="sec_duckdb_s3_publish",
  description="Publish SEC DuckDB staging to S3 (raw .duckdb).",
  selection=AssetSelection.assets(sec_duckdb_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "duckdb_s3_publish",
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

sec_historical_duckdb_s3_publish_job = define_asset_job(
  name="sec_historical_duckdb_s3_publish",
  description="Publish SEC historical DuckDB staging to S3 (raw .duckdb).",
  selection=AssetSelection.assets(sec_historical_duckdb_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "duckdb_s3_publish",
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


# Run ad hoc after a historical graph rebuild.
sec_historical_lbug_s3_publish_job = define_asset_job(
  name="sec_historical_lbug_s3_publish",
  description="Publish SEC historical database to S3 for replica cluster.",
  selection=AssetSelection.assets(sec_historical_lbug_s3_published),
  tags={
    "pipeline": "sec",
    "phase": "s3_publish",
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


# R2 has zero egress fees; the GraphBackup record puts the file in the
# subscriber download list.
sec_lbug_r2_publish_job = define_asset_job(
  name="sec_lbug_r2_publish",
  description="Publish SEC database to R2 for zero-egress subscriber downloads.",
  selection=AssetSelection.assets(sec_lbug_r2_published),
  tags={
    "pipeline": "sec",
    "phase": "r2_publish",
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


# Manual only. A Hub-side Job copies from R2, so the bytes never leave AWS
# a second time.
sec_lbug_hf_publish_job = define_asset_job(
  name="sec_lbug_hf_publish",
  description="Copy the SEC R2 snapshot to the public Hugging Face dataset. Manual only.",
  selection=AssetSelection.assets(sec_lbug_hf_published),
  tags={
    "pipeline": "sec",
    "phase": "hf_publish",
    # The HF Job moves the bytes; this task only presigns and polls.
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


SEC_INDEX_ECS_TAGS = {
  # Spot-safe: the incremental skip means a retry indexes only what remains.
  "dagster/max_retries": 5,
  # Sized for embeddings; override to 1 vCPU / 4 GB for text-only runs.
  "ecs/cpu": "4096",
  "ecs/memory": "16384",
  "ecs/ephemeral_storage": "21",
  "ecs/run_task_kwargs": {
    "capacityProviderStrategy": [
      {"capacityProvider": "FARGATE_SPOT", "weight": 9, "base": 0},
      {"capacityProvider": "FARGATE", "weight": 1, "base": 0},
    ],
  },
}

# The filer catalog: a fold over three small processed tables plus one
# manifest read per filing of a touched filer. Light, and idempotent on retry
# (every write is a whole file), so Spot is fine.
SEC_CATALOG_ECS_TAGS = {
  "dagster/max_retries": 3,
  "ecs/cpu": "1024",
  "ecs/memory": "4096",
  "ecs/run_task_kwargs": {
    "capacityProviderStrategy": [
      {"capacityProvider": "FARGATE_SPOT", "weight": 9, "base": 0},
      {"capacityProvider": "FARGATE", "weight": 1, "base": 0},
    ],
  },
}

# Named apart from its asset: Dagster requires job and op names to be unique
# within a repository, and an asset's op carries the asset's name.
sec_filing_catalog_job = define_asset_job(
  name="sec_catalog",
  description="Regenerate the per-filer catalog and corpus index on the public CDN (partitioned by quarter).",
  selection=AssetSelection.assets(sec_filing_catalog),
  partitions_def=sec_quarter_partitions,
  tags={
    "pipeline": "sec",
    "phase": "catalog",
    **SEC_CATALOG_ECS_TAGS,
  },
)


sec_narratives_index_job = define_asset_job(
  name="sec_narratives_index",
  description="Extract and index narrative sections from SEC filings into OpenSearch (partitioned by quarter).",
  selection=AssetSelection.assets(sec_narratives_indexed),
  partitions_def=sec_quarter_partitions,
  tags={
    "pipeline": "sec",
    "phase": "text_index",
    **SEC_INDEX_ECS_TAGS,
  },
)


sec_ixbrl_index_job = define_asset_job(
  name="sec_ixbrl_index",
  description="Extract iXBRL disclosure sections with XBRL element metadata into OpenSearch (partitioned by quarter).",
  selection=AssetSelection.assets(sec_ixbrl_disclosures_indexed),
  partitions_def=sec_quarter_partitions,
  tags={
    "pipeline": "sec",
    "phase": "text_index",
    **SEC_INDEX_ECS_TAGS,
  },
)


sec_artifact_generation_job = define_asset_job(
  name="sec_artifact_generation",
  description="Generate element + structure knowledge artifacts.",
  selection=AssetSelection.assets(sec_knowledge_artifacts),
  tags={
    "pipeline": "sec",
    "phase": "artifact",
    # Peak is the DuckDB budget (16 GB, see SECArtifactConfig; un-spillable
    # block pins OOM'd at 8 GB) plus the Python result on top, so 24 GB leaves
    # ~8 GB headroom. 4 vCPU unlocks the >16 GB Fargate tier. 200 GB ephemeral
    # holds the DuckDB file, spill, and artifacts.
    "ecs/cpu": "4096",
    "ecs/memory": "24576",
    "ecs/ephemeral_storage": "200",
    "ecs/run_task_kwargs": {
      "capacityProviderStrategy": [
        {"capacityProvider": "FARGATE", "weight": 1, "base": 1},
      ],
    },
  },
)
