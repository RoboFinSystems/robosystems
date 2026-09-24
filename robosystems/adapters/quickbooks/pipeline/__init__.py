"""QuickBooks Dagster pipeline: qb_extract (API → parquet) → qb_transform
(dbt-duckdb) → qb_load (DuckDB → extensions PostgreSQL)."""

from robosystems.adapters.quickbooks.pipeline.extract import qb_extract
from robosystems.adapters.quickbooks.pipeline.jobs import qb_sync_job
from robosystems.adapters.quickbooks.pipeline.load import qb_load
from robosystems.adapters.quickbooks.pipeline.transform import qb_transform


def get_dagster_components():
  return {
    "assets": [
      qb_extract,
      qb_transform,
      qb_load,
    ],
    "jobs": [
      qb_sync_job,
    ],
    "sensors": [],
    "schedules": [],
  }


__all__ = [
  "get_dagster_components",
  "qb_extract",
  "qb_load",
  "qb_sync_job",
  "qb_transform",
]
