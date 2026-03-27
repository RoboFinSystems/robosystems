"""QuickBooks Pipeline Dagster Components.

This package contains all Dagster orchestration for the QuickBooks adapter:
assets, jobs, and configuration.

Pipeline stages:

1. EXTRACT (qb_extract):
   - Fetch accounts, journal entries, company info from QuickBooks API
   - Write raw data as parquet files to temp directory
   - Full rebuild: all history; Incremental: last 60 days

2. TRANSFORM (qb_transform):
   - Run dbt-duckdb models against extracted parquet
   - Produce OLTP tables in DuckDB (accounts, transactions, etc.)

3. LOAD (qb_load):
   - Read OLTP tables from DuckDB via OLTPLoader
   - Delete existing data for this source + connection
   - Insert into extensions PostgreSQL tenant schema
   - Resolve foreign keys using external_id lookups
   - Update connection last_sync timestamp

Usage:
    from robosystems.adapters.quickbooks.pipeline import get_dagster_components

    components = get_dagster_components()
"""

from robosystems.adapters.quickbooks.pipeline.extract import qb_extract
from robosystems.adapters.quickbooks.pipeline.jobs import qb_sync_job
from robosystems.adapters.quickbooks.pipeline.load import qb_load
from robosystems.adapters.quickbooks.pipeline.transform import qb_transform


def get_dagster_components():
  """Return all Dagster components for the QuickBooks adapter."""
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
