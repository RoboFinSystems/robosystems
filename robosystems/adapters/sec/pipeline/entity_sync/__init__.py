"""SEC Entity Sync Pipeline.

Connection-based pipeline that syncs SEC filings for a single company (CIK)
into a user's graph database. Unlike the shared SEC pipeline which processes
all filings into the shared repository, this pipeline:

1. Reads raw XBRL ZIPs from the shared raw S3 bucket (no SEC API calls)
2. Processes them with XBRLGraphProcessor into parquet
3. Uploads parquet to the user's S3 staging area
4. Stages to DuckDB and materializes to LadybugDB on the user's graph

Precondition: The target graph must already exist with the RoboLedger schema.

Pipeline stages:

1. EXTRACT (sec_entity_extract):
   - List raw XBRL ZIPs in shared raw bucket for the CIK
   - No downloading — the shared pipeline populates the raw bucket

2. TRANSFORM (sec_entity_transform):
   - Process each filing with XBRLGraphProcessor
   - Consolidate parquet tables across filings (dedup shared tables)
   - Write output parquet to temp directory

3. LOAD (sec_entity_load):
   - Create GraphTable + GraphFile records in PostgreSQL
   - Upload parquet to USER_DATA_BUCKET
   - Stage to DuckDB via Graph API
   - Materialize to LadybugDB
   - Update connection last_sync timestamp

Usage:
    from robosystems.adapters.sec.pipeline.entity_sync import get_dagster_components

    components = get_dagster_components()
"""

from .extract import sec_entity_extract
from .jobs import sec_entity_sync_job
from .load import sec_entity_load
from .transform import sec_entity_transform


def get_dagster_components():
  """Return all Dagster components for the SEC entity sync pipeline."""
  return {
    "assets": [
      sec_entity_extract,
      sec_entity_transform,
      sec_entity_load,
    ],
    "jobs": [
      sec_entity_sync_job,
    ],
    "sensors": [],
    "schedules": [],
  }


__all__ = [
  "get_dagster_components",
  "sec_entity_extract",
  "sec_entity_load",
  "sec_entity_sync_job",
  "sec_entity_transform",
]
