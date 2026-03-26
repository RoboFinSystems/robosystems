"""SEC Entity Sync Dagster job definition."""

from dagster import AssetSelection, define_asset_job

from .extract import sec_entity_extract
from .load import sec_entity_load
from .transform import sec_entity_transform

# Full entity sync pipeline: extract → transform → load
# Config (SECEntitySyncConfig) provided at runtime via run_config
sec_entity_sync_job = define_asset_job(
  name="sec_entity_sync",
  description="SEC entity sync pipeline: discover filings → process XBRL → load to user graph",
  selection=AssetSelection.assets(
    sec_entity_extract, sec_entity_transform, sec_entity_load
  ),
  tags={
    "pipeline": "sec_entity",
  },
)
