"""QuickBooks Dagster job definitions."""

from dagster import AssetSelection, define_asset_job

from .extract import qb_extract
from .load import qb_load
from .transform import qb_transform

# Full sync pipeline: extract → transform → load
# Config (QBSyncConfig) provided at runtime via run_config
qb_sync_job = define_asset_job(
  name="qb_sync",
  description="QuickBooks sync pipeline: extract → transform → load to graph",
  selection=AssetSelection.assets(qb_extract, qb_transform, qb_load),
  tags={
    "pipeline": "quickbooks",
  },
)
