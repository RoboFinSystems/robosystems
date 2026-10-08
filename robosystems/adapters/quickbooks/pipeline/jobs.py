"""QuickBooks Dagster job definitions."""

from dagster import AssetSelection, define_asset_job

from .extract import qb_extract
from .load import qb_load
from .transform import qb_transform

# QBSyncConfig is supplied via run_config. Retried like the bank-feed jobs:
# a run whose worker died is run again from extract (the load is an UPSERT
# gated on SyncToken, so a repeat is safe); a run that failed in a stage is
# not, since that stage already recorded the failure on the connection.
qb_sync_job = define_asset_job(
  name="qb_sync",
  description="QuickBooks sync pipeline: extract → transform → load to graph",
  selection=AssetSelection.assets(qb_extract, qb_transform, qb_load),
  tags={
    "pipeline": "quickbooks",
    "dagster/max_retries": "3",
    "dagster/retry_on_asset_or_op_failure": "false",
  },
)
