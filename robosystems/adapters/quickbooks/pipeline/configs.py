"""QuickBooks pipeline configuration."""

from dagster import Config


class QBSyncConfig(Config):
  """Configuration for QuickBooks sync pipeline."""

  graph_id: str
  connection_id: str
  user_id: str
  realm_id: str = ""
  full_rebuild: bool = False
  lookback_days: int = 60
  since_date: str = ""
  # Lock token acquired by the API; qb_load releases it. Empty when Valkey
  # was down at acquire time.
  sync_lock_id: str = ""
