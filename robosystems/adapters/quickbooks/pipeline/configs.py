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
