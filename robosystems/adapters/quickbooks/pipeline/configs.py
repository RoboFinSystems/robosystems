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
  # Distributed-lock token acquired at the API endpoint;
  # qb_load releases the lock on completion so the next sync can
  # proceed immediately rather than waiting for the 30-min TTL. Empty
  # string means no lock was acquired (rare — defensive fallback path
  # when Valkey is down).
  sync_lock_id: str = ""
