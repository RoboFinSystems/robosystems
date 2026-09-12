"""Mercury pipeline — one Dagster asset, ``mercury_feed`` (job ``mercury_sync``).

Pull (accounts, the IO card, transactions since the window start) → link or
create one chart account per bank account → capture events through the
event-block kernel → stamp the connection, bootstrap the fiscal calendar on
a fresh company, mark the graph stale.

Usage:
    from robosystems.adapters.mercury.pipeline import get_dagster_components
"""

from robosystems.adapters.mercury.pipeline.assets import (
  MercurySyncConfig,
  get_dagster_components,
  mercury_feed,
  mercury_sync_job,
)

__all__ = [
  "MercurySyncConfig",
  "get_dagster_components",
  "mercury_feed",
  "mercury_sync_job",
]
