"""Plaid pipeline — one Dagster asset, ``plaid_feed`` (job ``plaid_sync``).

Sync the Item's transactions cursor → link or create one chart account per
cash and card account → load the added / modified / removed changes through
the event-block kernel → advance the cursor, stamp the connection, bootstrap
the fiscal calendar on a fresh company, mark the graph stale.

Usage:
    from robosystems.adapters.plaid.pipeline import get_dagster_components
"""

from robosystems.adapters.plaid.pipeline.assets import (
  PlaidSyncConfig,
  get_dagster_components,
  plaid_feed,
  plaid_sync_job,
)

__all__ = [
  "PlaidSyncConfig",
  "get_dagster_components",
  "plaid_feed",
  "plaid_sync_job",
]
