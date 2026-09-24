"""QuickBooks Online adapter: the OAuth API client, the dbt project (dbt/),
and the Dagster pipeline (pipeline/)."""

from robosystems.adapters.quickbooks.client import QBClient

__all__ = [
  "QBClient",
]
