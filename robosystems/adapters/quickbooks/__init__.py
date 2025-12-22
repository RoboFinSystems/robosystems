"""QuickBooks adapter for accounting data integration.

This adapter provides:
- QBClient: OAuth-authenticated API client for QuickBooks Online
- QBTransactionsProcessor: Graph ingestion for accounts and transactions (stubbed)
- URI utilities for consistent graph entity identifiers

NOTE: The transaction processor is currently stubbed. When QuickBooks
integration is reimplemented, connections will be managed in PostgreSQL
rather than the graph database.
"""

from robosystems.adapters.quickbooks.client import QBClient
from robosystems.adapters.quickbooks.processors import (
  QBTransactionsProcessor,
  qb_chart_of_accounts_uri,
  qb_coa_network_uri,
  qb_element_uri,
  qb_entity_uri,
  qb_line_item_uri,
  qb_stripped_account_name,
  qb_transaction_uri,
  rl_coa_root_element_uri,
  rl_entity_uri,
)

__all__ = [
  # Client
  "QBClient",
  # Processors
  "QBTransactionsProcessor",
  "qb_chart_of_accounts_uri",
  "qb_coa_network_uri",
  "qb_element_uri",
  # URI utilities
  "qb_entity_uri",
  "qb_line_item_uri",
  "qb_stripped_account_name",
  "qb_transaction_uri",
  "rl_coa_root_element_uri",
  "rl_entity_uri",
]
