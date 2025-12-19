"""Plaid adapter for banking data integration.

This adapter provides:
- PlaidClient: API client for Plaid financial data
- PlaidTransactionsProcessor: Graph ingestion for accounts and transactions
- URI utilities for consistent graph entity identifiers
"""

from robosystems.adapters.plaid.client import PlaidClient
from robosystems.adapters.plaid.processors import (
  PlaidTransactionsProcessor,
  plaid_account_element_uri,
  plaid_account_qname,
  plaid_line_item_uri,
  plaid_transaction_uri,
)

__all__ = [
  # Client
  "PlaidClient",
  # Processors
  "PlaidTransactionsProcessor",
  # URI utilities
  "plaid_account_element_uri",
  "plaid_account_qname",
  "plaid_line_item_uri",
  "plaid_transaction_uri",
]
