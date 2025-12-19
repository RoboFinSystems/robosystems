"""Adapters for external data source integrations.

This module provides adapters for integrating with external financial data sources:
- SEC EDGAR: Financial filings and XBRL data
- QuickBooks: Small business accounting data
- Plaid: Banking and transaction data

Each adapter follows a consistent structure:
- client/: API connection and authentication
- processors/: Data transformation for graph ingestion

AWS infrastructure services are in robosystems.operations.aws
"""

# SEC EDGAR adapter
# Plaid adapter
from robosystems.adapters.plaid import (
  PlaidClient,
  PlaidTransactionsProcessor,
)

# QuickBooks adapter
from robosystems.adapters.quickbooks import (
  QBClient,
  QBTransactionsProcessor,
)
from robosystems.adapters.sec import (
  ArelleClient,
  SECClient,
  XBRLDuckDBGraphProcessor,
  XBRLGraphProcessor,
)

__all__ = [
  "ArelleClient",
  # Plaid
  "PlaidClient",
  "PlaidTransactionsProcessor",
  # QuickBooks
  "QBClient",
  "QBTransactionsProcessor",
  # SEC
  "SECClient",
  "XBRLDuckDBGraphProcessor",
  "XBRLGraphProcessor",
]
