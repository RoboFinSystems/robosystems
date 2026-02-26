"""Adapters for external data source integrations.

This module provides adapters for integrating with external financial data sources:
- SEC EDGAR: Financial filings and XBRL data
- QuickBooks: Small business accounting data

Each adapter follows a consistent structure:
- client/: API connection and authentication
- processors/: Data transformation for graph ingestion

AWS infrastructure services are in robosystems.operations.aws
"""

# QuickBooks adapter
from robosystems.adapters.quickbooks import (
  QBClient,
)
from robosystems.adapters.sec import (
  ArelleClient,
  SECClient,
  XBRLDuckDBGraphProcessor,
  XBRLGraphProcessor,
)

__all__ = [
  "ArelleClient",
  # QuickBooks
  "QBClient",
  # SEC
  "SECClient",
  "XBRLDuckDBGraphProcessor",
  "XBRLGraphProcessor",
]
