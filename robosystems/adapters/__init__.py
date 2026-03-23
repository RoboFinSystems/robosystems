"""Adapters for external data source integrations.

This module provides adapters for integrating with external financial data sources:
- SEC EDGAR: Financial filings and XBRL data
- QuickBooks: Small business accounting data

Each adapter follows a consistent structure:
- client/: API connection and authentication
- processors/: Data transformation for graph ingestion

AWS infrastructure services are in robosystems.operations.aws
"""

_LAZY_IMPORTS = {
  # SEC
  "ArelleClient": "robosystems.adapters.sec",
  "SECClient": "robosystems.adapters.sec",
  "XBRLDuckDBGraphProcessor": "robosystems.adapters.sec",
  "XBRLGraphProcessor": "robosystems.adapters.sec",
  # Plaid
  "PlaidClient": "robosystems.adapters.plaid",
  "PlaidTransactionsProcessor": "robosystems.adapters.plaid",
  # QuickBooks
  "QBClient": "robosystems.adapters.quickbooks",
  "QBTransactionsProcessor": "robosystems.adapters.quickbooks",
}


def __getattr__(name: str):
  """Lazy import adapter classes on first access."""
  if name in _LAZY_IMPORTS:
    import importlib

    module = importlib.import_module(_LAZY_IMPORTS[name])
    return getattr(module, name)
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
