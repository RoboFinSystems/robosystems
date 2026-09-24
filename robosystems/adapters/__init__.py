"""Adapters for external data sources (SEC EDGAR, QuickBooks, Plaid, Mercury)."""

_LAZY_IMPORTS = {
  # SEC
  "XBRLDuckDBGraphProcessor": "robosystems.adapters.sec",
  "XBRLGraphProcessor": "robosystems.adapters.sec",
  # QuickBooks
  "QBClient": "robosystems.adapters.quickbooks",
}


def __getattr__(name: str):
  if name in _LAZY_IMPORTS:
    import importlib

    module = importlib.import_module(_LAZY_IMPORTS[name])
    return getattr(module, name)
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
