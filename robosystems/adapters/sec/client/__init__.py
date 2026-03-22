"""SEC EDGAR API clients."""

_LAZY_IMPORTS = {
  "ArelleClient": "robosystems.adapters.sec.client.arelle",
  "SEC_BASE_URL": "robosystems.adapters.sec.client.edgar",
  "SECClient": "robosystems.adapters.sec.client.edgar",
  "enable_test_mode": "robosystems.adapters.sec.client.edgar",
  "EFTSClient": "robosystems.adapters.sec.client.efts",
  "EFTSHit": "robosystems.adapters.sec.client.efts",
  "query_efts": "robosystems.adapters.sec.client.efts",
  "query_efts_sync": "robosystems.adapters.sec.client.efts",
  "SECDownloader": "robosystems.adapters.sec.client.downloader",
  "DownloadStats": "robosystems.adapters.sec.client.downloader",
  "download_sec_filings": "robosystems.adapters.sec.client.downloader",
  "download_sec_filings_sync": "robosystems.adapters.sec.client.downloader",
  "AsyncRateLimiter": "robosystems.adapters.sec.client.rate_limiter",
  "RateMonitor": "robosystems.adapters.sec.client.rate_limiter",
  "RateStats": "robosystems.adapters.sec.client.rate_limiter",
}


def __getattr__(name: str):
  """Lazy import all SEC client classes on first access."""
  if name in _LAZY_IMPORTS:
    import importlib

    module = importlib.import_module(_LAZY_IMPORTS[name])
    return getattr(module, name)
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
