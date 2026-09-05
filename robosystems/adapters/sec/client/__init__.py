"""SEC EDGAR access.

The clients themselves — EDGAR, EFTS, the Arelle load with its cache policy —
are xbrlkit's; this package binds them to the platform (``edgar_client``,
``load_filing``) and keeps what needs the platform: the async bulk downloader
to S3 and its rate limiter.
"""

_LAZY_IMPORTS = {
  "SEC_BASE_URL": "robosystems.adapters.sec.client.edgar",
  "edgar_client": "robosystems.adapters.sec.client.edgar",
  "load_filing": "robosystems.adapters.sec.client.arelle",
  "close_filing": "robosystems.adapters.sec.client.arelle",
  "arelle_cache_dir": "robosystems.adapters.sec.client.arelle",
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
