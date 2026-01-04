"""SEC EDGAR API clients."""

from robosystems.adapters.sec.client.arelle import ArelleClient
from robosystems.adapters.sec.client.edgar import (
  SEC_BASE_URL,
  SECClient,
  enable_test_mode,
)

# Lazy imports to avoid circular dependencies
# These modules import from robosystems.operations which imports back here


def __getattr__(name: str):
  """Lazy import for modules that cause circular imports."""
  if name in ("EFTSClient", "EFTSHit", "query_efts", "query_efts_sync"):
    from robosystems.adapters.sec.client import efts

    return getattr(efts, name)
  elif name in (
    "SECDownloader",
    "DownloadStats",
    "download_sec_filings",
    "download_sec_filings_sync",
  ):
    from robosystems.adapters.sec.client import downloader

    return getattr(downloader, name)
  elif name in ("AsyncRateLimiter", "RateMonitor", "RateStats"):
    from robosystems.adapters.sec.client import rate_limiter

    return getattr(rate_limiter, name)
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
  # Eagerly loaded (safe for `from ... import *`)
  "SEC_BASE_URL",
  "ArelleClient",
  "SECClient",
  "enable_test_mode",
]

# Note: The following are available via lazy import (use direct imports):
# - EFTSClient, EFTSHit, query_efts, query_efts_sync (from .efts)
# - SECDownloader, DownloadStats, download_sec_filings, download_sec_filings_sync (from .downloader)
# - AsyncRateLimiter, RateMonitor, RateStats (from .rate_limiter)
