"""SEC EDGAR adapter for XBRL financial data extraction."""

from datetime import datetime
from zoneinfo import ZoneInfo

# SEC filings follow the US-market (Eastern) calendar. The nightly pipeline runs
# at 21:00 ET; on a quarter's last day that is already the next day in UTC, so
# quarter selection must key off Eastern time, not the container's UTC clock.
EASTERN = ZoneInfo("America/New_York")

_LAZY_IMPORTS = {
  "SEC_BASE_URL": "robosystems.adapters.sec.client",
  "SECClient": "robosystems.adapters.sec.client",
  "enable_test_mode": "robosystems.adapters.sec.client",
  "ArelleClient": "robosystems.adapters.sec.client.arelle",
  "XBRL_GRAPH_PROCESSOR_VERSION": "robosystems.adapters.sec.processors",
  "IngestTableInfo": "robosystems.adapters.sec.processors",
  "MaterializeResult": "robosystems.adapters.sec.processors",
  "SchemaIngestConfig": "robosystems.adapters.sec.processors",
  "SECMetadataLoader": "robosystems.adapters.sec.processors",
  "StagingResult": "robosystems.adapters.sec.processors",
  "TableInfo": "robosystems.adapters.sec.processors",
  "XBRLDuckDBGraphProcessor": "robosystems.adapters.sec.processors",
  "XBRLGraphProcessor": "robosystems.adapters.sec.processors",
  "XBRLSchemaAdapter": "robosystems.adapters.sec.processors",
  "XBRLSchemaConfigGenerator": "robosystems.adapters.sec.processors",
  "create_custom_ingestion_processor": "robosystems.adapters.sec.processors",
  "create_roboledger_ingestion_processor": "robosystems.adapters.sec.processors",
}


def __getattr__(name: str):
  """Lazy import SEC adapter classes on first access."""
  if name in _LAZY_IMPORTS:
    import importlib

    module = importlib.import_module(_LAZY_IMPORTS[name])
    return getattr(module, name)
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_current_quarter(now: datetime | None = None) -> tuple[int, int]:
  """Get the current year and quarter.

  Args:
      now: Optional datetime to use (defaults to Eastern-time now, matching the
          SEC filing calendar).

  Returns:
      Tuple of (year, quarter) where quarter is 1-4.
  """
  if now is None:
    now = datetime.now(EASTERN)
  quarter = (now.month - 1) // 3 + 1
  return now.year, quarter


def get_quarters_to_scan(now: datetime | None = None) -> list[str]:
  """Get the partition key(s) to scan for the incremental nightly download.

  Hard cut-over: exactly one quarter per run — the current (Eastern-time)
  quarter. There is no previous-quarter overlap; the final batch of a quarter is
  trusted to capture that quarter's filings. Quarter selection keys off Eastern
  time so the last-day-of-quarter run (21:00 ET, already next-day in UTC) stays
  on the correct quarter.

  Args:
      now: Optional datetime to use (defaults to Eastern-time now).

  Returns:
      Single-element list of partition keys, e.g. ["2026-Q2"].
  """
  year, quarter = get_current_quarter(now)
  return [f"{year}-Q{quarter}"]
