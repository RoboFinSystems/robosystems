"""SEC EDGAR adapter for XBRL financial data extraction."""

from datetime import datetime
from zoneinfo import ZoneInfo

# Quarter selection keys off Eastern time: the 21:00 ET nightly run on a
# quarter's last day is already the next day in UTC.
EASTERN = ZoneInfo("America/New_York")

_LAZY_IMPORTS = {
  "SEC_BASE_URL": "robosystems.adapters.sec.client",
  "edgar_client": "robosystems.adapters.sec.client",
  "load_filing": "robosystems.adapters.sec.client",
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
  if name in _LAZY_IMPORTS:
    import importlib

    module = importlib.import_module(_LAZY_IMPORTS[name])
    return getattr(module, name)
  raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def get_current_quarter(now: datetime | None = None) -> tuple[int, int]:
  """(year, quarter) for ``now``, defaulting to Eastern-time now."""
  if now is None:
    now = datetime.now(EASTERN)
  quarter = (now.month - 1) // 3 + 1
  return now.year, quarter


def get_quarters_to_scan(now: datetime | None = None) -> list[str]:
  """Partition keys for the nightly incremental download, e.g. ["2026-Q2"].

  Only the current Eastern-time quarter, with no previous-quarter overlap: a
  quarter's final nightly run is trusted to capture its last filings.
  """
  year, quarter = get_current_quarter(now)
  return [f"{year}-Q{quarter}"]
