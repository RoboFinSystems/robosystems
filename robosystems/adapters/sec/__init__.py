"""SEC EDGAR adapter for XBRL financial data extraction."""

from datetime import UTC, datetime

from robosystems.adapters.sec.client import SEC_BASE_URL, SECClient, enable_test_mode
from robosystems.adapters.sec.client.arelle import ArelleClient
from robosystems.adapters.sec.metadata import SECMetadataLoader
from robosystems.adapters.sec.processors import (
  XBRL_GRAPH_PROCESSOR_VERSION,
  IngestTableInfo,
  MaterializeResult,
  SchemaIngestConfig,
  StagingResult,
  TableInfo,
  XBRLDuckDBGraphProcessor,
  XBRLGraphProcessor,
  XBRLSchemaAdapter,
  XBRLSchemaConfigGenerator,
  create_custom_ingestion_processor,
  create_roboledger_ingestion_processor,
)

# Quarter overlap window for incremental updates.
# SEC filings submitted near quarter-end may not appear in EFTS for 1-2 days
# due to SEC indexing delays and UTC/EST timing differences. We scan the
# previous quarter during the first N days of a new quarter to catch these.
QUARTER_OVERLAP_DAYS = 5


def get_current_quarter(now: datetime | None = None) -> tuple[int, int]:
  """Get the current year and quarter.

  Args:
      now: Optional datetime to use (defaults to UTC now).

  Returns:
      Tuple of (year, quarter) where quarter is 1-4.
  """
  if now is None:
    now = datetime.now(UTC)
  quarter = (now.month - 1) // 3 + 1
  return now.year, quarter


def get_previous_quarter(year: int, quarter: int) -> tuple[int, int]:
  """Get the previous quarter given a year and quarter.

  Args:
      year: The year.
      quarter: The quarter (1-4).

  Returns:
      Tuple of (year, quarter) for the previous quarter.
  """
  if quarter == 1:
    return year - 1, 4
  return year, quarter - 1


def is_in_quarter_overlap_window(now: datetime | None = None) -> bool:
  """Check if we're in the quarter overlap window (first N days of a new quarter).

  During this window, we should also scan the previous quarter to catch
  late-indexed filings from the prior quarter.

  Args:
      now: Optional datetime to use (defaults to UTC now).

  Returns:
      True if we're in the overlap window.
  """
  if now is None:
    now = datetime.now(UTC)
  _, quarter = get_current_quarter(now)
  quarter_start_month = (quarter - 1) * 3 + 1
  return now.month == quarter_start_month and now.day <= QUARTER_OVERLAP_DAYS


def get_quarters_to_scan(now: datetime | None = None) -> list[str]:
  """Get list of partition keys (quarters) to scan for incremental updates.

  Always scans current quarter. Also scans previous quarter during the
  overlap window (first N days of a new quarter).

  Args:
      now: Optional datetime to use (defaults to UTC now).

  Returns:
      List of partition keys like ["2025-Q1"] or ["2025-Q1", "2024-Q4"].
  """
  if now is None:
    now = datetime.now(UTC)
  year, quarter = get_current_quarter(now)
  quarters = [f"{year}-Q{quarter}"]

  if is_in_quarter_overlap_window(now):
    prev_year, prev_quarter = get_previous_quarter(year, quarter)
    quarters.append(f"{prev_year}-Q{prev_quarter}")

  return quarters


__all__ = [
  "QUARTER_OVERLAP_DAYS",
  "SEC_BASE_URL",
  "XBRL_GRAPH_PROCESSOR_VERSION",
  "ArelleClient",
  "IngestTableInfo",
  "MaterializeResult",
  "SECClient",
  "SECMetadataLoader",
  "SchemaIngestConfig",
  "StagingResult",
  "TableInfo",
  "XBRLDuckDBGraphProcessor",
  "XBRLGraphProcessor",
  "XBRLSchemaAdapter",
  "XBRLSchemaConfigGenerator",
  "create_custom_ingestion_processor",
  "create_roboledger_ingestion_processor",
  "enable_test_mode",
  "get_current_quarter",
  "get_previous_quarter",
  "get_quarters_to_scan",
  "is_in_quarter_overlap_window",
]
