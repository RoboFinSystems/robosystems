"""RoboLedger graph-backed analytical views over the XBRL hypercube in LadybugDB.

The schema is roboledger's and shared by tenant graphs and the SEC repository,
so the same reads serve either graph_id. Transactional lookups against the
extensions OLTP database live in `reads/`.
"""

from robosystems.operations.roboledger.views.fact_dedup import (
  mixed_units_note,
  units_reported_together,
)
from robosystems.operations.roboledger.views.fact_grid_builder import (
  FactGridBuilder,
  summarize_by_element,
)
from robosystems.operations.roboledger.views.fact_query import (
  FactGridTooBroadError,
  period_scope_hint,
  query_fact_grid,
  shared_only_selectors,
)
from robosystems.operations.roboledger.views.filing_text import (
  FilingRef,
  QueryError,
  filing_info,
  query_describe_filing,
  query_read_text,
  query_search_text,
  resolve_filing,
)
from robosystems.operations.roboledger.views.financial_statement_query import (
  deduplicate_facts,
  query_financial_statement,
)
from robosystems.operations.roboledger.views.information_blocks import (
  BlockNotFoundError,
  ReportNotFoundError,
  ReportNotPublishedError,
  ReportSelectorError,
  ReportTooLargeError,
  query_disclosures,
  query_information_block,
  report_coordinates,
  resolve_report,
  resolved_report_info,
)

__all__ = [
  "BlockNotFoundError",
  "FactGridBuilder",
  "FactGridTooBroadError",
  "FilingRef",
  "QueryError",
  "ReportNotFoundError",
  "ReportNotPublishedError",
  "ReportSelectorError",
  "ReportTooLargeError",
  "deduplicate_facts",
  "filing_info",
  "mixed_units_note",
  "period_scope_hint",
  "query_describe_filing",
  "query_disclosures",
  "query_fact_grid",
  "query_financial_statement",
  "query_information_block",
  "query_read_text",
  "query_search_text",
  "report_coordinates",
  "resolve_filing",
  "resolve_report",
  "resolved_report_info",
  "shared_only_selectors",
  "summarize_by_element",
  "units_reported_together",
]
