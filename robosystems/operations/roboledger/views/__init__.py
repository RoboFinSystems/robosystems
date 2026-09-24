"""RoboLedger graph-backed analytical views over the XBRL hypercube in LadybugDB.

The schema is roboledger's and shared by tenant graphs and the SEC repository,
so the same reads serve either graph_id. Transactional lookups against the
extensions OLTP database live in `reads/`.
"""

from robosystems.operations.roboledger.views.fact_grid_builder import (
  FactGridBuilder,
  summarize_by_element,
)
from robosystems.operations.roboledger.views.fact_query import query_fact_grid
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
  resolve_report,
  resolved_report_info,
)

__all__ = [
  "BlockNotFoundError",
  "FactGridBuilder",
  "ReportNotFoundError",
  "ReportNotPublishedError",
  "ReportSelectorError",
  "ReportTooLargeError",
  "deduplicate_facts",
  "query_disclosures",
  "query_fact_grid",
  "query_financial_statement",
  "query_information_block",
  "resolve_report",
  "resolved_report_info",
  "summarize_by_element",
]
