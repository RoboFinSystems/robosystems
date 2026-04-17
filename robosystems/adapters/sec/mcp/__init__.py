"""SEC-specific MCP helpers.

Today this package hosts report-resolution helpers used by the
``financial-statement-analysis`` view op when the graph is a shared
SEC repository. It is not a general "adapter contributes MCP tools"
framework — just a namespace for SEC-specific knowledge that would
otherwise leak into generic graph-schema code.
"""

from .report_resolver import (
  ANNUAL_FORMS,
  QUARTERLY_FORMS,
  resolve_sec_report,
)

__all__ = [
  "ANNUAL_FORMS",
  "QUARTERLY_FORMS",
  "resolve_sec_report",
]
