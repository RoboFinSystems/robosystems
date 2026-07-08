"""SEC-specific MCP helpers.

Namespace for SEC-specific knowledge that would otherwise leak into
generic graph-schema code. Current contents:

- ``resolve_sec_report`` — auto-resolves an SEC filing by ticker + form
  codes (10-K/10-Q/20-F/40-F). Used by the
  ``financial-statement-analysis`` view op.
- ``resolve_sec_element`` — natural-language → XBRL element resolution
  via canonical concept matching + a text-label fallback. Used by the
  ``resolve-element`` MCP tool. SEC-only because the canonical concept
  taxonomy is an SEC artifact.

Not a general "adapter contributes MCP tools" framework — just a home
for SEC-scoped logic the MCP tools can import.
"""

from .element_resolver import resolve_sec_element
from .report_resolver import (
  ANNUAL_FORMS,
  QUARTERLY_FORMS,
  resolve_sec_report,
)

__all__ = [
  "ANNUAL_FORMS",
  "QUARTERLY_FORMS",
  "resolve_sec_element",
  "resolve_sec_report",
]
