"""Resolve Element MCP tool — thin wrapper over
``adapters.sec.mcp.element_resolver.resolve_sec_element``.

The resolution logic (LanceDB vector search over the SEC Element
embeddings index, SEC canonical concept matching, text fallback,
query-hint construction) is SEC-specific and lives in the SEC adapter
alongside ``resolve_sec_report``. The tool itself is manifest-gated
(``has_semantic_enrichment``) so it only registers on graphs where
the required SEC artifacts are present.
"""

from __future__ import annotations

from typing import Any

from robosystems.adapters.sec.mcp import resolve_sec_element

from .base_tool import BaseTool


class ResolveElementTool(BaseTool):
  """MCP tool that resolves a natural-language concept to XBRL elements."""

  def __init__(self, client):
    super().__init__(client)
    self._vector_search_enabled: bool | None = None

  @property
  def vector_search_enabled(self) -> bool:
    """Read the ``MCP_VECTOR_SEARCH_ENABLED`` flag once, cache the result."""
    if self._vector_search_enabled is None:
      try:
        from robosystems.config import env

        self._vector_search_enabled = env.MCP_VECTOR_SEARCH_ENABLED
      except Exception:
        self._vector_search_enabled = False
    return self._vector_search_enabled

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "resolve-element",
      "description": """Resolve a financial concept (like "revenue" or "total assets") to the actual XBRL element names used in the graph.

**WHEN TO USE:**
- BEFORE writing Cypher queries that filter by element qname
- When you know the business concept but not the XBRL tag name
- To discover which companies report a given metric and how

**RETURNS:**
- Canonical concept match (if found in taxonomy)
- Top matching XBRL elements with qnames, labels, and fact counts
- A ready-to-use Cypher query hint

**EXAMPLES:**
- concept: "revenue" → finds us-gaap:Revenues, us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax, etc.
- concept: "total debt" → finds us-gaap:LongTermDebt, us-gaap:DebtAndCapitalLeaseObligations, etc.

**TIP:**
Use the returned query_hint directly in read-graph-cypher for immediate results.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "concept": {
            "type": "string",
            "description": "Natural-language financial concept to resolve (e.g. 'revenue', 'total assets', 'earnings per share')",
          },
          "ticker": {
            "type": "string",
            "description": "Optional: filter to elements reported by a specific company ticker (e.g. 'NVDA', 'AAPL')",
          },
          "report_id": {
            "type": "string",
            "description": "Optional: filter to elements used in a specific report by its identifier",
          },
        },
        "required": ["concept"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("resolve-element", arguments)

    concept = arguments.get("concept", "").strip()
    ticker = (
      arguments.get("ticker", "").strip().upper() if arguments.get("ticker") else None
    )
    report_id = (
      arguments.get("report_id", "").strip() if arguments.get("report_id") else None
    )

    if not concept:
      return {"error": "concept is required"}

    return await resolve_sec_element(
      self.client,
      concept=concept,
      ticker=ticker,
      report_id=report_id,
      vector_search_enabled=self.vector_search_enabled,
    )
