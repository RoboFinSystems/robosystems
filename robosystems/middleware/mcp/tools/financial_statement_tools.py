"""
Financial statement MCP tools — split into two purpose-built tools.

- ``live-financial-statement`` → OLTP-backed, tenant entity graphs only.
  Source of truth for RoboLedger users; reads live ledger data through
  the CoA→GAAP mapping.

- ``financial-statement-analysis`` → Graph-backed (LadybugDB) Cypher
  traversal. Works on the SEC shared repository today and on any
  tenant graph whose ledger has been materialized.

Both delegate to the ops layer (``operations/roboledger/{reads,views}/``)
so REST and MCP share behavior. SEC-specific report resolution lives in
``adapters/sec/mcp/report_resolver.py``.
"""

from __future__ import annotations

from datetime import date
from typing import Any

from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.db.extensions import extensions_session
from robosystems.middleware.operations import run_off_loop
from robosystems.operations.roboledger.reads.reports import (
  ANALYSIS_STATEMENT_TYPES,
  MCP_LIVE_STATEMENT_TYPES,
  CoaMappingNotFoundError,
  get_live_financial_statement,
  resolve_reporting_window,
)
from robosystems.operations.roboledger.views import (
  deduplicate_facts,
  query_financial_statement,
)

from .base_tool import BaseTool


class LiveFinancialStatementTool(BaseTool):
  """MCP tool: generate an ad-hoc OLTP-backed statement for a tenant graph."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "live-financial-statement",
      "description": """Generate a live financial statement directly from the current tenant's OLTP ledger data using the active CoA→GAAP mapping.

**WHEN TO USE:**
- When the user asks for *their* books — "show me my income statement", "my balance sheet for Q1"
- Live, up-to-the-minute data (no materialization required)
- Only available on RoboLedger tenant entity graphs (not SEC shared repo)

**PARAMETERS:**
- `statement_type` (required) — one of the three below (a statement of equity is not offered here yet: the live path renders equity balances, not a rollforward)
- income_statement — Revenue, expenses, net income
- balance_sheet — Assets, liabilities, equity (instant periods)
- cash_flow_statement — Operating (indirect: net income + non-cash add-backs + working-capital deltas), investing, financing. Investing/financing leaves resolve from each line's explicit flow tag when present, else from the mapped rs-gaap element's default-flow derivation arc — so untagged QB data renders too, provided the relevant accounts are mapped at the grain the arcs key on (e.g. PP&E Gross for capex). Needs ≥2 periods (current + prior) for the indirect deltas, so the prior period is pivoted as the delta basis and only the current period is rendered.
- Explicit `period_start` / `period_end` (YYYY-MM-DD) win over everything else
- `period_type="annual"` + `fiscal_year=2025` → window anchored on the graph's FiscalCalendar
- `period_type="quarterly"` → current calendar quarter
- `period_type="instant"` / unset → current calendar month

**RETURNS:**
Facts with element qnames, names, classifications, and values aligned with `periods` — current + prior (equal duration) for income_statement and balance_sheet, current only for cash_flow_statement. Abstract scaffolding rows and all-zero rows are filtered out; subtotals are kept and flagged `is_subtotal`. Capped at `limit` rows (default 1000 — the whole statement).
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "statement_type": {
            "type": "string",
            "description": "Type of financial statement to generate",
            "enum": list(MCP_LIVE_STATEMENT_TYPES),
          },
          "period_start": {
            "type": "string",
            "description": "Explicit window start (YYYY-MM-DD). Overrides period_type/fiscal_year.",
          },
          "period_end": {
            "type": "string",
            "description": "Explicit window end (YYYY-MM-DD). Overrides period_type/fiscal_year.",
          },
          "period_type": {
            "type": "string",
            "description": "annual | quarterly | instant (ignored when explicit dates supplied)",
            "enum": ["annual", "quarterly", "instant"],
          },
          "fiscal_year": {
            "type": "integer",
            "description": "Fiscal year for annual window (anchored on FiscalCalendar)",
          },
          "limit": {
            "type": "integer",
            "description": "Max fact rows returned (1-1000). Leave at the default for a whole statement; a lower cap cuts rows while subtotals still reflect the full set.",
            "default": 1000,
          },
        },
        "required": ["statement_type"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("live-financial-statement", arguments)

    statement_type = (arguments.get("statement_type") or "").strip()
    if not statement_type:
      return {"error": "statement_type is required"}
    if statement_type == "equity_statement":
      return {
        "error": (
          "equity_statement is not offered on this surface yet: the live "
          "path renders equity balances, not a rollforward, so the result "
          "would read as a statement it is not. Use balance_sheet for the "
          "equity section."
        ),
      }
    if statement_type not in MCP_LIVE_STATEMENT_TYPES:
      return {
        "error": (
          f"Unknown statement_type '{statement_type}'. "
          f"Valid types: {', '.join(MCP_LIVE_STATEMENT_TYPES)}. "
          "For graph-backed (materialized / SEC) statements, see "
          "financial-statement-analysis."
        ),
      }

    period_start = _parse_date(arguments.get("period_start"))
    period_end = _parse_date(arguments.get("period_end"))
    period_type = arguments.get("period_type")
    fiscal_year = arguments.get("fiscal_year")
    limit = max(1, min(int(arguments.get("limit", 1000)), 1000))

    graph_id = self.client.graph_id

    with extensions_session(graph_id) as session:
      start, end = resolve_reporting_window(
        session,
        period_start=period_start,
        period_end=period_end,
        period_type=period_type,
        fiscal_year=fiscal_year,
      )
      try:
        response = get_live_financial_statement(
          session,
          graph_id=graph_id,
          statement_type=statement_type,
          period_start=start,
          period_end=end,
          limit=limit,
        )
      except CoaMappingNotFoundError as exc:
        return {
          "error": str(exc),
          "statement_type": statement_type,
        }

    result = response.model_dump(mode="json")
    if not result["facts"]:
      result["tip"] = (
        f"No facts found for {statement_type}. "
        "Check that ledger data is loaded and CoA→GAAP mappings are complete."
      )
    return result


class FinancialStatementAnalysisTool(BaseTool):
  """MCP tool: run a graph-backed financial statement query.

  Available on shared-repo graphs (SEC) and on tenant graphs once
  their ledger has been materialized. Uses the roboledger XBRL
  hypercube schema (`Structure → FactSet → Fact`).
  """

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "financial-statement-analysis",
      "description": """Return a structured financial statement from the graph-backed XBRL hypercube.

**WHEN TO USE:**
- SEC shared repository: "show me NVDA's latest 10-K income statement"
- RoboLedger tenant graph post-materialization: historical analytical queries against the materialized ledger
- Use when you want cross-period or cross-entity analytical views rather than the current live books (use live-financial-statement for the latter)

**NOTES:**
- Shared-repo (SEC): `ticker` REQUIRED. If no `report_id`, auto-resolves the latest matching filing by form type (10-K/10-Q/20-F/40-F).
- Tenant graph: `report_id` REQUIRED.

**PARAMETERS:**
- `statement_type` (required) — one of income_statement, balance_sheet,
  cash_flow_statement, equity_statement
- `period_type` — annual (10-K/20-F/40-F, duration facts), quarterly (10-Q
  plus annuals for international filers, duration facts), or instant
  (point-in-time facts; the balance-sheet default)
- `ticker` / `report_id` — which one is required depends on the graph; see NOTES

**RETURNS:**
- Deduplicated facts (qname, name, value, end_date) ordered by end_date DESC
- resolved_report info when auto-resolution was used
- Dimensional/segment breakdowns are filtered out (consolidated totals only)
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "statement_type": {
            "type": "string",
            "description": "Type of financial statement",
            "enum": list(ANALYSIS_STATEMENT_TYPES),
          },
          "ticker": {
            "type": "string",
            "description": "Company ticker symbol. REQUIRED on shared-repo graphs (e.g. 'NVDA').",
          },
          "report_id": {
            "type": "string",
            "description": "Specific report identifier. If omitted on SEC, auto-resolved from ticker + filters.",
          },
          "fiscal_year": {
            "type": "integer",
            "description": "Filter auto-resolution by fiscal year focus (e.g. 2025).",
          },
          "period_type": {
            "type": "string",
            "description": "Filter by period type",
            "enum": ["annual", "quarterly", "instant"],
          },
          "limit": {
            "type": "integer",
            "description": "Max fact rows returned (1-1000). Leave at the default for a whole statement; a lower cap cuts rows while subtotals still reflect the full set.",
            "default": 1000,
          },
        },
        "required": ["statement_type"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("financial-statement-analysis", arguments)

    statement_type = (arguments.get("statement_type") or "").strip()
    if not statement_type:
      return {"error": "statement_type is required"}
    if statement_type not in ANALYSIS_STATEMENT_TYPES:
      return {
        "error": (
          f"Unknown statement_type '{statement_type}'. "
          f"Valid types: {', '.join(ANALYSIS_STATEMENT_TYPES)}"
        ),
      }

    ticker = (arguments.get("ticker") or "").strip().upper() or None
    report_id = (arguments.get("report_id") or "").strip() or None
    fiscal_year = arguments.get("fiscal_year")
    period_type = arguments.get("period_type")
    limit = max(1, min(int(arguments.get("limit", 1000)), 1000))

    graph_id = self.client.graph_id
    is_shared = is_shared_repository_or_subgraph(graph_id)

    if is_shared and not ticker and not report_id:
      return {
        "error": (
          "ticker is required on shared-repository graphs. "
          "Provide a ticker symbol (e.g. 'NVDA') or a specific report_id."
        ),
      }
    if not is_shared and not report_id:
      return {
        "error": (
          "report_id is required for tenant graphs. "
          "Only shared-repository graphs auto-resolve by ticker."
        ),
      }

    resolved: dict[str, Any] | None = None
    if is_shared and not report_id and ticker:
      from robosystems.adapters.sec.mcp import resolve_sec_report

      resolved = await resolve_sec_report(
        graph_id,
        ticker=ticker,
        period_type=period_type,
        fiscal_year=fiscal_year,
      )
      report_id = resolved.get("identifier") if resolved else None

      # Mirror of the REST view op: a requested fiscal_year that resolves
      # to nothing must be an error, not a silent fall-through to the
      # unscoped ticker sweep below (which orders by end_date DESC and
      # would answer with the newest filing instead).
      if fiscal_year is not None and not report_id:
        return {
          "error": (
            f"No {period_type or 'annual'} filing found for {ticker} "
            f"in fiscal year {fiscal_year}."
          ),
        }

    rows: list[dict] = []
    if report_id or ticker:
      rows = await query_financial_statement(
        graph_id,
        statement_type=statement_type,
        report_id=report_id,
        ticker=ticker,
        period_type=period_type,
        limit=limit,
      )

    deduped = deduplicate_facts(rows)[:limit]
    facts = [
      {
        "canonical_concept": row.get("canonical_concept"),
        "qname": row.get("qname"),
        "name": row.get("name"),
        "value": row.get("value"),
        "start_date": row.get("start_date"),
        "end_date": row.get("end_date"),
        "period_type": row.get("period_type"),
        "duration_type": row.get("duration_type"),
      }
      for row in deduped
    ]

    result: dict[str, Any] = {
      "graph_id": graph_id,
      "statement_type": statement_type,
      "ticker": ticker,
      "report_id": report_id,
      "facts": facts,
      "fact_count": len(facts),
    }

    if resolved:
      result["resolved_report"] = {
        "report_id": resolved.get("identifier"),
        "form": resolved.get("form"),
        "filing_date": resolved.get("filing_date"),
        "fiscal_year": resolved.get("fiscal_year"),
        "fiscal_period": resolved.get("fiscal_period"),
      }

    if not facts:
      hint_target = ticker or report_id or "this filter"
      result["tip"] = (
        f"No facts found for {hint_target} {statement_type}. "
        "Try a different statement_type, period_type, or report_id."
      )

    return result


def _parse_date(value: Any) -> date | None:
  """Parse an optional YYYY-MM-DD string into a date; pass through ``date`` objects."""
  if value is None or value == "":
    return None
  if isinstance(value, date):
    return value
  return date.fromisoformat(str(value))
