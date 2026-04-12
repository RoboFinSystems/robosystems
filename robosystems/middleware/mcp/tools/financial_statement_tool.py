"""
Get Financial Statement Tool — Returns structured financial data.

Routing is determined by graph type, not by ticker presence:

1. Shared repository (e.g., SEC): query the SEC graph via Structure → FactSet
   → Fact traversal. Ticker is REQUIRED — it identifies which public company's
   filings to pull.

2. User graph with roboledger extension: generate the statement from the
   current graph's OLTP ledger data, rendered through the same structure
   hierarchy. Ticker is OPTIONAL — it's a universal entity symbol (like a
   JIRA project key) used as a label, but routing is based on graph type.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any

from robosystems.logger import logger

from .base_tool import BaseTool

VALID_STATEMENT_TYPES = (
  "income_statement",
  "balance_sheet",
  "cash_flow_statement",
  "equity_statement",
)

# Forms used by domestic filers
ANNUAL_FORMS = ["10-K", "20-F", "40-F"]
# Quarterly forms (include annual since international filers may only have annual)
QUARTERLY_FORMS = ["10-K", "20-F", "40-F", "10-Q"]


class GetFinancialStatementTool(BaseTool):
  """MCP tool that returns a structured financial statement for a company/period."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-financial-statement",
      "description": """Return a structured financial statement (income statement, balance sheet, etc.) for the current graph.

**ROUTING — determined by graph type, NOT by ticker:**

1. **Shared repository (e.g., SEC)**: queries public filings for the given ticker.
   Ticker is REQUIRED in this mode.

2. **User graph with roboledger extension** (RoboLedger tenants): generates the
   statement from the graph's OLTP ledger data using the CoA→GAAP mapping.
   Ticker is OPTIONAL — in a user graph it's a symbol/label for the entity,
   not a route selector. The data always comes from the current graph's books.

**WHEN TO USE:**
- In SEC shared repo: "show me NVDA's latest income statement"
- In a RoboLedger user graph: "show me my income statement" or "show me this month's balance sheet"

**STATEMENT TYPES:**
- income_statement — Revenue, expenses, net income
- balance_sheet — Assets, liabilities, equity (instant periods)
- cash_flow_statement — Operating, investing, financing activities
- equity_statement — Equity components and changes

**RETURNS:**
- Facts with element names, values, periods, and period types
- Ordered by period end date (most recent first)
- Deduplicated by element and period (keeps most recent filing)
- Filters out dimensional/segment breakdowns by default

**TIP:**
For balance sheets, only instant-period facts are returned. For other statements, duration-period facts are returned by default. Use period_type to override.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "ticker": {
            "type": "string",
            "description": (
              "Company ticker symbol. REQUIRED in SEC shared repository "
              "(e.g. 'NVDA'). OPTIONAL in RoboLedger user graphs — "
              "informational label only; the data always comes from the current graph."
            ),
          },
          "statement_type": {
            "type": "string",
            "description": "Type of financial statement",
            "enum": list(VALID_STATEMENT_TYPES),
          },
          "report_id": {
            "type": "string",
            "description": (
              "Optional: filter to a specific report by its identifier. "
              "When omitted, the latest relevant filing "
              "is auto-resolved based on period_type."
            ),
          },
          "fiscal_year": {
            "type": "integer",
            "description": (
              "Optional: filter to a specific fiscal year (e.g. 2025). "
              "Used during auto-resolve to find the right filing."
            ),
          },
          "period_type": {
            "type": "string",
            "description": (
              "Filter by period type: 'annual' (10-K/20-F/40-F, duration facts), "
              "'quarterly' (10-Q or annual filings, duration facts), "
              "'instant' (point-in-time facts). Default depends on statement type."
            ),
            "enum": ["annual", "quarterly", "instant"],
          },
          "period_start": {
            "type": "string",
            "description": (
              "Start date for private company reports (YYYY-MM-DD). "
              "Defaults to first day of current month."
            ),
          },
          "period_end": {
            "type": "string",
            "description": (
              "End date for private company reports (YYYY-MM-DD). "
              "Defaults to last day of current month."
            ),
          },
          "limit": {
            "type": "integer",
            "description": "Maximum number of facts to return (default: 50)",
            "default": 50,
          },
        },
        "required": ["statement_type"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("get-financial-statement", arguments)

    ticker = (arguments.get("ticker", "") or "").strip().upper()
    statement_type = arguments.get("statement_type", "").strip()
    report_id = (
      arguments.get("report_id", "").strip() if arguments.get("report_id") else None
    )
    fiscal_year = arguments.get("fiscal_year")
    period_type = arguments.get("period_type")
    limit = max(1, min(int(arguments.get("limit", 50)), 1000))

    if not statement_type:
      return {"error": "statement_type is required"}
    if statement_type not in VALID_STATEMENT_TYPES:
      return {
        "error": f"Unknown statement_type '{statement_type}'. "
        f"Valid types: {', '.join(VALID_STATEMENT_TYPES)}"
      }

    # Routing is determined by graph type, not by ticker presence.
    #
    # - Shared repository (e.g., SEC): query the SEC graph for filings.
    #   Ticker is REQUIRED — it identifies which public company's filings
    #   to return.
    #
    # - User graph (with roboledger extension): generate the statement from
    #   the current graph's OLTP ledger data. Ticker is OPTIONAL — it's a
    #   universal entity identifier (like a JIRA project key) used as a
    #   label, but the data comes from the current graph regardless.
    #
    # The tool is only registered when roboledger extension is present, so
    # we don't need to check for that. We only need to distinguish shared
    # repos from user graphs.
    if self._is_shared_repository():
      if not ticker:
        return {
          "error": (
            "ticker is required in the SEC shared repository. "
            "Provide a ticker symbol (e.g. 'NVDA') to query public filings."
          )
        }
      return await self._get_public_statement(
        ticker, statement_type, report_id, fiscal_year, period_type, limit
      )

    # User graph with roboledger → OLTP path. Ticker (if provided) is
    # informational only — we generate from the current graph's ledger.
    period_start = arguments.get("period_start")
    period_end = arguments.get("period_end")
    return self._get_private_statement(
      statement_type,
      period_start,
      period_end,
      period_type=period_type,
      fiscal_year=fiscal_year,
      limit=limit,
    )

  def _resolve_private_window(
    self,
    *,
    period_start_str: str | None,
    period_end_str: str | None,
    period_type: str | None,
    fiscal_year: int | None,
    graph_id: str,
  ) -> tuple[date, date]:
    """Choose a reporting window for the private OLTP path.

    Explicit period_start / period_end take priority. Otherwise:

    - ``annual`` → fiscal year aligned to FiscalCalendar.fiscal_year_start_month
      (falls back to January when no calendar exists). ``fiscal_year`` selects
      which year; defaults to the year containing today.
    - ``quarterly`` → current calendar quarter (3 months).
    - anything else → current month (legacy default).
    """
    today = date.today()

    if period_end_str or period_start_str:
      if period_end_str:
        period_end = date.fromisoformat(period_end_str)
      else:
        if today.month == 12:
          period_end = date(today.year, 12, 31)
        else:
          period_end = date(today.year, today.month + 1, 1) - timedelta(days=1)
      if period_start_str:
        period_start = date.fromisoformat(period_start_str)
      else:
        period_start = period_end.replace(day=1)
      return period_start, period_end

    if period_type == "annual":
      fy_start_month = self._fiscal_year_start_month(graph_id)
      year = fiscal_year if fiscal_year is not None else today.year
      period_start = date(year, fy_start_month, 1)
      end_year = year if fy_start_month == 1 else year + 1
      end_month = fy_start_month - 1 if fy_start_month > 1 else 12
      if end_month == 12:
        period_end = date(end_year, 12, 31)
      else:
        period_end = date(end_year, end_month + 1, 1) - timedelta(days=1)
      return period_start, period_end

    if period_type == "quarterly":
      quarter = (today.month - 1) // 3
      q_start_month = quarter * 3 + 1
      period_start = date(today.year, q_start_month, 1)
      q_end_month = q_start_month + 2
      if q_end_month == 12:
        period_end = date(today.year, 12, 31)
      else:
        period_end = date(today.year, q_end_month + 1, 1) - timedelta(days=1)
      return period_start, period_end

    # instant / None → current month
    if today.month == 12:
      period_end = date(today.year, 12, 31)
    else:
      period_end = date(today.year, today.month + 1, 1) - timedelta(days=1)
    period_start = period_end.replace(day=1)
    return period_start, period_end

  def _fiscal_year_start_month(self, graph_id: str) -> int:
    """Look up the graph's fiscal year start month, defaulting to January."""
    try:
      from robosystems.db.extensions import extensions_session
      from robosystems.models.extensions.roboledger.fiscal_calendar import (
        FiscalCalendar,
      )

      with extensions_session(graph_id) as session:
        cal = session.query(FiscalCalendar).first()
        if cal and cal.fiscal_year_start_month:
          return int(cal.fiscal_year_start_month)
    except Exception as e:
      logger.debug(f"Fiscal year start month lookup failed for {graph_id}: {e}")
    return 1

  def _resolve_private_closed_through(self, session) -> date | None:
    """Resolve the graph's closed_through date for the RE-adjustment lower bound."""
    try:
      from robosystems.models.extensions.roboledger.fiscal_calendar import (
        FiscalCalendar,
      )
      from robosystems.operations.roboledger.fiscal_calendar import period_date_range

      cal = session.query(FiscalCalendar).first()
      if cal is None or not cal.closed_through_period:
        return None
      _, end = period_date_range(cal.closed_through_period)
      return end
    except Exception as e:
      logger.debug(f"closed_through lookup failed: {e}")
      return None

  def _is_shared_repository(self) -> bool:
    """Check whether the current graph is a shared repository (e.g., SEC).

    Matches the logic in the MCP manager — uses the shared_repositories
    config to distinguish managed SEC-style repos from per-user graphs.
    """
    try:
      from robosystems.config.shared_repositories import (
        is_shared_repository_or_subgraph,
      )

      return is_shared_repository_or_subgraph(self.client.graph_id)
    except Exception as e:
      logger.debug(f"Shared repository check failed for {self.client.graph_id}: {e}")
      return False

  # ── Private company path (OLTP) ──────────────────────────────────────────

  def _get_private_statement(
    self,
    statement_type: str,
    period_start_str: str | None,
    period_end_str: str | None,
    period_type: str | None = None,
    fiscal_year: int | None = None,
    limit: int = 50,
  ) -> dict[str, Any]:
    """Generate a financial statement from OLTP ledger data.

    Creates a report on the fly using the CoA→GAAP mapping, renders
    through the reporting structure hierarchy, and returns facts in the
    same format as the public company graph query path.

    `period_type` controls how the reporting window is chosen when explicit
    start/end dates are not supplied:

    - ``annual`` — fiscal year anchored on the graph's ``fiscal_year_start_month``
      (from FiscalCalendar). ``fiscal_year`` selects which year; defaults to
      the year containing today.
    - ``quarterly`` — current calendar quarter + prior quarter.
    - ``instant`` / unset — current month + prior month (the legacy default).

    ``limit`` caps the number of fact rows returned, mirroring the public
    company path.
    """
    from robosystems.db.extensions import extensions_session
    from robosystems.models.extensions.roboledger import Structure
    from robosystems.operations.roboledger.reports.fact_grid import (
      PeriodSpec,
      generate_report_facts,
      render_structure_view,
    )

    graph_id = self.client.graph_id

    # Resolve window: explicit dates win over period_type defaults.
    period_start, period_end = self._resolve_private_window(
      period_start_str=period_start_str,
      period_end_str=period_end_str,
      period_type=period_type,
      fiscal_year=fiscal_year,
      graph_id=graph_id,
    )

    # Build periods: current + prior of the same length
    duration = (period_end - period_start).days + 1
    prior_end = period_start - timedelta(days=1)
    prior_start = prior_end - timedelta(days=duration - 1)

    periods = [
      PeriodSpec(start=period_start, end=period_end, label="Current"),
      PeriodSpec(start=prior_start, end=prior_end, label="Prior"),
    ]

    try:
      with extensions_session(graph_id) as session:
        # Find the mapping structure (type is "coa_mapping")
        mapping = (
          session.query(Structure)
          .filter(
            Structure.structure_type == "coa_mapping",
          )
          .first()
        )
        if not mapping:
          return {
            "error": "No CoA→GAAP mapping found. Run the mapping workflow first.",
            "statement_type": statement_type,
          }

        # Generate facts from OLTP. Bound the synthetic retained-earnings
        # adjustment to (closed_through, period_end] so real RE balances
        # aren't double-counted.
        closed_through = self._resolve_private_closed_through(session)
        facts = generate_report_facts(
          session=session,
          taxonomy_id="tax_usgaap_reporting",
          mapping_id=mapping.id,
          periods=periods,
          closed_through=closed_through,
        )

        # Render through the structure hierarchy
        grid = render_structure_view(
          session=session,
          facts=facts.facts,
          structure_type=statement_type,
          periods=periods,
        )

      # Format output to match the public company response shape
      result: dict[str, Any] = {
        "graph_id": graph_id,
        "statement_type": statement_type,
        "source": "oltp",
        "periods": [
          {"start": str(p.start), "end": str(p.end), "label": p.label} for p in periods
        ],
        "facts": [],
        "fact_count": 0,
        "unmapped_count": facts.unmapped_count,
      }

      for row in grid.rows:
        if row.is_subtotal:
          continue  # Skip abstract section headers
        # Only include rows with at least one non-zero value
        if any(v != 0.0 for v in row.values):
          result["facts"].append(
            {
              "qname": row.element_qname,
              "name": row.element_name,
              "classification": row.classification,
              "values": row.values,
              "depth": row.depth,
              "is_subtotal": row.is_subtotal,
            }
          )

      # Cap at the caller's requested limit (mirrors public path)
      if len(result["facts"]) > limit:
        result["facts"] = result["facts"][:limit]
        result["truncated"] = True
      result["fact_count"] = len(result["facts"])

      if not result["facts"]:
        result["tip"] = (
          f"No facts found for {statement_type}. "
          "Check that ledger data is loaded and CoA→GAAP mappings are complete."
        )

      return result

    except Exception as e:
      logger.error(f"Private company statement generation failed: {e}", exc_info=True)
      return {"error": f"Statement generation failed: {e}"}

  # ── Public company path (graph) ──────────────────────────────────────────

  async def _resolve_report(
    self,
    ticker: str,
    period_type: str | None,
    fiscal_year: int | None,
  ) -> dict[str, Any] | None:
    """Auto-resolve the most recent relevant report for the given filters."""
    # Determine which forms to look for
    if period_type == "annual":
      forms = ANNUAL_FORMS
    elif period_type in ("quarterly", "instant"):
      # quarterly: 10-Q + annual forms for international filers
      # instant: balance sheet data appears in both annual and quarterly filings
      forms = QUARTERLY_FORMS
    else:
      # No period_type specified — default to annual forms
      forms = ANNUAL_FORMS

    where_parts = ["ent.ticker = $ticker", "r.form IN $forms"]
    params: dict[str, Any] = {"ticker": ticker, "forms": forms}

    if fiscal_year is not None:
      where_parts.append("r.fiscal_year_focus = $fiscal_year")
      params["fiscal_year"] = fiscal_year

    query = (
      "MATCH (ent:Entity)-[:ENTITY_HAS_REPORT]->(r:Report) "
      f"WHERE {' AND '.join(where_parts)} "
      "RETURN r.identifier AS identifier, "
      "r.form AS form, r.filing_date AS filing_date, "
      "r.fiscal_year_focus AS fiscal_year, r.fiscal_period_focus AS fiscal_period "
      "ORDER BY r.filing_date DESC LIMIT 1"
    )

    try:
      rows = await self.client.execute_query(query, parameters=params)
      if rows:
        return rows[0]
    except Exception as e:
      logger.warning(f"Report auto-resolve failed for {ticker}: {e}")

    # If quarterly didn't find a 10-Q, the company may only file annual forms
    # (international filers with 20-F/40-F). Already covered since QUARTERLY_FORMS
    # includes annual forms.
    return None

  async def _get_public_statement(
    self,
    ticker: str,
    statement_type: str,
    report_id: str | None,
    fiscal_year: int | None,
    period_type: str | None,
    limit: int,
  ) -> dict[str, Any]:
    resolved_report = None

    # Auto-resolve report when no report_id provided
    if not report_id:
      resolved_report = await self._resolve_report(ticker, period_type, fiscal_year)
      if resolved_report:
        report_id = resolved_report["identifier"]

    result: dict[str, Any] = {
      "ticker": ticker,
      "statement_type": statement_type,
      "report_id": report_id,
      "facts": [],
      "fact_count": 0,
    }

    if resolved_report:
      result["resolved_report"] = {
        "report_id": resolved_report.get("identifier"),
        "form": resolved_report.get("form"),
        "filing_date": resolved_report.get("filing_date"),
        "fiscal_year": resolved_report.get("fiscal_year"),
        "fiscal_period": resolved_report.get("fiscal_period"),
      }

    # Build query using inline node properties for planner hints.
    # When report_id is available, start from Report (highly selective) and
    # intersect with FactSet membership — avoids slow Structure→FactSet→Fact chain.
    params: dict[str, Any] = {"statement_type": statement_type}

    # Period filter — inline on Period node when possible
    period_props = ""
    if period_type == "instant":
      period_props = " {period_type: 'instant'}"
    elif period_type == "annual":
      period_props = " {duration_type: 'annual'}"
    elif period_type == "quarterly":
      period_props = " {duration_type: 'quarterly'}"
    elif statement_type == "balance_sheet":
      period_props = " {period_type: 'instant'}"

    period_match = f"(f)-[:FACT_HAS_PERIOD]->(p:Period{period_props})"

    if report_id:
      # Fast path: start from Report, intersect with FactSet for statement filtering
      match_parts = [
        "(s:Structure {canonical_type: $statement_type})"
        "-[:STRUCTURE_HAS_FACT_SET]->(fs:FactSet)",
        "(r:Report {identifier: $report_id})"
        "-[:REPORT_HAS_FACT]->(f:Fact {has_dimensions: false})"
        "-[:FACT_HAS_ELEMENT]->(e:Element)",
        "(fs)-[:FACT_SET_CONTAINS_FACT]->(f)",
        period_match,
      ]
      where_parts = ["f.numeric_value IS NOT NULL"]
      params["report_id"] = report_id
    else:
      # No report_id — use Structure-first traversal (slower but works globally)
      match_parts = [
        "(s:Structure {canonical_type: $statement_type})"
        "-[:STRUCTURE_HAS_FACT_SET]->(fs:FactSet)"
        "-[:FACT_SET_CONTAINS_FACT]->(f:Fact {has_dimensions: false})"
        "-[:FACT_HAS_ELEMENT]->(e:Element)",
        period_match,
        "(f)-[:FACT_HAS_ENTITY]->(ent:Entity {ticker: $ticker})",
      ]
      where_parts = ["f.numeric_value IS NOT NULL"]
      params["ticker"] = ticker

    # Request extra rows for dedup, then trim to limit
    fetch_limit = min(limit * 3, 1000)
    params["limit"] = fetch_limit

    query = (
      f"MATCH {', '.join(match_parts)} "
      f"WHERE {' AND '.join(where_parts)} "
      "RETURN DISTINCT e.canonical_concept AS canonical_concept, e.qname AS qname, "
      "e.name AS name, f.numeric_value AS value, "
      "p.end_date AS end_date, p.period_type AS period_type, "
      "p.duration_type AS duration_type "
      "ORDER BY end_date DESC "
      "LIMIT $limit"
    )

    try:
      rows = await self.client.execute_query(query, parameters=params)
      if rows:
        deduped = self._deduplicate_facts(rows)
        for row in deduped[:limit]:
          result["facts"].append(
            {
              "canonical_concept": row.get("canonical_concept"),
              "qname": row.get("qname"),
              "name": row.get("name"),
              "value": row.get("value"),
              "end_date": row.get("end_date"),
              "period_type": row.get("period_type"),
              "duration_type": row.get("duration_type"),
            }
          )
        result["fact_count"] = len(result["facts"])
    except Exception as e:
      logger.error(f"Financial statement query failed: {e}")
      result["error"] = f"Query failed: {e}"

    if not result["facts"] and "error" not in result:
      result["tip"] = (
        f"No facts found for {ticker} {statement_type}. "
        "The company may not be loaded, or try a different statement_type."
      )

    return result

  @staticmethod
  def _deduplicate_facts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Deduplicate facts by (qname, end_date), keeping the first occurrence.

    Since results are ordered by end_date DESC, the first occurrence for each
    (qname, end_date) pair comes from the most recent filing.
    """
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for row in rows:
      key = (row.get("qname", ""), row.get("end_date", ""))
      if key not in seen:
        seen.add(key)
        deduped.append(row)
    return deduped
