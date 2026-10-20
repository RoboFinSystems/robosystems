"""Financial statement MCP tools.

``live-financial-statement`` reads the tenant's OLTP ledger through the
CoA→GAAP mapping, ``consolidation-worksheet`` the same ledger with the entity
as the column, and ``preview-consolidation-entries`` the consolidating entries
a month needs; ``financial-statement-analysis`` reads the graph (SEC, or a
materialized tenant).
"""

from __future__ import annotations

from datetime import date
from typing import Any

from pydantic import ValidationError

from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.db.extensions import extensions_session
from robosystems.middleware.operations import run_off_loop
from robosystems.models.api.extensions.consolidation import (
  PreviewConsolidationEntriesRequest,
)
from robosystems.models.api.extensions.dimensions import StatementDimension
from robosystems.operations.roboledger.reads.consolidation import (
  NoConsolidationEntityError,
  preview_consolidation_entries,
)
from robosystems.operations.roboledger.reads.reports import (
  ANALYSIS_STATEMENT_TYPES,
  LIVE_STATEMENT_SCOPES,
  MCP_LIVE_STATEMENT_TYPES,
  CoaMappingNotFoundError,
  CombinedViewOnParentOnlyError,
  DimensionalStatementError,
  InvalidStatementScopeError,
  get_consolidation_worksheet,
  get_live_financial_statement,
  resolve_reporting_window,
)
from robosystems.operations.roboledger.views import (
  deduplicate_facts,
  mixed_units_note,
  query_financial_statement,
  units_reported_together,
)

from .base_tool import BaseTool
from .constants import ENTITY_ID_ARGUMENT

# The columns a filing presents: two balance-sheet instants, three years of
# flows. Anything more (a 10-K's quarterly note data) is on request.
PERIODS_DEFAULT_INSTANT = 2
PERIODS_DEFAULT_DURATION = 3
PERIODS_MAX = 100

# Independent of ``limit``, which applies after the period cap: tying the
# fetch to ``limit`` would hide older periods with no ``periods_omitted``.
QUERY_ROW_CEILING = 1000

_PERIOD_KEY_FIELDS = ("start_date", "end_date", "period_type", "duration_type")


def default_period_type(statement_type: str, form: str | None) -> str | None:
  """The period filter when the caller gave none.

  An annual form defaults to ``annual``: its hypercube also carries the
  quarterly note figures, often most of the rows. A 10-Q's quarter and YTD
  columns share end dates, so the period cap alone bounds it.
  """
  if statement_type == "balance_sheet":
    return None
  from robosystems.adapters.sec.mcp import ANNUAL_FORMS

  if form in ANNUAL_FORMS:
    return "annual"
  return None


def cap_periods(
  rows: list[dict[str, Any]], periods: int
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
  """Keep rows on the ``periods`` newest end dates.

  Returns (rows, distinct period keys newest first, count of end dates cut).
  """
  end_dates = sorted({str(r.get("end_date") or "") for r in rows}, reverse=True)
  kept_dates = set(end_dates[:periods])
  kept = [r for r in rows if str(r.get("end_date") or "") in kept_dates]
  keys: list[dict[str, Any]] = []
  seen: set[tuple[Any, ...]] = set()
  for row in kept:
    key = tuple(row.get(f) for f in _PERIOD_KEY_FIELDS)
    if key in seen:
      continue
    seen.add(key)
    keys.append({f: row.get(f) for f in _PERIOD_KEY_FIELDS if row.get(f) is not None})
  keys.sort(
    key=lambda k: (str(k.get("end_date") or ""), str(k.get("start_date") or "")),
    reverse=True,
  )
  return kept, keys, max(0, len(end_dates) - periods)


def compact_fact(row: dict[str, Any]) -> dict[str, Any]:
  """Drop null fields, and ``name`` when it only repeats the qname's local part.

  SEC names repeat the qname; tenant rs-gaap names are labels. Neither shape
  is pinned by the schema, so each row is judged on its own.
  """
  qname = row.get("qname") or ""
  name = row.get("name")
  if name == qname.rsplit(":", 1)[-1]:
    name = None
  fact = {
    "canonical_concept": row.get("canonical_concept"),
    "qname": qname or None,
    "name": name,
    "value": row.get("value"),
    "unit": row.get("unit"),
    "start_date": row.get("start_date"),
    "end_date": row.get("end_date"),
    "period_type": row.get("period_type"),
    "duration_type": row.get("duration_type"),
  }
  return {k: v for k, v in fact.items() if v is not None}


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
- `entity_id` (optional) — the entity whose books to render; omit for the group parent
- `scope` (optional, default `entity`) — `entity` is one entity's own books. On the group parent, `combined` sums every member consolidated in full under the parent's Reporting Style, per rs-gaap concept, for the months each was a member — the pre-adjustment total, nothing eliminated; `consolidated` adds the consolidation entity, whose eliminations and noncontrolling interest are the adjustments column, so a group that posts its consolidating entries there reads its consolidated statement. An equity-method or cost member (its `consolidation_method`) is never summed. A question about "the group" reads `consolidated`; `combined` is the worksheet's first total. `combined_entity_ids` on the response names who was summed, and an entity with no mapping yet is left out. `consolidated: true` is the older spelling of `scope: consolidated`
- `dimension` (optional, income statement only) — a management slice: `{"type": "department"}` returns the current window with one column per department, then `Unallocated` (lines no department tags), then `Total`, and `dimension.ties_out` says whether every row's members plus Unallocated equal its Total; `{"type": "department", "member_id": "<id>"}` reads one member's lines, current and prior (`member_id: "unallocated"` the untagged ones). `list-dimensions` names the axes and members. A consolidation entity's lines are untagged, so on `scope: consolidated` the eliminations read as Unallocated
- `include_sources` (optional, default false) — "why is this number what it is?": each row gains `origin` (`accounts`, `subtotal` or `computed`) and, per column, the accounts its figure was summed from with each one's value and the `entity_id` whose books hold it. Open one with `accountRegister(accountId, startDate, endDate, entityId)` over `query-graphql`. Ask it for the statement you are explaining, not for every read: it multiplies the response
- `statement_type` (required) — one of the four below
- income_statement — Revenue, expenses, net income
- balance_sheet — Assets, liabilities, equity (instant periods)
- cash_flow_statement — Operating (indirect: net income + non-cash add-backs + working-capital deltas), investing, financing. Investing/financing leaves resolve from each line's explicit flow tag when present, else from the mapped rs-gaap element's default-flow derivation arc — so untagged QB data renders too, provided the relevant accounts are mapped at the grain the arcs key on (e.g. PP&E Gross for capex). Needs ≥2 periods (current + prior) for the indirect deltas, so the prior period is pivoted as the delta basis and only the current period is rendered.
- equity_statement — The roll-forward of the Reporting Style's capital (stockholders' equity, partners' capital or members' equity): beginning balance, net income, contributions, distributions or dividends, and other changes, to the ending balance. Every equity line counts, cash or not — an owner paying a company bill directly is a contribution. The two balance rows share the capital concept's qname and are told apart by `period_role` (`start`, `end`). On a group with outside owners the noncontrolling interest rolls forward after it the same way (`rs-gaap:MinorityInterest`): its share of income, contributions, and distributions to the outside owners.
- Explicit `period_start` / `period_end` (YYYY-MM-DD) win over everything else
- `period_type="annual"` + `fiscal_year=2025` → window anchored on the graph's FiscalCalendar
- `period_type="quarterly"` → current calendar quarter
- `period_type="instant"` / unset → current calendar month

**RETURNS:**
Facts with element qnames, names, classifications, and values aligned with `periods` — current + prior (equal duration) for income_statement and balance_sheet, current only for cash_flow_statement. Abstract scaffolding rows and all-zero rows are filtered out; subtotals are kept and flagged `is_subtotal`. Capped at `limit` rows (default 1000 — the whole statement).

`validation` is the guard-rail outcome for the rendered columns (`status`: passed | failed | inconclusive; `failures`; `warnings`). Read it before quoting a number: a `failed` status means the statement does not foot or balance, and a warning about 'Other operating capital, net' means the cash flow was made to foot by a reconciling plug — say so rather than presenting the figure as clean. On the equity statement a failure means beginning balance + changes does not reach the ending balance.
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
          "entity_id": ENTITY_ID_ARGUMENT,
          "scope": {
            "type": "string",
            "enum": list(LIVE_STATEMENT_SCOPES),
            "description": (
              "entity: one entity's books. combined: on the group parent, "
              "every member consolidated in full, summed per rs-gaap concept "
              "for the months each was a member, nothing eliminated. "
              "consolidated: that sum plus the consolidation entity's "
              "eliminations and noncontrolling interest — what 'the group' "
              "reads."
            ),
          },
          "consolidated": {
            "type": "boolean",
            "default": False,
            "description": (
              "The older spelling of scope: consolidated; scope wins when "
              "both are given."
            ),
          },
          "dimension": {
            "type": "object",
            "description": (
              "Income statement only: slice by a management dimension. "
              "type alone gives a column per member plus Unallocated and "
              "Total; with member_id, one member's lines (or 'unallocated')."
            ),
            "properties": {
              "type": {
                "type": "string",
                "description": "The axis: department, class, location, project, or another.",
              },
              "member_id": {
                "type": "string",
                "description": "One member's id, or 'unallocated'. Omit for a column per member.",
              },
            },
            "required": ["type"],
            "additionalProperties": False,
          },
          "include_sources": {
            "type": "boolean",
            "default": False,
            "description": (
              "Add each row's origin and, per column, the accounts its "
              "figure was summed from, each with its entity."
            ),
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
          entity_id=arguments.get("entity_id"),
          consolidated=bool(arguments.get("consolidated", False)),
          scope=arguments.get("scope"),
          dimension=(
            StatementDimension.model_validate(arguments["dimension"])
            if arguments.get("dimension")
            else None
          ),
          include_sources=bool(arguments.get("include_sources", False)),
        )
      except (
        CoaMappingNotFoundError,
        CombinedViewOnParentOnlyError,
        DimensionalStatementError,
        InvalidStatementScopeError,
      ) as exc:
        return {
          "error": str(exc),
          "statement_type": statement_type,
        }

    result = response.model_dump(mode="json")
    if not arguments.get("include_sources"):
      for fact in result["facts"]:
        fact.pop("origin", None)
        fact.pop("sources", None)
    if not result["facts"]:
      result["tip"] = (
        f"No facts found for {statement_type}. "
        "Check that ledger data is loaded and CoA→GAAP mappings are complete."
      )
    return result


class ConsolidationWorksheetTool(BaseTool):
  """MCP tool: the group's statement with the entity as the column."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "consolidation-worksheet",
      "description": """The group's statement for one window with each entity as a column, live from the ledger — the worksheet a controller checks a consolidation against.

**WHEN TO USE:**
- "Show me the consolidation worksheet", "how does each company contribute to the group's balance sheet", "what do the eliminations take out"
- Checking the consolidating entries: each member's own number, the combined sum, the adjustments, the consolidated result, side by side
- For the group's statement alone, `live-financial-statement` with `scope: consolidated` is smaller

**PARAMETERS:**
- `statement_type` (required) — income_statement, balance_sheet, cash_flow_statement or equity_statement
- `period_start` / `period_end`, or `period_type` + `fiscal_year` — the window, as for `live-financial-statement`

**RETURNS:**
- `columns` in order: the group parent, each member consolidated in full, `combined` (their sum, nothing eliminated), the consolidation entity (`kind: adjustments`, when the group has one), `consolidated`, then each equity-method or cost member (`kind: not_summed`, `in_sum: false`) — shown for reference, never added in; its effect reaches the group through the adjustments
- `rows` — every line in the parent's Reporting Style order, `values` aligned with `columns`; a line zero in every column is left out
- `validation` — the guard rails on the consolidated column; read it before quoting a number
- A member outside its membership interval for the window has no column; `unmapped_entity_ids` names any entity with no mapping yet
- `omitted_periods` — a window the live statement would leave out (before a summed entity's books begin, or a cash flow or equity statement a member joined or left within) is named here and not totalled; when it is the worksheet's own window, `rows` is empty
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
        },
        "required": ["statement_type"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("consolidation-worksheet", arguments)

    statement_type = (arguments.get("statement_type") or "").strip()
    if statement_type not in MCP_LIVE_STATEMENT_TYPES:
      return {
        "error": (
          f"Unknown statement_type '{statement_type}'. "
          f"Valid types: {', '.join(MCP_LIVE_STATEMENT_TYPES)}."
        ),
      }

    graph_id = self.client.graph_id
    with extensions_session(graph_id) as session:
      start, end = resolve_reporting_window(
        session,
        period_start=_parse_date(arguments.get("period_start")),
        period_end=_parse_date(arguments.get("period_end")),
        period_type=arguments.get("period_type"),
        fiscal_year=arguments.get("fiscal_year"),
      )
      try:
        response = get_consolidation_worksheet(
          session,
          graph_id=graph_id,
          statement_type=statement_type,
          period_start=start,
          period_end=end,
        )
      except CoaMappingNotFoundError as exc:
        return {"error": str(exc), "statement_type": statement_type}
    return response.model_dump(mode="json")


class PreviewConsolidationEntriesTool(BaseTool):
  """MCP tool: the consolidating entries a group's month needs."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "preview-consolidation-entries",
      "description": """The consolidating entries a group's month needs, computed from the books. Posts nothing.

**WHEN TO USE:**
- "What consolidating entries does August need", "are the eliminations up to date", "why does the consolidated balance sheet still show the investment"
- Before posting a consolidating entry by hand: see what the engine would post, and whether a hand entry of the same kind already covers it
- After a member's month changes: what moved in the eliminations and the noncontrolling interest

**PARAMETERS:**
- `period` (required) — the month, YYYY-MM

**RETURNS:**
- `proposals` — one entry per kind and member, each the difference between what that adjustment should hold at the month end and what the consolidation entity already holds: `elimination_investment` (the parent's investment against the member's capital, the outside owners' share as noncontrolling interest), `elimination_intercompany` (the balances the companies owe each other, and the month's intercompany income against the matching expense), `nci_allocation` (the outside owners' share of the month's result), `equity_method_pickup` (the parent's share of an equity-method member's result). Each has its `lines` in the consolidation entity's accounts and a `basis` saying how it was computed
- `residuals` — the parent's investment in a member that differs from its share of the member's capital; that member's investment elimination is withheld until it is explained, never plugged
- `reconciling_items` — intercompany figures the two sides disagree on; the lower amount is eliminated
- `notes` — what was left out and why, e.g. a cost one company recharged to another at cost, which stays in the group's result
- `up_to_date` — nothing withheld, and the month's needs are posted or already drafted (`drafted_entry_ids`); drafted, they wait for the consolidation entity's close
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "period": {
            "type": "string",
            "description": "The month, as YYYY-MM.",
            "pattern": r"^\d{4}-(0[1-9]|1[0-2])$",
          },
        },
        "required": ["period"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("preview-consolidation-entries", arguments)
    try:
      body = PreviewConsolidationEntriesRequest.model_validate(
        {"period": (arguments.get("period") or "").strip()}
      )
    except ValidationError:
      return {"error": "period must be a month, as YYYY-MM."}
    with extensions_session(self.client.graph_id) as session:
      try:
        response = preview_consolidation_entries(session, period=body.period)
      except NoConsolidationEntityError as exc:
        return {"error": str(exc)}
    return response.model_dump(mode="json")


class FinancialStatementAnalysisTool(BaseTool):
  """MCP tool: a financial statement from the graph's XBRL hypercube."""

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
  (point-in-time facts; the balance-sheet default). Unset on an annual form
  it defaults to annual, so a 10-K's quarterly note figures stay out
- `periods` — how many period end dates to keep, newest first (default
  2 for the balance sheet, 3 for the flow statements — the columns a
  filing presents). Raise it for a longer series
- `ticker` / `report_id` — which one is required depends on the graph; see NOTES

**RETURNS:**
- Deduplicated facts (canonical_concept, qname, value, unit, start_date /
  end_date, period_type, duration_type; null fields omitted; `name` only when
  it is a label rather than the qname's local part) ordered by end_date DESC
- A line a filer reports in two units (its own currency and a US-dollar
  translation) comes back once per unit, and `units_note` says so — read one
  unit down the statement, never a mix
- `periods` — the period keys the facts span, newest first, and
  `periods_omitted` when older end dates were cut by the cap
- resolved_report info when auto-resolution was used, with `links` on a shared repository: `viewer` opens the filing in the xbrlkit viewer — give the user that URL to show the filing — beside `holon`, `tavi`, `edgar`
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
          "periods": {
            "type": "integer",
            "description": "Period end dates to keep, newest first (default 2 for balance_sheet, 3 otherwise; max 100). Raise it for a longer series.",
          },
          "limit": {
            "type": "integer",
            "description": "Max fact rows returned (1-1000), applied after the period cap. Leave at the default for a whole statement; a lower cap cuts rows while subtotals still reflect the full set.",
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
    periods_default = (
      PERIODS_DEFAULT_INSTANT
      if statement_type == "balance_sheet"
      else PERIODS_DEFAULT_DURATION
    )
    periods_arg = arguments.get("periods")
    periods = (
      periods_default
      if periods_arg is None
      else max(1, min(int(periods_arg), PERIODS_MAX))
    )

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

      # As in REST: an unmatched fiscal_year is an error, not a fall-through
      # to the ticker sweep, which would answer with the newest filing.
      if fiscal_year is not None and not report_id:
        return {
          "error": (
            f"No {period_type or 'annual'} filing found for {ticker} "
            f"in fiscal year {fiscal_year}."
          ),
        }

    if period_type is None:
      period_type = default_period_type(
        statement_type, resolved.get("form") if resolved else None
      )

    rows: list[dict] = []
    if report_id or ticker:
      rows = await query_financial_statement(
        graph_id,
        statement_type=statement_type,
        report_id=report_id,
        ticker=ticker,
        period_type=period_type,
        limit=QUERY_ROW_CEILING,
      )

    kept, period_keys, omitted = cap_periods(deduplicate_facts(rows), periods)
    facts = [compact_fact(row) for row in kept[:limit]]

    result: dict[str, Any] = {
      "graph_id": graph_id,
      "statement_type": statement_type,
      "ticker": ticker,
      "report_id": report_id,
      "periods": period_keys,
      "facts": facts,
      "fact_count": len(facts),
    }
    if mixed_units := units_reported_together(
      facts,
      line=lambda fact: (
        fact.get("qname"),
        fact.get("start_date"),
        fact.get("end_date"),
      ),
    ):
      result["units_note"] = mixed_units_note(mixed_units)
    if omitted:
      result["periods_omitted"] = omitted
      result["periods_tip"] = (
        f"{omitted} earlier period end date(s) were cut by the cap of {periods}; "
        "raise `periods` for a longer series."
      )
    if len(rows) >= QUERY_ROW_CEILING:
      # The graph fetch is newest-first and hit its ceiling, so the oldest
      # kept period may be incomplete and the cap cannot see past it.
      result["rows_truncated"] = True
      result["rows_tip"] = (
        f"The graph fetch hit its {QUERY_ROW_CEILING}-row ceiling before the "
        "cap ran; the oldest period kept may be incomplete. Narrow with "
        "`period_type` or `report_id`."
      )

    if resolved:
      from robosystems.operations.roboledger.views import resolved_report_info

      result["resolved_report"] = resolved_report_info(resolved)

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
