"""The map and the block — ``disclosures`` and ``information-block`` over the graph.

The two shaped tools ``xbrlkit serve`` offers over a loaded filing, served
here over a report the platform holds whole: the published filing on the SEC
shared repository, the ledger's own report on a tenant graph. Both delegate
to ``operations/roboledger/views/information_blocks.py``, which reads the
report into xbrlkit's model and runs xbrlkit's own tools over it, so REST,
MCP and the local server answer from one implementation. Read-only; the
graph is not in the path.

On a tenant graph these sit beside ``get-information-block`` /
``list-information-blocks``, which return one *authored* block's envelope
(its rules, verification and provenance). These two read a section whole —
values, breakdowns, footing, text. The descriptions say which is which; the
names are the contract `xbrlkit serve` set and do not change.
"""

from __future__ import annotations

from typing import Any

from robosystems.operations.roboledger.views import (
  BlockNotFoundError,
  ReportNotFoundError,
  ReportSelectorError,
  query_disclosures,
  query_information_block,
  resolve_report,
  resolved_report_info,
)
from robosystems.operations.roboledger.views.information_blocks import (
  MAX_BLOCK_MEMBERS,
  MAX_BLOCK_ROWS,
)

from .base_tool import BaseTool

_SELECTOR_PROPERTIES: dict[str, Any] = {
  "ticker": {
    "type": "string",
    "description": (
      "Company ticker symbol. On shared-repo graphs (SEC) it resolves the latest "
      "matching filing when report_id is not given (e.g. 'NVDA')."
    ),
  },
  "report_id": {
    "type": "string",
    "description": (
      "Specific report identifier. REQUIRED on tenant graphs; on SEC, optional "
      "when ticker is given."
    ),
  },
  "fiscal_year": {
    "type": "integer",
    "description": "Narrow auto-resolution to this fiscal year focus (e.g. 2025).",
  },
  "period_type": {
    "type": "string",
    "description": (
      "Which forms auto-resolution considers: annual (10-K / 20-F / 40-F, the "
      "default) or quarterly (10-Q as well)."
    ),
    "enum": ["annual", "quarterly"],
  },
}


def _clean(value: Any) -> str | None:
  text = (value or "").strip() if isinstance(value, str) else None
  return text or None


class DisclosuresTool(BaseTool):
  """MCP tool: the map of a report's sections, as families of blocks."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "disclosures",
      "description": """The map of a report's sections: one row per disclosure family — a note with its policies, tables and details, a statement with its parenthetical, the cover page — with block counts by level, fact counts and text-block counts, in filing order. With `topic`, one family's blocks: each block's id, level, name, fact count, axes and text blocks. Cheap. Call it first, then `information-block` for the one block you need.

**WHEN TO USE:**
- "What does this filing disclose about leases / debt / segments / taxes?" — the map, then the family
- Before `information-block`: to find the block id
- To see which sections carry dimensional breakdowns (`dimensional_facts`) or text blocks

**PARAMETERS:**
- `ticker` / `report_id` — which one is required depends on the graph: SEC needs `ticker` (auto-resolves the latest 10-K; `fiscal_year` and `period_type` narrow it) or a `report_id`; a tenant graph needs `report_id`
- `topic` — a family's name, or part of it ("leases", "income taxes"); omit for the whole map

**RETURNS:**
- Without `topic`: `disclosures` — one row per family: `disclosure`, `blocks`, `levels` (note / policies / tables / details / statement / parenthetical / document), `facts`, `text_blocks`, and `category` when the family is not a note
- With `topic`: `blocks` — each with `id` (what `information-block` takes), `level`, `name`, `facts`, `dimensional_facts`, `axes`, `has_calc`, `text_blocks`
- `resolved_report` when the report was resolved from a ticker

**NOTES:**
- Families are read off the filer's own role titles, so the map is the filing's complete section index — statements and the cover page included
- The whole map is a few thousand characters; one family about 1.5K. `information-block` is the expensive call, so narrow here first
- The same tool `xbrlkit serve` offers over a loaded filing: same name, same shape

**RELATED TOOLS:**
- `information-block` — one block read whole
- `financial-statement-analysis` — the primary statements as flat fact rows
- `get-information-block` / `list-information-blocks` (tenant graphs) — one authored block's envelope, with its rules and verification; this tool maps the report's sections as the ledger holds them
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          **_SELECTOR_PROPERTIES,
          "topic": {
            "type": "string",
            "description": (
              "A disclosure family's name, or part of it ('leases', 'income "
              "taxes'), for that family's blocks. Omit for the whole map."
            ),
          },
        },
        "required": [],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("disclosures", arguments)
    graph_id = self.client.graph_id
    try:
      report_id, resolved = await resolve_report(
        graph_id,
        report_id=_clean(arguments.get("report_id")),
        ticker=_clean(arguments.get("ticker")),
        fiscal_year=arguments.get("fiscal_year"),
        period_type=_clean(arguments.get("period_type")),
      )
      result = await query_disclosures(
        graph_id, report_id, topic=_clean(arguments.get("topic"))
      )
    except (ReportSelectorError, ReportNotFoundError, BlockNotFoundError) as exc:
      return {"error": str(exc)}
    if resolved:
      result["resolved_report"] = resolved_report_info(resolved)
    return result


class InformationBlockTool(BaseTool):
  """MCP tool: one section of a report read whole."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "information-block",
      "description": f"""One section of a report read whole — THE EXPENSIVE CALL (4–30K characters): rows in presentation order with depth and label; `values` = the consolidated fact per period column; `members` = the same row broken out by the block's own axes; `axes` with the members that carry facts; `calculation` = every total's children with weights and a footing check per period; `text` = the section's text blocks. Call `disclosures` first to find the block id, then call this once per block — not once per question.

**WHEN TO USE:**
- A derived question about one section: "which line items make up operating expenses", "how does long-term debt break down by instrument", "does the tax rate reconciliation foot"
- Segment, maturity, roll-forward and reconciliation tables — the breakdowns a statement view hides
- Never for a whole filing: one block per call

**PARAMETERS:**
- `block` (required) — the block `id` from `disclosures`; a role name or its last segment also resolves
- `ticker` / `report_id` — as for `disclosures`
- `periods` — period keys to keep, from a previous call's `columns`; the default keeps the budgeted set, year and balance columns first on an annual form
- `member` — keep only breakdowns whose member key contains this text (a segment name)
- `max_rows` (default {MAX_BLOCK_ROWS}) / `max_members` (up to {MAX_BLOCK_MEMBERS}) — explicit caps when you want less than the budget

**RETURNS:**
- `block` — id, role, name, disclosure, level; `siblings` (the rest of the family); `merged_roles` when a second drawer of the same section folded in
- `columns` — the period keys shown; `rows` — depth, concept, label, `values` and/or `members`, `abstract` on headers; `members_omitted` / `periods_omitted` on a row a cut touched
- `axes` (each with the members that carry facts and its default), `calculation` (`foots` / `checked` counts per total; `differences` only where |reported − computed| exceeds half a unit at the stated precision), `text`
- `truncated` when `max_rows` cut the walk; `resolved_report` when a ticker was resolved

**NOTES:**
- Budgeted, not counted: breakdowns and columns are kept most-reported / most-recent first up to about 16K characters of cells each; a small table is never cut, and a row is never left blank by a cut
- A footing difference is usually the filer's own tagging (an element swap, a fact in the wrong context) — read the section's text before calling it an error
- On the SEC repository the report is the published filing; a filing processed before its artifacts existed answers "not published yet" until the repository is reprocessed. A `text` entry marked `external` is a text block whose fragment could not be read; read it with `search-documents` and `get-document-section`

**RELATED TOOLS:**
- `disclosures` — the map; call it first
- `financial-statement-analysis` — the primary statements as flat rows, cheaper when no breakdown is needed
- `build-fact-grid` — a slice of facts across reports and entities
- `get-information-block` (tenant graphs) — one authored block's envelope with its rules and verification; this is the section read whole from the ledger's report
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "block": {
            "type": "string",
            "description": (
              "The block id from `disclosures` (a role name or its last segment "
              "also resolves)."
            ),
          },
          **_SELECTOR_PROPERTIES,
          "periods": {
            "type": "array",
            "items": {"type": "string"},
            "description": (
              "Period keys to keep, from a previous call's `columns`. Omit for "
              "the budgeted default."
            ),
          },
          "member": {
            "type": "string",
            "description": (
              "Keep only breakdowns whose member key contains this text (a "
              "segment name)."
            ),
          },
          "max_rows": {
            "type": "integer",
            "description": f"Cap on presentation rows (1–{MAX_BLOCK_ROWS}; default {MAX_BLOCK_ROWS}).",
          },
          "max_members": {
            "type": "integer",
            "description": (
              f"An explicit cap on member breakdowns (1–{MAX_BLOCK_MEMBERS}) "
              "instead of the response budget."
            ),
          },
        },
        "required": ["block"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("information-block", arguments)
    block = _clean(arguments.get("block"))
    if not block:
      return {"error": "block is required — take its id from `disclosures`."}
    periods_arg = arguments.get("periods")
    periods = (
      [str(p) for p in periods_arg if str(p).strip()]
      if isinstance(periods_arg, list)
      else None
    )
    max_rows = arguments.get("max_rows")
    max_members = arguments.get("max_members")
    graph_id = self.client.graph_id
    try:
      report_id, resolved = await resolve_report(
        graph_id,
        report_id=_clean(arguments.get("report_id")),
        ticker=_clean(arguments.get("ticker")),
        fiscal_year=arguments.get("fiscal_year"),
        period_type=_clean(arguments.get("period_type")),
      )
      result = await query_information_block(
        graph_id,
        report_id,
        block,
        periods=periods or None,
        member=_clean(arguments.get("member")),
        max_rows=max(1, min(int(max_rows), MAX_BLOCK_ROWS))
        if max_rows is not None
        else None,
        max_members=max(1, min(int(max_members), MAX_BLOCK_MEMBERS))
        if max_members is not None
        else None,
      )
    except (ReportSelectorError, ReportNotFoundError, BlockNotFoundError) as exc:
      return {"error": str(exc)}
    if resolved:
      result["resolved_report"] = resolved_report_info(resolved)
    return result
