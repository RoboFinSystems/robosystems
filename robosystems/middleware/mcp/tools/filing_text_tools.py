"""``describe-filing``, ``search-text`` and ``read-text``: xbrlkit's text tools
over one filing read whole — its own document on SEC (a 10-K, a 10-Q, an 8-K
with its exhibits), its tagged text blocks on a tenant.

Read-only; neither the graph nor EDGAR is in the path. The names are the
contract ``xbrlkit serve`` set.
"""

from __future__ import annotations

from typing import Any

from robosystems.operations.roboledger.views import (
  QueryError,
  ReportNotFoundError,
  ReportSelectorError,
  ReportTooLargeError,
  filing_info,
  query_describe_filing,
  query_read_text,
  query_search_text,
  resolve_filing,
)
from robosystems.operations.roboledger.views.filing_text import (
  READ_MAX_LENGTH,
  SEARCH_MAX_HITS,
  SEARCH_MAX_WINDOW,
)

from .base_tool import BaseTool
from .disclosure_tools import _cap, _clean

_FILING_PROPERTIES: dict[str, Any] = {
  "ticker": {
    "type": "string",
    "description": (
      "Company ticker. On SEC it opens the filer's filings: the latest annual "
      "report by default, narrowed by fiscal_year / period_type, or the one "
      "accession or form names (e.g. 'NVDA')."
    ),
  },
  "report_id": {
    "type": "string",
    "description": (
      "Specific report identifier. REQUIRED on tenant graphs; on SEC, a report "
      "in the graph when no ticker is given."
    ),
  },
  "fiscal_year": {
    "type": "integer",
    "description": "Narrow ticker resolution to this fiscal year focus (e.g. 2019).",
  },
  "period_type": {
    "type": "string",
    "description": (
      "Which forms ticker resolution considers: annual (10-K / 20-F / 40-F, the "
      "default) or quarterly (10-Q as well)."
    ),
    "enum": ["annual", "quarterly"],
  },
  "accession": {
    "type": "string",
    "description": (
      "SEC only, with ticker: one filing by accession number "
      "(0000320193-25-000077) — a report, or an 8-K from `recent_releases`."
    ),
  },
  "form": {
    "type": "string",
    "description": (
      "SEC only, with ticker: '8-K' reads the latest earnings release (Item "
      "2.02) with its exhibits; `resolved_report.recent_releases` lists others."
    ),
    "enum": ["8-K"],
  },
}

_ERRORS = (ReportSelectorError, ReportNotFoundError, ReportTooLargeError, QueryError)


async def _resolve(graph_id: str, arguments: dict[str, Any]):
  return await resolve_filing(
    graph_id,
    report_id=_clean(arguments.get("report_id")),
    ticker=_clean(arguments.get("ticker")),
    fiscal_year=arguments.get("fiscal_year"),
    period_type=_clean(arguments.get("period_type")),
    accession=_clean(arguments.get("accession")),
    form=_clean(arguments.get("form")),
  )


def _with_info(result: dict[str, Any], ref: Any) -> dict[str, Any]:
  info = filing_info(ref)
  if info:
    result["resolved_report"] = info
  return result


class DescribeFilingTool(BaseTool):
  """MCP tool: how a filing is laid out, with the offsets the text tools take."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "describe-filing",
      "description": """How one filing is laid out: entity, periods, statements and disclosures by role, axes, and the text — its Items (10-K / 10-Q) and largest text blocks with the character `offset` each starts at, which `read-text` pages from. Call it before `read-text` when you want a whole section rather than a match.

**WHEN TO USE:**
- Before reading a whole Item (MD&A, Risk Factors) with `read-text`: its offset is here
- To see what a filing is — form, dates, an 8-K's items — before searching it

**PARAMETERS:**
- `ticker` — on SEC, the filer: its latest annual report unless `fiscal_year` / `period_type` / `accession` / `form` say otherwise; any processed year reads
- `form: "8-K"` — the latest earnings release; `accession` — one filing (a report, or an 8-K from `recent_releases`)
- `report_id` — a tenant's report, or a report in the SEC graph on its own (not beside a ticker)

**RETURNS:**
- `profile.text` — "primary document" when the filing's own document is read, "tagged text blocks" when it has none (a tenant report)
- `filing` (form, dates, `items` on an 8-K), `entity`, `counts`, `periods`, `statements`, `disclosures`, `axes`
- `sections.items` — each Item's `id`, `label`, `offset`, `chars`; `sections.text_blocks` — the largest blocks
- `resolved_report.links` on SEC — `viewer` opens the filing in the xbrlkit viewer (give the user that URL to show it), `holon`, `tavi`, `as_filed`, an 8-K's `exhibits`, `edgar`

**RELATED TOOLS:**
- `search-text` — find words inside this filing; `read-text` — page it from an offset
- `disclosures` / `information-block` — a section's facts, breakdowns and footing
""",
      "inputSchema": {
        "type": "object",
        "properties": _FILING_PROPERTIES,
        "required": [],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("describe-filing", arguments)
    graph_id = self.client.graph_id
    try:
      ref = await _resolve(graph_id, arguments)
      result = await query_describe_filing(graph_id, ref)
    except _ERRORS as exc:
      return {"error": str(exc)}
    return _with_info(result, ref)


class SearchTextTool(BaseTool):
  """MCP tool: words searched inside one filing's whole text."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "search-text",
      "description": f"""Search INSIDE one filing: every place its whole text says something, in document order, with the surrounding text. On SEC the text is the filing's own document — a 10-K, 10-Q, 20-F or 40-F from any processed year, or an 8-K earnings release with its exhibits — so it finds what no indexed section carries: the cover page, a footnote to a table, an exhibit, a passage between Items.

**WHEN TO USE:**
- A question about one filing that its indexed sections or its facts do not answer: "does the 10-K mention a going concern", "what guidance did the earnings release give"
- To find every mention of a term in one filing, not the best-ranked few
- `search-documents` finds WHICH filings discuss something, across the corpus, ranked; `search-text` finds WHERE in one filing, exhaustively. Find the filing first, then search inside it

**PARAMETERS:**
- `query` (required) — words matched in order across any spacing, case-insensitive: `customer concentration`. Split phrases with `|` to match any of them; end a word with `*` for a stem (`terminat*`); a `*` on its own is ignored. Not a regular expression
- `ticker` (+ `fiscal_year` / `period_type`, or `form: "8-K"`, or `accession`) / `report_id` — as for `describe-filing`
- `window` — characters of context around each match (default 300, max {SEARCH_MAX_WINDOW}); `max_hits` (default 10, max {SEARCH_MAX_HITS})

**RETURNS:**
- `total` matches; `hits` — `offset`, `match`, `text` around it, and the `section` it falls in
- `sections` — where all matches fall when there are more than the hits shown
- `terms` — on no match, how often each of your words occurs alone: search again with the wording the filing uses
- `text` — "primary document", or "tagged text blocks" for a filing without its document
- `resolved_report.links` on SEC — `viewer` opens the filing in the xbrlkit viewer (give the user that URL to show it), `as_filed`, an 8-K's `exhibits`, `edgar`

**NOTES:**
- Pass a hit's `offset` to `read-text` to read on from it before quoting
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "query": {
            "type": "string",
            "description": (
              "Words to find, in order; `|` between alternatives, a trailing "
              "`*` for a stem. Not a regular expression."
            ),
          },
          **_FILING_PROPERTIES,
          "window": {
            "type": "integer",
            "description": f"Characters of context around each match (40–{SEARCH_MAX_WINDOW}, default 300).",
          },
          "max_hits": {
            "type": "integer",
            "description": f"Matches to return (1–{SEARCH_MAX_HITS}, default 10).",
          },
        },
        "required": ["query"],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("search-text", arguments)
    query = _clean(arguments.get("query"))
    if not query:
      return {"error": "query is required."}
    try:
      window = _cap(arguments.get("window"), SEARCH_MAX_WINDOW)
      max_hits = _cap(arguments.get("max_hits"), SEARCH_MAX_HITS)
    except (TypeError, ValueError):
      return {"error": "window and max_hits must be whole numbers."}
    graph_id = self.client.graph_id
    try:
      ref = await _resolve(graph_id, arguments)
      result = await query_search_text(
        graph_id, ref, query, window=window, max_hits=max_hits
      )
    except _ERRORS as exc:
      return {"error": str(exc)}
    return _with_info(result, ref)


class ReadTextTool(BaseTool):
  """MCP tool: one filing's whole text, a window at a time."""

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "read-text",
      "description": f"""Read one filing's whole text from a character offset — a `search-text` hit's `offset`, or a section's from `describe-filing`. Returns `next_offset` while more follows; call again with it to read on rather than answering from a partial read.

**WHEN TO USE:**
- After `search-text`, to read the passage around a hit before quoting it
- To read a whole Item from the offset `describe-filing` gives

**PARAMETERS:**
- `offset` (default 0), `length` (default 4000, max {READ_MAX_LENGTH})
- `ticker` / `report_id` / `fiscal_year` / `period_type` / `accession` / `form` — the same filing `search-text` read

**RETURNS:**
- `text`, `offset`, `length`, `text_chars` (the whole text), `next_offset`, and the `section` the window starts in
- `resolved_report.links` on SEC — `viewer`, `as_filed`, `edgar`, as for `search-text`
""",
      "inputSchema": {
        "type": "object",
        "properties": {
          **_FILING_PROPERTIES,
          "offset": {
            "type": "integer",
            "description": "Character offset to start from (default 0).",
          },
          "length": {
            "type": "integer",
            "description": f"Characters to return (default 4000, max {READ_MAX_LENGTH}).",
          },
        },
        "required": [],
        "additionalProperties": False,
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> dict[str, Any]:
    self._log_tool_execution("read-text", arguments)
    try:
      offset = max(0, int(arguments.get("offset") or 0))
      length = _cap(arguments.get("length"), READ_MAX_LENGTH)
    except (TypeError, ValueError):
      return {"error": "offset and length must be whole numbers."}
    graph_id = self.client.graph_id
    try:
      ref = await _resolve(graph_id, arguments)
      result = await query_read_text(graph_id, ref, offset=offset, length=length)
    except _ERRORS as exc:
      return {"error": str(exc)}
    return _with_info(result, ref)
