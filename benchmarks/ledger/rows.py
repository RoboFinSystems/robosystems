"""Rows: what the model holds. Each differs from the hub (C) in one thing."""

from __future__ import annotations

from .mcp import ToolDef

# The tools that preview or review work before it is committed.
CHECK_TOOLS = frozenset(
  {
    "evaluate-rules",
    "get-period-close-status",
    "list-period-drafts",
    "live-financial-statement",
    "preview-consolidation-entries",
    "preview-event-block",
    "preview-reconciliations",
    "preview-reconciling-item",
    "preview-schedule",
  }
)

# The tools a month-end close on one entity uses.
CLOSE_LIST = frozenset(
  {
    "accept-suggestions",
    "close-period",
    "create-event-block",
    "create-information-block",
    "create-mapping-association",
    "evaluate-rules",
    "get-close-playbook",
    "get-document-section",
    "get-event-block",
    "get-fiscal-calendar",
    "get-graphql-schema",
    "get-information-block",
    "get-period-close-status",
    "get-unmapped-elements",
    "list-agents",
    "list-event-blocks",
    "list-period-drafts",
    "live-financial-statement",
    "preview-event-block",
    "preview-schedule",
    "promote-obligations",
    "query-graphql",
    "recall",
    "search-documents",
    "suggest-mapping",
    "update-event-block",
    "update-taxonomy-block",
  }
)

ROWS = {
  "C": "full tool surface as shipped",
  "D": "full surface without the preview and review tools",
  "F": "the fixed close list",
  "O": "the stack's own author operator, its prompt, tools and model",
}

# The row that hands the task to the operator instead of driving the tools.
OPERATOR_ROW = "O"


def tools_for(row: str, surface: list[ToolDef]) -> list[ToolDef]:
  if row == "C":
    return surface
  if row == "D":
    return [t for t in surface if t.name not in CHECK_TOOLS]
  if row == "F":
    missing = CLOSE_LIST - {t.name for t in surface}
    if missing:
      raise SystemExit(f"close list names tools the server lacks: {sorted(missing)}")
    return [t for t in surface if t.name in CLOSE_LIST]
  raise SystemExit(f"unknown row {row!r} ({', '.join(ROWS)})")
