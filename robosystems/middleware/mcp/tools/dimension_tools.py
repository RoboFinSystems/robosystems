"""The management dimension catalog read; its writes are registrar operations."""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.operations.roboledger.reads.dimensions import list_dimensions

from ._errors import database_failure


class ListDimensionsTool:
  """The members of the graph's management dimensions."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-dimensions",
      "description": """The graph's management dimensions — departments, classes, locations, projects — and their members, the tags a ledger line can carry.

**WHEN TO USE:**
- "What departments do we have?", "show the P&L by location": find the axis and its members first
- Before tagging a journal line (`dimension_ids` on a line of a `journal_entry_recorded` event, or `line_dimensions` on `update-journal-entry`): a line takes a member's id, one per axis
- Before `create-dimension`, to check the member is not there already

**PARAMETERS:**
- dimension_type (optional): one axis, e.g. `department`; omit for all
- include_inactive (optional, default false): also list retired members, which stay on the lines that carry them

**RETURNS:**
`dimensions`: each member's `id`, `dimension_type`, `name`, `value`, `source` (`native` or `quickbooks`), `is_active` and `line_count`, by axis then name. `types`: the axes the graph has members on. An untagged line reads as its axis's Unallocated.

**RELATED TOOLS:**
- live-financial-statement — `dimension: {type}` for an income statement with a column per member
- create-dimension / update-dimension — add, rename or retire a member""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "dimension_type": {
            "type": "string",
            "description": "One axis: department, class, location, project, or another.",
          },
          "include_inactive": {"type": "boolean", "default": False},
        },
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    try:
      with extensions_session(self.client.graph_id) as session:
        return list_dimensions(
          session,
          dimension_type=arguments.get("dimension_type") or None,
          include_inactive=bool(arguments.get("include_inactive", False)),
        ).model_dump(mode="json")
    except SQLAlchemyError as exc:
      return database_failure("list-dimensions", exc)
    except Exception as exc:
      logger.warning(f"list-dimensions failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


__all__ = ["ListDimensionsTool"]
