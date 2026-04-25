"""Information Block MCP read tools for agents.

Hand-written reads that agents use to consume the molecular-layer
envelope for any registered block type. Two tools:

1. ``get-information-block`` — fetch one block envelope by id.
2. ``list-information-blocks`` — list blocks, optionally filtered by
   block_type + category.

These are the eventual unified replacement for the block-type-specific
read tools in ``schedule_tools.py`` (``list-schedule-structures``,
``get-schedule-facts``). Both surfaces ship in parallel during the
migration so callers can switch over without a breaking change.

The ``create-information-block`` **write** tool is NOT in this module —
it's auto-generated from the OperationSpec via
``build_tools_for_extension`` in the registrar pipeline.
"""

from __future__ import annotations

from typing import Any

from robosystems.db.extensions import LIBRARY_GRAPH_ID, extensions_session
from robosystems.logger import logger
from robosystems.operations.information_block import (
  get_information_block as ops_get_information_block,
)
from robosystems.operations.information_block import (
  list_information_blocks as ops_list_information_blocks,
)

# ────────────────────────────────────────────────────────────────────────────
# get-information-block
# ────────────────────────────────────────────────────────────────────────────


class GetInformationBlockTool:
  """Fetch a single Information Block envelope by id."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-information-block",
      "description": """Fetch a single Information Block envelope by id.

**WHEN TO USE:**
- To inspect an existing block (schedule, statement, disclosure, …) — full detail
- To see the block's atoms (elements, connections, facts) and typed mechanics
- Before drafting work that depends on the block's current state

**PARAMETERS:**
- id (required): The Information Block's structure id

**RETURNS:**
A typed envelope with:
- block_type, name, display_name, category
- information_model (concept + member arrangement)
- artifact (topic, mechanics — typed per block_type)
- elements, connections, facts (bundled atoms)
- rules, dimensions, fact_set, verification_results (populated as
  the rule engine + FactSet expand work progresses)

**RELATED TOOLS:**
- list-information-blocks — browse available blocks
- create-information-block — build a new block

Note: This is the generic reader. Block-type-specific tools like
list-schedule-structures and get-schedule-facts still ship but will
eventually be deprecated in favor of this pair.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "id": {
            "type": "string",
            "description": "Information Block (structure) id",
          },
        },
        "required": ["id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    block_id = arguments["id"]

    try:
      with extensions_session(graph_id) as session:
        envelope = ops_get_information_block(session, block_id)
        if envelope is None:
          return {
            "error": "not_found",
            "message": f"Information Block not found: {block_id}",
          }
        return envelope.model_dump(mode="json")
    except Exception as exc:
      logger.warning(f"get-information-block failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


# ────────────────────────────────────────────────────────────────────────────
# list-information-blocks
# ────────────────────────────────────────────────────────────────────────────


class ListInformationBlocksTool:
  """List Information Block envelopes with filters + pagination."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-information-blocks",
      "description": """List Information Block envelopes, optionally filtered.

**WHEN TO USE:**
- To discover what blocks exist for a graph
- To filter by block_type (e.g., all schedules, all statements)
- To browse by category (e.g., all Close-category blocks)

**PARAMETERS:**
- block_type (optional): Filter by block type id (e.g., 'schedule')
- category (optional): Filter by registry category (e.g., 'Close')
- limit (optional, default 50): Max results to return (1-1000)
- offset (optional, default 0): Pagination offset

**RETURNS:**
List of Information Block envelopes. Each envelope includes the full
atoms (elements, connections, facts) for the block — same shape as
get-information-block. Use limit/offset for paging when a category
has many blocks.

**RELATED TOOLS:**
- get-information-block — fetch a single block by id
- create-information-block — build a new block""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "block_type": {
            "type": "string",
            "description": "Filter to one block type id (e.g., 'schedule').",
          },
          "category": {
            "type": "string",
            "description": "Filter by registry category (e.g., 'Close').",
          },
          "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": 1000,
            "default": 50,
          },
          "offset": {
            "type": "integer",
            "minimum": 0,
            "default": 0,
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    block_type = arguments.get("block_type")
    category = arguments.get("category")
    limit = int(arguments.get("limit", 50))
    offset = int(arguments.get("offset", 0))

    # MCP inputSchema declares minimum/maximum bounds for limit + offset,
    # but the stdio server doesn't enforce them — some clients (and
    # direct HTTP callers) pass out-of-range values. Reassert the bounds
    # here so we match the declared contract instead of silently
    # clamping.
    if not 1 <= limit <= 1000:
      return {
        "error": "invalid_arguments",
        "message": "limit must be between 1 and 1000",
      }
    if offset < 0:
      return {
        "error": "invalid_arguments",
        "message": "offset must be >= 0",
      }

    try:
      with extensions_session(graph_id) as session:
        envelopes = ops_list_information_blocks(
          session,
          block_type=block_type,
          category=category,
          limit=limit,
          offset=offset,
          library_sentinel=(graph_id == LIBRARY_GRAPH_ID),
        )
        return {
          "block_count": len(envelopes),
          "blocks": [e.model_dump(mode="json") for e in envelopes],
        }
    except ValueError as exc:
      # Raised on unknown block_type — surface as an argument-level error.
      return {"error": "invalid_arguments", "message": str(exc)}
    except Exception as exc:
      logger.warning(f"list-information-blocks failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


__all__ = [
  "GetInformationBlockTool",
  "ListInformationBlocksTool",
]
