"""Information Block MCP read tools for agents.

Hand-written reads that agents use to consume the molecular-layer
envelope for any registered block type. Two tools:

1. ``get-information-block`` — fetch one block envelope by id.
2. ``list-information-blocks`` — list blocks, optionally filtered by
   block_type + category.

These are the only block reads: schedule envelopes surface here via
``block_type='schedule'`` rather than through block-type-specific tools.

The ``create-information-block`` **write** tool is NOT in this module —
it's auto-generated from the OperationSpec via
``build_tools_for_extension`` in the registrar pipeline.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import LIBRARY_GRAPH_ID, extensions_session
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.operations.information_block import (
  get_information_block as ops_get_information_block,
)
from robosystems.operations.information_block import (
  list_information_blocks as ops_list_information_blocks,
)

from ._errors import database_failure

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
- scenario_id (optional): A forecast block's structure id — binds that
  scenario's FactSet slice instead of actuals (statement envelopes show
  the latest computed forecast month; metric envelopes extend the series
  with the scenario's forward columns, labeled "(forecast)")
- series (optional): Render a statement block as its whole report-set
  time series — one column per period; combined with scenario_id the
  columns cross the actuals/forecast seam (forecast columns carry
  periods[].forecast = true). Non-statement block types ignore it
- series_history / series_forecast (optional): Window the series to its
  seam-adjacent columns — the last N actual columns and the first N
  forecast columns. Omitted = unbounded. Prefer a window on deep-history
  tenants: an unbounded series envelope grows with the ledger's age

**RETURNS:**
A typed envelope with:
- block_type, name, display_name, category
- information_model (concept + member arrangement)
- artifact (topic, mechanics — typed per block_type)
- elements, connections, facts (bundled atoms)
- rules, fact_set, verification_results (empty when the block has none)
- dimensions — always empty; the dimension catalog is not wired up yet, so
  an empty list here says nothing about the block

**RELATED TOOLS:**
- list-information-blocks — browse available blocks
- create-information-block — build a new block

**NOTES:** This is the generic reader for every block type, schedules
included.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "id": {
            "type": "string",
            "description": "Information Block (structure) id",
          },
          "scenario_id": {
            "type": "string",
            "description": (
              "Forecast block structure id — bind that scenario's "
              "FactSet slice instead of actuals."
            ),
          },
          "series": {
            "type": "boolean",
            "description": (
              "Statement blocks only: render the whole report-set time "
              "series (one column per period, actuals-preferred at the "
              "forecast seam)."
            ),
          },
          "series_history": {
            "type": "integer",
            "minimum": 0,
            "description": (
              "With series: keep only the last N actual columns "
              "(nearest the close boundary). Omitted = all history."
            ),
          },
          "series_forecast": {
            "type": "integer",
            "minimum": 0,
            "description": (
              "With series: keep only the first N forecast columns "
              "(nearest the seam). Omitted = the full horizon."
            ),
          },
        },
        "required": ["id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    block_id = arguments["id"]
    scenario_id = arguments.get("scenario_id")
    series = bool(arguments.get("series", False))
    series_history = arguments.get("series_history")
    series_forecast = arguments.get("series_forecast")

    try:
      with extensions_session(graph_id) as session:
        envelope = ops_get_information_block(
          session,
          block_id,
          scenario_id=scenario_id,
          series=series,
          series_history=int(series_history) if series_history is not None else None,
          series_forecast=int(series_forecast) if series_forecast is not None else None,
        )
        if envelope is None:
          return {
            "error": "not_found",
            "message": f"Information Block not found: {block_id}",
          }
        return envelope.model_dump(mode="json")
    except SQLAlchemyError as exc:
      return database_failure("get-information-block", exc)
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
- scenario_id (optional): A forecast block's structure id — each
  envelope binds that scenario's FactSet slice instead of actuals
  (list blocks with block_type='forecast' to discover scenarios)
- include_atoms (optional, default false): When false, returns a lean
  summary per block (id, type, name, display_name, category, taxonomy_id,
  taxonomy_name, disclosure_id, element_count, fact_count, rule_count).
  When true, returns the full envelope including elements, connections,
  facts, rules, dimensions, fact_set, verification_results, and view
  projections — same shape as get-information-block. Default is
  summary-only because the full envelope can run ~40 KB per block; a
  50-block list call with full atoms exceeds typical agent context.
  Use the default for browsing; fetch full atoms via
  get-information-block(id) for the specific blocks you need.

**RETURNS:**
- block_count: number of matching blocks in the page
- blocks: list of block summaries (or full envelopes when include_atoms=true)
- mode: "summary" | "full" — echoes which projection was returned

**RELATED TOOLS:**
- get-information-block — fetch a single block by id (always full envelope)
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
          "include_atoms": {
            "type": "boolean",
            "default": False,
            "description": (
              "Return full envelopes (elements + facts + rules + view) "
              "instead of the lean summary projection. Off by default to "
              "keep list calls compact."
            ),
          },
          "scenario_id": {
            "type": "string",
            "description": (
              "Forecast block structure id — bind each envelope to that "
              "scenario's FactSet slice instead of actuals."
            ),
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    block_type = arguments.get("block_type")
    category = arguments.get("category")
    limit = int(arguments.get("limit", 50))
    offset = int(arguments.get("offset", 0))
    include_atoms = bool(arguments.get("include_atoms", False))
    scenario_id = arguments.get("scenario_id")

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
          scenario_id=scenario_id,
        )
        if include_atoms:
          blocks = [e.model_dump(mode="json") for e in envelopes]
          mode = "full"
        else:
          blocks = [_summarize_information_block(e) for e in envelopes]
          mode = "summary"
        return {
          "mode": mode,
          "block_count": len(blocks),
          "blocks": blocks,
        }
    except ValueError as exc:
      # Raised on unknown block_type — surface as an argument-level error.
      return {"error": "invalid_arguments", "message": str(exc)}
    except SQLAlchemyError as exc:
      return database_failure("list-information-blocks", exc)
    except Exception as exc:
      logger.warning(f"list-information-blocks failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


def _summarize_information_block(envelope) -> dict[str, Any]:
  """Project a full Information Block envelope to a lean summary shape.

  Drops the heavy atoms (elements, connections, facts, rules, dimensions,
  fact_set, verification_results) and the view-projection block, replacing
  them with counts so the caller can decide whether to fetch the full
  envelope via ``get-information-block``. Identity, type, category, and
  taxonomy linkage are preserved.
  """
  return {
    "id": envelope.id,
    "block_type": envelope.block_type,
    "name": envelope.name,
    "display_name": envelope.display_name,
    "category": envelope.category,
    "taxonomy_id": envelope.taxonomy_id,
    "taxonomy_name": envelope.taxonomy_name,
    "disclosure_id": envelope.disclosure_id,
    "element_count": len(envelope.elements or []),
    "connection_count": len(envelope.connections or []),
    "fact_count": len(envelope.facts or []),
    "rule_count": len(envelope.rules or []),
    "has_fact_set": envelope.fact_set is not None,
    "verification_result_count": len(envelope.verification_results or []),
  }


__all__ = [
  "GetInformationBlockTool",
  "ListInformationBlocksTool",
]
