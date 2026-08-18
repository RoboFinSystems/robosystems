"""Event Handler MCP read tools.

Two hand-written read tools:

1. ``get-event-handler``    — fetch one handler by id.
2. ``list-event-handlers``  — list handlers with optional filters.

Write tools (``create-event-handler``, ``update-event-handler``,
``preview-event-block``) are auto-generated from their OperationSpec entries
in the registrar pipeline.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.operations.roboledger.reads.event_handler import (
  get_event_handler as ops_get_event_handler,
)
from robosystems.operations.roboledger.reads.event_handler import (
  list_event_handlers as ops_list_event_handlers,
)

from ._errors import database_failure


class GetEventHandlerTool:
  """Fetch a single event handler by id."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-event-handler",
      "description": """Fetch a single event handler rule by id.

**WHEN TO USE:**
- To inspect what a specific handler does before modifying it
- To verify whether a handler is active or approved
- To check the transaction_template DSL for a handler

**PARAMETERS:**
- id (required): EventHandler id (hdl_ prefixed ULID)

**RETURNS:**
EventHandlerResponse with match criteria, transaction_template, priority,
is_active, origin, and AI provenance fields.

**RELATED TOOLS:**
- list-event-handlers — browse handlers with filters
- preview-event-block — dry-run to see which handler fires for an event
- create-event-handler — define a new rule
- update-event-handler — patch or approve a handler""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "id": {
            "type": "string",
            "description": "EventHandler id (hdl_ prefixed)",
          },
        },
        "required": ["id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    handler_id = arguments["id"]

    try:
      with extensions_session(graph_id) as session:
        handler = ops_get_event_handler(session, handler_id)
        if handler is None:
          return {
            "error": "not_found",
            "message": f"EventHandler not found: {handler_id}",
          }
        return handler.model_dump(mode="json")
    except SQLAlchemyError as exc:
      return database_failure("get-event-handler", exc)
    except Exception as exc:
      logger.warning(f"get-event-handler failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


class ListEventHandlersTool:
  """List event handlers with optional filters."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-event-handlers",
      "description": """List event handler rules with optional filters.

**WHEN TO USE:**
- To see all handlers for a specific event type (e.g., 'invoice_issued')
- To find handlers awaiting approval (approved=false)
- To audit the active rule set before adding a new handler
- To check which event types currently have an active handler set

**PARAMETERS:**
- event_type (optional): Filter by event type (e.g., 'invoice_issued', 'bank_transaction')
- is_active (optional, default true): Include inactive handlers by passing false
- origin (optional): 'hub' | 'tenant'
- approved (optional): true = only approved; false = only unapproved AI suggestions
- limit (optional, default 50, max 1000)
- offset (optional, default 0)

**RETURNS:**
List of EventHandlerResponse objects ordered by priority descending then name.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "event_type": {"type": "string"},
          "is_active": {"type": "boolean", "default": True},
          "origin": {"type": "string", "enum": ["hub", "tenant"]},
          "approved": {"type": "boolean"},
          "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 50},
          "offset": {"type": "integer", "minimum": 0, "default": 0},
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    limit = int(arguments.get("limit", 50))
    offset = int(arguments.get("offset", 0))

    if not 1 <= limit <= 1000:
      return {
        "error": "invalid_arguments",
        "message": "limit must be between 1 and 1000",
      }

    is_active_raw = arguments.get("is_active")
    if is_active_raw is None:
      is_active = True
    elif isinstance(is_active_raw, str):
      is_active = is_active_raw.lower() != "false"
    else:
      is_active = bool(is_active_raw)

    approved_raw = arguments.get("approved")
    approved: bool | None = None
    if approved_raw is not None:
      approved = bool(approved_raw)

    try:
      with extensions_session(graph_id) as session:
        handlers = ops_list_event_handlers(
          session,
          event_type=arguments.get("event_type"),
          is_active=is_active,
          origin=arguments.get("origin"),
          approved=approved,
          limit=limit,
          offset=offset,
        )
        return {
          "handler_count": len(handlers),
          "handlers": [h.model_dump(mode="json") for h in handlers],
        }
    except SQLAlchemyError as exc:
      return database_failure("list-event-handlers", exc)
    except Exception as exc:
      logger.warning(f"list-event-handlers failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


__all__ = ["GetEventHandlerTool", "ListEventHandlersTool"]
