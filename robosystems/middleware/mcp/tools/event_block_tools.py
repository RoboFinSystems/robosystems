"""Event Block MCP read tools for agents.

Two hand-written read tools:

1. ``get-event-block``   — fetch one event envelope by id.
2. ``list-event-blocks`` — list events with optional filters.

The write tools (``create-event-block``, ``update-event-block``) are
auto-generated from their OperationSpec entries in the registrar pipeline.
"""

from __future__ import annotations

from typing import Any

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.operations.roboledger.reads.event_block import (
  get_event_block as ops_get_event_block,
)
from robosystems.operations.roboledger.reads.event_block import (
  list_event_blocks as ops_list_event_blocks,
)


class GetEventBlockTool:
  """Fetch a single Event Block envelope by id."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-event-block",
      "description": """Fetch a single Event Block envelope by id.

**WHEN TO USE:**
- To inspect an existing business event (invoice, contract, bank transaction, …)
- To check an event's current status and audit chain
- Before updating or voiding an event

**PARAMETERS:**
- id (required): The event id (evt_ prefixed ULID)

**RETURNS:**
An EventBlockEnvelope with:
- event_type, event_category, status
- REA primitives: agent_id, resource_type
- Occurrence: occurred_at, effective_at
- Provenance: source, external_id
- Economic value: amount, currency
- metadata (event-type-specific payload)
- dimension_ids, correction chain (replaced_by_event_id, replaces_event_id)

**RELATED TOOLS:**
- list-event-blocks — browse events with filters
- create-event-block — record a new business event
- update-event-block — transition status or correct fields""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "id": {
            "type": "string",
            "description": "Event Block id (evt_ prefixed)",
          },
        },
        "required": ["id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    event_id = arguments["id"]

    try:
      with extensions_session(graph_id) as session:
        envelope = ops_get_event_block(session, event_id)
        if envelope is None:
          return {"error": "not_found", "message": f"Event Block not found: {event_id}"}
        return envelope.model_dump(mode="json")
    except Exception as exc:
      logger.warning(f"get-event-block failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


class ListEventBlocksTool:
  """List Event Block envelopes with optional filters."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-event-blocks",
      "description": """List Event Block envelopes with optional filters.

**WHEN TO USE:**
- To find events of a specific type (e.g., all invoice_issued events)
- To review events by status (e.g., all captured events awaiting classification)
- To see a counterparty's event history (filter by agent_id)
- To audit events from a specific source (quickbooks, plaid, etc.)
- Close workflow: find unposted events (status=captured or committed)

**PARAMETERS:**
- event_type (optional): e.g., 'invoice_issued', 'contract_signed', 'bank_transaction'
- event_category (optional): 'sales' | 'purchase' | 'financing' | 'payroll' | 'treasury' | 'adjustment' | 'recognition' | 'other'
- status (optional): 'captured' | 'classified' | 'committed' | 'pending' | 'fulfilled' | 'voided' | 'superseded'
- agent_id (optional): Filter to a specific counterparty
- source (optional): 'quickbooks' | 'xero' | 'plaid' | 'native' | 'scheduled' | ...
- limit (optional, default 50, max 1000)
- offset (optional, default 0)

**RETURNS:**
List of EventBlockEnvelopes ordered by occurred_at descending.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "event_type": {"type": "string"},
          "event_category": {"type": "string"},
          "status": {"type": "string"},
          "agent_id": {"type": "string"},
          "source": {"type": "string"},
          "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 50},
          "offset": {"type": "integer", "minimum": 0, "default": 0},
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    limit = int(arguments.get("limit", 50))
    offset = int(arguments.get("offset", 0))

    if not 1 <= limit <= 1000:
      return {
        "error": "invalid_arguments",
        "message": "limit must be between 1 and 1000",
      }
    if offset < 0:
      return {"error": "invalid_arguments", "message": "offset must be >= 0"}

    try:
      with extensions_session(graph_id) as session:
        envelopes = ops_list_event_blocks(
          session,
          event_type=arguments.get("event_type"),
          event_category=arguments.get("event_category"),
          status=arguments.get("status"),
          agent_id=arguments.get("agent_id"),
          source=arguments.get("source"),
          limit=limit,
          offset=offset,
        )
        return {
          "event_count": len(envelopes),
          "events": [e.model_dump(mode="json") for e in envelopes],
        }
    except Exception as exc:
      logger.warning(f"list-event-blocks failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


__all__ = ["GetEventBlockTool", "ListEventBlocksTool"]
