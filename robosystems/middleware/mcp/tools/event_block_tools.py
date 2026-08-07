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
- Post-sync reconciliation: is_reconciling_item=true lists reconciling
  items — committed events whose upstream payload changed after posting,
  so the local GL no longer mirrors the source. Each awaits an explicit
  disposition (restate vs catch-up). The stashed incoming payload is in
  the event's metadata.drift_payload (fetch via get-event-block or
  include_metadata=true)

**PARAMETERS:**
- event_type (optional): e.g., 'invoice_issued', 'contract_signed', 'bank_transaction'
- event_category (optional): 'sales' | 'purchase' | 'financing' | 'payroll' | 'treasury' | 'adjustment' | 'recognition' | 'other'
- status (optional): 'captured' | 'classified' | 'committed' | 'pending' | 'fulfilled' | 'voided' | 'superseded'
- agent_id (optional): Filter to a specific counterparty
- source (optional): 'manual' | 'schedule' | 'system', a connected provider name, or a registered external source_name
- is_reconciling_item (optional): true → only reconciling items (the
  reconciliation worklist); false → exclude them; omit for all
- limit (optional, default 50, max 1000)
- offset (optional, default 0)
- include_metadata (optional, default false): When false, returns a lean
  per-event summary (id, event_type, event_category, status, occurred_at,
  source, agent_id, amount, currency, description, has_discharge_link,
  is_reconciling_item).
  When true, includes the full event_block metadata blob — `entries`,
  `qb_*` fields, connection_id, etc. — same shape as get-event-block.
  Default is summary-only because bulk metadata across hundreds of
  events can exceed agent context (1000 events with full metadata
  ≈ 150 KB / 4600 lines). Fetch full metadata via get-event-block(id)
  for specific events.

**RETURNS:**
- event_count: number of matching events in the page
- mode: "summary" | "full"
- events: list of event summaries (or full envelopes when include_metadata=true)
  ordered by occurred_at descending""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "event_type": {"type": "string"},
          "event_category": {"type": "string"},
          "status": {"type": "string"},
          "agent_id": {"type": "string"},
          "source": {"type": "string"},
          "is_reconciling_item": {"type": "boolean"},
          "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 50},
          "offset": {"type": "integer", "minimum": 0, "default": 0},
          "include_metadata": {
            "type": "boolean",
            "default": False,
            "description": (
              "Return full event envelopes (including the per-event "
              "metadata blob) instead of the lean summary projection. "
              "Off by default to keep list calls compact."
            ),
          },
        },
        "required": [],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    limit = int(arguments.get("limit", 50))
    offset = int(arguments.get("offset", 0))
    include_metadata = bool(arguments.get("include_metadata", False))

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
          is_reconciling_item=arguments.get("is_reconciling_item"),
          limit=limit,
          offset=offset,
        )
        if include_metadata:
          events = [e.model_dump(mode="json") for e in envelopes]
          mode = "full"
        else:
          events = [_summarize_event_block(e) for e in envelopes]
          mode = "summary"
        return {
          "mode": mode,
          "event_count": len(events),
          "events": events,
        }
    except Exception as exc:
      logger.warning(f"list-event-blocks failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


def _summarize_event_block(envelope) -> dict[str, Any]:
  """Project a full Event Block envelope to a lean summary shape.

  Drops the per-event metadata blob (which carries the full QB
  transaction payload, including all entries + line_items + every
  `qb_*` field) and replaces it with surface signals: amount, currency,
  description, and a `has_discharge_link` flag derived from
  ``discharges_event_id``. Identity, type, status, and occurrence info
  are preserved.
  """
  occurred_at = envelope.occurred_at
  return {
    "id": envelope.id,
    "event_type": envelope.event_type,
    "event_category": envelope.event_category,
    "event_class": envelope.event_class,
    "status": envelope.status,
    "source": envelope.source,
    "occurred_at": occurred_at.isoformat() if occurred_at else None,
    "agent_id": envelope.agent_id,
    "external_id": envelope.external_id,
    "amount": envelope.amount,
    "currency": envelope.currency,
    "description": envelope.description,
    "has_discharge_link": envelope.discharges_event_id is not None,
    "has_obligation_link": envelope.obligated_by_event_id is not None,
    "is_reconciling_item": envelope.is_reconciling_item,
  }


__all__ = ["GetEventBlockTool", "ListEventBlocksTool"]
