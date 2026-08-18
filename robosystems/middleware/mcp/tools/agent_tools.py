"""Agent MCP read tools for agents.

Three hand-written read tools:

1. ``get-agent``       — fetch one agent by id.
2. ``list-agents``     — list agents with optional filters.
3. ``agent-activity``  — agent + recent events + recent transactions.

Write tools (``create-agent``, ``update-agent``) are auto-generated from
their OperationSpec entries in the registrar pipeline.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy.exc import SQLAlchemyError

from robosystems.db.extensions import extensions_session
from robosystems.logger import logger
from robosystems.middleware.operations import run_off_loop
from robosystems.operations.roboledger.reads.agent import (
  get_agent as ops_get_agent,
)
from robosystems.operations.roboledger.reads.agent import (
  get_agent_activity as ops_get_agent_activity,
)
from robosystems.operations.roboledger.reads.agent import (
  list_agents as ops_list_agents,
)

from ._errors import database_failure


class GetAgentTool:
  """Fetch a single agent (counterparty) by id."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-agent",
      "description": """Fetch a single counterparty (agent) by id.

**WHEN TO USE:**
- To look up a customer, vendor, or employee record
- To check an agent's contact info and source linkage before creating an event
- To verify whether a counterparty exists before assigning agent_id on an event

**PARAMETERS:**
- id (required): Agent id (agt_ prefixed ULID)

**RETURNS:**
An AgentResponse with id, agent_type, name, legal_name, contact fields,
source/external_id provenance, and is_active status.

**RELATED TOOLS:**
- list-agents — browse agents with filters
- agent-activity — events and transactions for an agent
- create-event-block — record a business event with this agent as counterparty""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "id": {
            "type": "string",
            "description": "Agent id (agt_ prefixed)",
          },
        },
        "required": ["id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    agent_id = arguments["id"]

    try:
      with extensions_session(graph_id) as session:
        agent = ops_get_agent(session, agent_id)
        if agent is None:
          return {"error": "not_found", "message": f"Agent not found: {agent_id}"}
        return agent.model_dump(mode="json")
    except SQLAlchemyError as exc:
      return database_failure("get-agent", exc)
    except Exception as exc:
      logger.warning(f"get-agent failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


class ListAgentsTool:
  """List agents with optional filters."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "list-agents",
      "description": """List counterparty agents with optional filters.

**WHEN TO USE:**
- To find all customers or vendors
- To search for agents from a specific source (quickbooks, xero, plaid)
- To list inactive agents when reviewing counterparty records

**PARAMETERS:**
- agent_type (optional): 'customer' | 'vendor' | 'employee' | 'owner' | 'supplier' | 'government' | 'lender' | 'self' | 'other'
- source (optional): 'quickbooks' | 'xero' | 'plaid' | 'native'
- is_active (optional, default true): pass false to include inactive agents
- limit (optional, default 50, max 1000)
- offset (optional, default 0)

**RETURNS:**
List of AgentResponse objects ordered by name.""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "agent_type": {"type": "string"},
          "source": {"type": "string"},
          "is_active": {"type": "boolean", "default": True},
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
    if offset < 0:
      return {"error": "invalid_arguments", "message": "offset must be >= 0"}

    is_active_raw = arguments.get("is_active")
    if is_active_raw is None:
      is_active = True
    elif isinstance(is_active_raw, str):
      is_active = is_active_raw.lower() != "false"
    else:
      is_active = bool(is_active_raw)

    try:
      with extensions_session(graph_id) as session:
        agents = ops_list_agents(
          session,
          agent_type=arguments.get("agent_type"),
          source=arguments.get("source"),
          is_active=is_active,
          limit=limit,
          offset=offset,
        )
        return {
          "agent_count": len(agents),
          "agents": [a.model_dump(mode="json") for a in agents],
        }
    except SQLAlchemyError as exc:
      return database_failure("list-agents", exc)
    except Exception as exc:
      logger.warning(f"list-agents failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


class AgentActivityTool:
  """Agent + recent events + recent transactions."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "agent-activity",
      "description": """Fetch recent events and transactions for a counterparty.

**WHEN TO USE:**
- To review a customer's or vendor's event history
- To answer "what has happened with this counterparty?"
- To find all transactions triggered by events from this agent
- AR/AP analysis starting from a specific counterparty

**PARAMETERS:**
- id (required): Agent id (agt_ prefixed ULID)
- limit (optional, default 100, max 1000): max events and transactions returned

**RETURNS:**
- agent: full AgentResponse
- recent_events: list of EventBlockEnvelopes for this agent (desc by occurred_at)
- recent_transactions: list of transactions triggered by those events
- event_count: total event count for this agent
- transaction_count: total transaction count""",
      "inputSchema": {
        "type": "object",
        "properties": {
          "id": {
            "type": "string",
            "description": "Agent id (agt_ prefixed)",
          },
          "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
        },
        "required": ["id"],
      },
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return await run_off_loop(self._execute_sync, arguments)

  def _execute_sync(self, arguments: dict[str, Any]) -> Any:
    graph_id = self.client.graph_id
    agent_id = arguments["id"]
    limit = int(arguments.get("limit", 100))

    if not 1 <= limit <= 1000:
      return {
        "error": "invalid_arguments",
        "message": "limit must be between 1 and 1000",
      }

    try:
      with extensions_session(graph_id) as session:
        activity = ops_get_agent_activity(session, agent_id, limit=limit)
        if activity is None:
          return {"error": "not_found", "message": f"Agent not found: {agent_id}"}
        return activity.model_dump(mode="json")
    except SQLAlchemyError as exc:
      return database_failure("agent-activity", exc)
    except Exception as exc:
      logger.warning(f"agent-activity failed: {exc}")
      return {"error": "command_failed", "message": str(exc)}


__all__ = ["AgentActivityTool", "GetAgentTool", "ListAgentsTool"]
