"""A tool's refusal reaches MCP clients as a failed call (``isError: true``).

External agents read ``isError`` to decide whether a call worked. The
in-house operator loop already treats ``{"error": ...}`` as a failure, so
both surfaces must agree.
"""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp import GraphMCPTools
from robosystems.routers.graphs.mcp.handlers import MCPHandler, tool_error_kind
from robosystems.routers.graphs.mcp.remote import _to_tool_result

GRAPH_ID = "kg01234567890abcdef"


def _repository():
  repo = MagicMock()
  repo.graph_id = GRAPH_ID
  return repo


def _user():
  user = MagicMock()
  user.id = "usr_test"
  return user


async def _handler(mcp_tools=None, *, factory=None) -> MCPHandler:
  client = AsyncMock()
  client.graph_id = GRAPH_ID
  client.user_id = "usr_test"
  with (
    patch(
      "robosystems.routers.graphs.mcp.handlers.create_graph_mcp_client",
      new_callable=AsyncMock,
      return_value=client,
    ),
    patch(
      "robosystems.middleware.mcp.tools.manager.resolve_schema_extensions",
      return_value=["roboledger"],
    ),
  ):
    handler = MCPHandler(_repository(), GRAPH_ID, _user())
    await handler._ensure_initialized()
  handler.mcp_tools = factory(client) if factory is not None else mcp_tools
  return handler


@pytest.mark.unit
@pytest.mark.asyncio
async def test_a_registrar_refusal_is_an_error_end_to_end():
  """The real registrar rejects bad arguments before touching a database."""
  handler = await _handler(
    factory=lambda client: GraphMCPTools(
      client, schema_extensions=["roboledger"], read_only=False
    )
  )
  with patch(
    "robosystems.middleware.mcp.tools.registrar.require_graph_extension_mcp",
    return_value=MagicMock(),
  ):
    result = await handler.call_tool("create-agent", {"bogus": 1})

  mcp = _to_tool_result(result)
  assert mcp["isError"] is True
  assert json.loads(mcp["content"][0]["text"])["error"] == "invalid_arguments"
  # A caller's mistake, not backend health: the breaker must not count it.
  assert tool_error_kind(result) == "constraint"


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
  "payload",
  [
    {"error": "closed_period", "message": "2026-03 is closed"},
    {"error": "not_found", "message": "no such event"},
    {"error": "invalid_arguments", "message": "validation failed"},
    {"error": "in_progress", "message": "retry later"},
    {"error": "not_initialized", "message": "ledger not provisioned"},
    {"error": "command_failed", "message": "close blocked"},
    {"error": "repository_write_forbidden", "message": "read-only"},
    {"error": "access_denied", "message": "no access"},
  ],
)
async def test_every_refusal_class_is_an_error(payload):
  tools = AsyncMock()
  tools.call_tool = AsyncMock(return_value=payload)
  handler = await _handler(tools)

  result = await handler.call_tool("create-agent", {})

  mcp = _to_tool_result(result)
  assert mcp["isError"] is True
  # The whole envelope stays readable, not just the code.
  assert json.loads(mcp["content"][0]["text"]) == payload


@pytest.mark.unit
@pytest.mark.asyncio
@pytest.mark.parametrize(
  "payload",
  [
    [{"label": "Entity", "count": 100}],
    {"outcome": "rejected", "blockers": ["sync_stale"]},
    {"id": "agent_1", "error": None},
  ],
)
async def test_a_result_without_an_error_code_is_not_an_error(payload):
  tools = AsyncMock()
  tools.call_tool = AsyncMock(return_value=payload)
  handler = await _handler(tools)

  result = await handler.call_tool("get-graph-schema", {})

  assert _to_tool_result(result)["isError"] is False
