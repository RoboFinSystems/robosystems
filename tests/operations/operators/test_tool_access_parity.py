"""Both ToolAccess implementations must satisfy the protocol identically.

The bug this file exists for: `get_tool_instance` was implemented only on
`DirectToolAccess` while `adapters/api.py` constructs `HttpToolAccess`, so
every operator driving tools imperatively worked from the worker and raised
`AttributeError` on the API path. Nothing tested the two against each other,
so the divergence was invisible.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.operations.operators.operator_context import ToolAccess
from robosystems.operations.operators.tool_access import (
  DirectToolAccess,
  HttpToolAccess,
)

MODULE = "robosystems.operations.operators.tool_access"


class FakeTool:
  """Stands in for an MCP tool class — same shape the real ones expose."""

  def __init__(self, graph_client):
    self.client = graph_client

  def get_tool_definition(self) -> dict[str, Any]:
    return {
      "name": "get-unmapped-elements",
      "description": "test tool",
      "inputSchema": {"type": "object"},
    }

  async def execute(self, arguments: dict[str, Any]) -> Any:
    return {"executed_in_process": True, "arguments": arguments}


@pytest.mark.unit
class TestToolAccessParity:
  @pytest.mark.parametrize(
    "access_factory",
    [
      pytest.param(lambda: DirectToolAccess("kg_test"), id="direct"),
      pytest.param(lambda: HttpToolAccess("kg_test", read_only=False), id="http"),
    ],
  )
  def test_both_implementations_satisfy_the_protocol(self, access_factory):
    """The regression in one line: HttpToolAccess lacked get_tool_instance."""
    access = access_factory()

    assert isinstance(access, ToolAccess)
    for method in ("call_tool", "get_tool_schemas", "get_tool_instance"):
      assert hasattr(access, method), f"missing {method}"

  @pytest.mark.asyncio
  async def test_http_tool_handle_executes_over_the_mcp_surface(self):
    """The handle must route through call_tool, not run the tool in-process.

    In-process execution would work and would bypass both the read_only
    gating and the registrar dispatch that GraphMCPTools applies, so this
    asserts the indirection rather than merely asserting a result.
    """
    access = HttpToolAccess("kg_test", read_only=False)

    with patch.object(
      access, "call_tool", new=AsyncMock(return_value={"unmapped": []})
    ) as mock_call:
      handle = access.get_tool_instance(FakeTool)
      result = await handle.execute({"mapping_id": "map_1"})

    mock_call.assert_awaited_once_with(
      "get-unmapped-elements", {"mapping_id": "map_1"}, return_raw=True
    )
    assert result == {"unmapped": []}
    # Not the in-process path — FakeTool.execute was never reached.
    assert "executed_in_process" not in result

  @pytest.mark.asyncio
  async def test_http_handle_returns_a_dict_not_a_json_string(self):
    """`return_raw=True` is load-bearing, not stylistic.

    GraphMCPTools.call_tool returns a JSON *string* by default. Callers
    test outcomes with `"error" in result`; against a string that becomes
    substring matching, which passes on any payload containing the word —
    a wrong answer rather than a failure.
    """
    access = HttpToolAccess("kg_test", read_only=False)
    fake_tools = AsyncMock()
    fake_tools.call_tool = AsyncMock(return_value={"error": "boom"})
    access._tools = fake_tools

    handle = access.get_tool_instance(FakeTool)
    result = await handle.execute({})

    assert isinstance(result, dict)
    assert "error" in result
    assert fake_tools.call_tool.await_args.kwargs["return_raw"] is True

  @pytest.mark.asyncio
  async def test_direct_access_still_executes_in_process(self):
    """The worker path is unchanged — this fix must not move it."""
    access = DirectToolAccess("kg_test")

    tool = access.get_tool_instance(FakeTool)
    result = await tool.execute({"mapping_id": "map_1"})

    assert result["executed_in_process"] is True

  def test_direct_access_caches_instances_http_need_not(self):
    """Documents the one intentional behavioural difference.

    DirectToolAccess memoizes because the instance holds the tool; the HTTP
    handle is a stateless name binding, so identity carries no meaning.
    """
    direct = DirectToolAccess("kg_test")
    assert direct.get_tool_instance(FakeTool) is direct.get_tool_instance(FakeTool)

    http = HttpToolAccess("kg_test", read_only=False)
    a = http.get_tool_instance(FakeTool)
    b = http.get_tool_instance(FakeTool)
    assert a._tool_name == b._tool_name


@pytest.mark.unit
class TestWithoutTitles:
  """An operator's tool schemas drop Pydantic's ``title`` annotations and
  nothing else."""

  def test_annotations_go_and_a_field_named_title_stays(self):
    from robosystems.operations.operators.tool_access import without_titles

    schema = {
      "title": "CreateDocumentRequest",
      "type": "object",
      "properties": {
        "title": {"title": "Title", "type": "string", "description": "Shown"},
        "tags": {
          "title": "Tags",
          "type": "array",
          "items": {"title": "Tag", "type": "string"},
        },
      },
      "required": ["title"],
    }

    assert without_titles(schema) == {
      "type": "object",
      "properties": {
        "title": {"type": "string", "description": "Shown"},
        "tags": {"type": "array", "items": {"type": "string"}},
      },
      "required": ["title"],
    }

  def test_example_and_default_payloads_are_left_alone(self):
    from robosystems.operations.operators.tool_access import without_titles

    schema = {
      "type": "object",
      "examples": [{"title": "Q3 board memo"}],
      "properties": {
        "doc": {
          "title": "Doc",
          "default": {"title": "Untitled"},
          "anyOf": [{"title": "A", "type": "object"}, {"const": {"title": "x"}}],
        }
      },
      "$defs": {"title": {"title": "Title", "type": "string"}},
    }

    out = without_titles(schema)
    assert out["examples"] == [{"title": "Q3 board memo"}]
    assert out["properties"]["doc"] == {
      "default": {"title": "Untitled"},
      "anyOf": [{"type": "object"}, {"const": {"title": "x"}}],
    }
    assert out["$defs"] == {"title": {"type": "string"}}

  def test_a_field_named_like_a_data_key_is_still_a_schema(self):
    from robosystems.operations.operators.tool_access import without_titles

    schema = {"properties": {"default": {"title": "Default", "type": "boolean"}}}
    assert without_titles(schema) == {"properties": {"default": {"type": "boolean"}}}

  @pytest.mark.asyncio
  async def test_the_http_surface_serves_schemas_without_them(self):
    access = HttpToolAccess("kg_test", read_only=False)
    access._tools = MagicMock()
    access._tools.get_tool_definitions_as_dict.return_value = [
      {
        "name": "create-agent",
        "description": "d",
        "inputSchema": {
          "title": "CreateAgentRequest",
          "type": "object",
          "properties": {"name": {"title": "Name", "type": "string"}},
        },
      }
    ]

    schemas = await access.get_tool_schemas(["create-agent"])

    assert schemas[0]["inputSchema"] == {
      "type": "object",
      "properties": {"name": {"type": "string"}},
    }
