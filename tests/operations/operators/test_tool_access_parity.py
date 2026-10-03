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
  """An operator's tool schemas drop the titles that repeat a name, and
  nothing else."""

  def test_a_field_s_generated_title_goes_and_the_field_stays(self):
    from robosystems.operations.operators.tool_access import without_titles

    schema = {
      "title": "CreateDocumentRequest",
      "type": "object",
      "properties": {
        "title": {"title": "Title", "type": "string", "description": "Shown"},
        "period_start": {"title": "Period Start", "type": "string"},
        "tags": {
          "title": "Tags",
          "type": "array",
          "items": {
            "type": "object",
            "properties": {"event_id": {"title": "Event Id", "type": "string"}},
          },
        },
      },
      "required": ["title"],
      "$defs": {"PeriodSpec": {"title": "PeriodSpec", "type": "object"}},
    }

    assert without_titles(schema) == {
      # No name to compare the root with, so it stays.
      "title": "CreateDocumentRequest",
      "type": "object",
      "properties": {
        "title": {"type": "string", "description": "Shown"},
        "period_start": {"type": "string"},
        "tags": {
          "type": "array",
          "items": {
            "type": "object",
            "properties": {"event_id": {"type": "string"}},
          },
        },
      },
      "required": ["title"],
      "$defs": {"PeriodSpec": {"type": "object"}},
    }

  def test_a_title_that_says_something_is_kept(self):
    from robosystems.operations.operators.tool_access import without_titles

    schema = {
      "properties": {
        "payload": {
          "title": "Payload",
          "anyOf": [
            {"title": "payload when block_type is 'schedule'", "type": "object"},
            {"title": "payload when block_type is 'forecast'", "type": "object"},
          ],
        },
        "amount": {"title": "Amount in cents", "type": "integer"},
      }
    }

    out = without_titles(schema)
    assert "title" not in out["properties"]["payload"]
    assert [o["title"] for o in out["properties"]["payload"]["anyOf"]] == [
      "payload when block_type is 'schedule'",
      "payload when block_type is 'forecast'",
    ]
    assert out["properties"]["amount"]["title"] == "Amount in cents"

  def test_example_and_default_payloads_are_left_alone(self):
    from robosystems.operations.operators.tool_access import without_titles

    schema = {
      "type": "object",
      "examples": [{"properties": {"title": {"title": "Title"}}}],
      "properties": {
        "doc": {
          "title": "Doc",
          "default": {"properties": {"doc": {"title": "Doc"}}},
          "const": {"title": "x"},
        },
        "default": {"title": "Default", "type": "boolean"},
      },
    }

    out = without_titles(schema)
    assert out["examples"] == [{"properties": {"title": {"title": "Title"}}}]
    assert out["properties"]["doc"] == {
      "default": {"properties": {"doc": {"title": "Doc"}}},
      "const": {"title": "x"},
    }
    # A field named like a data key is still a schema.
    assert out["properties"]["default"] == {"type": "boolean"}

  def test_the_input_is_not_mutated(self):
    from robosystems.operations.operators.tool_access import without_titles

    schema = {"properties": {"name": {"title": "Name", "type": "string"}}}
    without_titles(schema)
    assert schema == {"properties": {"name": {"title": "Name", "type": "string"}}}

  def test_the_registrar_s_union_labels_survive_on_a_real_tool(self):
    """create-information-block tells the model to pick the anyOf option
    whose title matches its block_type, so those titles must reach it."""
    from robosystems.middleware.mcp.tools.registrar import build_tools_for_extension
    from robosystems.operations.operators.tool_access import without_titles

    client = MagicMock()
    client.graph_id = "kg1"
    tool = build_tools_for_extension("roboledger", client)["create-information-block"]
    definition = tool.get_tool_definition()

    def titles(node, found):
      if isinstance(node, dict):
        if isinstance(node.get("title"), str):
          found.append(node["title"])
        for value in node.values():
          titles(value, found)
      elif isinstance(node, list):
        for value in node:
          titles(value, found)
      return found

    before = [
      t for t in titles(definition["inputSchema"], []) if t.startswith("payload when")
    ]
    after = [
      t
      for t in titles(without_titles(definition["inputSchema"]), [])
      if t.startswith("payload when")
    ]
    assert before, "the registrar no longer labels the payload union"
    assert after == before

  @pytest.mark.asyncio
  async def test_the_http_surface_serves_schemas_without_them(self):
    access = HttpToolAccess("kg_test", read_only=False)
    access._tools = MagicMock()
    access._tools.get_tool_definitions_as_dict.return_value = [
      {
        "name": "create-agent",
        "description": "d",
        "inputSchema": {
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
