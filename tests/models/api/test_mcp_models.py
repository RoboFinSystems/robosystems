"""Unit tests for MCP API models."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.mcp import MCPToolCall


@pytest.mark.unit
class TestMCPToolCall:
  def test_valid_tool_call(self):
    model = MCPToolCall(
      name="read-graph-cypher",
      arguments={"query": "MATCH (n) RETURN n LIMIT 10"},
    )
    assert model.name == "read-graph-cypher"
    assert model.arguments["query"] == "MATCH (n) RETURN n LIMIT 10"

  def test_empty_arguments(self):
    model = MCPToolCall(name="get-graph-schema")
    assert model.arguments == {}

  def test_name_required(self):
    with pytest.raises(ValidationError):
      MCPToolCall()  # type: ignore[call-arg]

  def test_name_min_length(self):
    with pytest.raises(ValidationError) as exc_info:
      MCPToolCall(name="")
    errors = exc_info.value.errors()
    assert any(e["loc"] == ("name",) for e in errors)
