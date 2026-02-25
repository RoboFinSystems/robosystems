"""Unit tests for MCP API models."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.mcp import (
  MCPQueryRequest,
  MCPQueryResponse,
  MCPSchemaResponse,
  MCPToolCall,
  MCPToolResult,
  MCPToolsResponse,
)


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


@pytest.mark.unit
class TestMCPQueryRequest:
  def test_valid_query(self):
    model = MCPQueryRequest(query="MATCH (n) RETURN n LIMIT 10")
    assert model.query == "MATCH (n) RETURN n LIMIT 10"
    assert model.params == {}
    assert model.timeout_override is None

  def test_query_with_params(self):
    model = MCPQueryRequest(
      query="MATCH (n:Entity) WHERE n.name = $name RETURN n",
      params={"name": "Apple"},
    )
    assert model.params["name"] == "Apple"

  def test_query_with_timeout(self):
    model = MCPQueryRequest(
      query="MATCH (n) RETURN n",
      timeout_override=120,
    )
    assert model.timeout_override == 120

  def test_query_required(self):
    with pytest.raises(ValidationError):
      MCPQueryRequest()  # type: ignore[call-arg]

  def test_query_min_length(self):
    with pytest.raises(ValidationError):
      MCPQueryRequest(query="")

  def test_query_max_length(self):
    with pytest.raises(ValidationError):
      MCPQueryRequest(query="x" * 50001)

  def test_timeout_minimum(self):
    model = MCPQueryRequest(query="MATCH (n) RETURN n", timeout_override=1)
    assert model.timeout_override == 1

  def test_timeout_below_minimum(self):
    with pytest.raises(ValidationError):
      MCPQueryRequest(query="MATCH (n) RETURN n", timeout_override=0)

  def test_timeout_maximum(self):
    model = MCPQueryRequest(query="MATCH (n) RETURN n", timeout_override=300)
    assert model.timeout_override == 300

  def test_timeout_above_maximum(self):
    with pytest.raises(ValidationError):
      MCPQueryRequest(query="MATCH (n) RETURN n", timeout_override=301)


@pytest.mark.unit
class TestMCPToolsResponse:
  def test_valid_response(self):
    model = MCPToolsResponse(
      tools=[
        {
          "name": "read-graph-cypher",
          "description": "Execute Cypher query",
          "inputSchema": {"type": "object"},
        }
      ]
    )
    assert len(model.tools) == 1

  def test_empty_tools(self):
    model = MCPToolsResponse(tools=[])
    assert model.tools == []


@pytest.mark.unit
class TestMCPToolResult:
  def test_valid_result(self):
    model = MCPToolResult(
      result={
        "content": [{"type": "text", "text": "Found 5 entities"}],
        "rows_returned": 5,
        "execution_time_ms": 87,
      }
    )
    assert model.result["rows_returned"] == 5


@pytest.mark.unit
class TestMCPSchemaResponse:
  def test_valid_schema_response(self):
    model = MCPSchemaResponse(
      **{
        "schema": [
          {
            "label": "Entity",
            "properties": [
              {"name": "cik", "type": "STRING"},
              {"name": "name", "type": "STRING"},
            ],
          }
        ]
      }
    )
    assert len(model.schema_data) == 1
    assert model.schema_data[0]["label"] == "Entity"

  def test_empty_schema(self):
    model = MCPSchemaResponse(**{"schema": []})
    assert model.schema_data == []

  def test_alias_field(self):
    """Test that schema_data uses alias 'schema'."""
    model = MCPSchemaResponse(**{"schema": [{"label": "Test"}]})
    dumped = model.model_dump(by_alias=True)
    assert "schema" in dumped


@pytest.mark.unit
class TestMCPQueryResponse:
  def test_valid_response(self):
    model = MCPQueryResponse(
      results=[
        {"ticker": "AAPL", "name": "Apple Inc."},
        {"ticker": "MSFT", "name": "Microsoft"},
      ]
    )
    assert len(model.results) == 2

  def test_empty_results(self):
    model = MCPQueryResponse(results=[])
    assert model.results == []
