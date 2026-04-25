"""Tests for GraphQL MCP tools (get-graphql-schema, query-graphql).

Mocks are placed at the ``_ensure_gql_schema`` boundary so the tool logic
(parse, reject, complexity, context building, caching) is exercised without
a live DB or real Strawberry schema.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

MODULE = "robosystems.middleware.mcp.tools.graphql_tool"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_schema_cache():
  """Reset the process-lifetime SDL/introspection cache between tests."""
  import robosystems.middleware.mcp.tools.graphql_tool as gt

  gt._SCHEMA_CACHE.clear()
  yield
  gt._SCHEMA_CACHE.clear()


@pytest.fixture(autouse=True)
def reset_gql_schema():
  """Reset the lazy gql_schema reference so each test starts clean."""
  import robosystems.middleware.mcp.tools.graphql_tool as gt

  original = gt.gql_schema
  gt.gql_schema = None
  yield
  gt.gql_schema = original


@pytest.fixture
def mock_client():
  c = MagicMock()
  c.graph_id = "kgtest123"
  c.user_id = "usr_test"
  return c


@pytest.fixture
def mock_user():
  u = MagicMock()
  u.id = "usr_test"
  return u


# ---------------------------------------------------------------------------
# GraphqlSchemaTool
# ---------------------------------------------------------------------------


class TestGraphqlSchemaTool:
  def _make_tool(self, mock_client):
    from robosystems.middleware.mcp.tools.graphql_tool import GraphqlSchemaTool

    return GraphqlSchemaTool(mock_client)

  def _fake_schema(self, sdl: str = "type Query { hello: String }"):
    """Create a MagicMock whose str() returns `sdl`."""
    s = MagicMock()
    s.__str__.return_value = sdl
    return s

  def test_tool_definition(self, mock_client):
    tool = self._make_tool(mock_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "get-graphql-schema"
    assert "format" in defn["inputSchema"]["properties"]
    assert "sdl" in defn["inputSchema"]["properties"]["format"]["enum"]
    assert "introspection" in defn["inputSchema"]["properties"]["format"]["enum"]

  @pytest.mark.asyncio
  async def test_returns_sdl_by_default(self, mock_client):
    tool = self._make_tool(mock_client)
    fake = self._fake_schema()

    with patch(f"{MODULE}._ensure_gql_schema", return_value=fake):
      result = await tool.execute({})

    assert result["schema"] == "type Query { hello: String }"

  @pytest.mark.asyncio
  async def test_returns_sdl_when_format_explicit(self, mock_client):
    tool = self._make_tool(mock_client)
    fake = self._fake_schema("type Query { entity: Entity }")

    with patch(f"{MODULE}._ensure_gql_schema", return_value=fake):
      result = await tool.execute({"format": "sdl"})

    assert "schema" in result
    assert "Query" in result["schema"]

  @pytest.mark.asyncio
  async def test_returns_introspection_json(self, mock_client):
    tool = self._make_tool(mock_client)
    fake = MagicMock()
    fake_exec_result = MagicMock()
    fake_exec_result.data = {"__schema": {"types": []}}
    fake_exec_result.errors = None
    fake.execute_sync = MagicMock(return_value=fake_exec_result)

    with patch(f"{MODULE}._ensure_gql_schema", return_value=fake):
      result = await tool.execute({"format": "introspection"})

    assert "schema" in result
    parsed = json.loads(result["schema"])
    assert "__schema" in parsed

  @pytest.mark.asyncio
  async def test_sdl_cached_on_second_call(self, mock_client):
    tool = self._make_tool(mock_client)
    fake = self._fake_schema()

    with patch(f"{MODULE}._ensure_gql_schema", return_value=fake) as mock_get:
      await tool.execute({})
      await tool.execute({})

    # _ensure_gql_schema (and thus str(schema)) called only once
    assert mock_get.call_count == 1

  @pytest.mark.asyncio
  async def test_invalid_format_returns_error(self, mock_client):
    tool = self._make_tool(mock_client)
    # No schema needed — format validation happens before schema access
    result = await tool.execute({"format": "yaml"})
    assert result["error"] == "invalid_argument"


# ---------------------------------------------------------------------------
# GraphqlQueryTool
# ---------------------------------------------------------------------------


class TestGraphqlQueryTool:
  def _make_tool(self, mock_client, schema_extensions=("roboledger",)):
    from robosystems.middleware.mcp.tools.graphql_tool import GraphqlQueryTool

    return GraphqlQueryTool(mock_client, schema_extensions=schema_extensions)

  def _fake_schema_with_result(self, data=None, errors=None):
    """Return (fake_schema, fake_execute_result) pair."""
    fake_result = MagicMock()
    fake_result.data = data
    fake_result.errors = errors
    fake_schema = MagicMock()
    fake_schema.execute = AsyncMock(return_value=fake_result)
    return fake_schema, fake_result

  def test_tool_definition(self, mock_client):
    tool = self._make_tool(mock_client)
    defn = tool.get_tool_definition()
    assert defn["name"] == "query-graphql"
    assert "query" in defn["inputSchema"]["required"]
    assert "variables" in defn["inputSchema"]["properties"]
    assert "operationName" in defn["inputSchema"]["properties"]

  @pytest.mark.asyncio
  async def test_empty_query_returns_error(self, mock_client):
    tool = self._make_tool(mock_client)
    result = await tool.execute({"query": ""})
    assert result["error"] == "invalid_query"

  @pytest.mark.asyncio
  async def test_mutation_rejected(self, mock_client):
    tool = self._make_tool(mock_client)
    result = await tool.execute({"query": "mutation { createFoo { id } }"})
    assert result["error"] == "read_only_violation"
    assert "mutation" in result["message"].lower()

  @pytest.mark.asyncio
  async def test_subscription_rejected(self, mock_client):
    tool = self._make_tool(mock_client)
    result = await tool.execute({"query": "subscription { onFoo { id } }"})
    assert result["error"] == "read_only_violation"
    assert "subscription" in result["message"].lower()

  @pytest.mark.asyncio
  async def test_syntax_error_returns_parse_error(self, mock_client):
    tool = self._make_tool(mock_client)
    result = await tool.execute({"query": "{ { { invalid"})
    assert result["error"] == "parse_error"

  @pytest.mark.asyncio
  async def test_depth_limit_exceeded(self, mock_client):
    tool = self._make_tool(mock_client)
    # Build a query 12 levels deep (exceeds limit of 10)
    deep = "{ a" + " { b" * 11 + " }" * 11 + " }"
    result = await tool.execute({"query": deep})
    assert result["error"] == "query_too_complex"
    assert "depth" in result["message"].lower()

  @pytest.mark.asyncio
  async def test_field_limit_exceeded(self, mock_client):
    tool = self._make_tool(mock_client)
    # Build a flat query with 201 fields (exceeds limit of 200)
    fields = " ".join(f"f{i}" for i in range(201))
    wide = "{ " + fields + " }"
    result = await tool.execute({"query": wide})
    assert result["error"] == "query_too_complex"
    assert "field" in result["message"].lower()

  @pytest.mark.asyncio
  async def test_alias_limit_exceeded(self, mock_client):
    tool = self._make_tool(mock_client)
    # 21 aliased fields (exceeds limit of 20)
    fields = " ".join(f"a{i}: f{i}" for i in range(21))
    aliased = "{ " + fields + " }"
    result = await tool.execute({"query": aliased})
    assert result["error"] == "query_too_complex"
    assert "alias" in result["message"].lower()

  @pytest.mark.asyncio
  async def test_valid_query_returns_data(self, mock_client, mock_user):
    tool = self._make_tool(mock_client)
    fake_schema, _ = self._fake_schema_with_result(
      data={"fiscalCalendar": {"closedThrough": "2026-03"}}
    )

    with (
      patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema),
      patch.object(tool, "_fetch_user", return_value=mock_user),
    ):
      result = await tool.execute({"query": "{ fiscalCalendar { closedThrough } }"})

    assert result["data"]["fiscalCalendar"]["closedThrough"] == "2026-03"
    assert "errors" not in result

  @pytest.mark.asyncio
  async def test_graphql_errors_forwarded(self, mock_client, mock_user):
    tool = self._make_tool(mock_client)

    gql_err = MagicMock()
    gql_err.message = "Not authenticated"
    gql_err.extensions = {"code": "UNAUTHENTICATED"}

    fake_schema, _ = self._fake_schema_with_result(data=None, errors=[gql_err])

    with (
      patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema),
      patch.object(tool, "_fetch_user", return_value=mock_user),
    ):
      result = await tool.execute({"query": "{ entity { id } }"})

    assert "errors" in result
    assert result["errors"][0]["message"] == "Not authenticated"
    assert result["errors"][0]["extensions"]["code"] == "UNAUTHENTICATED"

  @pytest.mark.asyncio
  async def test_variables_threaded_through(self, mock_client, mock_user):
    tool = self._make_tool(mock_client)
    fake_schema, _ = self._fake_schema_with_result(data={"node": {"id": "x"}})
    variables = {"id": "struct_abc"}

    with (
      patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema),
      patch.object(tool, "_fetch_user", return_value=mock_user),
    ):
      await tool.execute(
        {
          "query": "query GetBlock($id: String!) { informationBlock(id: $id) { id } }",
          "variables": variables,
        }
      )

    call_kwargs = fake_schema.execute.call_args.kwargs
    assert call_kwargs["variable_values"] == variables

  @pytest.mark.asyncio
  async def test_operation_name_threaded_through(self, mock_client, mock_user):
    tool = self._make_tool(mock_client)
    fake_schema, _ = self._fake_schema_with_result(data={"a": 1})

    with (
      patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema),
      patch.object(tool, "_fetch_user", return_value=mock_user),
    ):
      await tool.execute(
        {
          "query": "query GetA { a } query GetB { b }",
          "operationName": "GetA",
        }
      )

    call_kwargs = fake_schema.execute.call_args.kwargs
    assert call_kwargs["operation_name"] == "GetA"

  @pytest.mark.asyncio
  async def test_schema_extensions_in_context(self, mock_client, mock_user):
    tool = self._make_tool(
      mock_client, schema_extensions=("roboledger", "roboinvestor")
    )
    fake_schema, _ = self._fake_schema_with_result(data={})

    with (
      patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema),
      patch.object(tool, "_fetch_user", return_value=mock_user),
    ):
      await tool.execute({"query": "{ hello }"})

    ctx = fake_schema.execute.call_args.kwargs["context_value"]
    assert ctx["schema_extensions"] == ("roboledger", "roboinvestor")
    assert ctx["graph_id"] == "kgtest123"
    assert ctx["user"] is mock_user

  @pytest.mark.asyncio
  async def test_unauthenticated_when_no_user_id(self):
    from robosystems.middleware.mcp.tools.graphql_tool import GraphqlQueryTool

    client = MagicMock()
    client.graph_id = "kgtest123"
    client.user_id = None

    tool = GraphqlQueryTool(client, schema_extensions=("roboledger",))

    gql_err = MagicMock()
    gql_err.message = "Authentication required"
    gql_err.extensions = {"code": "UNAUTHENTICATED"}

    fake_schema, _ = self._fake_schema_with_result(data=None, errors=[gql_err])

    with patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema):
      result = await tool.execute({"query": "{ entity { id } }"})

    ctx = fake_schema.execute.call_args.kwargs["context_value"]
    assert ctx["user"] is None
    assert "errors" in result
