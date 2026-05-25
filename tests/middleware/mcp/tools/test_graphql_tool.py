"""Tests for GraphQL MCP tools (get-graphql-schema, query-graphql).

Mocks are placed at the ``_ensure_gql_schema`` boundary so the tool logic
(parse, reject, complexity, context building, caching) is exercised without
a live DB or real Strawberry schema.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import robosystems.middleware.mcp.tools.graphql_tool as gt

MODULE = "robosystems.middleware.mcp.tools.graphql_tool"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def clear_schema_cache():
  """Reset the process-lifetime SDL/introspection cache between tests."""
  gt._SCHEMA_CACHE.clear()
  yield
  gt._SCHEMA_CACHE.clear()


@pytest.fixture(autouse=True)
def reset_gql_schema():
  """Reset the lazy gql_schema reference so each test starts clean."""
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
    return gt.GraphqlSchemaTool(mock_client)

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
  async def test_introspection_failure_not_cached(self, mock_client):
    """If introspection returns errors, don't cache the failure as 'null'."""
    tool = self._make_tool(mock_client)

    failing = MagicMock()
    failing_result = MagicMock()
    failing_result.data = None
    err = MagicMock()
    err.message = "schema unavailable"
    failing_result.errors = [err]
    failing.execute_sync = MagicMock(return_value=failing_result)

    succeeding = MagicMock()
    succeeding_result = MagicMock()
    succeeding_result.data = {"__schema": {"types": []}}
    succeeding_result.errors = None
    succeeding.execute_sync = MagicMock(return_value=succeeding_result)

    with patch(f"{MODULE}._ensure_gql_schema", return_value=failing):
      first = await tool.execute({"format": "introspection"})

    assert first["error"] == "introspection_failed"
    # Cache should NOT contain "null" — failure must not poison the cache
    assert "introspection" not in gt._SCHEMA_CACHE

    # Subsequent call with a working schema should succeed
    with patch(f"{MODULE}._ensure_gql_schema", return_value=succeeding):
      second = await tool.execute({"format": "introspection"})

    assert "schema" in second
    parsed = json.loads(second["schema"])
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
    return gt.GraphqlQueryTool(mock_client, schema_extensions=schema_extensions)

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
  async def test_fragment_spread_field_limit(self, mock_client):
    """Fragment spreads must be expanded for complexity counting.

    Without expansion, an agent could hide 1000+ fields behind a single
    fragment spread that counts as 0. This test ensures the bypass is
    closed: a 199-field fragment spread twice yields 398 total fields,
    which exceeds the 200 limit.
    """
    tool = self._make_tool(mock_client)
    fragment_fields = " ".join(f"f{i}" for i in range(199))
    query = f"fragment A on Query {{ {fragment_fields} }} {{ ...A ...A }}"
    result = await tool.execute({"query": query})
    assert result["error"] == "query_too_complex"
    assert "field" in result["message"].lower()

  @pytest.mark.asyncio
  async def test_fragment_spread_under_limit_passes(self, mock_client, mock_user):
    """A fragment spread expanding to under the limit should execute."""
    tool = self._make_tool(mock_client)
    fake_schema, _ = self._fake_schema_with_result(data={"f0": 1})

    # 50 fields x 2 spreads = 100 total, well under 200
    fragment_fields = " ".join(f"f{i}" for i in range(50))
    query = f"fragment A on Query {{ {fragment_fields} }} {{ ...A ...A }}"

    with (
      patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema),
      patch.object(tool, "_fetch_user", return_value=mock_user),
    ):
      result = await tool.execute({"query": query})

    # Should reach execution (no complexity error returned)
    assert "error" not in result or result.get("error") != "query_too_complex"

  @pytest.mark.asyncio
  async def test_fragment_cycle_does_not_loop(self, mock_client, mock_user):
    """Cyclic fragments must not cause infinite recursion in the walker.

    GraphQL itself rejects cyclic spreads at validation time, but the MCP
    complexity walker runs before validation. The visited-set guard ensures
    we don't loop forever.
    """
    tool = self._make_tool(mock_client)
    fake_schema, _ = self._fake_schema_with_result(data={"f": 1})

    query = "fragment A on Query { ...B } fragment B on Query { ...A f } { ...A }"

    with (
      patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema),
      patch.object(tool, "_fetch_user", return_value=mock_user),
    ):
      # Should terminate (not hang) and not raise RecursionError
      result = await tool.execute({"query": query})

    # Either passes complexity (small enough) or fails at execution — either way
    # the test simply asserts the walker didn't blow up.
    assert isinstance(result, dict)

  @pytest.mark.asyncio
  async def test_unused_fragment_does_not_inflate_count(self, mock_client, mock_user):
    """A fragment never spread should contribute zero to complexity."""
    tool = self._make_tool(mock_client)
    fake_schema, _ = self._fake_schema_with_result(data={"hello": "world"})

    # Fragment with 250 fields, but operation only selects 1 — should pass
    huge = " ".join(f"f{i}" for i in range(250))
    query = f"fragment Unused on Query {{ {huge} }} {{ hello }}"

    with (
      patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema),
      patch.object(tool, "_fetch_user", return_value=mock_user),
    ):
      result = await tool.execute({"query": query})

    assert "data" in result

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
    client = MagicMock()
    client.graph_id = "kgtest123"
    client.user_id = None
    client.user = None

    tool = gt.GraphqlQueryTool(client, schema_extensions=("roboledger",))

    gql_err = MagicMock()
    gql_err.message = "Authentication required"
    gql_err.extensions = {"code": "UNAUTHENTICATED"}

    fake_schema, _ = self._fake_schema_with_result(data=None, errors=[gql_err])

    with patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema):
      result = await tool.execute({"query": "{ entity { id } }"})

    ctx = fake_schema.execute.call_args.kwargs["context_value"]
    assert ctx["user"] is None
    assert "errors" in result

  def test_fetch_user_prefers_attached_user_object(self, mock_user):
    """The MCP handler attaches the authenticated User to `client.user`;
    `_fetch_user` returns it directly (no by-id DB round-trip). Regression
    guard for the UNAUTHENTICATED bug where the handler set `client.user`
    but the tool only read `client.user_id`."""
    client = MagicMock()
    client.user = mock_user
    tool = gt.GraphqlQueryTool(client)
    assert tool._fetch_user() is mock_user

  def test_fetch_user_returns_none_without_user_or_id(self):
    client = MagicMock()
    client.user = None
    client.user_id = None
    tool = gt.GraphqlQueryTool(client)
    assert tool._fetch_user() is None

  @pytest.mark.asyncio
  async def test_user_lookup_runs_on_thread(self, mock_client, mock_user):
    """`_fetch_user` (sync DB) must run via asyncio.to_thread, not block loop."""
    tool = self._make_tool(mock_client)
    fake_schema, _ = self._fake_schema_with_result(data={})

    with (
      patch(f"{MODULE}._ensure_gql_schema", return_value=fake_schema),
      patch.object(tool, "_fetch_user", return_value=mock_user) as fetch,
      patch(
        f"{MODULE}.asyncio.to_thread", new=AsyncMock(return_value=mock_user)
      ) as to_thread,
    ):
      await tool.execute({"query": "{ hello }"})

    # _fetch_user should have been wrapped by asyncio.to_thread
    to_thread.assert_called_once_with(fetch)
