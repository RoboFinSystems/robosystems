"""MCP tools manager helper and error handling tests."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from robosystems.middleware.mcp.tools.manager import (
  GraphMCPTools,
  resolve_schema_extensions,
)


@pytest.fixture
def mock_client():
  client = AsyncMock()
  client.graph_id = "kg0123456789abcdef"
  client.close = AsyncMock()
  return client


@pytest.fixture
def tools(mock_client):
  """Create GraphMCPTools with all feature flags disabled."""
  with (
    patch.object(GraphMCPTools, "_should_include_semantic_tools", return_value=False),
    patch("robosystems.middleware.mcp.tools.manager.env") as mock_env,
  ):
    mock_env.MCP_WORKSPACE_ENABLED = False
    mock_env.MCP_MEMORY_ENABLED = False
    mock_env.FACT_GRID_ENABLED = False
    return GraphMCPTools(mock_client, schema_extensions=[])


@pytest.fixture
def tools_with_roboledger(mock_client):
  """Create GraphMCPTools with roboledger extension."""
  with (
    patch.object(GraphMCPTools, "_should_include_semantic_tools", return_value=False),
    patch("robosystems.middleware.mcp.tools.manager.env") as mock_env,
  ):
    mock_env.MCP_WORKSPACE_ENABLED = False
    mock_env.MCP_MEMORY_ENABLED = False
    mock_env.FACT_GRID_ENABLED = False
    return GraphMCPTools(mock_client, schema_extensions=["roboledger"])


class TestHasExtension:
  def test_has_extension_true(self, tools_with_roboledger):
    assert tools_with_roboledger._has_extension("roboledger") is True

  def test_has_extension_false(self, tools):
    assert tools._has_extension("roboledger") is False

  def test_has_extension_empty(self, tools):
    assert tools._has_extension("anything") is False


class TestGetToolDefinitionsAsDict:
  def test_core_tools_always_present(self, tools):
    defs = tools.get_tool_definitions_as_dict()
    assert len(defs) >= 2  # cypher, schema

  def test_roboledger_adds_tools(self, tools_with_roboledger):
    defs = tools_with_roboledger.get_tool_definitions_as_dict()
    assert len(defs) > 2  # Core + extension tools


class TestGetToolDefinitionHelpers:
  def test_workspace_tools_empty_when_disabled(self, tools):
    assert tools._get_workspace_tool_definitions() == []

  def test_memory_tools_empty_when_disabled(self, tools):
    assert tools._get_memory_tool_definitions() == []

  def test_data_tools_empty_when_disabled(self, tools):
    assert tools._get_data_tool_definitions() == []

  def test_curated_tools_empty_without_roboledger(self, tools):
    assert tools._get_curated_tool_definitions() == []

  def test_semantic_tools_empty_when_not_initialized(self, tools):
    assert tools._get_semantic_tool_definitions() == []

  def test_curated_tools_present_with_roboledger(self, tools_with_roboledger):
    defs = tools_with_roboledger._get_curated_tool_definitions()
    assert len(defs) == 1  # financial_statement only


class TestCallToolErrors:
  @pytest.mark.asyncio
  async def test_unknown_tool_raises(self, tools):
    result = await tools.call_tool("nonexistent-tool", {})
    assert "Error" in result or "Unknown tool" in result

  @pytest.mark.asyncio
  async def test_disabled_example_queries_raises(self, tools):
    result = await tools.call_tool("get-example-queries", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_workspace_raises(self, tools):
    result = await tools.call_tool("create-workspace", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_memory_raises(self, tools):
    result = await tools.call_tool("write-graph-cypher", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_fact_grid_raises(self, tools):
    result = await tools.call_tool("build-fact-grid", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_resolve_element_raises(self, tools):
    result = await tools.call_tool("resolve-element", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_resolve_structure_raises(self, tools):
    result = await tools.call_tool("resolve-structure", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_financial_statement_raises(self, tools):
    result = await tools.call_tool("get-financial-statement", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_delete_workspace_raises(self, tools):
    result = await tools.call_tool("delete-workspace", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_list_workspaces_raises(self, tools):
    result = await tools.call_tool("list-workspaces", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_switch_workspace_raises(self, tools):
    result = await tools.call_tool("switch-workspace", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_add_node_table_raises(self, tools):
    result = await tools.call_tool("add-node-table", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_disabled_add_relationship_table_raises(self, tools):
    result = await tools.call_tool("add-relationship-table", {})
    assert "not available" in result or "Error" in result

  @pytest.mark.asyncio
  async def test_unknown_tool_raw_raises(self, tools):
    with pytest.raises(Exception):
      await tools.call_tool("nonexistent-tool", {}, return_raw=True)


class TestCallToolSuccess:
  @pytest.mark.asyncio
  async def test_cypher_tool(self, tools):
    tools.cypher_tool.execute = AsyncMock(return_value=[{"n": "test"}])
    result = await tools.call_tool("read-graph-cypher", {"query": "MATCH (n) RETURN n"})
    assert isinstance(result, str)

  @pytest.mark.asyncio
  async def test_cypher_tool_raw(self, tools):
    tools.cypher_tool.execute = AsyncMock(return_value=[{"n": "test"}])
    result = await tools.call_tool(
      "read-graph-cypher", {"query": "MATCH (n) RETURN n"}, return_raw=True
    )
    assert result == [{"n": "test"}]

  @pytest.mark.asyncio
  async def test_schema_tool(self, tools):
    tools.schema_tool.execute = AsyncMock(return_value=[{"label": "Entity"}])
    tools.schema_tool.get_cache_stats = MagicMock(
      return_value={
        "cache_hits": 1,
        "cache_misses": 0,
        "is_cached": True,
        "cache_age_seconds": 10,
        "hit_rate_percent": 100.0,
      }
    )
    result = await tools.call_tool("get-graph-schema", {})
    assert isinstance(result, str)
    assert "cache" in result.lower()


class TestBuildErrorContext:
  def test_basic_context(self, tools):
    ctx = tools._build_error_context("read-graph-cypher", {}, ValueError("test"))
    assert ctx["tool_name"] == "read-graph-cypher"
    assert ctx["exception_type"] == "ValueError"
    assert ctx["graph_id"] == "kg0123456789abcdef"

  def test_with_query_argument(self, tools):
    args = {"query": "MATCH (n) WHERE n.name = 'test' RETURN n LIMIT 10"}
    ctx = tools._build_error_context("read-graph-cypher", args, ValueError("err"))
    assert ctx["arguments"]["query"]["length"] > 0
    assert ctx["arguments"]["query"]["has_limit"] is True
    assert ctx["arguments"]["query"]["has_where"] is True

  def test_with_parameters_argument(self, tools):
    args = {"parameters": {"cik": "123", "name": "test"}}
    ctx = tools._build_error_context("read-graph-cypher", args, ValueError("err"))
    assert ctx["arguments"]["parameters"]["param_count"] == 2

  def test_with_error_code(self, tools):
    exc = MagicMock()
    exc.error_code = "QUERY_TIMEOUT"
    exc.details = {"timeout_ms": 30000}
    ctx = tools._build_error_context("read-graph-cypher", {}, exc)
    assert ctx["error_code"] == "QUERY_TIMEOUT"
    assert ctx["exception_details"]["timeout_ms"] == 30000

  def test_empty_arguments(self, tools):
    ctx = tools._build_error_context("tool", {}, ValueError("err"))
    assert "arguments" not in ctx


class TestEnhanceErrorMessage:
  def test_parser_exception_adds_help(self, tools):
    msg = tools._enhance_error_message(
      "Parser exception: syntax error", "read-graph-cypher", {}
    )
    assert "Syntax Help" in msg

  def test_property_not_found_adds_help(self, tools):
    msg = tools._enhance_error_message(
      "property 'foo' not found", "read-graph-cypher", {}
    )
    assert "Property Help" in msg

  def test_connection_error_adds_help(self, tools):
    msg = tools._enhance_error_message("connection refused", "read-graph-cypher", {})
    assert "Connection Help" in msg

  def test_schema_timeout(self, tools):
    msg = tools._enhance_error_message(
      "timeout during schema fetch", "get-graph-schema", {}
    )
    assert "read-graph-cypher" in msg

  def test_unauthorized_adds_auth_help(self, tools):
    msg = tools._enhance_error_message("unauthorized access", "any-tool", {})
    assert "permissions" in msg.lower()

  def test_rate_limit_adds_wait_help(self, tools):
    msg = tools._enhance_error_message("rate limit exceeded", "any-tool", {})
    assert "Wait" in msg or "wait" in msg

  def test_no_enhancement_for_generic_error(self, tools):
    msg = tools._enhance_error_message("generic error", "some-tool", {})
    assert msg == "generic error"


class TestSanitizeErrorMessage:
  def test_removes_file_paths(self, tools):
    msg = tools._sanitize_error_message("Error at /data/graph.db location")
    assert "/data/graph.db" not in msg

  def test_removes_passwords(self, tools):
    msg = tools._sanitize_error_message("Failed: password=secret123")
    assert "secret123" not in msg

  def test_removes_tokens(self, tools):
    msg = tools._sanitize_error_message("Auth: token=abc123xyz")
    assert "abc123xyz" not in msg

  def test_maps_connection_error(self, tools):
    msg = tools._sanitize_error_message("TCP connection refused to host")
    assert msg == "Database connection failed"

  def test_maps_timeout_error(self, tools):
    msg = tools._sanitize_error_message("Query timeout after 30s")
    assert msg == "Query execution timed out"

  def test_maps_syntax_error(self, tools):
    msg = tools._sanitize_error_message("Cypher syntax error at line 1")
    assert msg == "Query syntax error"

  def test_maps_permission_error(self, tools):
    msg = tools._sanitize_error_message("Insufficient permission for operation")
    assert msg == "Insufficient permissions"

  def test_passthrough_generic_error(self, tools):
    msg = tools._sanitize_error_message("Something went wrong")
    assert msg == "Something went wrong"


class TestCacheAndClose:
  def test_get_cache_stats(self, tools):
    tools.schema_tool.get_cache_stats = MagicMock(
      return_value={"cache_hits": 5, "cache_misses": 2, "hit_rate_percent": 71.4}
    )
    stats = tools.get_cache_stats()
    assert stats["cache_hits"] == 5

  def test_clear_schema_cache(self, tools):
    tools.schema_tool.clear_schema_cache = MagicMock()
    tools.clear_schema_cache()
    tools.schema_tool.clear_schema_cache.assert_called_once()

  @pytest.mark.asyncio
  async def test_close(self, tools):
    tools.schema_tool.get_cache_stats = MagicMock(
      return_value={"cache_hits": 0, "cache_misses": 0, "hit_rate_percent": 0}
    )
    await tools.close()


class TestResolveSchemaExtensions:
  def test_shared_repo_resolves_from_manifest(self):
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=True,
      ),
      patch(
        "robosystems.config.shared_repositories.resolve_shared_repository_parent",
        return_value="sec",
      ),
      patch(
        "robosystems.config.shared_repositories.get_manifest",
      ) as mock_manifest,
    ):
      manifest = MagicMock()
      manifest.schema_extensions = ["roboledger"]
      mock_manifest.return_value = manifest
      result = resolve_schema_extensions("sec")
      assert result == ["roboledger"]

  def test_unknown_graph_returns_empty(self):
    result = resolve_schema_extensions("nonexistent_graph_xyz")
    assert result == []

  def test_shared_repo_no_manifest(self):
    with (
      patch(
        "robosystems.config.shared_repositories.is_shared_repository_or_subgraph",
        return_value=True,
      ),
      patch(
        "robosystems.config.shared_repositories.resolve_shared_repository_parent",
        return_value="sec",
      ),
      patch(
        "robosystems.config.shared_repositories.get_manifest",
        return_value=None,
      ),
    ):
      result = resolve_schema_extensions("sec")
      assert result == []
