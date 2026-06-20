"""Extended unit tests for Graph MCP client - covering uncovered methods."""

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.middleware.mcp.client import GraphMCPClient
from robosystems.middleware.mcp.exceptions import (
  GraphAPIError,
  GraphQueryComplexityError,
)


def _create_client(**kwargs):
  """Create a GraphMCPClient with mocked dependencies."""
  defaults = {
    "api_base_url": "http://test:8001",
    "graph_id": "test",
  }
  defaults.update(kwargs)

  with (
    patch("robosystems.middleware.mcp.client.GraphClient"),
    patch("robosystems.middleware.mcp.client.httpx.AsyncClient"),
  ):
    return GraphMCPClient(**defaults)


@pytest.mark.unit
class TestValidateQueryComplexity:
  """Tests for _validate_query_complexity method."""

  def test_valid_short_query(self):
    """Test that a short, valid query passes validation."""
    client = _create_client()
    # Should not raise
    client._validate_query_complexity("MATCH (n:Entity) RETURN n LIMIT 10")

  def test_query_exceeding_max_length(self):
    """Test that queries exceeding max length are rejected."""
    client = _create_client(max_query_length=100)
    long_query = "MATCH (n) RETURN n " + "a" * 200

    with pytest.raises(GraphQueryComplexityError, match=r"exceeds maximum.*characters"):
      client._validate_query_complexity(long_query)

  def test_too_many_subqueries(self):
    """Test that queries with too many subqueries/WITH clauses are rejected."""
    client = _create_client()
    # Create a query with 11+ WITH clauses
    query = " ".join([f"WITH n{i} AS n{i}" for i in range(12)]) + " RETURN n0"

    with pytest.raises(GraphQueryComplexityError, match="too complex"):
      client._validate_query_complexity(query)

  def test_risky_pattern_logs_warning(self):
    """Test that risky patterns generate warnings but do not raise."""
    client = _create_client()
    # These should warn but not raise
    risky_queries = [
      "MATCH () RETURN count(*)",
      "MATCH ()-[]->() RETURN count(*)",
    ]
    for query in risky_queries:
      # Should not raise
      client._validate_query_complexity(query)

  def test_cartesian_pattern_warns(self):
    """Test that CARTESIAN pattern is detected."""
    client = _create_client()
    # Should not raise, just warn
    client._validate_query_complexity(
      "MATCH (a), (b) // CARTESIAN product RETURN a, b LIMIT 10"
    )

  def test_empty_query_passes(self):
    """Test that an empty query passes complexity validation."""
    client = _create_client()
    client._validate_query_complexity("")


@pytest.mark.unit
class TestHasAggregationFunction:
  """Tests for _has_aggregation_function method."""

  def test_count_detected(self):
    client = _create_client()
    assert client._has_aggregation_function("RETURN COUNT(n)") is True

  def test_sum_detected(self):
    client = _create_client()
    assert client._has_aggregation_function("RETURN SUM(n.value)") is True

  def test_avg_detected(self):
    client = _create_client()
    assert client._has_aggregation_function("RETURN AVG(n.score)") is True

  def test_min_detected(self):
    client = _create_client()
    assert client._has_aggregation_function("RETURN MIN(n.date)") is True

  def test_max_detected(self):
    client = _create_client()
    assert client._has_aggregation_function("RETURN MAX(n.date)") is True

  def test_collect_detected(self):
    client = _create_client()
    assert client._has_aggregation_function("RETURN COLLECT(n.name)") is True

  def test_group_by_detected(self):
    client = _create_client()
    assert client._has_aggregation_function("GROUP BY n.type") is True

  def test_distinct_detected(self):
    client = _create_client()
    assert client._has_aggregation_function("RETURN DISTINCT n.name") is True

  def test_count_subquery_detected(self):
    client = _create_client()
    assert client._has_aggregation_function("WHERE COUNT{(n)-->()} > 5") is True

  def test_no_aggregation(self):
    client = _create_client()
    assert client._has_aggregation_function("RETURN N.NAME, N.VALUE") is False

  def test_partial_match_not_detected(self):
    """Test that partial names like 'COUNTER' do not trigger."""
    client = _create_client()
    # "COUNTER" does not contain "COUNT(" - only "COUNT" without paren
    assert client._has_aggregation_function("RETURN N.COUNTER") is False


@pytest.mark.unit
class TestInjectLimitIntelligently:
  """Tests for _inject_limit_intelligently method."""

  def test_simple_query(self):
    client = _create_client()
    result = client._inject_limit_intelligently("MATCH (n) RETURN n", 100)
    assert result == "MATCH (n) RETURN n LIMIT 100"

  def test_query_with_order_by(self):
    client = _create_client()
    result = client._inject_limit_intelligently(
      "MATCH (n) RETURN n ORDER BY n.name", 100
    )
    assert result == "MATCH (n) RETURN n ORDER BY n.name LIMIT 100"

  def test_query_with_order_by_desc(self):
    client = _create_client()
    result = client._inject_limit_intelligently(
      "MATCH (n) RETURN n ORDER BY n.name DESC", 50
    )
    assert result == "MATCH (n) RETURN n ORDER BY n.name DESC LIMIT 50"

  def test_query_with_existing_limit_unchanged(self):
    client = _create_client()
    original = "MATCH (n) RETURN n LIMIT 50"
    result = client._inject_limit_intelligently(original, 100)
    assert result == original

  def test_union_query_gets_limit_on_each_part(self):
    client = _create_client()
    result = client._inject_limit_intelligently(
      "MATCH (a:TypeA) RETURN a.name UNION MATCH (b:TypeB) RETURN b.name", 100
    )
    parts = result.split("UNION")
    assert len(parts) == 2
    assert all("LIMIT 100" in part for part in parts)

  def test_query_with_semicolon(self):
    client = _create_client()
    result = client._inject_limit_intelligently("MATCH (n) RETURN n;", 100)
    assert result == "MATCH (n) RETURN n LIMIT 100"

  def test_query_with_trailing_whitespace(self):
    client = _create_client()
    result = client._inject_limit_intelligently("MATCH (n) RETURN n   ", 100)
    assert result == "MATCH (n) RETURN n LIMIT 100"

  def test_with_clause_query(self):
    client = _create_client()
    result = client._inject_limit_intelligently(
      "MATCH (n) WITH n, count(*) as cnt WHERE cnt > 5 RETURN n ORDER BY cnt", 100
    )
    assert "LIMIT 100" in result
    assert result.endswith("LIMIT 100")


@pytest.mark.unit
class TestInjectLimitToSimpleQuery:
  """Tests for _inject_limit_to_simple_query method."""

  def test_simple_query(self):
    client = _create_client()
    result = client._inject_limit_to_simple_query("MATCH (n) RETURN n", 50)
    assert result == "MATCH (n) RETURN n LIMIT 50"

  def test_query_with_semicolon(self):
    client = _create_client()
    result = client._inject_limit_to_simple_query("MATCH (n) RETURN n;", 50)
    assert result == "MATCH (n) RETURN n LIMIT 50"

  def test_query_with_order_by(self):
    client = _create_client()
    result = client._inject_limit_to_simple_query(
      "MATCH (n) RETURN n ORDER BY n.id", 50
    )
    assert result == "MATCH (n) RETURN n ORDER BY n.id LIMIT 50"


@pytest.mark.unit
class TestSanitizeErrorMessage:
  """Tests for _sanitize_error_message method."""

  def test_connection_refused(self):
    client = _create_client()
    result = client._sanitize_error_message(
      Exception("connection refused"), "operation"
    )
    assert "Service temporarily unavailable" in result

  def test_timed_out(self):
    client = _create_client()
    result = client._sanitize_error_message(
      Exception("request timed out"), "query execution"
    )
    assert "timed out" in result.lower()

  def test_out_of_memory(self):
    client = _create_client()
    result = client._sanitize_error_message(Exception("out of memory"), "operation")
    assert "too many resources" in result.lower()

  def test_unauthorized(self):
    client = _create_client()
    result = client._sanitize_error_message(Exception("unauthorized"), "operation")
    assert "Authentication required" in result

  def test_forbidden(self):
    client = _create_client()
    result = client._sanitize_error_message(Exception("forbidden access"), "operation")
    assert "Access denied" in result

  def test_ip_address_redacted(self):
    client = _create_client()
    result = client._sanitize_error_message(
      Exception("Error connecting to 192.168.1.1"), "operation"
    )
    assert "192.168.1.1" not in result

  def test_file_path_redacted(self):
    client = _create_client()
    result = client._sanitize_error_message(
      Exception("Error at /home/user/app.py"), "operation"
    )
    assert "/home/user/app.py" not in result

  def test_memory_address_redacted(self):
    client = _create_client()
    result = client._sanitize_error_message(
      Exception("Segfault at 0xdeadbeef"), "operation"
    )
    assert "0xdeadbeef" not in result

  def test_db_file_redacted(self):
    client = _create_client()
    result = client._sanitize_error_message(
      Exception("Cannot open test.lbug"), "operation"
    )
    assert "test.lbug" not in result

  def test_port_number_redacted(self):
    client = _create_client()
    result = client._sanitize_error_message(
      Exception("Error on port 5432"), "operation"
    )
    assert "port 5432" not in result

  def test_timeout_error_type(self):
    client = _create_client()
    result = client._sanitize_error_message(TimeoutError("some timeout"), "operation")
    assert "timed out" in result.lower()

  def test_connection_error_type(self):
    client = _create_client()
    result = client._sanitize_error_message(ConnectionError("conn failed"), "operation")
    assert "Connection error" in result

  def test_value_error_type(self):
    client = _create_client()
    result = client._sanitize_error_message(ValueError("bad value"), "operation")
    assert "Invalid input" in result

  def test_generic_fallback(self):
    client = _create_client()
    result = client._sanitize_error_message(
      Exception("something went wrong"), "operation"
    )
    assert "Error during operation" in result

  def test_query_error_preserved_in_dev(self):
    """Test that query errors are preserved in dev/staging."""
    with patch("robosystems.middleware.mcp.client.env") as mock_env:
      mock_env.ENVIRONMENT = "dev"
      mock_env.GRAPH_API_KEY = "test-key"
      mock_env.MCP_AUTO_LIMIT_ENABLED = True

      client = _create_client()
      result = client._sanitize_error_message(
        Exception("Syntax error near MATCH"), "query"
      )
      # In dev, the error message should be preserved (possibly sanitized paths)
      assert "Syntax error" in result or "Query validation failed" in result


@pytest.mark.unit
class TestSchemaInference:
  """Tests for schema inference methods."""

  def test_get_common_properties_known_node(self):
    client = _create_client()
    props = client._get_common_properties("Entity")
    assert "name" in props
    assert "cik" in props
    assert "ticker" in props

  def test_get_common_properties_unknown_node(self):
    client = _create_client()
    # Unknown nodes return None so get_schema introspects real columns from
    # the catalog instead of emitting a misleading generic guess.
    assert client._get_common_properties("UnknownNode") is None

  @pytest.mark.asyncio
  async def test_introspect_node_properties_uses_catalog(self):
    client = _create_client()
    client.execute_query = AsyncMock(
      return_value=[
        {"name": "identifier", "type": "STRING"},
        {"name": "canonical_type", "type": "STRING"},
        {"name": "embedding", "type": "FLOAT[384]"},  # vector noise — dropped
      ]
    )
    props = await client._introspect_node_properties("Structure")
    assert props == ["identifier", "canonical_type"]
    assert "embedding" not in props

  @pytest.mark.asyncio
  async def test_introspect_node_properties_falls_back_on_error(self):
    client = _create_client()
    client.execute_query = AsyncMock(side_effect=RuntimeError("catalog down"))
    props = await client._introspect_node_properties("Structure")
    assert "identifier" in props

  def test_get_node_description_known(self):
    client = _create_client()
    desc = client._get_node_description("Entity")
    assert "Business entities" in desc

  def test_get_node_description_unknown(self):
    client = _create_client()
    desc = client._get_node_description("CustomNode")
    assert desc == "CustomNode entities in the graph"

  def test_get_relationship_description_known(self):
    client = _create_client()
    desc = client._get_relationship_description("ENTITY_HAS_REPORT")
    assert "SEC filings" in desc

  def test_get_relationship_description_unknown(self):
    client = _create_client()
    desc = client._get_relationship_description("UNKNOWN_REL")
    assert "UNKNOWN_REL relationship" in desc

  def test_infer_relationship_nodes_known(self):
    client = _create_client()
    from_node, to_node = client._infer_relationship_nodes("ENTITY_HAS_REPORT")
    assert from_node == "Entity"
    assert to_node == "Report"

  def test_infer_relationship_nodes_has_pattern(self):
    """Test inference from _HAS_ pattern for unknown relationships."""
    client = _create_client()
    from_node, to_node = client._infer_relationship_nodes("COMPANY_HAS_PRODUCT")
    assert from_node == "Company"
    assert to_node == "Product"

  def test_infer_relationship_nodes_owns_pattern(self):
    client = _create_client()
    from_node, to_node = client._infer_relationship_nodes("PARENT_OWNS_CHILD")
    assert from_node == "Parent"
    assert to_node == "Child"

  def test_infer_relationship_nodes_relates_to_pattern(self):
    client = _create_client()
    from_node, to_node = client._infer_relationship_nodes("ITEM_RELATES_TO_CATEGORY")
    assert from_node == "Item"
    assert to_node == "Category"

  def test_infer_relationship_nodes_unknown_pattern(self):
    client = _create_client()
    from_node, to_node = client._infer_relationship_nodes("SOME_UNKNOWN_TYPE")
    assert from_node == "Unknown"
    assert to_node == "Unknown"


@pytest.mark.unit
class TestIsReadOnlyQuery:
  """Tests for _is_read_only_query method."""

  def test_read_queries_allowed(self):
    client = _create_client()
    read_queries = [
      "MATCH (n) RETURN n",
      "MATCH (n:Entity) WHERE n.name = 'Test' RETURN n",
      "CALL SHOW_TABLES() RETURN name",
      "WITH 'test' AS val MATCH (n) RETURN n",
    ]
    for query in read_queries:
      assert client._is_read_only_query(query) is True, f"Should allow: {query}"

  def test_write_queries_blocked(self):
    client = _create_client()
    write_queries = [
      "CREATE (n:Person {name: 'John'})",
      "MATCH (n) SET n.name = 'new'",
      "MATCH (n) DELETE n",
      "MATCH (n) REMOVE n.prop",
      "MERGE (n:Entity {name: 'test'})",
      "DROP INDEX idx",
    ]
    for query in write_queries:
      assert client._is_read_only_query(query) is False, f"Should block: {query}"


@pytest.mark.unit
class TestConfigCacheTTL:
  """Tests for configuration cache TTL logic."""

  def test_get_config_cache_ttl(self):
    """Test that _get_config_cache_ttl returns a value from TuningConfig."""
    result = GraphMCPClient._get_config_cache_ttl()
    assert isinstance(result, int)
    assert result > 0


@pytest.mark.unit
class TestResultTruncation:
  """Tests for result truncation by row count and size."""

  @pytest.mark.asyncio
  async def test_truncation_marker_added_when_limit_reached(self):
    """Test that truncation marker is appended when auto-limit rows are returned."""
    mock_graph_client = AsyncMock()
    mock_graph_client.query.return_value = {
      "data": [{"id": i} for i in range(1000)],
      "execution_time_ms": 50,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = _create_client()
      client.graph_client = mock_graph_client
      client.max_result_rows = 1000
      client.auto_limit_enabled = True

      result = await client.execute_query("MATCH (n) RETURN n")

      # Should have 1000 + 1 truncation marker
      assert len(result) == 1001
      assert result[-1]["_mcp_note"] == "RESULTS_TRUNCATED"

  @pytest.mark.asyncio
  async def test_size_based_truncation(self):
    """Test that results are truncated when exceeding size limit."""
    large_data = [{"id": i, "payload": "x" * 10000} for i in range(100)]

    mock_graph_client = AsyncMock()
    mock_graph_client.query.return_value = {
      "data": large_data,
      "execution_time_ms": 100,
    }

    with (
      patch("robosystems.middleware.mcp.client.httpx.AsyncClient"),
      patch(
        "robosystems.middleware.mcp.client.TuningConfig.get_mcp_max_result_size_mb",
        return_value=0.001,
      ),
    ):
      client = _create_client()
      client.graph_client = mock_graph_client
      client.max_result_rows = 10000
      client.auto_limit_enabled = True

      result = await client.execute_query("MATCH (n) RETURN n")

      assert len(result) < len(large_data)
      assert result[-1]["_mcp_note"] == "RESULTS_TRUNCATED_BY_SIZE"


@pytest.mark.unit
class TestCloseMethod:
  """Tests for close() method handling."""

  @pytest.mark.asyncio
  async def test_close_closes_both_clients(self):
    """Test that close() closes both graph and httpx clients."""
    mock_graph = AsyncMock()
    mock_httpx = AsyncMock()

    client = _create_client()
    client.graph_client = mock_graph
    client.client = mock_httpx

    await client.close()

    mock_graph.close.assert_called_once()
    mock_httpx.aclose.assert_called_once()

  @pytest.mark.asyncio
  async def test_close_handles_graph_client_error(self):
    """Test that close() handles errors from graph client."""
    mock_graph = AsyncMock()
    mock_graph.close = AsyncMock(side_effect=Exception("close error"))
    mock_httpx = AsyncMock()

    client = _create_client()
    client.graph_client = mock_graph
    client.client = mock_httpx

    # Should not raise
    await client.close()
    # httpx client should still be closed
    mock_httpx.aclose.assert_called_once()

  @pytest.mark.asyncio
  async def test_close_handles_httpx_client_error(self):
    """Test that close() handles errors from httpx client."""
    mock_graph = AsyncMock()
    mock_httpx = AsyncMock()
    mock_httpx.aclose = AsyncMock(side_effect=Exception("httpx close error"))

    client = _create_client()
    client.graph_client = mock_graph
    client.client = mock_httpx

    # Should not raise
    await client.close()

  @pytest.mark.asyncio
  async def test_execute_query_timeout(self):
    """Test that query timeout raises GraphQueryTimeoutError."""
    from robosystems.middleware.mcp.exceptions import GraphQueryTimeoutError

    mock_graph = AsyncMock()
    mock_graph.query = AsyncMock(side_effect=TimeoutError("timeout"))

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = _create_client()
      client.graph_client = mock_graph

      with pytest.raises(GraphQueryTimeoutError):
        await client.execute_query("MATCH (n) RETURN n LIMIT 1")

  @pytest.mark.asyncio
  async def test_execute_query_unexpected_error(self):
    """Test that unexpected errors are wrapped in GraphAPIError."""
    mock_graph = AsyncMock()
    mock_graph.query = AsyncMock(side_effect=RuntimeError("unexpected"))

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = _create_client()
      client.graph_client = mock_graph

      with pytest.raises(GraphAPIError):
        await client.execute_query("MATCH (n) RETURN n LIMIT 1")

  @pytest.mark.asyncio
  async def test_execute_query_non_dict_result(self):
    """Test that non-dict result raises GraphAPIError (sanitized)."""
    mock_graph = AsyncMock()
    mock_graph.query.return_value = "not a dict"

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = _create_client()
      client.graph_client = mock_graph

      with pytest.raises(GraphAPIError):
        await client.execute_query("MATCH (n) RETURN n LIMIT 1")

  @pytest.mark.asyncio
  async def test_aggregation_query_skips_auto_limit(self):
    """Test that aggregation queries do not get auto-LIMIT injected."""
    mock_graph = AsyncMock()
    mock_graph.query.return_value = {
      "data": [{"count": 42}],
      "execution_time_ms": 5,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = _create_client()
      client.graph_client = mock_graph
      client.auto_limit_enabled = True

      result = await client.execute_query("MATCH (n:Entity) RETURN COUNT(n) as count")

      # Should not inject LIMIT for aggregation
      call_args = mock_graph.query.call_args
      assert "LIMIT" not in call_args[1]["cypher"]
      assert result[0]["count"] == 42
