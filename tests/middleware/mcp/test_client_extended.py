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
class TestPrepareReadQuery:
  """The final RETURN of each branch is capped at max_result_rows."""

  @pytest.mark.parametrize(
    ("query", "expected"),
    [
      ("MATCH (n) RETURN n", "MATCH (n) RETURN n LIMIT 100"),
      (
        "MATCH (n) RETURN DISTINCT n.name",
        "MATCH (n) RETURN DISTINCT n.name LIMIT 100",
      ),
      (
        "MATCH (n) RETURN n.type, count(n) AS c",
        "MATCH (n) RETURN n.type, count(n) AS c LIMIT 100",
      ),
      (
        "MATCH (n) RETURN n ORDER BY n.name DESC",
        "MATCH (n) RETURN n ORDER BY n.name DESC LIMIT 100",
      ),
      ("MATCH (n) RETURN n;", "MATCH (n) RETURN n LIMIT 100"),
      ("MATCH (n) RETURN n   ", "MATCH (n) RETURN n LIMIT 100"),
      ("MATCH (n) RETURN n // note", "MATCH (n) RETURN n LIMIT 100 // note"),
      ("MATCH (n) RETURN n LIMIT 50", "MATCH (n) RETURN n LIMIT 50"),
      ("MATCH (n) RETURN n LIMIT 50000", "MATCH (n) RETURN n LIMIT 100"),
      (
        "MATCH (n) RETURN n SKIP 10 LIMIT 500;",
        "MATCH (n) RETURN n SKIP 10 LIMIT 100;",
      ),
      ("MATCH (n) RETURN n LIMIT $n", "MATCH (n) RETURN n LIMIT $n"),
      (
        "MATCH (n) WITH n LIMIT 10 MATCH (n)-->(m) RETURN m",
        "MATCH (n) WITH n LIMIT 10 MATCH (n)-->(m) RETURN m LIMIT 100",
      ),
      (
        "CALL { MATCH (n) RETURN n LIMIT 5 } RETURN n",
        "CALL { MATCH (n) RETURN n LIMIT 5 } RETURN n LIMIT 100",
      ),
      (
        "MATCH (n) RETURN n, 'LIMIT 5' AS x",
        "MATCH (n) RETURN n, 'LIMIT 5' AS x LIMIT 100",
      ),
      ("MATCH (n) RETURN n.limit", "MATCH (n) RETURN n.limit LIMIT 100"),
      (
        "MATCH (a) RETURN a.name AS `Company Name`",
        "MATCH (a) RETURN a.name AS `Company Name` LIMIT 100",
      ),
      ("MATCH (a) RETURN a.`name`", "MATCH (a) RETURN a.`name` LIMIT 100"),
      ("MATCH (a) RETURN a.name, 'x'", "MATCH (a) RETURN a.name, 'x' LIMIT 100"),
      (
        "MATCH (a:A) RETURN a.name UNION ALL MATCH (b:B) RETURN b.name LIMIT 9999",
        "MATCH (a:A) RETURN a.name LIMIT 100 UNION ALL MATCH (b:B) RETURN b.name "
        "LIMIT 100",
      ),
      ("CALL show_tables()", "CALL show_tables()"),
    ],
  )
  def test_caps_rows(self, query, expected):
    client = _create_client()
    client.max_result_rows = 100
    client.auto_limit_enabled = True
    assert client.prepare_read_query(query) == expected

  def test_disabled_leaves_the_query_alone(self):
    client = _create_client()
    client.auto_limit_enabled = False
    assert client.prepare_read_query("MATCH (n) RETURN n") == "MATCH (n) RETURN n"

  def test_long_query_is_not_polynomial(self):
    import time

    client = _create_client(max_query_length=100000)
    client.auto_limit_enabled = True
    query = "MATCH (n) RETURN " + "a" * 40000
    start = time.perf_counter()
    assert client.prepare_read_query(query).endswith(" LIMIT 1000")
    assert time.perf_counter() - start < 0.5


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

  def test_ledger_spine_is_introspected_not_curated(self):
    """Transaction/Entry/LineItem carry the live-row columns (is_live,
    status) a curated hint list omitted, so they must come from the catalog."""
    client = _create_client()
    for label in ("Transaction", "Entry", "LineItem", "Event"):
      assert client._get_common_properties(label) is None

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

  def test_infer_relationship_nodes_prefers_the_declared_schema(self):
    """Platform relationships resolve from the declared schema, including the
    ones no naming heuristic could guess: ENTRY_FROM_SCHEDULE targets a
    Structure, and the Event verbs are neither HAS nor OWNS nor RELATES_TO."""
    client = _create_client()
    assert client._infer_relationship_nodes("ENTRY_FROM_SCHEDULE") == (
      "Entry",
      "Structure",
    )
    assert client._infer_relationship_nodes("EVENT_TRIGGERS_TRANSACTION") == (
      "Event",
      "Transaction",
    )
    assert client._infer_relationship_nodes("EVENT_INVOLVES_AGENT") == (
      "Event",
      "Agent",
    )
    assert client._infer_relationship_nodes("ENTITY_HAS_EVENT") == ("Entity", "Event")

  def test_infer_relationship_nodes_survives_a_loader_fault(self):
    """A declared-schema lookup failure degrades to the name heuristics
    rather than breaking schema retrieval."""
    client = _create_client()
    with patch(
      "robosystems.schemas.loader.get_schema_loader",
      side_effect=RuntimeError("extensions not importable"),
    ):
      assert client._infer_relationship_nodes("COMPANY_HAS_PRODUCT") == (
        "Company",
        "Product",
      )


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
  async def test_truncation_marker_added_when_explicit_limit_is_lowered(self):
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

      result = await client.execute_query("MATCH (n) RETURN n LIMIT 50000")

      assert mock_graph_client.query.call_args[1]["cypher"].endswith("LIMIT 1000")
      assert result[-1]["_mcp_note"] == "RESULTS_TRUNCATED"

  @pytest.mark.asyncio
  async def test_rows_past_an_unreadable_limit_are_cut_at_the_cap(self):
    mock_graph_client = AsyncMock()
    mock_graph_client.query.return_value = {
      "data": [{"id": i} for i in range(1500)],
      "execution_time_ms": 50,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = _create_client()
      client.graph_client = mock_graph_client
      client.max_result_rows = 1000
      client.auto_limit_enabled = True

      result = await client.execute_query("MATCH (n) RETURN n LIMIT $n", {"n": 1500})

      assert len(result) == 1001
      assert result[-1]["_mcp_note"] == "RESULTS_TRUNCATED"

  @pytest.mark.asyncio
  async def test_no_marker_when_the_callers_limit_is_within_the_cap(self):
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

      result = await client.execute_query("MATCH (n) RETURN n LIMIT 1000")

      assert len(result) == 1000

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
  async def test_execute_query_rejected_query_is_a_validation_error(self):
    """A query the engine refuses is the caller's mistake, not a backend fault."""
    from robosystems.middleware.mcp.exceptions import GraphValidationError

    mock_graph = AsyncMock()
    mock_graph.query = AsyncMock(
      side_effect=RuntimeError(
        "Query binding error: Binder exception: In WITH clause, ORDER BY must "
        "be followed by SKIP or LIMIT."
      )
    )

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = _create_client()
      client.graph_client = mock_graph

      with pytest.raises(GraphValidationError):
        await client.execute_query("MATCH (n) WITH n ORDER BY n.x RETURN n")

  @pytest.mark.asyncio
  async def test_execute_query_engine_fault_is_not_a_validation_error(self):
    from robosystems.middleware.mcp.exceptions import GraphValidationError

    mock_graph = AsyncMock()
    mock_graph.query = AsyncMock(
      side_effect=RuntimeError(
        "Query execution error: Buffer manager exception: Unable to allocate memory!"
      )
    )

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = _create_client()
      client.graph_client = mock_graph

      with pytest.raises(GraphAPIError) as excinfo:
        await client.execute_query("MATCH (n) RETURN n LIMIT 1")
      assert not isinstance(excinfo.value, GraphValidationError)

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
  async def test_aggregation_query_is_capped(self):
    """An aggregation's output groups are capped like any other rows."""
    mock_graph = AsyncMock()
    mock_graph.query.return_value = {
      "data": [{"count": 42}],
      "execution_time_ms": 5,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = _create_client()
      client.graph_client = mock_graph
      client.auto_limit_enabled = True
      client.max_result_rows = 1000

      result = await client.execute_query("MATCH (n:Entity) RETURN COUNT(n) as count")

      assert mock_graph.query.call_args[1]["cypher"].endswith("LIMIT 1000")
      assert result == [{"count": 42}]
