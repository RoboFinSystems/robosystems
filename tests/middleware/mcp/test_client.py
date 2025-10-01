"""Tests for the Kuzu MCP client implementation."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from robosystems.middleware.mcp import KuzuMCPClient, KuzuMCPTools, KuzuAPIError


@pytest.fixture
def mock_async_kuzu_client():
  """Mock the KuzuClient."""
  with patch("robosystems.middleware.mcp.client.KuzuClient") as mock:
    client_instance = AsyncMock()
    mock.return_value = client_instance
    yield client_instance


@pytest.fixture
def mock_httpx_client():
  """Mock the httpx.AsyncClient."""
  with patch("robosystems.middleware.mcp.client.httpx.AsyncClient") as mock:
    client_instance = AsyncMock()
    mock.return_value = client_instance
    yield client_instance


class TestKuzuMCPClient:
  """Unit tests for the KuzuMCPClient class."""

  @pytest.mark.unit
  def test_init_with_api_url(self):
    """Test initialization with API URL."""
    with (
      patch("robosystems.middleware.mcp.client.KuzuClient"),
      patch("robosystems.middleware.mcp.client.httpx.AsyncClient"),
    ):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      assert client.api_base_url == "http://test:8001"
      assert client.graph_id == "test"
      assert client.timeout == 60  # Now uses env.KUZU_HTTP_TIMEOUT default

  @pytest.mark.unit
  def test_init_custom_timeout(self):
    """Test initialization with custom timeout."""
    with (
      patch("robosystems.middleware.mcp.client.KuzuClient"),
      patch("robosystems.middleware.mcp.client.httpx.AsyncClient"),
    ):
      client = KuzuMCPClient(
        api_base_url="http://test:8001", graph_id="test", timeout=60
      )
      assert client.timeout == 60

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_execute_query_success(self, mock_async_kuzu_client):
    """Test successful query execution."""
    # Mock successful response from KuzuClient
    mock_async_kuzu_client.query.return_value = {
      "data": [{"name": "Test Entity", "cik": "0000123456"}],
      "execution_time_ms": 15.5,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      result = await client.execute_query(
        "MATCH (c:Entity) RETURN c.name as name, c.cik as cik LIMIT 1"
      )

      assert len(result) == 1
      assert result[0]["name"] == "Test Entity"
      assert result[0]["cik"] == "0000123456"

      # Verify the correct API call was made
      # Note: Auto-LIMIT feature will modify queries without explicit LIMIT
      mock_async_kuzu_client.query.assert_called_once_with(
        cypher="MATCH (c:Entity) RETURN c.name as name, c.cik as cik LIMIT 1",
        graph_id="test",
        parameters=None,
      )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_execute_query_with_parameters(self, mock_async_kuzu_client):
    """Test query execution with parameters."""
    mock_async_kuzu_client.query.return_value = {
      "data": [{"name": "Apple Inc", "cik": "0000320193"}],
      "execution_time_ms": 12.3,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      result = await client.execute_query(
        "MATCH (c:Entity {cik: $cik}) RETURN c.name as name, c.cik as cik",
        {"cik": "0000320193"},
      )

      assert len(result) == 1
      assert result[0]["name"] == "Apple Inc"

      # Verify parameters were passed
      # Note: Auto-LIMIT of 1000 is appended to queries without explicit LIMIT
      mock_async_kuzu_client.query.assert_called_once_with(
        cypher="MATCH (c:Entity {cik: $cik}) RETURN c.name as name, c.cik as cik LIMIT 1000",
        graph_id="test",
        parameters={"cik": "0000320193"},
      )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_execute_query_http_error(self, mock_async_kuzu_client):
    """Test query execution with HTTP error."""
    from httpx import HTTPError

    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Bad Request"
    mock_response.json.side_effect = Exception(
      "JSON decode error"
    )  # Simulate JSON parse failure

    http_error = HTTPError("HTTP Error")
    http_error.response = mock_response
    mock_async_kuzu_client.query.side_effect = http_error

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      with pytest.raises(KuzuAPIError, match="Invalid query"):
        await client.execute_query("INVALID QUERY")

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_get_schema(self, mock_async_kuzu_client):
    """Test schema retrieval."""
    # Mock responses for the various schema queries
    query_responses = [
      # SHOW_TABLES response
      [
        {"name": "Entity", "type": "NODE"},
        {"name": "Report", "type": "NODE"},
        {"name": "HAS_REPORT", "type": "REL"},
      ],
      # TABLE_INFO Entity response
      [
        {"name": "identifier", "type": "STRING"},
        {"name": "name", "type": "STRING"},
        {"name": "cik", "type": "STRING"},
      ],
      # Count query for Entity
      [{"count": 100}],
      # TABLE_INFO Report response
      [
        {"name": "identifier", "type": "STRING"},
        {"name": "form", "type": "STRING"},
      ],
      # Count query for Report
      [{"count": 50}],
    ]

    # Set up the KuzuClient mock to return different responses for each call
    mock_async_kuzu_client.query.side_effect = [
      {"data": response, "execution_time_ms": 10} for response in query_responses
    ]

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      schema = await client.get_schema()

      assert len(schema) == 3
      # Check node tables
      entity_schema = next(s for s in schema if s["label"] == "Entity")
      assert entity_schema["type"] == "node"
      # Schema no longer includes properties for performance reasons
      assert "description" in entity_schema
      assert "count" in entity_schema

      # Check relationship table
      rel_schema = next(s for s in schema if s["label"] == "HAS_REPORT")
      assert rel_schema["type"] == "relationship"
      assert "from_node" in rel_schema
      assert "to_node" in rel_schema

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_get_graph_info(self, mock_async_kuzu_client, mock_httpx_client):
    """Test graph info retrieval."""
    # Mock API info response
    info_response = MagicMock()
    info_response.raise_for_status = MagicMock()
    info_response.json.return_value = {
      "database_path": "/path/to/db",
      "read_only": True,
      "uptime_seconds": 3600,
    }

    # Mock the queries responses
    query_responses = [
      # SHOW_TABLES response
      [
        {"name": "Entity", "type": "NODE"},
        {"name": "Report", "type": "NODE"},
      ],
      # MATCH (n) RETURN count(n) as count - total node count
      [{"count": 150}],
      # MATCH ()-[r]->() RETURN count(r) as count - relationship count
      [{"count": 75}],
    ]

    mock_httpx_client.get.return_value = info_response
    mock_async_kuzu_client.query.side_effect = [
      {"data": response, "execution_time_ms": 10} for response in query_responses
    ]

    with patch(
      "robosystems.middleware.mcp.client.httpx.AsyncClient",
      return_value=mock_httpx_client,
    ):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      info = await client.get_graph_info()

      assert info["graph_id"] == "test"
      assert info["total_nodes"] == 150  # Total from MATCH (n) query
      assert info["read_only"] is True
      assert "Entity" in info["node_labels"]
      assert "Report" in info["node_labels"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_close(self, mock_async_kuzu_client, mock_httpx_client):
    """Test client close."""
    with patch(
      "robosystems.middleware.mcp.client.httpx.AsyncClient",
      return_value=mock_httpx_client,
    ):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      await client.close()

      mock_async_kuzu_client.close.assert_called_once()
      mock_httpx_client.aclose.assert_called_once()


class TestKuzuMCPTools:
  """Unit tests for the KuzuMCPTools class."""

  @pytest.fixture
  def mock_kuzu_client(self):
    """Create mock KuzuMCPClient."""
    return AsyncMock(spec=KuzuMCPClient)

  @pytest.mark.unit
  def test_get_tool_definitions(self, mock_kuzu_client):
    """Test tool definitions retrieval."""
    # Set up mock client with required attributes
    mock_kuzu_client.graph_id = "test_graph"
    tools = KuzuMCPTools(mock_kuzu_client)
    definitions = tools.get_tool_definitions_as_dict()

    # Should have 5 or 6 tools (6 if Element discovery is included)
    # Element discovery is conditional based on graph type/schema
    assert len(definitions) >= 5
    assert len(definitions) <= 6

    # Check example queries tool
    example_tool = next(t for t in definitions if t["name"] == "get-example-queries")
    assert "Get example Cypher queries" in example_tool["description"]
    assert "category" in example_tool["inputSchema"]["properties"]

    # Check Cypher tool
    cypher_tool = next(t for t in definitions if t["name"] == "read-graph-cypher")
    assert "Execute read-only Cypher queries" in cypher_tool["description"]
    assert "query" in cypher_tool["inputSchema"]["properties"]

    # Check if element discovery tool is conditionally present
    tool_names = {t["name"] for t in definitions}
    if "discover-common-elements" in tool_names:
      element_tool = next(
        t for t in definitions if t["name"] == "discover-common-elements"
      )
      assert "commonly used Element nodes" in element_tool["description"]
      assert "limit" in element_tool["inputSchema"]["properties"]

    # Check schema tool
    schema_tool = next(t for t in definitions if t["name"] == "get-graph-schema")
    assert "Get the complete database schema" in schema_tool["description"]

    # Check discover properties tool
    discover_tool = next(t for t in definitions if t["name"] == "discover-properties")
    assert "Discover available properties" in discover_tool["description"]
    assert "node_type" in discover_tool["inputSchema"]["properties"]

    # Check graph structure description tool
    describe_tool = next(
      t for t in definitions if t["name"] == "describe-graph-structure"
    )
    assert "Get a natural language description" in describe_tool["description"]

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_call_tool_cypher_success(self, mock_kuzu_client):
    """Test successful Cypher tool execution."""
    mock_kuzu_client.graph_id = "test_graph"  # Add missing graph_id attribute
    mock_kuzu_client.execute_query.return_value = [
      {"name": "Apple Inc", "cik": "0000320193"}
    ]

    tools = KuzuMCPTools(mock_kuzu_client)
    result = await tools.call_tool(
      "read-graph-cypher",
      {"query": "MATCH (c:Entity) RETURN c.name as name, c.cik as cik LIMIT 1"},
      return_raw=True,
    )

    assert len(result) == 1
    assert result[0]["name"] == "Apple Inc"
    mock_kuzu_client.execute_query.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_call_tool_cypher_write_blocked(self, mock_kuzu_client):
    """Test that write operations are blocked."""
    mock_kuzu_client.graph_id = "test_graph"  # Add missing graph_id attribute
    # Add the _is_read_only_query method to the mock
    mock_kuzu_client._is_read_only_query = MagicMock(return_value=False)

    tools = KuzuMCPTools(mock_kuzu_client)

    result = await tools.call_tool(
      "read-graph-cypher", {"query": "CREATE (c:Entity {name: 'Test'})"}
    )

    # The tool catches the error and returns an error message
    assert "Only read-only queries are allowed" in result

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_call_tool_schema(self, mock_kuzu_client):
    """Test schema tool execution."""
    mock_schema = [{"label": "Entity", "type": "node", "properties": []}]
    mock_kuzu_client.get_schema.return_value = mock_schema

    tools = KuzuMCPTools(mock_kuzu_client)
    result = await tools.call_tool("get-graph-schema", {}, return_raw=True)

    assert result == mock_schema
    mock_kuzu_client.get_schema.assert_called_once()

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_call_tool_unknown(self, mock_kuzu_client):
    """Test unknown tool handling."""
    tools = KuzuMCPTools(mock_kuzu_client)

    result = await tools.call_tool("unknown-tool", {})
    assert "Unknown tool" in result

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_execute_cypher_tool(self, mock_kuzu_client):
    """Test Cypher tool wrapper method."""
    mock_kuzu_client.graph_id = "test_graph"  # Add missing graph_id attribute
    mock_kuzu_client.execute_query.return_value = [{"count": 5}]

    tools = KuzuMCPTools(mock_kuzu_client)
    result = await tools.execute_cypher_tool(
      "MATCH (c:Entity) RETURN count(c) as count"
    )

    assert result[0]["count"] == 5

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_execute_schema_tool(self, mock_kuzu_client):
    """Test schema tool wrapper method."""
    mock_schema = [{"label": "Entity", "type": "node"}]
    mock_kuzu_client.get_schema.return_value = mock_schema

    tools = KuzuMCPTools(mock_kuzu_client)
    result = await tools.execute_schema_tool()

    assert result == mock_schema


class TestKuzuMCPConfigurableSchema:
  """Test configurable schema count tables."""

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_schema_all_tables_counted(self, mock_async_kuzu_client, monkeypatch):
    """Test that all node tables are counted for schema information."""

    # Mock table response
    tables_response = [
      {"name": "CustomTable1", "type": "NODE"},
      {"name": "CustomTable2", "type": "NODE"},
      {"name": "OtherTable", "type": "NODE"},
    ]

    # Mock count responses for ALL tables
    count_responses = [
      [{"count": 100}],  # CustomTable1 count
      [{"count": 200}],  # CustomTable2 count
      [{"count": 50}],  # OtherTable count
    ]

    mock_async_kuzu_client.query.side_effect = [
      {"data": tables_response, "execution_time_ms": 10},  # SHOW_TABLES
      {"data": count_responses[0], "execution_time_ms": 5},  # COUNT CustomTable1
      {"data": count_responses[1], "execution_time_ms": 5},  # COUNT CustomTable2
      {"data": count_responses[2], "execution_time_ms": 5},  # COUNT OtherTable
    ]

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      schema = await client.get_schema()

      # Verify all tables were counted
      assert len(schema) == 3

      # Check that all tables have counts
      custom1 = next(s for s in schema if s["label"] == "CustomTable1")
      assert custom1["count"] == 100

      custom2 = next(s for s in schema if s["label"] == "CustomTable2")
      assert custom2["count"] == 200

      # Check that OtherTable now also has a count (since all tables are counted)
      other = next(s for s in schema if s["label"] == "OtherTable")
      assert other["count"] == 50

      # Verify query calls
      query_calls = [call for call in mock_async_kuzu_client.query.call_args_list]
      # Should have: 1 SHOW_TABLES + 3 COUNT queries (for all 3 tables)
      assert len(query_calls) == 4


class TestKuzuMCPFactory:
  """Test the factory function for creating Kuzu MCP clients."""

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_kuzu_mcp_client_default(self):
    """Test factory with default parameters."""
    with (
      patch("robosystems.middleware.mcp.client.KuzuClient"),
      patch("robosystems.middleware.mcp.client.httpx.AsyncClient"),
      patch(
        "robosystems.kuzu_api.client.factory.KuzuClientFactory.create_client"
      ) as mock_factory,
      patch(
        "robosystems.middleware.graph.multitenant_utils.MultiTenantUtils.is_shared_repository",
        return_value=True,
      ),
    ):
      # Mock the factory to return a client with config.base_url attribute
      mock_kuzu_client = MagicMock()
      mock_kuzu_client.config.base_url = "http://localhost:8001"
      mock_kuzu_client.close = AsyncMock()

      # Mock factory returns the mock client directly (not a coroutine)
      async def create_client_async(*args, **kwargs):
        return mock_kuzu_client

      mock_factory.side_effect = create_client_async

      with patch.dict("os.environ", {"ENVIRONMENT": "dev"}):
        from robosystems.middleware.mcp import create_kuzu_mcp_client

        client = await create_kuzu_mcp_client()
        assert client.graph_id == "sec"
        assert "localhost" in client.api_base_url

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_kuzu_mcp_client_prod(self):
    """Test factory with production environment."""
    with (
      patch("robosystems.middleware.mcp.client.KuzuClient"),
      patch("robosystems.middleware.mcp.client.httpx.AsyncClient"),
      patch(
        "robosystems.kuzu_api.client.factory.KuzuClientFactory.create_client"
      ) as mock_factory,
      patch(
        "robosystems.middleware.graph.multitenant_utils.MultiTenantUtils.is_shared_repository",
        return_value=True,
      ),
    ):
      # Mock the factory to return a client with production config.base_url
      mock_kuzu_client = MagicMock()
      mock_kuzu_client.config.base_url = "http://robosystems.internal:8001"
      mock_kuzu_client.close = AsyncMock()

      # Mock factory returns the mock client directly (not a coroutine)
      async def create_client_async(*args, **kwargs):
        return mock_kuzu_client

      mock_factory.side_effect = create_client_async

      with patch(
        "robosystems.middleware.mcp.factory.env.is_production", return_value=True
      ):
        from robosystems.middleware.mcp import create_kuzu_mcp_client

        client = await create_kuzu_mcp_client()
        assert "robosystems.internal" in client.api_base_url

  @pytest.mark.unit
  @pytest.mark.asyncio
  async def test_create_kuzu_mcp_client_custom_url(self):
    """Test factory with custom URL override."""
    with (
      patch("robosystems.middleware.mcp.client.KuzuClient"),
      patch("robosystems.middleware.mcp.client.httpx.AsyncClient"),
    ):
      from robosystems.middleware.mcp import create_kuzu_mcp_client

      client = await create_kuzu_mcp_client(
        api_base_url="http://custom:8001", graph_id="custom"
      )
      assert client.api_base_url == "http://custom:8001"
      assert client.graph_id == "custom"


class TestKuzuMCPErrorSanitization:
  """Test error message sanitization for security."""

  @pytest.mark.unit
  def test_sanitize_error_removes_sensitive_info(self):
    """Test that sensitive information is removed from errors."""
    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")

      # Test various error types with sensitive information
      test_cases = [
        # File paths
        (
          Exception("Error at /home/user/robosystems/mcp.py line 123"),
          "Error at [REDACTED] [REDACTED]",
        ),
        # Memory addresses
        (
          Exception("Segfault at 0x7fff8b4c2340 in function"),
          "Segfault at [REDACTED] in function",
        ),
        # IP addresses
        (
          Exception("Cannot connect to 192.168.1.100 port 8001"),
          "Cannot connect to [REDACTED] [REDACTED]",
        ),
        # Database files
        (
          Exception("Cannot open database file test_entity.kuzu"),
          "Cannot open database file [REDACTED]",
        ),
      ]

      for error, expected in test_cases:
        result = client._sanitize_error_message(error)
        assert result == expected, f"Expected '{expected}', got '{result}'"

  @pytest.mark.unit
  def test_sanitize_error_maps_common_errors(self):
    """Test that common errors are mapped to user-friendly messages."""
    from robosystems.config import env

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")

      # Determine expected query error message based on environment
      # In production, query errors get generic messages for security
      # In dev/staging, they're preserved for debugging
      if env.ENVIRONMENT in ("dev", "staging"):
        query_error_expected = "Syntax error near 'MATCH' at position 15"
      else:
        query_error_expected = (
          "Query validation failed. Please check your query syntax."
        )

      test_cases = [
        # Connection errors
        (
          ConnectionError("Connection refused to localhost:8001"),
          "Service temporarily unavailable. Please try again later.",
        ),
        # Timeout
        (
          TimeoutError("Request timed out after 30s"),
          "Request timed out during operation. Try a simpler query or increase timeout.",
        ),
        # Auth errors
        (
          Exception("Unauthorized: Invalid API key abc123"),
          "Authentication required. Please check your credentials.",
        ),
        # Query errors (environment-dependent handling)
        (
          Exception("Syntax error near 'MATCH' at position 15"),
          query_error_expected,
        ),
        # Resource errors
        (
          Exception("Out of memory while processing query"),
          "Query requires too many resources. Try limiting results or simplifying the query.",
        ),
      ]

      for error, expected in test_cases:
        result = client._sanitize_error_message(error)
        assert result == expected, (
          f"For error '{error}', expected '{expected}', got '{result}'"
        )

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_http_error_sanitization(
    self, mock_async_kuzu_client, mock_httpx_client
  ):
    """Test that HTTP errors are properly sanitized."""
    from httpx import HTTPError

    # Create mock response with sensitive details
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.return_value = {
      "detail": "Database error at /var/lib/kuzu/test.db: disk full"
    }

    http_error = HTTPError("Internal server error")
    http_error.response = mock_response
    mock_async_kuzu_client.query.side_effect = http_error

    with patch(
      "robosystems.middleware.mcp.client.httpx.AsyncClient",
      return_value=mock_httpx_client,
    ):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      with pytest.raises(KuzuAPIError) as exc_info:
        await client.execute_query("MATCH (n) RETURN n")

      # Error should be sanitized - sensitive path should NOT be in the error
      error_msg = str(exc_info.value)
      assert "/var/lib/kuzu/" not in error_msg
      assert "test.db" not in error_msg
      # Should get generic 500 error message instead
      assert "server error" in error_msg.lower()


class TestKuzuMCPReadOnlyValidation:
  """Test the enhanced read-only query validation."""

  @pytest.fixture
  def mock_async_kuzu_client(self):
    """Create a mock KuzuClient."""
    mock = AsyncMock()
    return mock

  @pytest.mark.unit
  def test_read_only_validation_allows_valid_queries(self):
    """Test that valid read-only queries are allowed."""
    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")

      valid_queries = [
        "MATCH (n) RETURN n",
        "MATCH (c:Entity) WHERE c.name = 'Apple' RETURN c",
        "MATCH (c:Entity)-[:HAS_REPORT]->(r:Report) RETURN c, r",
        "MATCH (n) WHERE n.created_at > date('2024-01-01') RETURN n",
        "WITH 'test' as value MATCH (n) WHERE n.name = value RETURN n",
        "MATCH (n) RETURN n ORDER BY n.created_at DESC LIMIT 10",
        "MATCH (n) RETURN count(n) as total",
        "CALL SHOW_TABLES() RETURN name, type",
      ]

      for query in valid_queries:
        # Should not raise an exception
        assert client._is_read_only_query(query), f"Query should be valid: {query}"

  @pytest.mark.unit
  def test_read_only_validation_blocks_write_queries(self):
    """Test that write operations are blocked."""
    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")

      write_queries = [
        "CREATE (n:Person {name: 'John'})",
        "MATCH (n) SET n.name = 'NewName'",
        "MATCH (n) DELETE n",
        "MATCH (n) REMOVE n.property",
        "MERGE (n:Entity {name: 'Test'})",
        "DROP INDEX idx_name",
        "CREATE INDEX idx_name FOR (n:Person) ON (n.name)",
        "MATCH (a), (b) CREATE (a)-[:KNOWS]->(b)",
        "CALL db.createLabel('NewLabel')",
        "CALL apoc.refactor.rename.label('Old', 'New')",
      ]

      for query in write_queries:
        assert not client._is_read_only_query(query), (
          f"Query should be blocked: {query}"
        )

  @pytest.mark.unit
  def test_read_only_validation_handles_edge_cases(self):
    """Test edge cases and tricky patterns."""
    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")

      # These should be allowed (field names that look like keywords)
      allowed_edge_cases = [
        "MATCH (n) WHERE n.created_at > date('2024-01-01') RETURN n",
        "MATCH (n) WHERE n.set_name = 'test' RETURN n",
        "MATCH (n) WHERE n.delete_flag = false RETURN n",
        "MATCH (n) RETURN n.created_at, n.updated_at",
      ]

      for query in allowed_edge_cases:
        assert client._is_read_only_query(query), f"Query should be valid: {query}"

      # These should be blocked (sneaky write attempts)
      blocked_edge_cases = [
        "MATCH (n) SET n.value = 123",  # SET with property
        "match (n) create (m:Node) return n, m",  # lowercase keywords
        "MATCH (n) /* comment */ DELETE n",  # with comments
        "MATCH (a), (b) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:KNOWS]->(b)",
      ]

      for query in blocked_edge_cases:
        assert not client._is_read_only_query(query), (
          f"Query should be blocked: {query}"
        )


class TestKuzuMCPAutoLimit:
  """Test the auto-LIMIT functionality for MCP context safety."""

  @pytest.fixture
  def mock_async_kuzu_client(self):
    """Create a mock KuzuClient."""
    mock = AsyncMock()
    return mock

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_auto_limit_appended(self, mock_async_kuzu_client):
    """Test that LIMIT is automatically appended to queries without one."""
    mock_async_kuzu_client.query.return_value = {
      "data": [{"id": i} for i in range(1000)],
      "execution_time_ms": 50,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      # Query without LIMIT should get one added
      result = await client.execute_query("MATCH (n) RETURN n")

      # Verify LIMIT was appended
      mock_async_kuzu_client.query.assert_called_once()
      actual_query = mock_async_kuzu_client.query.call_args[1]["cypher"]
      assert "LIMIT 1000" in actual_query

      # Check for truncation marker
      assert len(result) == 1001  # 1000 results + 1 truncation marker
      assert result[-1]["_mcp_note"] == "RESULTS_TRUNCATED"

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_existing_limit_preserved(self, mock_async_kuzu_client):
    """Test that queries with existing LIMIT are not modified."""
    mock_async_kuzu_client.query.return_value = {
      "data": [{"id": i} for i in range(50)],
      "execution_time_ms": 20,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      # Query with existing LIMIT should not be modified
      original_query = "MATCH (n) RETURN n LIMIT 50"
      result = await client.execute_query(original_query)

      # Verify query was not modified
      mock_async_kuzu_client.query.assert_called_once_with(
        cypher=original_query,
        graph_id="test",
        parameters=None,
      )

      # No truncation marker for queries with explicit LIMIT
      assert len(result) == 50
      assert not any("_mcp_note" in row for row in result)

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_auto_limit_disabled(self, mock_async_kuzu_client, monkeypatch):
    """Test that auto-limit can be disabled via environment variable."""
    monkeypatch.setattr("robosystems.config.env.MCP_AUTO_LIMIT_ENABLED", False)

    mock_async_kuzu_client.query.return_value = {
      "data": [{"id": i} for i in range(100)],
      "execution_time_ms": 30,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      original_query = "MATCH (n) RETURN n"
      result = await client.execute_query(original_query)

      # Verify query was not modified when auto-limit is disabled
      mock_async_kuzu_client.query.assert_called_once_with(
        cypher=original_query,
        graph_id="test",
        parameters=None,
      )

      assert len(result) == 100

  @pytest.mark.asyncio
  @pytest.mark.unit
  async def test_size_based_truncation(self, mock_async_kuzu_client, monkeypatch):
    """Test that results are truncated when exceeding size limit."""
    monkeypatch.setattr(
      "robosystems.config.env.MCP_MAX_RESULT_SIZE_MB", 0.001
    )  # 1KB limit

    # Create large result set
    large_data = [
      {"id": i, "data": "x" * 10000}  # Each row is ~10KB
      for i in range(100)
    ]

    mock_async_kuzu_client.query.return_value = {
      "data": large_data,
      "execution_time_ms": 100,
    }

    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")
      client.kuzu_client = mock_async_kuzu_client

      result = await client.execute_query("MATCH (n) RETURN n")

      # Results should be truncated due to size limit
      assert len(result) < len(large_data)
      assert result[-1]["_mcp_note"] == "RESULTS_TRUNCATED_BY_SIZE"
      assert "_mcp_size_mb" in result[-1]

  @pytest.mark.unit
  def test_intelligent_limit_injection(self):
    """Test intelligent LIMIT injection for complex queries."""
    with patch("robosystems.middleware.mcp.client.httpx.AsyncClient"):
      client = KuzuMCPClient(api_base_url="http://test:8001", graph_id="test")

      # Test simple query
      result = client._inject_limit_intelligently("MATCH (n) RETURN n", 100)
      assert result == "MATCH (n) RETURN n LIMIT 100"

      # Test query with ORDER BY
      result = client._inject_limit_intelligently(
        "MATCH (n) RETURN n ORDER BY n.name DESC", 100
      )
      assert result == "MATCH (n) RETURN n ORDER BY n.name DESC LIMIT 100"

      # Test UNION query
      result = client._inject_limit_intelligently(
        "MATCH (a:TypeA) RETURN a.name UNION MATCH (b:TypeB) RETURN b.name", 100
      )
      assert "LIMIT 100" in result
      # Both parts should have LIMIT
      parts = result.split("UNION")
      assert all("LIMIT 100" in part for part in parts)

      # Test query with semicolon
      result = client._inject_limit_intelligently("MATCH (n) RETURN n;", 100)
      assert result == "MATCH (n) RETURN n LIMIT 100"

      # Test query that already has LIMIT
      original = "MATCH (n) RETURN n LIMIT 50"
      result = client._inject_limit_intelligently(original, 100)
      assert result == original  # Should not change

      # Test complex query with WITH clause
      result = client._inject_limit_intelligently(
        "MATCH (n) WITH n, count(*) as cnt WHERE cnt > 5 RETURN n ORDER BY cnt", 100
      )
      assert (
        result
        == "MATCH (n) WITH n, count(*) as cnt WHERE cnt > 5 RETURN n ORDER BY cnt LIMIT 100"
      )
