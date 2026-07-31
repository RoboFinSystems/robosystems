"""Comprehensive unit tests for the async GraphClient.

Covers initialization, query execution, response parsing, error classification,
connection timeout scenarios, context manager lifecycle, and cleanup.
"""

import json
import time
from unittest.mock import AsyncMock, Mock, patch

import httpx
import pytest

from robosystems.graph_api.client.client import GraphClient
from robosystems.graph_api.client.config import GraphClientConfig
from robosystems.graph_api.client.exceptions import (
  GraphAPIError,
  GraphClientError,
  GraphServerError,
  GraphSyntaxError,
  GraphTimeoutError,
  GraphTransientError,
)


@pytest.fixture
def mock_env():
  """Mock environment variables for all tests."""
  with patch("robosystems.config.env") as mock:
    mock.GRAPH_API_KEY = "test-api-key"
    mock.ENVIRONMENT = "test"
    yield mock


@pytest.fixture
async def client(mock_env):
  """Create a test client with mocked env."""
  _ = mock_env
  c = GraphClient(base_url="http://localhost:8001")
  yield c
  await c.close()


@pytest.mark.unit
class TestGraphClientInitialization:
  """Tests for GraphClient initialization with various configs."""

  def test_init_with_base_url(self, mock_env):
    """Client initializes with a base URL."""
    client = GraphClient(base_url="http://graph.example.com:8001")
    assert client.config.base_url == "http://graph.example.com:8001"

  def test_init_strips_trailing_slash(self, mock_env):
    """Base URL trailing slash is stripped."""
    client = GraphClient(base_url="http://graph.example.com:8001/")
    assert client.config.base_url == "http://graph.example.com:8001"

  def test_init_with_custom_config(self, mock_env):
    """Client initializes with a custom GraphClientConfig."""
    config = GraphClientConfig(
      base_url="http://custom:9999",
      timeout=60,
      max_retries=5,
      verify_ssl=False,
      max_connections=50,
      max_keepalive_connections=10,
      keepalive_expiry=10.0,
    )
    client = GraphClient(config=config)
    assert client.config.base_url == "http://custom:9999"
    assert client.config.timeout == 60
    assert client.config.max_retries == 5
    assert client.config.verify_ssl is False

  def test_init_base_url_overrides_config(self, mock_env):
    """Explicit base_url takes precedence over config.base_url."""
    config = GraphClientConfig(base_url="http://config-url:8001")
    client = GraphClient(base_url="http://override-url:8001", config=config)
    assert client.config.base_url == "http://override-url:8001"

  def test_init_sets_api_key_header(self, mock_env):
    """API key from env is set in headers."""
    client = GraphClient(base_url="http://localhost:8001")
    assert "X-Graph-API-Key" in client.config.headers
    assert client.config.headers["X-Graph-API-Key"] == "test-api-key"

  def test_init_with_explicit_api_key(self, mock_env):
    """Explicit api_key kwarg is used in headers."""
    client = GraphClient(
      base_url="http://localhost:8001",
      api_key="explicit-key-123",
    )
    assert client.config.headers["X-Graph-API-Key"] == "explicit-key-123"

  def test_init_without_api_key_in_dev(self):
    """In dev mode, missing API key logs debug instead of warning."""
    with patch("robosystems.config.env") as mock:
      mock.GRAPH_API_KEY = None
      mock.ENVIRONMENT = "dev"
      client = GraphClient(base_url="http://localhost:8001")
      # Should not raise
      assert client.config.base_url == "http://localhost:8001"

  def test_init_raises_without_base_url(self, mock_env):
    """Raises ValueError when no base_url is provided or in config."""
    with pytest.raises(ValueError, match="base_url must be provided"):
      GraphClient()

  def test_init_creates_httpx_client(self, mock_env):
    """httpx.AsyncClient is created with proper settings."""
    client = GraphClient(base_url="http://localhost:8001")
    assert isinstance(client.client, httpx.AsyncClient)

  def test_init_sets_default_routing_metadata(self, mock_env):
    """Routing metadata starts as None."""
    client = GraphClient(base_url="http://localhost:8001")
    assert client._route_target is None
    assert client._graph_id is None
    assert client._database_name is None
    assert client._instance_id is None
    assert client._purpose is None

  def test_init_circuit_breaker_state(self, mock_env):
    """Circuit breaker starts in closed state."""
    client = GraphClient(base_url="http://localhost:8001")
    assert client._circuit_breaker_failures == 0
    assert client._circuit_breaker_last_failure == 0
    assert client._circuit_breaker_open is False

  def test_init_with_custom_headers_merge(self, mock_env):
    """Custom headers are merged with API key header."""
    client = GraphClient(
      base_url="http://localhost:8001",
      headers={"X-Custom": "value"},
    )
    assert client.config.headers.get("X-Custom") == "value"
    assert client.config.headers.get("X-Graph-API-Key") == "test-api-key"

  def test_init_with_ssl_disabled(self, mock_env):
    """SSL verification can be disabled."""
    config = GraphClientConfig(
      base_url="http://localhost:8001",
      verify_ssl=False,
    )
    client = GraphClient(config=config)
    assert client.config.verify_ssl is False

  def test_init_with_custom_timeout(self, mock_env):
    """Custom timeout is applied."""
    config = GraphClientConfig(
      base_url="http://localhost:8001",
      timeout=120,
    )
    client = GraphClient(config=config)
    assert client.config.timeout == 120


@pytest.mark.unit
class TestGraphClientContextManager:
  """Tests for async context manager lifecycle."""

  @pytest.mark.asyncio
  async def test_aenter_returns_self(self, client):
    """__aenter__ returns the client instance."""
    async with client as c:
      assert c is client

  @pytest.mark.asyncio
  async def test_aexit_calls_close(self, mock_env):
    """__aexit__ calls close to cleanup resources."""
    c = GraphClient(base_url="http://localhost:8001")
    with patch.object(c, "close", new_callable=AsyncMock) as mock_close:
      async with c:
        pass
      mock_close.assert_called_once()

  @pytest.mark.asyncio
  async def test_close_calls_aclose_on_httpx_client(self, mock_env):
    """close() calls aclose() on the underlying httpx client."""
    c = GraphClient(base_url="http://localhost:8001")
    with patch.object(c.client, "aclose", new_callable=AsyncMock) as mock_aclose:
      await c.close()
      mock_aclose.assert_called_once()


@pytest.mark.unit
class TestGraphClientQuery:
  """Tests for the query() method with mocked httpx responses."""

  @pytest.mark.asyncio
  async def test_query_returns_json_result(self, client):
    """Non-streaming query returns parsed JSON."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = (
      b'{"data": [{"name": "Alice"}], "columns": ["name"], "row_count": 1}'
    )
    mock_response.headers = {"content-type": "application/json"}
    mock_response.json.return_value = {
      "data": [{"name": "Alice"}],
      "columns": ["name"],
      "row_count": 1,
    }

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.query(
        "MATCH (n:Person) RETURN n.name as name", graph_id="test_db"
      )
      assert result["data"] == [{"name": "Alice"}]
      assert result["columns"] == ["name"]
      assert result["row_count"] == 1

  @pytest.mark.asyncio
  async def test_query_with_parameters(self, client):
    """Query passes parameters in payload."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b'{"data": [{"id": 1}], "columns": ["id"], "row_count": 1}'
    mock_response.headers = {"content-type": "application/json"}
    mock_response.json.return_value = {
      "data": [{"id": 1}],
      "columns": ["id"],
      "row_count": 1,
    }

    with patch.object(client.client, "request", return_value=mock_response) as mock_req:
      await client.query(
        "MATCH (n) WHERE n.id = $id RETURN n",
        graph_id="test_db",
        parameters={"id": 1},
      )
      call_kwargs = mock_req.call_args
      payload = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
      assert payload["parameters"] == {"id": 1}

  @pytest.mark.asyncio
  async def test_query_empty_response_body(self, client):
    """Empty response body returns default empty result."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b""
    mock_response.headers = {"content-type": "application/json"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.query("MATCH (n) RETURN n", graph_id="test_db")
      assert result == {"data": [], "columns": [], "row_count": 0}

  @pytest.mark.asyncio
  async def test_query_none_content(self, client):
    """None content returns default empty result."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = None
    mock_response.headers = {"content-type": "application/json"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.query("MATCH (n) RETURN n", graph_id="test_db")
      assert result == {"data": [], "columns": [], "row_count": 0}

  @pytest.mark.asyncio
  async def test_query_invalid_json_response(self, client):
    """Invalid JSON response returns error dict with empty data."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b"not valid json at all"
    mock_response.headers = {"content-type": "application/json"}
    mock_response.json.side_effect = json.JSONDecodeError("msg", "doc", 0)

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.query("MATCH (n) RETURN n", graph_id="test_db")
      assert "error" in result
      assert result["data"] == []
      assert result["row_count"] == 0

  @pytest.mark.asyncio
  async def test_query_uses_graph_id_in_url(self, client):
    """Query constructs URL with graph_id."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b'{"data": [], "columns": [], "row_count": 0}'
    mock_response.headers = {"content-type": "application/json"}
    mock_response.json.return_value = {"data": [], "columns": [], "row_count": 0}

    with patch.object(client.client, "request", return_value=mock_response) as mock_req:
      await client.query("MATCH (n) RETURN n", graph_id="my_graph_db")
      call_kwargs = mock_req.call_args
      url = call_kwargs.kwargs.get("url") or call_kwargs[1].get("url")
      assert "my_graph_db" in url


@pytest.mark.unit
class TestGraphClientStreamingQuery:
  """Tests for streaming query execution."""

  @pytest.mark.asyncio
  async def test_streaming_query_yields_chunks(self, client):
    """Streaming query returns async generator of NDJSON chunks."""
    mock_stream = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status_code = 200

    async def mock_aiter_lines():
      yield '{"data": [{"id": 1}], "chunk_index": 0}'
      yield '{"data": [{"id": 2}], "chunk_index": 1}'

    mock_response.aiter_lines = mock_aiter_lines
    mock_stream.__aenter__.return_value = mock_response
    mock_stream.__aexit__.return_value = None

    with patch.object(client.client, "stream", return_value=mock_stream):
      gen = await client.query("MATCH (n) RETURN n", graph_id="test_db", streaming=True)
      chunks = []
      async for chunk in gen:
        chunks.append(chunk)
      assert len(chunks) == 2
      assert chunks[0]["chunk_index"] == 0
      assert chunks[1]["chunk_index"] == 1

  @pytest.mark.asyncio
  async def test_streaming_query_error_response(self, client):
    """Streaming query raises appropriate error on 4xx/5xx."""
    mock_stream = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status_code = 500
    mock_response.aread.return_value = b'{"detail": "Internal error"}'

    mock_stream.__aenter__.return_value = mock_response
    mock_stream.__aexit__.return_value = None

    with patch.object(client.client, "stream", return_value=mock_stream):
      gen = await client.query("BAD QUERY", graph_id="test_db", streaming=True)
      with pytest.raises(GraphServerError):
        async for _ in gen:
          pass

  @pytest.mark.asyncio
  async def test_streaming_query_skips_invalid_json_lines(self, client):
    """Streaming skips lines that fail JSON parsing."""
    mock_stream = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status_code = 200

    async def mock_aiter_lines():
      yield '{"data": [{"id": 1}], "chunk": 0}'
      yield "not json"
      yield '{"data": [{"id": 2}], "chunk": 1}'

    mock_response.aiter_lines = mock_aiter_lines
    mock_stream.__aenter__.return_value = mock_response
    mock_stream.__aexit__.return_value = None

    with patch.object(client.client, "stream", return_value=mock_stream):
      gen = await client.query("MATCH (n) RETURN n", graph_id="test_db", streaming=True)
      chunks = []
      async for chunk in gen:
        chunks.append(chunk)
      # Invalid line should be skipped
      assert len(chunks) == 2

  @pytest.mark.asyncio
  async def test_streaming_query_skips_empty_lines(self, client):
    """Streaming skips empty lines."""
    mock_stream = AsyncMock()
    mock_response = AsyncMock()
    mock_response.status_code = 200

    async def mock_aiter_lines():
      yield '{"data": [{"id": 1}]}'
      yield ""
      yield '{"data": [{"id": 2}]}'

    mock_response.aiter_lines = mock_aiter_lines
    mock_stream.__aenter__.return_value = mock_response
    mock_stream.__aexit__.return_value = None

    with patch.object(client.client, "stream", return_value=mock_stream):
      gen = await client.query("MATCH (n) RETURN n", graph_id="test_db", streaming=True)
      chunks = []
      async for chunk in gen:
        chunks.append(chunk)
      assert len(chunks) == 2


@pytest.mark.unit
class TestGraphClientRequest:
  """Tests for the _request method and error handling."""

  @pytest.mark.asyncio
  async def test_request_success(self, client):
    """Successful request returns response."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"ok": True}

    with patch.object(client.client, "request", return_value=mock_response):
      response = await client._request("GET", "/health")
      assert response.status_code == 200

  @pytest.mark.asyncio
  async def test_request_raises_client_error_on_400(self, client):
    """400 status raises GraphClientError."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 400
    mock_response.json.return_value = {"detail": "Bad request"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphClientError):
        await client._request("GET", "/bad-endpoint")

  @pytest.mark.asyncio
  async def test_request_raises_client_error_on_401(self, client):
    """401 status raises GraphClientError."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 401
    mock_response.json.return_value = {"detail": "Unauthorized"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphClientError):
        await client._request("GET", "/secure-endpoint")

  @pytest.mark.asyncio
  async def test_request_raises_client_error_on_404(self, client):
    """404 status raises GraphClientError."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 404
    mock_response.json.return_value = {"detail": "Not found"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphClientError):
        await client._request("GET", "/missing")

  @pytest.mark.asyncio
  async def test_request_raises_transient_error_on_502(self, client):
    """502 status raises GraphTransientError."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 502
    mock_response.json.return_value = {"detail": "Bad gateway"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphTransientError):
        await client._request("GET", "/endpoint")

  @pytest.mark.asyncio
  async def test_request_raises_transient_error_on_503(self, client):
    """503 status raises GraphTransientError."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 503
    mock_response.json.return_value = {"detail": "Service unavailable"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphTransientError):
        await client._request("GET", "/endpoint")

  @pytest.mark.asyncio
  async def test_request_raises_transient_error_on_504(self, client):
    """504 status raises GraphTransientError."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 504
    mock_response.json.return_value = {"detail": "Gateway timeout"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphTransientError):
        await client._request("GET", "/endpoint")

  @pytest.mark.asyncio
  async def test_request_raises_server_error_on_500(self, client):
    """500 status raises GraphServerError."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 500
    mock_response.json.return_value = {"detail": "Internal error"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphServerError):
        await client._request("GET", "/endpoint")

  @pytest.mark.asyncio
  async def test_request_raises_syntax_error_on_parser_exception(self, client):
    """422 with Parser exception raises GraphSyntaxError."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 422
    mock_response.json.return_value = {"detail": "Parser exception: Invalid syntax"}

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphSyntaxError):
        await client._request("POST", "/query")

  @pytest.mark.asyncio
  async def test_request_raises_syntax_error_on_binder_exception(self, client):
    """500 with Binder exception raises GraphSyntaxError."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 500
    mock_response.json.return_value = {
      "detail": "Binder exception: Cannot find property"
    }

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphSyntaxError):
        await client._request("POST", "/query")

  @pytest.mark.asyncio
  async def test_request_no_retries_when_retries_zero(self, client):
    """With retries=0, request is executed once without retry."""
    call_count = 0

    async def mock_request(**kwargs):
      nonlocal call_count
      call_count += 1
      mock_resp = Mock(spec=httpx.Response)
      mock_resp.status_code = 200
      mock_resp.json.return_value = {"ok": True}
      return mock_resp

    with patch.object(client.client, "request", side_effect=mock_request):
      await client._request("POST", "/materialize", retries=0)
      assert call_count == 1

  @pytest.mark.asyncio
  async def test_request_error_with_non_json_body(self, client):
    """Error response with non-JSON body still raises properly."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 500
    mock_response.json.side_effect = ValueError("not json")
    mock_response.text = "Internal Server Error"

    with patch.object(client.client, "request", return_value=mock_response):
      with pytest.raises(GraphServerError):
        await client._request("GET", "/endpoint")


@pytest.mark.unit
class TestGraphClientErrorClassification:
  """Tests for error type classification via _handle_response_error."""

  def test_400_returns_client_error(self, mock_env):
    """400 maps to GraphClientError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(400, {"detail": "Bad request"})
    assert isinstance(error, GraphClientError)
    assert error.status_code == 400

  def test_401_returns_client_error(self, mock_env):
    """401 maps to GraphClientError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(401, {"detail": "Unauthorized"})
    assert isinstance(error, GraphClientError)

  def test_403_returns_client_error(self, mock_env):
    """403 maps to GraphClientError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(403, {"detail": "Forbidden"})
    assert isinstance(error, GraphClientError)

  def test_404_returns_client_error(self, mock_env):
    """404 maps to GraphClientError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(404, {"detail": "Not found"})
    assert isinstance(error, GraphClientError)

  def test_422_returns_client_error(self, mock_env):
    """422 maps to GraphClientError (no syntax pattern)."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(422, {"detail": "Validation error"})
    assert isinstance(error, GraphClientError)

  def test_422_with_parser_exception_returns_syntax_error(self, mock_env):
    """422 with Parser exception maps to GraphSyntaxError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(
      422, {"detail": "Parser exception: Invalid input"}
    )
    assert isinstance(error, GraphSyntaxError)

  def test_500_returns_server_error(self, mock_env):
    """500 maps to GraphServerError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(500, {"detail": "Server error"})
    assert isinstance(error, GraphServerError)

  def test_500_with_binder_exception_returns_syntax_error(self, mock_env):
    """500 with Binder exception maps to GraphSyntaxError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(
      500, {"detail": "Binder exception: Cannot find property"}
    )
    assert isinstance(error, GraphSyntaxError)

  def test_500_with_table_does_not_exist_returns_syntax_error(self, mock_env):
    """500 with 'Table does not exist' maps to GraphSyntaxError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(
      500, {"detail": "Table does not exist: MyTable"}
    )
    assert isinstance(error, GraphSyntaxError)

  def test_502_returns_transient_error(self, mock_env):
    """502 maps to GraphTransientError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(502, {"detail": "Bad gateway"})
    assert isinstance(error, GraphTransientError)

  def test_503_returns_transient_error(self, mock_env):
    """503 maps to GraphTransientError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(503, {"detail": "Unavailable"})
    assert isinstance(error, GraphTransientError)

  def test_504_returns_transient_error(self, mock_env):
    """504 maps to GraphTransientError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(504, {"detail": "Gateway timeout"})
    assert isinstance(error, GraphTransientError)

  def test_unknown_status_returns_base_error(self, mock_env):
    """Unknown status code maps to base GraphAPIError."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(301, {"detail": "Redirect"})
    assert type(error) is GraphAPIError

  def test_none_response_data(self, mock_env):
    """None response data uses default error message."""
    client = GraphClient(base_url="http://localhost:8001")
    error = client._handle_response_error(500, None)
    assert isinstance(error, GraphServerError)
    assert "API request failed" in str(error)

  def test_non_dict_response_data(self, mock_env):
    """Non-dict response data uses default error message."""
    client = GraphClient(base_url="http://localhost:8001")
    # Pass a non-dict to test fallback
    error = client._handle_response_error(500, "just a string")  # type: ignore[arg-type]
    assert isinstance(error, GraphServerError)


@pytest.mark.unit
class TestGraphClientConnectionTimeout:
  """Tests for connection timeout scenarios."""

  @pytest.mark.asyncio
  async def test_timeout_exception_converts_to_graph_timeout(self, client):
    """httpx.TimeoutException is converted to GraphTimeoutError."""

    async def raise_timeout(**kwargs):
      raise httpx.TimeoutException("Connection timed out")

    with patch.object(client.client, "request", side_effect=raise_timeout):
      with pytest.raises(GraphTimeoutError, match="Request timeout"):
        await client._request("GET", "/health")

  @pytest.mark.asyncio
  async def test_connect_error_converts_to_transient(self, client):
    """httpx.ConnectError is converted to GraphTransientError."""

    async def raise_connect_error(**kwargs):
      raise httpx.ConnectError("Connection refused")

    with patch.object(client.client, "request", side_effect=raise_connect_error):
      with pytest.raises(GraphTransientError, match="Connection error"):
        await client._request("GET", "/health")

  @pytest.mark.asyncio
  async def test_request_error_converts_to_transient(self, client):
    """httpx.RequestError is converted to GraphTransientError."""

    async def raise_request_error(**kwargs):
      raise httpx.RequestError("Request failed")

    with patch.object(client.client, "request", side_effect=raise_request_error):
      with pytest.raises(GraphTransientError, match="Request error"):
        await client._request("GET", "/health")


@pytest.mark.unit
class TestGraphClientRetryLogic:
  """Tests for the _execute_with_retry method."""

  @pytest.mark.asyncio
  async def test_retry_on_transient_error(self, client):
    """Transient errors trigger retry."""
    client.config.retry_delay = 0.001
    attempt = 0

    async def mock_func():
      nonlocal attempt
      attempt += 1
      if attempt < 2:
        raise GraphTransientError("Temporary failure")
      return {"ok": True}

    result = await client._execute_with_retry(mock_func)
    assert result == {"ok": True}
    assert attempt == 2

  @pytest.mark.asyncio
  async def test_retry_on_server_error(self, client):
    """Server errors trigger retry."""
    client.config.retry_delay = 0.001
    attempt = 0

    async def mock_func():
      nonlocal attempt
      attempt += 1
      if attempt < 2:
        raise GraphServerError("Server error", status_code=500)
      return {"recovered": True}

    result = await client._execute_with_retry(mock_func)
    assert result == {"recovered": True}
    assert attempt == 2

  @pytest.mark.asyncio
  async def test_no_retry_on_client_error(self, client):
    """Client errors do not trigger retry."""
    attempt = 0

    async def mock_func():
      nonlocal attempt
      attempt += 1
      raise GraphClientError("Bad request", status_code=400)

    with pytest.raises(GraphClientError):
      await client._execute_with_retry(mock_func)
    assert attempt == 1

  @pytest.mark.asyncio
  async def test_no_retry_on_syntax_error(self, client):
    """Syntax errors are never retried."""
    attempt = 0

    async def mock_func():
      nonlocal attempt
      attempt += 1
      raise GraphSyntaxError("Parser exception")

    with pytest.raises(GraphSyntaxError):
      await client._execute_with_retry(mock_func)
    assert attempt == 1

  @pytest.mark.asyncio
  async def test_max_retries_exhausted(self, client):
    """Error raised after all retries exhausted."""
    client.config.retry_delay = 0.001
    attempt = 0

    async def mock_func():
      nonlocal attempt
      attempt += 1
      raise GraphTransientError("Always fails")

    with pytest.raises(GraphTransientError, match="Always fails"):
      await client._execute_with_retry(mock_func)
    # max_retries=3 means 4 total attempts (initial + 3 retries)
    assert attempt == client.config.max_retries + 1

  @pytest.mark.asyncio
  async def test_circuit_breaker_blocks_when_open(self, client):
    """Open circuit breaker prevents execution."""
    client._circuit_breaker_open = True
    client._circuit_breaker_last_failure = time.time()
    client.config.circuit_breaker_timeout = 9999

    async def mock_func():
      return {"should_not_reach": True}

    with pytest.raises(GraphTransientError, match="Circuit breaker open"):
      await client._execute_with_retry(mock_func)

  @pytest.mark.asyncio
  async def test_circuit_breaker_resets_after_timeout(self, client):
    """Circuit breaker resets after timeout expires."""
    client._circuit_breaker_open = True
    client._circuit_breaker_last_failure = time.time() - 100
    client.config.circuit_breaker_timeout = 1

    async def mock_func():
      return {"recovered": True}

    result = await client._execute_with_retry(mock_func)
    assert result == {"recovered": True}
    assert client._circuit_breaker_open is False
    assert client._circuit_breaker_failures == 0


@pytest.mark.unit
class TestGraphClientApiMethods:
  """Tests for specific API methods (health_check, get_info, etc.)."""

  @pytest.mark.asyncio
  async def test_health_check(self, client):
    """health_check calls GET /health."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"status": "healthy"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.health_check()
      assert result["status"] == "healthy"

  @pytest.mark.asyncio
  async def test_get_info(self, client):
    """get_info calls GET /info."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"databases": 5, "version": "1.0.0"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.get_info()
      assert result["databases"] == 5

  @pytest.mark.asyncio
  async def test_list_databases(self, client):
    """list_databases calls GET /databases."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"databases": ["db1", "db2"]}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.list_databases()
      assert len(result["databases"]) == 2

  @pytest.mark.asyncio
  async def test_create_database(self, client):
    """create_database calls POST /databases."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 201
    mock_response.json.return_value = {"database": "new_db", "status": "created"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.create_database(
        graph_id="new_db",
        schema_type="entity",
        repository_name="test",
        is_subgraph=False,
      )
      assert result["status"] == "created"

  @pytest.mark.asyncio
  async def test_delete_database(self, client):
    """delete_database calls DELETE /databases/{graph_id}."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.json.return_value = {"message": "Database deleted"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.delete_database("old_db")
      assert result["message"] == "Database deleted"

  @pytest.mark.asyncio
  async def test_execute_query_returns_data_list(self, client):
    """execute_query returns data as list of dicts."""
    with patch.object(client, "query") as mock_query:
      mock_query.return_value = {
        "data": [{"id": 1}, {"id": 2}],
        "columns": ["id"],
      }
      result = await client.execute_query("MATCH (n) RETURN n.id as id")
      assert len(result) == 2
      assert result[0]["id"] == 1

  @pytest.mark.asyncio
  async def test_execute_single_returns_first_row(self, client):
    """execute_single returns first row or None."""
    with patch.object(client, "execute_query") as mock_execute:
      mock_execute.return_value = [{"count": 42}]
      result = await client.execute_single("MATCH (n) RETURN count(n) as count")
      assert result == {"count": 42}

  @pytest.mark.asyncio
  async def test_execute_single_returns_none_on_empty(self, client):
    """execute_single returns None when no results."""
    with patch.object(client, "execute_query") as mock_execute:
      mock_execute.return_value = []
      result = await client.execute_single("MATCH (n:Missing) RETURN n")
      assert result is None

  @pytest.mark.asyncio
  async def test_export_database_returns_bytes(self, client):
    """export_database returns raw bytes."""
    mock_response = Mock(spec=httpx.Response)
    mock_response.status_code = 200
    mock_response.content = b"binary_export_data"
    mock_response.headers = {"content-type": "application/octet-stream"}

    with patch.object(client.client, "request", return_value=mock_response):
      result = await client.export_database("test_db")
      assert result == b"binary_export_data"


@pytest.mark.unit
class TestGraphClientBaseHelpers:
  """Tests for base class helper methods."""

  def test_build_url(self, mock_env):
    """_build_url constructs proper URL from base and path."""
    client = GraphClient(base_url="http://localhost:8001")
    url = client._build_url("/databases/test/query")
    assert url == "http://localhost:8001/databases/test/query"

  def test_build_url_without_leading_slash(self, mock_env):
    """_build_url handles paths without leading slash."""
    client = GraphClient(base_url="http://localhost:8001")
    url = client._build_url("health")
    assert url == "http://localhost:8001/health"

  def test_should_retry_transient_error(self, mock_env):
    """Transient errors are retriable."""
    client = GraphClient(base_url="http://localhost:8001")
    error = GraphTransientError("Temp failure")
    assert client._should_retry(error, 0) is True

  def test_should_retry_server_error(self, mock_env):
    """Server errors are retriable."""
    client = GraphClient(base_url="http://localhost:8001")
    error = GraphServerError("Server error", status_code=500)
    assert client._should_retry(error, 0) is True

  def test_should_not_retry_client_error(self, mock_env):
    """Client errors are not retriable."""
    client = GraphClient(base_url="http://localhost:8001")
    error = GraphClientError("Bad request", status_code=400)
    assert client._should_retry(error, 0) is False

  def test_should_not_retry_syntax_error(self, mock_env):
    """Syntax errors are never retriable."""
    client = GraphClient(base_url="http://localhost:8001")
    error = GraphSyntaxError("Parser exception")
    assert client._should_retry(error, 0) is False

  def test_should_not_retry_at_max(self, mock_env):
    """No retry at max retries."""
    client = GraphClient(base_url="http://localhost:8001")
    error = GraphTransientError("Temp failure")
    assert client._should_retry(error, client.config.max_retries) is False

  def test_should_not_retry_unknown_error(self, mock_env):
    """Unknown errors are not retried."""
    client = GraphClient(base_url="http://localhost:8001")
    error = RuntimeError("Unknown")
    assert client._should_retry(error, 0) is False

  @pytest.mark.real_retry_delay
  def test_calculate_retry_delay(self, mock_env):
    """Retry delay uses exponential backoff."""
    client = GraphClient(base_url="http://localhost:8001")
    client.config.retry_delay = 1.0
    client.config.retry_backoff = 2.0

    delay0 = client._calculate_retry_delay(0)
    delay1 = client._calculate_retry_delay(1)
    delay2 = client._calculate_retry_delay(2)

    # delay0 should be ~1.0 + jitter
    assert 1.0 <= delay0 <= 1.1
    # delay1 should be ~2.0 + jitter
    assert 2.0 <= delay1 <= 2.2
    # delay2 should be ~4.0 + jitter
    assert 4.0 <= delay2 <= 4.4

  def test_record_failure_opens_circuit_breaker(self, mock_env):
    """Recording enough failures opens the circuit breaker."""
    client = GraphClient(base_url="http://localhost:8001")
    client.config.circuit_breaker_threshold = 3

    for _ in range(3):
      client._record_failure()

    assert client._circuit_breaker_open is True
    assert client._circuit_breaker_failures == 3

  def test_record_success_resets_circuit_breaker(self, mock_env):
    """Recording success resets circuit breaker."""
    client = GraphClient(base_url="http://localhost:8001")
    client._circuit_breaker_failures = 3
    client._circuit_breaker_open = True

    client._record_success()

    assert client._circuit_breaker_failures == 0
    assert client._circuit_breaker_open is False

  def test_check_circuit_breaker_passes_when_closed(self, mock_env):
    """No error when circuit breaker is closed."""
    client = GraphClient(base_url="http://localhost:8001")
    client._check_circuit_breaker()  # Should not raise

  def test_check_circuit_breaker_raises_when_open(self, mock_env):
    """Raises GraphTransientError when circuit breaker is open."""
    client = GraphClient(base_url="http://localhost:8001")
    client._circuit_breaker_open = True
    client._circuit_breaker_last_failure = time.time()
    client.config.circuit_breaker_timeout = 9999

    with pytest.raises(GraphTransientError, match="Circuit breaker open"):
      client._check_circuit_breaker()
