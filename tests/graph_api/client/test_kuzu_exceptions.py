"""Tests for Kuzu API client exceptions."""

import pytest

from robosystems.graph_api.client.exceptions import (
  GraphAPIError,
  GraphTransientError,
  GraphTimeoutError,
  GraphClientError,
  GraphSyntaxError,
  GraphServerError,
)


class TestGraphAPIError:
  """Test cases for GraphAPIError base exception."""

  def test_basic_exception(self):
    """Test creating basic exception with message only."""
    error = GraphAPIError("Something went wrong")

    assert str(error) == "Something went wrong"
    assert error.status_code is None
    assert error.response_data is None

  def test_exception_with_status_code(self):
    """Test creating exception with status code."""
    error = GraphAPIError("Bad request", status_code=400)

    assert str(error) == "Bad request"
    assert error.status_code == 400
    assert error.response_data is None

  def test_exception_with_response_data(self):
    """Test creating exception with response data."""
    response_data = {"error": "Invalid query", "details": {"line": 1, "column": 10}}
    error = GraphAPIError("Query failed", status_code=422, response_data=response_data)

    assert str(error) == "Query failed"
    assert error.status_code == 422
    assert error.response_data == response_data
    assert error.response_data is not None
    assert error.response_data["error"] == "Invalid query"

  def test_exception_inheritance(self):
    """Test that GraphAPIError inherits from Exception."""
    error = GraphAPIError("Test error")

    assert isinstance(error, Exception)
    assert isinstance(error, GraphAPIError)


class TestGraphTransientError:
  """Test cases for GraphTransientError."""

  def test_transient_error_inheritance(self):
    """Test inheritance chain for transient errors."""
    error = GraphTransientError("Network issue", status_code=503)

    assert isinstance(error, GraphAPIError)
    assert isinstance(error, GraphTransientError)
    assert not isinstance(error, GraphClientError)
    assert not isinstance(error, GraphServerError)

  def test_transient_error_with_details(self):
    """Test transient error with full details."""
    error = GraphTransientError(
      "Service temporarily unavailable",
      status_code=503,
      response_data={"retry_after": 30},
    )

    assert str(error) == "Service temporarily unavailable"
    assert error.status_code == 503
    assert error.response_data is not None
    assert error.response_data["retry_after"] == 30


class TestGraphTimeoutError:
  """Test cases for GraphTimeoutError."""

  def test_timeout_error_inheritance(self):
    """Test that timeout error is a transient error."""
    error = GraphTimeoutError("Request timed out after 30s")

    assert isinstance(error, GraphAPIError)
    assert isinstance(error, GraphTransientError)
    assert isinstance(error, GraphTimeoutError)
    assert not isinstance(error, GraphClientError)

  def test_timeout_error_details(self):
    """Test timeout error with details."""
    error = GraphTimeoutError(
      "Query execution timeout",
      status_code=408,
      response_data={"timeout": 30, "query_id": "abc123"},
    )

    assert error.status_code == 408
    assert error.response_data is not None
    assert error.response_data["timeout"] == 30
    assert error.response_data["query_id"] == "abc123"


class TestGraphClientError:
  """Test cases for GraphClientError."""

  def test_client_error_inheritance(self):
    """Test inheritance for client errors."""
    error = GraphClientError("Bad request", status_code=400)

    assert isinstance(error, GraphAPIError)
    assert isinstance(error, GraphClientError)
    assert not isinstance(error, GraphTransientError)
    assert not isinstance(error, GraphServerError)

  def test_client_error_variants(self):
    """Test different client error scenarios."""
    # 400 Bad Request
    bad_request = GraphClientError("Invalid parameters", status_code=400)
    assert bad_request.status_code == 400

    # 404 Not Found
    not_found = GraphClientError("Database not found", status_code=404)
    assert not_found.status_code == 404

    # 422 Unprocessable Entity
    unprocessable = GraphClientError(
      "Validation failed",
      status_code=422,
      response_data={"errors": ["field1 required"]},
    )
    assert unprocessable.status_code == 422
    assert unprocessable.response_data is not None
    assert "errors" in unprocessable.response_data


class TestGraphSyntaxError:
  """Test cases for GraphSyntaxError."""

  def test_syntax_error_inheritance(self):
    """Test that syntax error is a client error."""
    error = GraphSyntaxError("Invalid Cypher syntax")

    assert isinstance(error, GraphAPIError)
    assert isinstance(error, GraphClientError)
    assert isinstance(error, GraphSyntaxError)
    assert not isinstance(error, GraphTransientError)

  def test_syntax_error_with_details(self):
    """Test syntax error with parsing details."""
    error = GraphSyntaxError(
      "Parser exception: unexpected token",
      status_code=400,
      response_data={
        "type": "ParserException",
        "line": 2,
        "column": 15,
        "token": "INVALID",
      },
    )

    assert error.status_code == 400
    assert error.response_data is not None
    assert error.response_data["type"] == "ParserException"
    assert error.response_data["line"] == 2
    assert error.response_data["column"] == 15

  def test_binder_exception(self):
    """Test syntax error for binder exceptions."""
    error = GraphSyntaxError(
      "Binder exception: Table 'users' does not exist",
      status_code=400,
      response_data={"type": "BinderException", "missing_table": "users"},
    )

    assert "Binder exception" in str(error)
    assert error.response_data is not None
    assert error.response_data["missing_table"] == "users"


class TestGraphServerError:
  """Test cases for GraphServerError."""

  def test_server_error_inheritance(self):
    """Test inheritance for server errors."""
    error = GraphServerError("Internal server error", status_code=500)

    assert isinstance(error, GraphAPIError)
    assert isinstance(error, GraphServerError)
    assert not isinstance(error, GraphClientError)
    assert not isinstance(error, GraphTransientError)

  def test_server_error_with_details(self):
    """Test server error with details."""
    error = GraphServerError(
      "Database connection failed",
      status_code=500,
      response_data={"error_id": "err_12345", "timestamp": "2024-01-01T10:00:00Z"},
    )

    assert error.status_code == 500
    assert error.response_data is not None
    assert error.response_data["error_id"] == "err_12345"
    assert "timestamp" in error.response_data


class TestExceptionHierarchy:
  """Test the overall exception hierarchy."""

  def test_can_distinguish_error_types(self):
    """Test that we can distinguish between error types for retry logic."""
    transient = GraphTransientError("Temporary issue")
    timeout = GraphTimeoutError("Timeout")
    client = GraphClientError("Bad request")
    syntax = GraphSyntaxError("Invalid query")
    server = GraphServerError("Server error")

    # Transient errors (can retry)
    assert isinstance(transient, GraphTransientError)
    assert isinstance(timeout, GraphTransientError)

    # Client errors (should not retry)
    assert isinstance(client, GraphClientError)
    assert isinstance(syntax, GraphClientError)

    # Server errors (might retry)
    assert isinstance(server, GraphServerError)

    # Syntax errors should never be retried
    assert isinstance(syntax, GraphSyntaxError)
    assert not isinstance(syntax, GraphTransientError)

  def test_raising_and_catching(self):
    """Test raising and catching different exception types."""
    with pytest.raises(GraphAPIError):
      raise GraphTimeoutError("Timeout occurred")

    with pytest.raises(GraphTransientError):
      raise GraphTimeoutError("Timeout occurred")

    with pytest.raises(GraphClientError):
      raise GraphSyntaxError("Invalid syntax")

    # Should not catch as transient
    with pytest.raises(GraphSyntaxError):
      try:
        raise GraphSyntaxError("Invalid syntax")
      except GraphTransientError:
        pass  # Won't catch

  def test_exception_messages(self):
    """Test that exception messages are preserved."""
    test_cases = [
      (GraphAPIError, "Base error"),
      (GraphTransientError, "Transient error"),
      (GraphTimeoutError, "Timeout error"),
      (GraphClientError, "Client error"),
      (GraphSyntaxError, "Syntax error"),
      (GraphServerError, "Server error"),
    ]

    for error_class, message in test_cases:
      error = error_class(message)
      assert str(error) == message
