"""Tests for Kuzu API client exceptions."""

import pytest

from robosystems.kuzu_api.client.exceptions import (
  KuzuAPIError,
  KuzuTransientError,
  KuzuTimeoutError,
  KuzuClientError,
  KuzuSyntaxError,
  KuzuServerError,
)


class TestKuzuAPIError:
  """Test cases for KuzuAPIError base exception."""

  def test_basic_exception(self):
    """Test creating basic exception with message only."""
    error = KuzuAPIError("Something went wrong")

    assert str(error) == "Something went wrong"
    assert error.status_code is None
    assert error.response_data is None

  def test_exception_with_status_code(self):
    """Test creating exception with status code."""
    error = KuzuAPIError("Bad request", status_code=400)

    assert str(error) == "Bad request"
    assert error.status_code == 400
    assert error.response_data is None

  def test_exception_with_response_data(self):
    """Test creating exception with response data."""
    response_data = {"error": "Invalid query", "details": {"line": 1, "column": 10}}
    error = KuzuAPIError("Query failed", status_code=422, response_data=response_data)

    assert str(error) == "Query failed"
    assert error.status_code == 422
    assert error.response_data == response_data
    assert error.response_data is not None
    assert error.response_data["error"] == "Invalid query"

  def test_exception_inheritance(self):
    """Test that KuzuAPIError inherits from Exception."""
    error = KuzuAPIError("Test error")

    assert isinstance(error, Exception)
    assert isinstance(error, KuzuAPIError)


class TestKuzuTransientError:
  """Test cases for KuzuTransientError."""

  def test_transient_error_inheritance(self):
    """Test inheritance chain for transient errors."""
    error = KuzuTransientError("Network issue", status_code=503)

    assert isinstance(error, KuzuAPIError)
    assert isinstance(error, KuzuTransientError)
    assert not isinstance(error, KuzuClientError)
    assert not isinstance(error, KuzuServerError)

  def test_transient_error_with_details(self):
    """Test transient error with full details."""
    error = KuzuTransientError(
      "Service temporarily unavailable",
      status_code=503,
      response_data={"retry_after": 30},
    )

    assert str(error) == "Service temporarily unavailable"
    assert error.status_code == 503
    assert error.response_data is not None
    assert error.response_data["retry_after"] == 30


class TestKuzuTimeoutError:
  """Test cases for KuzuTimeoutError."""

  def test_timeout_error_inheritance(self):
    """Test that timeout error is a transient error."""
    error = KuzuTimeoutError("Request timed out after 30s")

    assert isinstance(error, KuzuAPIError)
    assert isinstance(error, KuzuTransientError)
    assert isinstance(error, KuzuTimeoutError)
    assert not isinstance(error, KuzuClientError)

  def test_timeout_error_details(self):
    """Test timeout error with details."""
    error = KuzuTimeoutError(
      "Query execution timeout",
      status_code=408,
      response_data={"timeout": 30, "query_id": "abc123"},
    )

    assert error.status_code == 408
    assert error.response_data is not None
    assert error.response_data["timeout"] == 30
    assert error.response_data["query_id"] == "abc123"


class TestKuzuClientError:
  """Test cases for KuzuClientError."""

  def test_client_error_inheritance(self):
    """Test inheritance for client errors."""
    error = KuzuClientError("Bad request", status_code=400)

    assert isinstance(error, KuzuAPIError)
    assert isinstance(error, KuzuClientError)
    assert not isinstance(error, KuzuTransientError)
    assert not isinstance(error, KuzuServerError)

  def test_client_error_variants(self):
    """Test different client error scenarios."""
    # 400 Bad Request
    bad_request = KuzuClientError("Invalid parameters", status_code=400)
    assert bad_request.status_code == 400

    # 404 Not Found
    not_found = KuzuClientError("Database not found", status_code=404)
    assert not_found.status_code == 404

    # 422 Unprocessable Entity
    unprocessable = KuzuClientError(
      "Validation failed",
      status_code=422,
      response_data={"errors": ["field1 required"]},
    )
    assert unprocessable.status_code == 422
    assert unprocessable.response_data is not None
    assert "errors" in unprocessable.response_data


class TestKuzuSyntaxError:
  """Test cases for KuzuSyntaxError."""

  def test_syntax_error_inheritance(self):
    """Test that syntax error is a client error."""
    error = KuzuSyntaxError("Invalid Cypher syntax")

    assert isinstance(error, KuzuAPIError)
    assert isinstance(error, KuzuClientError)
    assert isinstance(error, KuzuSyntaxError)
    assert not isinstance(error, KuzuTransientError)

  def test_syntax_error_with_details(self):
    """Test syntax error with parsing details."""
    error = KuzuSyntaxError(
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
    error = KuzuSyntaxError(
      "Binder exception: Table 'users' does not exist",
      status_code=400,
      response_data={"type": "BinderException", "missing_table": "users"},
    )

    assert "Binder exception" in str(error)
    assert error.response_data is not None
    assert error.response_data["missing_table"] == "users"


class TestKuzuServerError:
  """Test cases for KuzuServerError."""

  def test_server_error_inheritance(self):
    """Test inheritance for server errors."""
    error = KuzuServerError("Internal server error", status_code=500)

    assert isinstance(error, KuzuAPIError)
    assert isinstance(error, KuzuServerError)
    assert not isinstance(error, KuzuClientError)
    assert not isinstance(error, KuzuTransientError)

  def test_server_error_with_details(self):
    """Test server error with details."""
    error = KuzuServerError(
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
    transient = KuzuTransientError("Temporary issue")
    timeout = KuzuTimeoutError("Timeout")
    client = KuzuClientError("Bad request")
    syntax = KuzuSyntaxError("Invalid query")
    server = KuzuServerError("Server error")

    # Transient errors (can retry)
    assert isinstance(transient, KuzuTransientError)
    assert isinstance(timeout, KuzuTransientError)

    # Client errors (should not retry)
    assert isinstance(client, KuzuClientError)
    assert isinstance(syntax, KuzuClientError)

    # Server errors (might retry)
    assert isinstance(server, KuzuServerError)

    # Syntax errors should never be retried
    assert isinstance(syntax, KuzuSyntaxError)
    assert not isinstance(syntax, KuzuTransientError)

  def test_raising_and_catching(self):
    """Test raising and catching different exception types."""
    with pytest.raises(KuzuAPIError):
      raise KuzuTimeoutError("Timeout occurred")

    with pytest.raises(KuzuTransientError):
      raise KuzuTimeoutError("Timeout occurred")

    with pytest.raises(KuzuClientError):
      raise KuzuSyntaxError("Invalid syntax")

    # Should not catch as transient
    with pytest.raises(KuzuSyntaxError):
      try:
        raise KuzuSyntaxError("Invalid syntax")
      except KuzuTransientError:
        pass  # Won't catch

  def test_exception_messages(self):
    """Test that exception messages are preserved."""
    test_cases = [
      (KuzuAPIError, "Base error"),
      (KuzuTransientError, "Transient error"),
      (KuzuTimeoutError, "Timeout error"),
      (KuzuClientError, "Client error"),
      (KuzuSyntaxError, "Syntax error"),
      (KuzuServerError, "Server error"),
    ]

    for error_class, message in test_cases:
      error = error_class(message)
      assert str(error) == message
