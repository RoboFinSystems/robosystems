"""
Test token redaction in logging middleware.

This test ensures sensitive query parameters like tokens are never logged.
"""

from starlette.datastructures import URL

from robosystems.middleware.logging import (
  SENSITIVE_QUERY_PARAMS,
  get_safe_url_for_logging,
  redact_sensitive_query_params,
)


class TestTokenRedaction:
  """Test suite for token redaction in logging."""

  def test_redact_sensitive_query_params(self):
    """Test that sensitive query parameters are redacted."""
    # Test with token parameter
    query = "token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9&user=123"
    result = redact_sensitive_query_params(query)
    assert "token=REDACTED" in result
    assert "user=123" in result
    assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in result

    # Test with multiple sensitive params
    query = "api_key=secret123&password=pass456&normal=value"
    result = redact_sensitive_query_params(query)
    assert "api_key=REDACTED" in result
    assert "password=REDACTED" in result
    assert "normal=value" in result
    assert "secret123" not in result
    assert "pass456" not in result

  def test_redact_case_insensitive(self):
    """Test that redaction is case-insensitive."""
    query = "Token=abc&API_KEY=def&ApiKey=ghi"
    result = redact_sensitive_query_params(query)
    assert "Token=REDACTED" in result
    assert "API_KEY=REDACTED" in result
    assert "ApiKey=REDACTED" in result
    assert "abc" not in result
    assert "def" not in result
    assert "ghi" not in result

  def test_empty_query_string(self):
    """Test handling of empty query strings."""
    assert redact_sensitive_query_params("") == ""
    assert redact_sensitive_query_params(None) == ""

  def test_malformed_query_string(self):
    """Test that malformed query strings return empty string."""
    # Malformed queries should safely return empty string
    result = redact_sensitive_query_params("not=a&valid&query=string&&&")
    # Should still attempt to parse and redact
    assert result != ""

  def test_all_sensitive_params_covered(self):
    """Ensure all expected sensitive parameters are in the redaction list."""
    expected_params = {
      "token",
      "api_key",
      "apikey",
      "api-key",
      "authorization",
      "auth",
      "password",
      "secret",
      "jwt",
      "bearer",
      "access_token",
      "refresh_token",
      "session",
      "sessionid",
      "session_id",
    }
    assert expected_params.issubset(SENSITIVE_QUERY_PARAMS)

  def test_get_safe_url_for_logging(self):
    """Test safe URL generation for logging."""

    # Mock request with sensitive query parameters
    class MockRequest:
      def __init__(self, path: str, query: str | None = None):
        self.url = URL(
          f"http://example.com{path}?{query}" if query else f"http://example.com{path}"
        )

    # Test SSE endpoint with token
    request = MockRequest(
      "/v1/operations/123/stream", "token=secret_jwt_token&from_sequence=0"
    )
    safe_url = get_safe_url_for_logging(request)
    assert safe_url == "/v1/operations/123/stream?token=REDACTED&from_sequence=0"
    assert "secret_jwt_token" not in safe_url

    # Test normal endpoint without sensitive params
    request = MockRequest("/v1/user/info", "include_graphs=true")
    safe_url = get_safe_url_for_logging(request)
    assert safe_url == "/v1/user/info?include_graphs=true"

    # Test endpoint without query params
    request = MockRequest("/v1/health")
    safe_url = get_safe_url_for_logging(request)
    assert safe_url == "/v1/health"

  def test_sse_token_redaction(self):
    """Specifically test SSE endpoints with JWT tokens are redacted."""
    # Simulate SSE connection with JWT in query
    query = "token=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ.SflKxwRJSMeKKF2QT4fwpMeJf36POk6yJV_adQssw5c"
    result = redact_sensitive_query_params(query)
    assert result == "token=REDACTED"
    assert "eyJ" not in result  # JWT always starts with eyJ

    # Test with additional parameters
    query = "token=jwt_here&from_sequence=10&other=value"
    result = redact_sensitive_query_params(query)
    assert "token=REDACTED" in result
    assert "from_sequence=10" in result
    assert "other=value" in result
    assert "jwt_here" not in result
