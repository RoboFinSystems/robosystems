"""
Test token redaction in logging middleware.

This test ensures sensitive query parameters like tokens are never logged.
"""

import io
import logging

from starlette.datastructures import URL

from robosystems.middleware.logging import (
  SENSITIVE_QUERY_PARAMS,
  UvicornAccessRedactionFilter,
  get_safe_url_for_logging,
  install_uvicorn_log_redaction,
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


class TestUvicornAccessLogRedaction:
  """Uvicorn's own access log, which application logging cannot reach.

  The API runs with `--access-log`, and Uvicorn formats its line straight
  from the ASGI scope — the raw query string included. Every other redaction
  in this module operates on application logs, so a graph-scoped key riding
  in an MCP connector URL reached the access log intact regardless.

  These tests assert against the real `uvicorn.access` logger and the real
  record shape Uvicorn emits, so a change to either is caught here rather
  than in production logs.
  """

  @staticmethod
  def _emit(path_with_query: str) -> str:
    """Log one record exactly as Uvicorn does; return the formatted line."""
    install_uvicorn_log_redaction()

    logger = logging.getLogger("uvicorn.access")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(handler)
    previous_level, previous_propagate = logger.level, logger.propagate
    logger.setLevel(logging.INFO)
    logger.propagate = False
    try:
      # Verbatim from uvicorn/protocols/http/httptools_impl.py.
      logger.info(
        '%s - "%s %s HTTP/%s" %d',
        "10.0.0.1:54321",
        "POST",
        path_with_query,
        "1.1",
        200,
      )
    finally:
      logger.removeHandler(handler)
      logger.setLevel(previous_level)
      logger.propagate = previous_propagate
    return stream.getvalue()

  def test_a_connector_token_never_reaches_the_access_log(self):
    line = self._emit("/v1/graphs/kg1234567890abcdef/mcp?token=rfsc_supersecret_key")

    assert "rfsc_supersecret_key" not in line
    assert "token=REDACTED" in line
    # The rest of the line has to survive, or the access log stops being useful.
    assert "/v1/graphs/kg1234567890abcdef/mcp" in line
    assert "POST" in line
    assert "200" in line

  def test_the_uvicorn_scope_helper_still_produces_what_we_redact(self):
    """Guard the premise: if Uvicorn stops putting the query string in this
    argument, the filter is aimed at nothing and these tests would pass
    vacuously."""
    from uvicorn.protocols.utils import get_path_with_query_string

    raw = get_path_with_query_string(
      {"path": "/v1/graphs/kg1234567890abcdef/mcp", "query_string": b"token=secret123"}
    )
    assert raw.endswith("?token=secret123")
    assert "secret123" not in self._emit(raw)

  def test_non_sensitive_query_params_are_left_alone(self):
    line = self._emit("/v1/graphs/kg1234567890abcdef/backups?limit=50&offset=10")

    assert "limit=50" in line
    assert "offset=10" in line

  def test_a_path_with_no_query_string_is_unchanged(self):
    line = self._emit("/v1/status")

    assert "/v1/status" in line
    assert "?" not in line

  def test_installing_twice_does_not_double_filter(self):
    logger = logging.getLogger("uvicorn.access")
    install_uvicorn_log_redaction()
    install_uvicorn_log_redaction()

    installed = [
      f for f in logger.filters if isinstance(f, UvicornAccessRedactionFilter)
    ]
    assert len(installed) == 1
