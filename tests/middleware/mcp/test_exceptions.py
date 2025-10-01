"""Tests for MCP exceptions."""

import pytest

from robosystems.middleware.mcp.exceptions import (
  KuzuAPIError,
  KuzuQueryTimeoutError,
  KuzuQueryComplexityError,
  KuzuValidationError,
  KuzuAuthenticationError,
  KuzuAuthorizationError,
  KuzuConnectionError,
  KuzuResourceNotFoundError,
  KuzuRateLimitError,
  KuzuSchemaError,
)


class TestKuzuAPIError:
  """Test base KuzuAPIError exception."""

  def test_basic_initialization(self):
    """Test basic exception initialization."""
    error = KuzuAPIError("Test error")

    assert str(error) == "Test error"
    assert error.error_code is None
    assert error.details == {}

  def test_initialization_with_error_code(self):
    """Test exception with error code."""
    error = KuzuAPIError("Test error", error_code="TEST_ERROR")

    assert str(error) == "Test error"
    assert error.error_code == "TEST_ERROR"
    assert error.details == {}

  def test_initialization_with_details(self):
    """Test exception with details."""
    details = {"field": "value", "count": 42}
    error = KuzuAPIError("Test error", details=details)

    assert str(error) == "Test error"
    assert error.error_code is None
    assert error.details == details

  def test_initialization_with_all_params(self):
    """Test exception with all parameters."""
    details = {"debug_info": "detailed information"}
    error = KuzuAPIError("Complete error", error_code="COMPLETE", details=details)

    assert str(error) == "Complete error"
    assert error.error_code == "COMPLETE"
    assert error.details == details

  def test_details_defaults_to_empty_dict(self):
    """Test that details defaults to empty dict when None."""
    error = KuzuAPIError("Test", details=None)
    assert error.details == {}

  def test_inheritance_from_exception(self):
    """Test that KuzuAPIError inherits from Exception."""
    error = KuzuAPIError("Test error")
    assert isinstance(error, Exception)


class TestKuzuQueryTimeoutError:
  """Test KuzuQueryTimeoutError exception."""

  def test_default_initialization(self):
    """Test default timeout error."""
    error = KuzuQueryTimeoutError()

    assert str(error) == "Query execution timed out"
    assert error.error_code == "QUERY_TIMEOUT"
    assert error.details == {}

  def test_custom_message(self):
    """Test timeout error with custom message."""
    error = KuzuQueryTimeoutError("Custom timeout message")

    assert str(error) == "Custom timeout message"
    assert error.error_code == "QUERY_TIMEOUT"

  def test_with_timeout_seconds(self):
    """Test timeout error with timeout duration."""
    error = KuzuQueryTimeoutError(timeout_seconds=30)

    assert error.details["timeout_seconds"] == 30
    assert error.error_code == "QUERY_TIMEOUT"

  def test_custom_message_and_timeout(self):
    """Test timeout error with both custom message and timeout."""
    error = KuzuQueryTimeoutError("Query took too long", timeout_seconds=60)

    assert str(error) == "Query took too long"
    assert error.details["timeout_seconds"] == 60

  def test_inheritance_from_kuzu_api_error(self):
    """Test inheritance from KuzuAPIError."""
    error = KuzuQueryTimeoutError()
    assert isinstance(error, KuzuAPIError)


class TestKuzuQueryComplexityError:
  """Test KuzuQueryComplexityError exception."""

  def test_default_initialization(self):
    """Test default complexity error."""
    error = KuzuQueryComplexityError()

    assert str(error) == "Query is too complex"
    assert error.error_code == "QUERY_COMPLEXITY"
    assert error.details == {}

  def test_custom_message(self):
    """Test complexity error with custom message."""
    error = KuzuQueryComplexityError("Query exceeds complexity limits")

    assert str(error) == "Query exceeds complexity limits"

  def test_with_complexity_score(self):
    """Test complexity error with score."""
    error = KuzuQueryComplexityError(complexity_score=150)

    assert error.details["complexity_score"] == 150

  def test_custom_message_and_score(self):
    """Test complexity error with message and score."""
    error = KuzuQueryComplexityError("Too complex", complexity_score=200)

    assert str(error) == "Too complex"
    assert error.details["complexity_score"] == 200

  def test_inheritance_from_kuzu_api_error(self):
    """Test inheritance from KuzuAPIError."""
    error = KuzuQueryComplexityError()
    assert isinstance(error, KuzuAPIError)


class TestKuzuValidationError:
  """Test KuzuValidationError exception."""

  def test_default_initialization(self):
    """Test default validation error."""
    error = KuzuValidationError()

    assert str(error) == "Query validation failed"
    assert error.error_code == "QUERY_VALIDATION"
    assert error.details == {}

  def test_custom_message(self):
    """Test validation error with custom message."""
    error = KuzuValidationError("Invalid query syntax")

    assert str(error) == "Invalid query syntax"

  def test_with_validation_errors(self):
    """Test validation error with error list."""
    validation_errors = ["Missing RETURN clause", "Invalid node syntax"]
    error = KuzuValidationError(validation_errors=validation_errors)

    assert error.details["validation_errors"] == validation_errors

  def test_custom_message_and_errors(self):
    """Test validation error with message and errors."""
    errors = ["Syntax error on line 1"]
    error = KuzuValidationError("Validation failed", validation_errors=errors)

    assert str(error) == "Validation failed"
    assert error.details["validation_errors"] == errors

  def test_inheritance_from_kuzu_api_error(self):
    """Test inheritance from KuzuAPIError."""
    error = KuzuValidationError()
    assert isinstance(error, KuzuAPIError)


class TestKuzuAuthenticationError:
  """Test KuzuAuthenticationError exception."""

  def test_default_initialization(self):
    """Test default authentication error."""
    error = KuzuAuthenticationError()

    assert str(error) == "Authentication failed"
    assert error.error_code == "AUTH_FAILED"
    assert error.details == {}

  def test_custom_message(self):
    """Test authentication error with custom message."""
    error = KuzuAuthenticationError("Invalid credentials")

    assert str(error) == "Invalid credentials"
    assert error.error_code == "AUTH_FAILED"

  def test_inheritance_from_kuzu_api_error(self):
    """Test inheritance from KuzuAPIError."""
    error = KuzuAuthenticationError()
    assert isinstance(error, KuzuAPIError)


class TestKuzuAuthorizationError:
  """Test KuzuAuthorizationError exception."""

  def test_default_initialization(self):
    """Test default authorization error."""
    error = KuzuAuthorizationError()

    assert str(error) == "Authorization failed"
    assert error.error_code == "AUTH_DENIED"
    assert error.details == {}

  def test_custom_message(self):
    """Test authorization error with custom message."""
    error = KuzuAuthorizationError("Insufficient permissions")

    assert str(error) == "Insufficient permissions"

  def test_with_required_permission(self):
    """Test authorization error with required permission."""
    error = KuzuAuthorizationError(required_permission="admin:write")

    assert error.details["required_permission"] == "admin:write"

  def test_custom_message_and_permission(self):
    """Test authorization error with message and permission."""
    error = KuzuAuthorizationError("Access denied", required_permission="user:read")

    assert str(error) == "Access denied"
    assert error.details["required_permission"] == "user:read"

  def test_inheritance_from_kuzu_api_error(self):
    """Test inheritance from KuzuAPIError."""
    error = KuzuAuthorizationError()
    assert isinstance(error, KuzuAPIError)


class TestKuzuConnectionError:
  """Test KuzuConnectionError exception."""

  def test_default_initialization(self):
    """Test default connection error."""
    error = KuzuConnectionError()

    assert str(error) == "Connection failed"
    assert error.error_code == "CONNECTION_FAILED"
    assert error.details == {}

  def test_custom_message(self):
    """Test connection error with custom message."""
    error = KuzuConnectionError("Network timeout")

    assert str(error) == "Network timeout"

  def test_with_endpoint(self):
    """Test connection error with endpoint."""
    error = KuzuConnectionError(endpoint="https://api.example.com")

    assert error.details["endpoint"] == "https://api.example.com"

  def test_custom_message_and_endpoint(self):
    """Test connection error with message and endpoint."""
    error = KuzuConnectionError("Failed to connect", endpoint="api.test.com")

    assert str(error) == "Failed to connect"
    assert error.details["endpoint"] == "api.test.com"

  def test_inheritance_from_kuzu_api_error(self):
    """Test inheritance from KuzuAPIError."""
    error = KuzuConnectionError()
    assert isinstance(error, KuzuAPIError)


class TestKuzuResourceNotFoundError:
  """Test KuzuResourceNotFoundError exception."""

  def test_default_initialization(self):
    """Test default resource not found error."""
    error = KuzuResourceNotFoundError()

    assert str(error) == "Resource not found"
    assert error.error_code == "RESOURCE_NOT_FOUND"
    assert error.details == {}

  def test_custom_message(self):
    """Test resource not found error with custom message."""
    error = KuzuResourceNotFoundError("Database not found")

    assert str(error) == "Database not found"

  def test_with_resource_type(self):
    """Test error with resource type."""
    error = KuzuResourceNotFoundError(resource_type="database")

    assert error.details["resource_type"] == "database"

  def test_with_resource_id(self):
    """Test error with resource ID."""
    error = KuzuResourceNotFoundError(resource_id="kg123abc")

    assert error.details["resource_id"] == "kg123abc"

  def test_with_type_and_id(self):
    """Test error with both resource type and ID."""
    error = KuzuResourceNotFoundError(resource_type="graph", resource_id="kg456def")

    assert error.details["resource_type"] == "graph"
    assert error.details["resource_id"] == "kg456def"

  def test_all_parameters(self):
    """Test error with all parameters."""
    error = KuzuResourceNotFoundError(
      "Graph database not found", resource_type="graph", resource_id="kg789ghi"
    )

    assert str(error) == "Graph database not found"
    assert error.details["resource_type"] == "graph"
    assert error.details["resource_id"] == "kg789ghi"

  def test_inheritance_from_kuzu_api_error(self):
    """Test inheritance from KuzuAPIError."""
    error = KuzuResourceNotFoundError()
    assert isinstance(error, KuzuAPIError)


class TestKuzuRateLimitError:
  """Test KuzuRateLimitError exception."""

  def test_default_initialization(self):
    """Test default rate limit error."""
    error = KuzuRateLimitError()

    assert str(error) == "Rate limit exceeded"
    assert error.error_code == "RATE_LIMIT"
    assert error.details == {}

  def test_custom_message(self):
    """Test rate limit error with custom message."""
    error = KuzuRateLimitError("Too many requests")

    assert str(error) == "Too many requests"

  def test_with_retry_after(self):
    """Test rate limit error with retry after."""
    error = KuzuRateLimitError(retry_after_seconds=60)

    assert error.details["retry_after_seconds"] == 60

  def test_custom_message_and_retry_after(self):
    """Test rate limit error with message and retry after."""
    error = KuzuRateLimitError("Rate limited", retry_after_seconds=300)

    assert str(error) == "Rate limited"
    assert error.details["retry_after_seconds"] == 300

  def test_inheritance_from_kuzu_api_error(self):
    """Test inheritance from KuzuAPIError."""
    error = KuzuRateLimitError()
    assert isinstance(error, KuzuAPIError)


class TestKuzuSchemaError:
  """Test KuzuSchemaError exception."""

  def test_default_initialization(self):
    """Test default schema error."""
    error = KuzuSchemaError()

    assert str(error) == "Schema error"
    assert error.error_code == "SCHEMA_ERROR"
    assert error.details == {}

  def test_custom_message(self):
    """Test schema error with custom message."""
    error = KuzuSchemaError("Invalid schema definition")

    assert str(error) == "Invalid schema definition"

  def test_with_schema_item(self):
    """Test schema error with schema item."""
    error = KuzuSchemaError(schema_item="Entity")

    assert error.details["schema_item"] == "Entity"

  def test_custom_message_and_schema_item(self):
    """Test schema error with message and schema item."""
    error = KuzuSchemaError("Node type not found", schema_item="Person")

    assert str(error) == "Node type not found"
    assert error.details["schema_item"] == "Person"

  def test_inheritance_from_kuzu_api_error(self):
    """Test inheritance from KuzuAPIError."""
    error = KuzuSchemaError()
    assert isinstance(error, KuzuAPIError)


class TestExceptionHierarchy:
  """Test exception inheritance hierarchy."""

  def test_all_exceptions_inherit_from_kuzu_api_error(self):
    """Test that all custom exceptions inherit from KuzuAPIError."""
    exception_classes = [
      KuzuQueryTimeoutError,
      KuzuQueryComplexityError,
      KuzuValidationError,
      KuzuAuthenticationError,
      KuzuAuthorizationError,
      KuzuConnectionError,
      KuzuResourceNotFoundError,
      KuzuRateLimitError,
      KuzuSchemaError,
    ]

    for exception_class in exception_classes:
      error = exception_class()
      assert isinstance(error, KuzuAPIError)
      assert isinstance(error, Exception)

  def test_all_exceptions_have_error_codes(self):
    """Test that all exceptions have appropriate error codes."""
    expected_codes = {
      KuzuQueryTimeoutError: "QUERY_TIMEOUT",
      KuzuQueryComplexityError: "QUERY_COMPLEXITY",
      KuzuValidationError: "QUERY_VALIDATION",
      KuzuAuthenticationError: "AUTH_FAILED",
      KuzuAuthorizationError: "AUTH_DENIED",
      KuzuConnectionError: "CONNECTION_FAILED",
      KuzuResourceNotFoundError: "RESOURCE_NOT_FOUND",
      KuzuRateLimitError: "RATE_LIMIT",
      KuzuSchemaError: "SCHEMA_ERROR",
    }

    for exception_class, expected_code in expected_codes.items():
      error = exception_class()
      assert error.error_code == expected_code

  def test_exception_catching_hierarchy(self):
    """Test that specific exceptions can be caught as KuzuAPIError."""
    exceptions = [
      KuzuQueryTimeoutError("timeout"),
      KuzuValidationError("validation"),
      KuzuAuthenticationError("auth"),
      KuzuConnectionError("connection"),
      KuzuSchemaError("schema"),
    ]

    for exc in exceptions:
      try:
        raise exc
      except KuzuAPIError as caught:
        assert caught is exc
        assert hasattr(caught, "error_code")
        assert hasattr(caught, "details")


class TestExceptionUsage:
  """Test practical exception usage scenarios."""

  def test_raising_and_catching_timeout_error(self):
    """Test raising and catching timeout error."""
    with pytest.raises(KuzuQueryTimeoutError) as exc_info:
      raise KuzuQueryTimeoutError("Query timed out", timeout_seconds=30)

    error = exc_info.value
    assert str(error) == "Query timed out"
    assert error.details["timeout_seconds"] == 30

  def test_raising_and_catching_validation_error(self):
    """Test raising and catching validation error."""
    validation_errors = ["Missing RETURN", "Invalid syntax"]

    with pytest.raises(KuzuValidationError) as exc_info:
      raise KuzuValidationError(
        "Validation failed", validation_errors=validation_errors
      )

    error = exc_info.value
    assert error.details["validation_errors"] == validation_errors

  def test_catching_as_base_exception(self):
    """Test catching specific exception as base KuzuAPIError."""
    with pytest.raises(KuzuAPIError) as exc_info:
      raise KuzuAuthorizationError("Access denied", required_permission="admin")

    error = exc_info.value
    assert isinstance(error, KuzuAuthorizationError)
    assert error.error_code == "AUTH_DENIED"
    assert error.details["required_permission"] == "admin"

  def test_error_details_modification(self):
    """Test that error details can be modified after creation."""
    error = KuzuAPIError("Test error")
    assert error.details == {}

    # Modify details
    error.details["custom_field"] = "custom_value"
    assert error.details["custom_field"] == "custom_value"

  def test_error_details_immutability_of_defaults(self):
    """Test that default empty dicts don't affect other instances."""
    error1 = KuzuAPIError("Error 1")
    error2 = KuzuAPIError("Error 2")

    error1.details["field1"] = "value1"
    error2.details["field2"] = "value2"

    assert "field1" in error1.details
    assert "field1" not in error2.details
    assert "field2" in error2.details
    assert "field2" not in error1.details
