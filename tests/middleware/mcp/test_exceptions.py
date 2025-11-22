"""Tests for MCP exceptions."""

import pytest

from robosystems.middleware.mcp.exceptions import (
  GraphAPIError,
  GraphQueryTimeoutError,
  GraphQueryComplexityError,
  GraphValidationError,
  GraphAuthenticationError,
  GraphAuthorizationError,
  GraphConnectionError,
  GraphResourceNotFoundError,
  GraphRateLimitError,
  GraphSchemaError,
)


class TestGraphAPIError:
  """Test base GraphAPIError exception."""

  def test_basic_initialization(self):
    """Test basic exception initialization."""
    error = GraphAPIError("Test error")

    assert str(error) == "Test error"
    assert error.error_code is None
    assert error.details == {}

  def test_initialization_with_error_code(self):
    """Test exception with error code."""
    error = GraphAPIError("Test error", error_code="TEST_ERROR")

    assert str(error) == "Test error"
    assert error.error_code == "TEST_ERROR"
    assert error.details == {}

  def test_initialization_with_details(self):
    """Test exception with details."""
    details = {"field": "value", "count": 42}
    error = GraphAPIError("Test error", details=details)

    assert str(error) == "Test error"
    assert error.error_code is None
    assert error.details == details

  def test_initialization_with_all_params(self):
    """Test exception with all parameters."""
    details = {"debug_info": "detailed information"}
    error = GraphAPIError("Complete error", error_code="COMPLETE", details=details)

    assert str(error) == "Complete error"
    assert error.error_code == "COMPLETE"
    assert error.details == details

  def test_details_defaults_to_empty_dict(self):
    """Test that details defaults to empty dict when None."""
    error = GraphAPIError("Test", details=None)
    assert error.details == {}

  def test_inheritance_from_exception(self):
    """Test that GraphAPIError inherits from Exception."""
    error = GraphAPIError("Test error")
    assert isinstance(error, Exception)


class TestGraphQueryTimeoutError:
  """Test GraphQueryTimeoutError exception."""

  def test_default_initialization(self):
    """Test default timeout error."""
    error = GraphQueryTimeoutError()

    assert str(error) == "Query execution timed out"
    assert error.error_code == "QUERY_TIMEOUT"
    assert error.details == {}

  def test_custom_message(self):
    """Test timeout error with custom message."""
    error = GraphQueryTimeoutError("Custom timeout message")

    assert str(error) == "Custom timeout message"
    assert error.error_code == "QUERY_TIMEOUT"

  def test_with_timeout_seconds(self):
    """Test timeout error with timeout duration."""
    error = GraphQueryTimeoutError(timeout_seconds=30)

    assert error.details["timeout_seconds"] == 30
    assert error.error_code == "QUERY_TIMEOUT"

  def test_custom_message_and_timeout(self):
    """Test timeout error with both custom message and timeout."""
    error = GraphQueryTimeoutError("Query took too long", timeout_seconds=60)

    assert str(error) == "Query took too long"
    assert error.details["timeout_seconds"] == 60

  def test_inheritance_from_graph_api_error(self):
    """Test inheritance from GraphAPIError."""
    error = GraphQueryTimeoutError()
    assert isinstance(error, GraphAPIError)


class TestGraphQueryComplexityError:
  """Test GraphQueryComplexityError exception."""

  def test_default_initialization(self):
    """Test default complexity error."""
    error = GraphQueryComplexityError()

    assert str(error) == "Query is too complex"
    assert error.error_code == "QUERY_COMPLEXITY"
    assert error.details == {}

  def test_custom_message(self):
    """Test complexity error with custom message."""
    error = GraphQueryComplexityError("Query exceeds complexity limits")

    assert str(error) == "Query exceeds complexity limits"

  def test_with_complexity_score(self):
    """Test complexity error with score."""
    error = GraphQueryComplexityError(complexity_score=150)

    assert error.details["complexity_score"] == 150

  def test_custom_message_and_score(self):
    """Test complexity error with message and score."""
    error = GraphQueryComplexityError("Too complex", complexity_score=200)

    assert str(error) == "Too complex"
    assert error.details["complexity_score"] == 200

  def test_inheritance_from_graph_api_error(self):
    """Test inheritance from GraphAPIError."""
    error = GraphQueryComplexityError()
    assert isinstance(error, GraphAPIError)


class TestGraphValidationError:
  """Test GraphValidationError exception."""

  def test_default_initialization(self):
    """Test default validation error."""
    error = GraphValidationError()

    assert str(error) == "Query validation failed"
    assert error.error_code == "QUERY_VALIDATION"
    assert error.details == {}

  def test_custom_message(self):
    """Test validation error with custom message."""
    error = GraphValidationError("Invalid query syntax")

    assert str(error) == "Invalid query syntax"

  def test_with_validation_errors(self):
    """Test validation error with error list."""
    validation_errors = ["Missing RETURN clause", "Invalid node syntax"]
    error = GraphValidationError(validation_errors=validation_errors)

    assert error.details["validation_errors"] == validation_errors

  def test_custom_message_and_errors(self):
    """Test validation error with message and errors."""
    errors = ["Syntax error on line 1"]
    error = GraphValidationError("Validation failed", validation_errors=errors)

    assert str(error) == "Validation failed"
    assert error.details["validation_errors"] == errors

  def test_inheritance_from_graph_api_error(self):
    """Test inheritance from GraphAPIError."""
    error = GraphValidationError()
    assert isinstance(error, GraphAPIError)


class TestGraphAuthenticationError:
  """Test GraphAuthenticationError exception."""

  def test_default_initialization(self):
    """Test default authentication error."""
    error = GraphAuthenticationError()

    assert str(error) == "Authentication failed"
    assert error.error_code == "AUTH_FAILED"
    assert error.details == {}

  def test_custom_message(self):
    """Test authentication error with custom message."""
    error = GraphAuthenticationError("Invalid credentials")

    assert str(error) == "Invalid credentials"
    assert error.error_code == "AUTH_FAILED"

  def test_inheritance_from_graph_api_error(self):
    """Test inheritance from GraphAPIError."""
    error = GraphAuthenticationError()
    assert isinstance(error, GraphAPIError)


class TestGraphAuthorizationError:
  """Test GraphAuthorizationError exception."""

  def test_default_initialization(self):
    """Test default authorization error."""
    error = GraphAuthorizationError()

    assert str(error) == "Authorization failed"
    assert error.error_code == "AUTH_DENIED"
    assert error.details == {}

  def test_custom_message(self):
    """Test authorization error with custom message."""
    error = GraphAuthorizationError("Insufficient permissions")

    assert str(error) == "Insufficient permissions"

  def test_with_required_permission(self):
    """Test authorization error with required permission."""
    error = GraphAuthorizationError(required_permission="admin:write")

    assert error.details["required_permission"] == "admin:write"

  def test_custom_message_and_permission(self):
    """Test authorization error with message and permission."""
    error = GraphAuthorizationError("Access denied", required_permission="user:read")

    assert str(error) == "Access denied"
    assert error.details["required_permission"] == "user:read"

  def test_inheritance_from_graph_api_error(self):
    """Test inheritance from GraphAPIError."""
    error = GraphAuthorizationError()
    assert isinstance(error, GraphAPIError)


class TestGraphConnectionError:
  """Test GraphConnectionError exception."""

  def test_default_initialization(self):
    """Test default connection error."""
    error = GraphConnectionError()

    assert str(error) == "Connection failed"
    assert error.error_code == "CONNECTION_FAILED"
    assert error.details == {}

  def test_custom_message(self):
    """Test connection error with custom message."""
    error = GraphConnectionError("Network timeout")

    assert str(error) == "Network timeout"

  def test_with_endpoint(self):
    """Test connection error with endpoint."""
    error = GraphConnectionError(endpoint="https://api.example.com")

    assert error.details["endpoint"] == "https://api.example.com"

  def test_custom_message_and_endpoint(self):
    """Test connection error with message and endpoint."""
    error = GraphConnectionError("Failed to connect", endpoint="api.test.com")

    assert str(error) == "Failed to connect"
    assert error.details["endpoint"] == "api.test.com"

  def test_inheritance_from_graph_api_error(self):
    """Test inheritance from GraphAPIError."""
    error = GraphConnectionError()
    assert isinstance(error, GraphAPIError)


class TestGraphResourceNotFoundError:
  """Test GraphResourceNotFoundError exception."""

  def test_default_initialization(self):
    """Test default resource not found error."""
    error = GraphResourceNotFoundError()

    assert str(error) == "Resource not found"
    assert error.error_code == "RESOURCE_NOT_FOUND"
    assert error.details == {}

  def test_custom_message(self):
    """Test resource not found error with custom message."""
    error = GraphResourceNotFoundError("Database not found")

    assert str(error) == "Database not found"

  def test_with_resource_type(self):
    """Test error with resource type."""
    error = GraphResourceNotFoundError(resource_type="database")

    assert error.details["resource_type"] == "database"

  def test_with_resource_id(self):
    """Test error with resource ID."""
    error = GraphResourceNotFoundError(resource_id="kg123abc")

    assert error.details["resource_id"] == "kg123abc"

  def test_with_type_and_id(self):
    """Test error with both resource type and ID."""
    error = GraphResourceNotFoundError(resource_type="graph", resource_id="kg456def")

    assert error.details["resource_type"] == "graph"
    assert error.details["resource_id"] == "kg456def"

  def test_all_parameters(self):
    """Test error with all parameters."""
    error = GraphResourceNotFoundError(
      "Graph database not found", resource_type="graph", resource_id="kg789ghi"
    )

    assert str(error) == "Graph database not found"
    assert error.details["resource_type"] == "graph"
    assert error.details["resource_id"] == "kg789ghi"

  def test_inheritance_from_graph_api_error(self):
    """Test inheritance from GraphAPIError."""
    error = GraphResourceNotFoundError()
    assert isinstance(error, GraphAPIError)


class TestGraphRateLimitError:
  """Test GraphRateLimitError exception."""

  def test_default_initialization(self):
    """Test default rate limit error."""
    error = GraphRateLimitError()

    assert str(error) == "Rate limit exceeded"
    assert error.error_code == "RATE_LIMIT"
    assert error.details == {}

  def test_custom_message(self):
    """Test rate limit error with custom message."""
    error = GraphRateLimitError("Too many requests")

    assert str(error) == "Too many requests"

  def test_with_retry_after(self):
    """Test rate limit error with retry after."""
    error = GraphRateLimitError(retry_after_seconds=60)

    assert error.details["retry_after_seconds"] == 60

  def test_custom_message_and_retry_after(self):
    """Test rate limit error with message and retry after."""
    error = GraphRateLimitError("Rate limited", retry_after_seconds=300)

    assert str(error) == "Rate limited"
    assert error.details["retry_after_seconds"] == 300

  def test_inheritance_from_graph_api_error(self):
    """Test inheritance from GraphAPIError."""
    error = GraphRateLimitError()
    assert isinstance(error, GraphAPIError)


class TestGraphSchemaError:
  """Test GraphSchemaError exception."""

  def test_default_initialization(self):
    """Test default schema error."""
    error = GraphSchemaError()

    assert str(error) == "Schema error"
    assert error.error_code == "SCHEMA_ERROR"
    assert error.details == {}

  def test_custom_message(self):
    """Test schema error with custom message."""
    error = GraphSchemaError("Invalid schema definition")

    assert str(error) == "Invalid schema definition"

  def test_with_schema_item(self):
    """Test schema error with schema item."""
    error = GraphSchemaError(schema_item="Entity")

    assert error.details["schema_item"] == "Entity"

  def test_custom_message_and_schema_item(self):
    """Test schema error with message and schema item."""
    error = GraphSchemaError("Node type not found", schema_item="Person")

    assert str(error) == "Node type not found"
    assert error.details["schema_item"] == "Person"

  def test_inheritance_from_graph_api_error(self):
    """Test inheritance from GraphAPIError."""
    error = GraphSchemaError()
    assert isinstance(error, GraphAPIError)


class TestExceptionHierarchy:
  """Test exception inheritance hierarchy."""

  def test_all_exceptions_inherit_from_graph_api_error(self):
    """Test that all custom exceptions inherit from GraphAPIError."""
    exception_classes = [
      GraphQueryTimeoutError,
      GraphQueryComplexityError,
      GraphValidationError,
      GraphAuthenticationError,
      GraphAuthorizationError,
      GraphConnectionError,
      GraphResourceNotFoundError,
      GraphRateLimitError,
      GraphSchemaError,
    ]

    for exception_class in exception_classes:
      error = exception_class()
      assert isinstance(error, GraphAPIError)
      assert isinstance(error, Exception)

  def test_all_exceptions_have_error_codes(self):
    """Test that all exceptions have appropriate error codes."""
    expected_codes = {
      GraphQueryTimeoutError: "QUERY_TIMEOUT",
      GraphQueryComplexityError: "QUERY_COMPLEXITY",
      GraphValidationError: "QUERY_VALIDATION",
      GraphAuthenticationError: "AUTH_FAILED",
      GraphAuthorizationError: "AUTH_DENIED",
      GraphConnectionError: "CONNECTION_FAILED",
      GraphResourceNotFoundError: "RESOURCE_NOT_FOUND",
      GraphRateLimitError: "RATE_LIMIT",
      GraphSchemaError: "SCHEMA_ERROR",
    }

    for exception_class, expected_code in expected_codes.items():
      error = exception_class()
      assert error.error_code == expected_code

  def test_exception_catching_hierarchy(self):
    """Test that specific exceptions can be caught as GraphAPIError."""
    exceptions = [
      GraphQueryTimeoutError("timeout"),
      GraphValidationError("validation"),
      GraphAuthenticationError("auth"),
      GraphConnectionError("connection"),
      GraphSchemaError("schema"),
    ]

    for exc in exceptions:
      try:
        raise exc
      except GraphAPIError as caught:
        assert caught is exc
        assert hasattr(caught, "error_code")
        assert hasattr(caught, "details")


class TestExceptionUsage:
  """Test practical exception usage scenarios."""

  def test_raising_and_catching_timeout_error(self):
    """Test raising and catching timeout error."""
    with pytest.raises(GraphQueryTimeoutError) as exc_info:
      raise GraphQueryTimeoutError("Query timed out", timeout_seconds=30)

    error = exc_info.value
    assert str(error) == "Query timed out"
    assert error.details["timeout_seconds"] == 30

  def test_raising_and_catching_validation_error(self):
    """Test raising and catching validation error."""
    validation_errors = ["Missing RETURN", "Invalid syntax"]

    with pytest.raises(GraphValidationError) as exc_info:
      raise GraphValidationError(
        "Validation failed", validation_errors=validation_errors
      )

    error = exc_info.value
    assert error.details["validation_errors"] == validation_errors

  def test_catching_as_base_exception(self):
    """Test catching specific exception as base GraphAPIError."""
    with pytest.raises(GraphAPIError) as exc_info:
      raise GraphAuthorizationError("Access denied", required_permission="admin")

    error = exc_info.value
    assert isinstance(error, GraphAuthorizationError)
    assert error.error_code == "AUTH_DENIED"
    assert error.details["required_permission"] == "admin"

  def test_error_details_modification(self):
    """Test that error details can be modified after creation."""
    error = GraphAPIError("Test error")
    assert error.details == {}

    # Modify details
    error.details["custom_field"] = "custom_value"
    assert error.details["custom_field"] == "custom_value"

  def test_error_details_immutability_of_defaults(self):
    """Test that default empty dicts don't affect other instances."""
    error1 = GraphAPIError("Error 1")
    error2 = GraphAPIError("Error 2")

    error1.details["field1"] = "value1"
    error2.details["field2"] = "value2"

    assert "field1" in error1.details
    assert "field1" not in error2.details
    assert "field2" in error2.details
    assert "field2" not in error1.details
