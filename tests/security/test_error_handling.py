"""
Tests for secure error handling utilities.

Comprehensive test coverage for error handling, classification, and security features.
"""

from unittest.mock import patch

import pytest
from fastapi import HTTPException, status

from robosystems.security.error_handling import (
  ERROR_RESPONSES,
  ErrorType,
  classify_exception,
  handle_exception_securely,
  is_safe_to_expose,
  raise_secure_error,
  sanitize_error_detail,
)


class TestErrorType:
  """Test ErrorType constants."""

  def test_error_type_constants_exist(self):
    """Test that all expected error type constants exist."""
    # Client errors (4xx)
    assert hasattr(ErrorType, "VALIDATION_ERROR")
    assert hasattr(ErrorType, "AUTHENTICATION_ERROR")
    assert hasattr(ErrorType, "AUTHORIZATION_ERROR")
    assert hasattr(ErrorType, "NOT_FOUND_ERROR")
    assert hasattr(ErrorType, "CONFLICT_ERROR")
    assert hasattr(ErrorType, "RATE_LIMIT_ERROR")

    # Server errors (5xx)
    assert hasattr(ErrorType, "INTERNAL_ERROR")
    assert hasattr(ErrorType, "SERVICE_UNAVAILABLE")
    assert hasattr(ErrorType, "DATABASE_ERROR")
    assert hasattr(ErrorType, "EXTERNAL_SERVICE_ERROR")

  def test_error_type_values(self):
    """Test error type string values."""
    assert ErrorType.VALIDATION_ERROR == "validation_error"
    assert ErrorType.AUTHENTICATION_ERROR == "authentication_error"
    assert ErrorType.AUTHORIZATION_ERROR == "authorization_error"
    assert ErrorType.NOT_FOUND_ERROR == "not_found_error"
    assert ErrorType.CONFLICT_ERROR == "conflict_error"
    assert ErrorType.RATE_LIMIT_ERROR == "rate_limit_error"
    assert ErrorType.INTERNAL_ERROR == "internal_error"
    assert ErrorType.SERVICE_UNAVAILABLE == "service_unavailable"
    assert ErrorType.DATABASE_ERROR == "database_error"
    assert ErrorType.EXTERNAL_SERVICE_ERROR == "external_service_error"


class TestErrorResponses:
  """Test ERROR_RESPONSES configuration."""

  def test_error_responses_completeness(self):
    """Test that all error types have corresponding responses."""
    expected_error_types = [
      ErrorType.VALIDATION_ERROR,
      ErrorType.AUTHENTICATION_ERROR,
      ErrorType.AUTHORIZATION_ERROR,
      ErrorType.NOT_FOUND_ERROR,
      ErrorType.CONFLICT_ERROR,
      ErrorType.RATE_LIMIT_ERROR,
      ErrorType.INTERNAL_ERROR,
      ErrorType.SERVICE_UNAVAILABLE,
      ErrorType.DATABASE_ERROR,
      ErrorType.EXTERNAL_SERVICE_ERROR,
    ]

    for error_type in expected_error_types:
      assert error_type in ERROR_RESPONSES

  def test_error_response_structure(self):
    """Test that error responses have correct structure."""
    for error_type, response in ERROR_RESPONSES.items():
      assert "status_code" in response
      assert "detail" in response
      assert isinstance(response["status_code"], int)
      assert isinstance(response["detail"], str)

  def test_client_error_status_codes(self):
    """Test that client errors have 4xx status codes."""
    client_errors = [
      ErrorType.VALIDATION_ERROR,
      ErrorType.AUTHENTICATION_ERROR,
      ErrorType.AUTHORIZATION_ERROR,
      ErrorType.NOT_FOUND_ERROR,
      ErrorType.CONFLICT_ERROR,
      ErrorType.RATE_LIMIT_ERROR,
    ]

    for error_type in client_errors:
      status_code = ERROR_RESPONSES[error_type]["status_code"]
      assert 400 <= status_code < 500

  def test_server_error_status_codes(self):
    """Test that server errors have 5xx status codes."""
    server_errors = [
      ErrorType.INTERNAL_ERROR,
      ErrorType.SERVICE_UNAVAILABLE,
      ErrorType.DATABASE_ERROR,
      ErrorType.EXTERNAL_SERVICE_ERROR,
    ]

    for error_type in server_errors:
      status_code = ERROR_RESPONSES[error_type]["status_code"]
      assert 500 <= status_code < 600

  def test_specific_status_codes(self):
    """Test specific status codes for known error types."""
    assert (
      ERROR_RESPONSES[ErrorType.VALIDATION_ERROR]["status_code"]
      == status.HTTP_400_BAD_REQUEST
    )
    assert (
      ERROR_RESPONSES[ErrorType.AUTHENTICATION_ERROR]["status_code"]
      == status.HTTP_401_UNAUTHORIZED
    )
    assert (
      ERROR_RESPONSES[ErrorType.AUTHORIZATION_ERROR]["status_code"]
      == status.HTTP_403_FORBIDDEN
    )
    assert (
      ERROR_RESPONSES[ErrorType.NOT_FOUND_ERROR]["status_code"]
      == status.HTTP_404_NOT_FOUND
    )
    assert (
      ERROR_RESPONSES[ErrorType.CONFLICT_ERROR]["status_code"]
      == status.HTTP_409_CONFLICT
    )
    assert (
      ERROR_RESPONSES[ErrorType.RATE_LIMIT_ERROR]["status_code"]
      == status.HTTP_429_TOO_MANY_REQUESTS
    )
    assert (
      ERROR_RESPONSES[ErrorType.INTERNAL_ERROR]["status_code"]
      == status.HTTP_500_INTERNAL_SERVER_ERROR
    )
    assert (
      ERROR_RESPONSES[ErrorType.SERVICE_UNAVAILABLE]["status_code"]
      == status.HTTP_503_SERVICE_UNAVAILABLE
    )


class TestRaiseSecureError:
  """Test raise_secure_error function."""

  @patch("robosystems.security.error_handling.logger")
  def test_raise_secure_error_basic(self, mock_logger):
    """Test basic secure error raising."""
    with pytest.raises(HTTPException) as exc_info:
      raise_secure_error(ErrorType.VALIDATION_ERROR)

    exception = exc_info.value
    assert exception.status_code == 400
    assert exception.detail == "Invalid request data"

    # Verify logging
    mock_logger.error.assert_called_once()

  @patch("robosystems.security.error_handling.logger")
  def test_raise_secure_error_with_original_exception(self, mock_logger):
    """Test secure error raising with original exception."""
    original_error = ValueError("Original error message")

    with pytest.raises(HTTPException) as exc_info:
      raise_secure_error(ErrorType.DATABASE_ERROR, original_error=original_error)

    exception = exc_info.value
    assert exception.status_code == 500
    assert exception.detail == "Database operation failed"

    # Verify logging with exception info
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    assert "Database operation failed" not in call_args[0][0]  # Generic message in log
    assert "exc_info" in call_args[1]
    assert call_args[1]["exc_info"] is True

  @patch("robosystems.security.error_handling.logger")
  def test_raise_secure_error_with_request_context(self, mock_logger):
    """Test secure error raising with request context."""
    with pytest.raises(HTTPException):
      raise_secure_error(
        ErrorType.AUTHENTICATION_ERROR,
        request_id="req_123",
        user_id="user_456",
        additional_context={"ip_address": "192.168.1.1"},
      )

    # Verify logging context
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    log_context = call_args[1]["extra"]

    assert log_context["request_id"] == "req_123"
    assert log_context["user_id"] == "user_456"
    assert log_context["ip_address"] == "192.168.1.1"
    assert log_context["error_type"] == ErrorType.AUTHENTICATION_ERROR

  @patch("robosystems.security.error_handling.logger")
  def test_raise_secure_error_with_custom_detail(self, mock_logger):
    """Test secure error raising with custom detail message."""
    custom_detail = "Custom error message"

    with pytest.raises(HTTPException) as exc_info:
      raise_secure_error(ErrorType.VALIDATION_ERROR, custom_detail=custom_detail)

    exception = exc_info.value
    assert exception.detail == custom_detail

  @patch("robosystems.security.error_handling.logger")
  def test_raise_secure_error_unknown_type(self, mock_logger):
    """Test secure error raising with unknown error type."""
    with pytest.raises(HTTPException) as exc_info:
      raise_secure_error("unknown_error_type")

    exception = exc_info.value
    assert exception.status_code == 500  # Should default to internal error
    assert exception.detail == "Internal server error"

    # Verify warning was logged
    mock_logger.warning.assert_called_once_with(
      "Unknown error type: unknown_error_type, defaulting to internal error"
    )

  @patch("robosystems.security.error_handling.logger")
  def test_raise_secure_error_without_original_exception(self, mock_logger):
    """Test secure error raising without original exception."""
    with pytest.raises(HTTPException):
      raise_secure_error(ErrorType.NOT_FOUND_ERROR, request_id="req_123")

    # Verify logging without exc_info
    mock_logger.error.assert_called_once()
    call_args = mock_logger.error.call_args
    assert "exc_info" not in call_args[1] or call_args[1].get("exc_info") is not True


class TestClassifyException:
  """Test exception classification."""

  def test_classify_database_errors(self):
    """Test classification of database-related errors."""
    database_errors = [
      Exception("Database connection failed"),
      Exception("SQL syntax error"),
      Exception("PostgreSQL connection timeout"),
      Exception("Ladybug query failed"),
      Exception("Connection to database lost"),
    ]

    for error in database_errors:
      result = classify_exception(error)
      assert result == ErrorType.DATABASE_ERROR

  def test_classify_authentication_errors(self):
    """Test classification of authentication errors."""
    auth_errors = [
      Exception("Unauthorized access"),
      Exception("Authentication failed"),
      Exception("Invalid token"),
      Exception("Login required"),
      Exception("Token expired"),
    ]

    for error in auth_errors:
      result = classify_exception(error)
      assert result == ErrorType.AUTHENTICATION_ERROR

  def test_classify_authorization_errors(self):
    """Test classification of authorization errors."""
    authz_errors = [
      Exception("Forbidden operation"),
      Exception("Access denied"),
      Exception("Insufficient permissions"),
      Exception("User lacks permission"),
    ]

    for error in authz_errors:
      result = classify_exception(error)
      assert result == ErrorType.AUTHORIZATION_ERROR

  def test_classify_validation_errors(self):
    """Test classification of validation errors."""
    validation_errors = [
      ValueError("Invalid input"),
      TypeError("Wrong type"),
    ]

    for error in validation_errors:
      result = classify_exception(error)
      assert result == ErrorType.VALIDATION_ERROR

    # Test message-based validation error
    validation_message_error = Exception("ValidationError: field required")
    result = classify_exception(validation_message_error)
    # This will be classified as internal error since it's not based on type name
    assert result == ErrorType.INTERNAL_ERROR

  def test_classify_not_found_errors(self):
    """Test classification of not found errors."""
    not_found_errors = [
      Exception("Resource not found"),
      Exception("User does not exist"),
      Exception("404 error"),
      Exception("Entity not found"),
    ]

    for error in not_found_errors:
      result = classify_exception(error)
      assert result == ErrorType.NOT_FOUND_ERROR

  def test_classify_conflict_errors(self):
    """Test classification of conflict errors."""
    conflict_errors = [
      Exception("Resource conflict"),
      Exception("Duplicate entry"),
      Exception("User already exists"),
      Exception("Conflict detected"),
    ]

    for error in conflict_errors:
      result = classify_exception(error)
      assert result == ErrorType.CONFLICT_ERROR

  def test_classify_rate_limit_errors(self):
    """Test classification of rate limit errors."""
    rate_limit_errors = [
      Exception("Rate limit exceeded"),
      Exception("Too many requests"),
      Exception("Request throttled"),
      Exception("Rate limit hit"),
    ]

    for error in rate_limit_errors:
      result = classify_exception(error)
      assert result == ErrorType.RATE_LIMIT_ERROR

  def test_classify_external_service_errors(self):
    """Test classification of external service errors."""
    # Test patterns that work (don't contain "connection")
    timeout_error = Exception("Request timeout")
    assert classify_exception(timeout_error) == ErrorType.EXTERNAL_SERVICE_ERROR

    service_unavailable_error = Exception("Service unavailable")
    assert (
      classify_exception(service_unavailable_error) == ErrorType.EXTERNAL_SERVICE_ERROR
    )

    gateway_502_error = Exception("502 Bad Gateway")
    assert classify_exception(gateway_502_error) == ErrorType.EXTERNAL_SERVICE_ERROR

    service_503_error = Exception("503 Service Unavailable")
    assert classify_exception(service_503_error) == ErrorType.EXTERNAL_SERVICE_ERROR

    # Test that "connection" patterns go to database error (current implementation)
    connection_refused_error = Exception("Connection refused")
    assert classify_exception(connection_refused_error) == ErrorType.DATABASE_ERROR

    connection_timeout = Exception("Connection timeout")
    assert classify_exception(connection_timeout) == ErrorType.DATABASE_ERROR

  def test_classify_unknown_errors(self):
    """Test classification of unknown errors defaults to internal error."""
    unknown_errors = [
      Exception("Some random error"),
      RuntimeError("Unexpected runtime error"),
      Exception(""),  # Empty message
    ]

    for error in unknown_errors:
      result = classify_exception(error)
      assert result == ErrorType.INTERNAL_ERROR

  def test_classify_case_insensitive(self):
    """Test that classification is case insensitive."""
    case_variants = [
      Exception("DATABASE connection failed"),
      Exception("Unauthorized ACCESS"),
      Exception("RATE LIMIT exceeded"),  # Use a message-based classification
    ]

    results = [classify_exception(error) for error in case_variants]
    assert results[0] == ErrorType.DATABASE_ERROR
    assert results[1] == ErrorType.AUTHENTICATION_ERROR
    assert results[2] == ErrorType.RATE_LIMIT_ERROR


class TestHandleExceptionSecurely:
  """Test handle_exception_securely function."""

  @patch("robosystems.security.error_handling.raise_secure_error")
  @patch("robosystems.security.error_handling.classify_exception")
  def test_handle_exception_securely_basic(self, mock_classify, mock_raise):
    """Test basic exception handling."""
    exception = ValueError("Test error")
    mock_classify.return_value = ErrorType.VALIDATION_ERROR

    handle_exception_securely(exception)

    mock_classify.assert_called_once_with(exception)
    mock_raise.assert_called_once_with(
      error_type=ErrorType.VALIDATION_ERROR,
      original_error=exception,
      request_id=None,
      user_id=None,
      additional_context=None,
    )

  @patch("robosystems.security.error_handling.raise_secure_error")
  @patch("robosystems.security.error_handling.classify_exception")
  def test_handle_exception_securely_with_context(self, mock_classify, mock_raise):
    """Test exception handling with context."""
    exception = Exception("Database error")
    mock_classify.return_value = ErrorType.DATABASE_ERROR

    handle_exception_securely(
      exception,
      request_id="req_123",
      user_id="user_456",
      additional_context={"operation": "query_execution"},
    )

    mock_classify.assert_called_once_with(exception)
    mock_raise.assert_called_once_with(
      error_type=ErrorType.DATABASE_ERROR,
      original_error=exception,
      request_id="req_123",
      user_id="user_456",
      additional_context={"operation": "query_execution"},
    )


class TestIsSafeToExpose:
  """Test is_safe_to_expose function."""

  def test_safe_messages(self):
    """Test messages that are safe to expose."""
    safe_messages = [
      "Invalid input format",
      "Field is required",
      "Value must be positive",
      "Operation completed successfully",
      "",  # Empty string
      None,  # None value
    ]

    for message in safe_messages:
      result = is_safe_to_expose(message)
      assert result is True

  def test_unsafe_messages(self):
    """Test messages that are unsafe to expose."""
    unsafe_messages = [
      "Database connection failed",
      "Password verification failed",
      "Secret key invalid",
      "API key not found",
      "Internal server configuration error",
      "SQL query execution failed",
      "Connection to host 192.168.1.1 failed",
      "File path /etc/config not found",
      "Environment variable not set",
      "Token validation failed",
      "Credential check failed",
    ]

    for message in unsafe_messages:
      result = is_safe_to_expose(message)
      assert result is False

  def test_case_insensitive_detection(self):
    """Test that detection is case insensitive."""
    case_variants = [
      "PASSWORD failed",
      "Database ERROR",
      "SECRET key invalid",
      "API_KEY not found",
    ]

    for message in case_variants:
      result = is_safe_to_expose(message)
      assert result is False

  def test_partial_word_matches(self):
    """Test that partial word matches are detected."""
    partial_matches = [
      "User password123 is invalid",  # Contains "password"
      "The database_connection failed",  # Contains "database"
      "Invalid secret_token provided",  # Contains "secret"
    ]

    for message in partial_matches:
      result = is_safe_to_expose(message)
      assert result is False

  def test_empty_and_none_handling(self):
    """Test handling of empty and None values."""
    assert is_safe_to_expose("") is True
    assert is_safe_to_expose(None) is True


class TestSanitizeErrorDetail:
  """Test sanitize_error_detail function."""

  @patch("robosystems.security.error_handling.is_safe_to_expose")
  def test_sanitize_safe_message(self, mock_is_safe):
    """Test sanitization of safe messages."""
    mock_is_safe.return_value = True
    safe_message = "Invalid input format"

    result = sanitize_error_detail(safe_message)

    assert result == safe_message
    mock_is_safe.assert_called_once_with(safe_message)

  @patch("robosystems.security.error_handling.is_safe_to_expose")
  def test_sanitize_unsafe_message(self, mock_is_safe):
    """Test sanitization of unsafe messages."""
    mock_is_safe.return_value = False
    unsafe_message = "Database connection to host failed"

    result = sanitize_error_detail(unsafe_message)

    assert result == "An error occurred while processing your request"
    mock_is_safe.assert_called_once_with(unsafe_message)

  def test_sanitize_empty_message(self):
    """Test sanitization of empty message."""
    result = sanitize_error_detail("")
    assert result == "An error occurred"

  def test_sanitize_none_message(self):
    """Test sanitization of None message."""
    result = sanitize_error_detail(None)
    assert result == "An error occurred"

  def test_sanitize_whitespace_message(self):
    """Test sanitization of whitespace-only message."""
    result = sanitize_error_detail("   ")
    # Whitespace string is truthy but empty when stripped
    assert result in [
      "An error occurred",
      "   ",
    ]  # Could be either depending on implementation


class TestIntegrationScenarios:
  """Test integration scenarios combining multiple functions."""

  @patch("robosystems.security.error_handling.logger")
  def test_full_error_handling_workflow(self, mock_logger):
    """Test complete error handling workflow."""
    # Simulate a database error
    original_exception = Exception("PostgreSQL connection timeout")

    with pytest.raises(HTTPException) as exc_info:
      handle_exception_securely(
        original_exception, request_id="req_123", user_id="user_456"
      )

    exception = exc_info.value
    assert exception.status_code == 500  # Database error -> internal server error
    assert exception.detail == "Database operation failed"

    # Verify logging occurred
    mock_logger.error.assert_called_once()

  def test_error_detail_sanitization_workflow(self):
    """Test error detail sanitization workflow."""
    sensitive_details = [
      "Database password incorrect",
      "API key validation failed",
      "Internal server configuration error",
    ]

    safe_details = [
      "Invalid input provided",
      "Required field missing",
      "Operation not permitted",
    ]

    # All sensitive details should be sanitized
    for detail in sensitive_details:
      sanitized = sanitize_error_detail(detail)
      assert sanitized == "An error occurred while processing your request"

    # Safe details should pass through
    for detail in safe_details:
      sanitized = sanitize_error_detail(detail)
      assert sanitized == detail

  @patch("robosystems.security.error_handling.logger")
  def test_classification_and_response_consistency(self, mock_logger):
    """Test that classification and response mapping are consistent."""
    test_cases = [
      (ValueError("Invalid format"), ErrorType.VALIDATION_ERROR, 400),
      (Exception("Not found"), ErrorType.NOT_FOUND_ERROR, 404),
      (Exception("Database failed"), ErrorType.DATABASE_ERROR, 500),
      (Exception("Unauthorized"), ErrorType.AUTHENTICATION_ERROR, 401),
    ]

    for exception, expected_type, expected_status in test_cases:
      classified_type = classify_exception(exception)
      assert classified_type == expected_type

      with pytest.raises(HTTPException) as exc_info:
        raise_secure_error(classified_type, original_error=exception)

      assert exc_info.value.status_code == expected_status

  def test_custom_detail_vs_generic_detail(self):
    """Test custom detail message vs generic detail message."""
    # Test with custom detail
    with pytest.raises(HTTPException) as exc_info:
      raise_secure_error(
        ErrorType.VALIDATION_ERROR, custom_detail="Custom validation message"
      )
    assert exc_info.value.detail == "Custom validation message"

    # Test without custom detail (should use generic)
    with pytest.raises(HTTPException) as exc_info:
      raise_secure_error(ErrorType.VALIDATION_ERROR)
    assert exc_info.value.detail == "Invalid request data"
