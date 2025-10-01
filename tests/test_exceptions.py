"""
Test custom exceptions module.

This module tests the custom exception hierarchy to ensure proper
error handling and debugging capabilities.
"""

import pytest

from robosystems.exceptions import (
  RoboSystemsError,
  GraphError,
  GraphNotFoundError,
  GraphAllocationError,
  GraphSchemaError,
  EntityNotFoundError,
  EntityValidationError,
  DuplicateEntityError,
  AuthenticationError,
  TokenExpiredError,
  InsufficientPermissionsError,
  RateLimitExceededError,
  InsufficientCreditsError,
  DataIngestionError,
  DataValidationError,
  PipelineError,
  SECAPIError,
  S3Error,
  ConfigurationError,
  validate_graph_id,
  validate_entity_identifier,
)


class TestBaseException:
  """Test the base RoboSystemsError class."""

  def test_base_exception_creation(self):
    """Test creating a base exception with all attributes."""
    error = RoboSystemsError(
      message="Test error",
      error_code="TEST_ERROR",
      details={"key": "value"},
    )

    assert error.message == "Test error"
    assert error.error_code == "TEST_ERROR"
    assert error.details == {"key": "value"}
    assert error.timestamp is not None

  def test_base_exception_to_dict(self):
    """Test converting exception to dictionary."""
    error = RoboSystemsError(
      message="Test error",
      error_code="TEST_ERROR",
      details={"key": "value"},
    )

    error_dict = error.to_dict()
    assert error_dict["error"] == "TEST_ERROR"
    assert error_dict["message"] == "Test error"
    assert error_dict["details"] == {"key": "value"}
    assert "timestamp" in error_dict


class TestGraphExceptions:
  """Test graph-related exceptions."""

  def test_graph_not_found_error(self):
    """Test GraphNotFoundError creation."""
    error = GraphNotFoundError("kg12345")

    assert "kg12345" in str(error)
    assert error.error_code == "GRAPH_NOT_FOUND"
    assert error.details["graph_id"] == "kg12345"

  def test_graph_allocation_error(self):
    """Test GraphAllocationError with various details."""
    error = GraphAllocationError(
      reason="No capacity available",
      graph_id="kg12345",
      instance_type="c6g.medium",
      region="us-east-1",
    )

    assert "No capacity available" in str(error)
    assert error.error_code == "GRAPH_ALLOCATION_FAILED"
    assert error.details["graph_id"] == "kg12345"
    assert error.details["instance_type"] == "c6g.medium"

  def test_graph_schema_error(self):
    """Test GraphSchemaError."""
    error = GraphSchemaError(
      message="Invalid schema definition",
      schema_type="custom",
      validation_errors=["Missing required field: id"],
    )

    assert "Invalid schema definition" in str(error)
    assert error.details["schema_type"] == "custom"
    assert "validation_errors" in error.details


class TestEntityExceptions:
  """Test entity-related exceptions."""

  def test_entity_not_found_error(self):
    """Test EntityNotFoundError."""
    error = EntityNotFoundError("kg1a2b3c", "Entity")

    assert "Entity with ID 'kg1a2b3c' not found" in str(error)
    assert error.error_code == "ENTITY_NOT_FOUND"

  def test_entity_validation_error(self):
    """Test EntityValidationError."""
    error = EntityValidationError(
      field="cik",
      value="invalid_cik",
      reason="CIK must be 10 digits",
    )

    assert "cik" in str(error)
    assert "CIK must be 10 digits" in str(error)
    assert error.details["field"] == "cik"

  def test_duplicate_entity_error(self):
    """Test DuplicateEntityError."""
    error = DuplicateEntityError("kg1a2b3c", "Entity")

    assert "already exists" in str(error)
    assert error.error_code == "DUPLICATE_ENTITY"


class TestAuthExceptions:
  """Test authentication and authorization exceptions."""

  def test_authentication_error(self):
    """Test AuthenticationError."""
    error = AuthenticationError("Invalid API key")

    assert "Invalid API key" in str(error)
    assert error.error_code == "AUTHENTICATION_FAILED"

  def test_token_expired_error(self):
    """Test TokenExpiredError."""
    error = TokenExpiredError("refresh")

    assert "refresh token has expired" in str(error)
    assert error.error_code == "TOKEN_EXPIRED"

  def test_insufficient_permissions_error(self):
    """Test InsufficientPermissionsError."""
    error = InsufficientPermissionsError(
      required_permission="write",
      resource="graph_kg12345",
      user_id="user_abc",
    )

    assert "write required" in str(error)
    assert error.details["resource"] == "graph_kg12345"
    assert error.details["user_id"] == "user_abc"

  def test_rate_limit_exceeded_error(self):
    """Test RateLimitExceededError."""
    error = RateLimitExceededError(
      limit=100,
      window="minute",
      retry_after=30,
    )

    assert "100 requests per minute" in str(error)
    assert error.details["retry_after_seconds"] == 30


class TestCreditExceptions:
  """Test credit-related exceptions."""

  def test_insufficient_credits_error(self):
    """Test InsufficientCreditsError."""
    error = InsufficientCreditsError(
      required=1000,
      available=500,
      operation="data_import",
      graph_id="kg12345",
    )

    assert "1000 required, 500 available" in str(error)
    assert error.details["operation"] == "data_import"
    assert error.details["graph_id"] == "kg12345"


class TestDataProcessingExceptions:
  """Test data processing exceptions."""

  def test_data_ingestion_error(self):
    """Test DataIngestionError."""
    error = DataIngestionError(
      source="S3",
      reason="File not found",
      file_path="s3://bucket/file.parquet",
      bucket="data-bucket",
    )

    assert "S3" in str(error)
    assert "File not found" in str(error)
    assert error.details["file_path"] == "s3://bucket/file.parquet"

  def test_data_validation_error(self):
    """Test DataValidationError."""
    errors = [
      "Missing required field: id",
      "Invalid date format",
      "Duplicate entry",
    ]
    error = DataValidationError(
      validation_type="schema",
      errors=errors,
      data_sample={"name": "test"},
    )

    assert "3 error(s) found" in str(error)
    assert error.details["total_errors"] == 3

  def test_pipeline_error(self):
    """Test PipelineError."""
    error = PipelineError(
      pipeline_name="SEC_XBRL",
      stage="ingestion",
      reason="Database connection failed",
      pipeline_id="pipeline_123",
    )

    assert "SEC_XBRL" in str(error)
    assert "ingestion" in str(error)
    assert error.details["pipeline_id"] == "pipeline_123"


class TestExternalServiceExceptions:
  """Test external service exceptions."""

  def test_sec_api_error(self):
    """Test SECAPIError."""
    error = SECAPIError(
      message="Rate limit exceeded",
      cik="0001234567",
      status_code=429,
    )

    assert "SEC API error" in str(error)
    assert error.details["service"] == "SEC_EDGAR"
    assert error.details["cik"] == "0001234567"

  def test_s3_error(self):
    """Test S3Error."""
    error = S3Error(
      operation="download",
      bucket="data-bucket",
      key="path/to/file.parquet",
      reason="Access denied",
    )

    assert "download" in str(error)
    assert "data-bucket" in str(error)
    assert error.details["key"] == "path/to/file.parquet"


class TestConfigurationExceptions:
  """Test configuration exceptions."""

  def test_configuration_error(self):
    """Test ConfigurationError."""
    error = ConfigurationError(
      config_key="DATABASE_URL",
      reason="Invalid connection string",
    )

    assert "DATABASE_URL" in str(error)
    assert "Invalid connection string" in str(error)


class TestValidationHelpers:
  """Test validation helper functions."""

  def test_validate_graph_id_valid(self):
    """Test valid graph ID."""
    # Should not raise
    validate_graph_id("kg12345abcdef")

  def test_validate_graph_id_empty(self):
    """Test empty graph ID."""
    with pytest.raises(GraphError) as exc_info:
      validate_graph_id("")
    assert "empty" in str(exc_info.value)

  def test_validate_graph_id_invalid_prefix(self):
    """Test graph ID with invalid prefix."""
    with pytest.raises(GraphError) as exc_info:
      validate_graph_id("invalid_id")
    assert "Invalid graph ID format" in str(exc_info.value)

  def test_validate_entity_identifier_valid(self):
    """Test valid entity identifier."""
    # Should not raise
    validate_entity_identifier("kg1a2b3c")

  def test_validate_entity_identifier_empty(self):
    """Test empty entity identifier."""
    with pytest.raises(EntityValidationError) as exc_info:
      validate_entity_identifier("")
    assert "Cannot be empty" in str(exc_info.value)

  def test_validate_entity_identifier_too_long(self):
    """Test entity identifier that's too long."""
    long_id = "x" * 256
    with pytest.raises(EntityValidationError) as exc_info:
      validate_entity_identifier(long_id)
    assert "Exceeds maximum length" in str(exc_info.value)


class TestExceptionHierarchy:
  """Test exception inheritance and hierarchy."""

  def test_all_exceptions_inherit_from_base(self):
    """Test that all custom exceptions inherit from RoboSystemsError."""
    exceptions = [
      GraphNotFoundError("kg123"),
      EntityNotFoundError("kg1a2b3c"),
      AuthenticationError(),
      InsufficientCreditsError(100, 50, "op"),
      DataIngestionError("S3", "error"),
      SECAPIError("error"),
      ConfigurationError("key", "reason"),
    ]

    for exc in exceptions:
      assert isinstance(exc, RoboSystemsError)
      assert hasattr(exc, "to_dict")
      assert hasattr(exc, "timestamp")

  def test_exception_can_be_caught_by_base(self):
    """Test that specific exceptions can be caught by base class."""
    try:
      raise GraphNotFoundError("kg123")
    except RoboSystemsError as e:
      assert e.error_code == "GRAPH_NOT_FOUND"

    try:
      raise InsufficientCreditsError(100, 50, "operation")
    except RoboSystemsError as e:
      assert e.error_code == "INSUFFICIENT_CREDITS"
