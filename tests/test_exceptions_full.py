"""Full exception hierarchy tests."""

from datetime import UTC, datetime

import pytest

from robosystems.exceptions import (
  AuthenticationError,
  CircuitBreakerOpenError,
  ConfigurationError,
  CreditAllocationError,
  DataIngestionError,
  DataValidationError,
  DuplicateEntityError,
  EntityNotFoundError,
  EntityValidationError,
  EnvironmentError,
  ExternalServiceError,
  GraphAllocationError,
  GraphNotFoundError,
  GraphQueryError,
  GraphSchemaError,
  InsufficientCreditsError,
  InsufficientPermissionsError,
  PipelineError,
  RateLimitExceededError,
  RetryableError,
  RoboSystemsError,
  S3Error,
  SECAPIError,
  TokenExpiredError,
  validate_entity_identifier,
  validate_graph_id,
)


class TestRoboSystemsError:
  def test_basic(self):
    err = RoboSystemsError("test error")
    assert err.message == "test error"
    assert err.error_code == "RoboSystemsError"
    assert err.details == {}
    assert err.timestamp is not None

  def test_with_code_and_details(self):
    err = RoboSystemsError("fail", error_code="CUSTOM", details={"k": "v"})
    assert err.error_code == "CUSTOM"
    assert err.details["k"] == "v"

  def test_to_dict(self):
    err = RoboSystemsError("test", error_code="CODE")
    d = err.to_dict()
    assert d["error"] == "CODE"
    assert d["message"] == "test"
    assert "timestamp" in d


class TestGraphExceptions:
  def test_graph_not_found(self):
    err = GraphNotFoundError("kg123")
    assert "kg123" in err.message
    assert err.error_code == "GRAPH_NOT_FOUND"
    assert err.details["graph_id"] == "kg123"

  def test_graph_allocation_error(self):
    err = GraphAllocationError("no capacity", graph_id="kg123")
    assert "no capacity" in err.message
    assert err.details["graph_id"] == "kg123"

  def test_graph_allocation_without_graph_id(self):
    err = GraphAllocationError("no capacity")
    assert "graph_id" not in err.details

  def test_graph_schema_error(self):
    err = GraphSchemaError("invalid schema", schema_type="base")
    assert err.details["schema_type"] == "base"

  def test_graph_schema_error_no_type(self):
    err = GraphSchemaError("generic error")
    assert "schema_type" not in err.details

  def test_graph_query_error(self):
    err = GraphQueryError("syntax error", query="MATCH (n) RETURN n", graph_id="kg1")
    assert err.details["query"] == "MATCH (n) RETURN n"
    assert err.details["graph_id"] == "kg1"

  def test_graph_query_truncates_long_query(self):
    long_query = "MATCH (n) " * 100
    err = GraphQueryError("error", query=long_query)
    assert len(err.details["query"]) <= 504  # 500 + "..."

  def test_graph_query_no_query(self):
    err = GraphQueryError("error")
    assert "query" not in err.details


class TestEntityExceptions:
  def test_entity_not_found(self):
    err = EntityNotFoundError("ent-123", "Company")
    assert "Company" in err.message
    assert err.details["entity_type"] == "Company"

  def test_entity_validation_error(self):
    err = EntityValidationError("name", "", "Cannot be empty")
    assert "name" in err.message
    assert err.details["reason"] == "Cannot be empty"

  def test_duplicate_entity(self):
    err = DuplicateEntityError("CIK:123", "Company")
    assert "already exists" in err.message


class TestAuthExceptions:
  def test_authentication_error(self):
    err = AuthenticationError()
    assert err.error_code == "AUTHENTICATION_FAILED"

  def test_token_expired(self):
    err = TokenExpiredError("refresh")
    assert "refresh" in err.message

  def test_insufficient_permissions(self):
    err = InsufficientPermissionsError("admin", resource="graph:kg1", user_id="u1")
    assert err.details["resource"] == "graph:kg1"
    assert err.details["user_id"] == "u1"

  def test_insufficient_permissions_minimal(self):
    err = InsufficientPermissionsError("read")
    assert "resource" not in err.details

  def test_rate_limit_exceeded(self):
    err = RateLimitExceededError(100, "minute", retry_after=60)
    assert err.details["retry_after_seconds"] == 60

  def test_rate_limit_no_retry(self):
    err = RateLimitExceededError(10, "hour")
    assert "retry_after_seconds" not in err.details


class TestCreditExceptions:
  def test_insufficient_credits(self):
    err = InsufficientCreditsError(100, 50, "query", graph_id="kg1")
    assert err.details["required_credits"] == 100
    assert err.details["available_credits"] == 50
    assert err.details["graph_id"] == "kg1"

  def test_insufficient_credits_no_graph(self):
    err = InsufficientCreditsError(10, 5, "mcp_call")
    assert "graph_id" not in err.details

  def test_credit_allocation_error(self):
    err = CreditAllocationError("no plan", graph_id="kg1")
    assert err.details["graph_id"] == "kg1"


class TestDataProcessingExceptions:
  def test_data_ingestion_error(self):
    err = DataIngestionError("SEC", "parse failed", file_path="/tmp/data.csv")
    assert err.details["source"] == "SEC"
    assert err.details["file_path"] == "/tmp/data.csv"

  def test_data_validation_error(self):
    err = DataValidationError("schema", ["err1", "err2"], data_sample={"k": "v"})
    assert err.details["total_errors"] == 2
    assert "data_sample" in err.details

  def test_data_validation_truncates_errors(self):
    errors = [f"error_{i}" for i in range(20)]
    err = DataValidationError("check", errors)
    assert len(err.details["errors"]) == 10
    assert err.details["total_errors"] == 20

  def test_pipeline_error(self):
    err = PipelineError("sec_ingest", "download", "timeout", pipeline_id="p123")
    assert err.details["pipeline_id"] == "p123"
    assert err.details["stage"] == "download"


class TestExternalServiceExceptions:
  def test_external_service_error(self):
    err = ExternalServiceError("stripe", "payment failed", status_code=402)
    assert err.details["service"] == "stripe"
    assert err.details["status_code"] == "402"

  def test_sec_api_error(self):
    err = SECAPIError("rate limited", cik="0000320193")
    assert "SEC" in err.message
    assert err.details["cik"] == "0000320193"

  def test_sec_api_error_no_cik(self):
    err = SECAPIError("server error")
    assert "cik" not in err.details

  def test_s3_error(self):
    err = S3Error("upload", "my-bucket", key="data/file.csv", reason="access denied")
    assert "my-bucket" in err.message
    assert err.details["key"] == "data/file.csv"

  def test_s3_error_minimal(self):
    err = S3Error("list", "bucket")
    assert "key" not in err.details


class TestConfigExceptions:
  def test_configuration_error(self):
    err = ConfigurationError("DATABASE_URL", "not set")
    assert "DATABASE_URL" in err.message

  def test_environment_error(self):
    err = EnvironmentError("prod", "missing secret")
    assert "prod" in err.message


class TestRetryExceptions:
  def test_retryable_error(self):
    err = RetryableError("timeout", retry_after=30, max_retries=3)
    assert err.details["retry_after_seconds"] == "30"
    assert err.details["max_retries"] == "3"

  def test_retryable_error_minimal(self):
    err = RetryableError("temporary failure")
    assert "retry_after_seconds" not in err.details

  def test_circuit_breaker_open(self):
    err = CircuitBreakerOpenError("graph-api")
    assert "graph-api" in err.message

  def test_circuit_breaker_with_reset_time(self):
    reset = datetime(2024, 1, 15, 12, 0, 0, tzinfo=UTC)
    err = CircuitBreakerOpenError("graph-api", reset_time=reset)
    assert "reset_time" in err.details


class TestValidationHelpers:
  def test_validate_graph_id_valid(self):
    validate_graph_id("kg1234567890abcdef")  # Should not raise

  def test_validate_graph_id_empty(self):
    with pytest.raises(Exception):
      validate_graph_id("")

  def test_validate_graph_id_wrong_prefix(self):
    with pytest.raises(Exception):
      validate_graph_id("abc123")

  def test_validate_entity_identifier_valid(self):
    validate_entity_identifier("ent-123")  # Should not raise

  def test_validate_entity_identifier_empty(self):
    with pytest.raises(Exception):
      validate_entity_identifier("")

  def test_validate_entity_identifier_too_long(self):
    with pytest.raises(Exception):
      validate_entity_identifier("x" * 256)
