"""
Custom Exception Types for RoboSystems.

This module provides a comprehensive hierarchy of exceptions for better error handling
and debugging throughout the application. Each exception type provides specific context
about the nature of the error and can include additional metadata for debugging.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime, timezone


class RoboSystemsError(Exception):
  """
  Base exception for all RoboSystems application errors.

  Attributes:
      message: Human-readable error message
      error_code: Application-specific error code for categorization
      details: Additional error context and metadata
      timestamp: When the error occurred
  """

  def __init__(
    self,
    message: str,
    error_code: Optional[str] = None,
    details: Optional[Dict[str, Any]] = None,
  ):
    super().__init__(message)
    self.message = message
    self.error_code = error_code or self.__class__.__name__
    self.details = details or {}
    self.timestamp = datetime.now(timezone.utc).isoformat()

  def to_dict(self) -> Dict[str, Any]:
    """Convert exception to dictionary for API responses."""
    return {
      "error": self.error_code,
      "message": self.message,
      "details": self.details,
      "timestamp": self.timestamp,
    }


# ============================================================================
# Graph Database Exceptions
# ============================================================================


class GraphError(RoboSystemsError):
  """Base exception for graph database operations."""

  pass


class GraphNotFoundError(GraphError):
  """Raised when a requested graph database does not exist."""

  def __init__(self, graph_id: str, details: Optional[Dict[str, Any]] = None):
    super().__init__(
      f"Graph database '{graph_id}' not found",
      error_code="GRAPH_NOT_FOUND",
      details={"graph_id": graph_id, **(details or {})},
    )


class GraphAllocationError(GraphError):
  """Raised when graph database allocation fails."""

  def __init__(self, reason: str, graph_id: Optional[str] = None, **kwargs):
    details = {"reason": reason}
    if graph_id:
      details["graph_id"] = graph_id
    details.update(kwargs)
    super().__init__(
      f"Failed to allocate graph database: {reason}",
      error_code="GRAPH_ALLOCATION_FAILED",
      details=details,
    )


class GraphSchemaError(GraphError):
  """Raised when there are schema-related issues."""

  def __init__(self, message: str, schema_type: Optional[str] = None, **kwargs):
    details = {"schema_type": schema_type} if schema_type else {}
    details.update(kwargs)
    super().__init__(
      message,
      error_code="GRAPH_SCHEMA_ERROR",
      details=details,
    )


class GraphQueryError(GraphError):
  """Raised when a graph query fails."""

  def __init__(
    self,
    message: str,
    query: Optional[str] = None,
    graph_id: Optional[str] = None,
    **kwargs,
  ):
    details = {}
    if query:
      # Truncate long queries for error messages
      details["query"] = query[:500] + "..." if len(query) > 500 else query
    if graph_id:
      details["graph_id"] = graph_id
    details.update(kwargs)
    super().__init__(
      message,
      error_code="GRAPH_QUERY_ERROR",
      details=details,
    )


# ============================================================================
# Entity and Business Logic Exceptions
# ============================================================================


class EntityError(RoboSystemsError):
  """Base exception for entity-related operations."""

  pass


class EntityNotFoundError(EntityError):
  """Raised when an entity is not found."""

  def __init__(self, entity_id: str, entity_type: str = "Entity"):
    super().__init__(
      f"{entity_type} with ID '{entity_id}' not found",
      error_code="ENTITY_NOT_FOUND",
      details={"entity_id": entity_id, "entity_type": entity_type},
    )


class EntityValidationError(EntityError):
  """Raised when entity data validation fails."""

  def __init__(self, field: str, value: Any, reason: str):
    super().__init__(
      f"Validation failed for field '{field}': {reason}",
      error_code="ENTITY_VALIDATION_ERROR",
      details={"field": field, "value": str(value)[:100], "reason": reason},
    )


class DuplicateEntityError(EntityError):
  """Raised when attempting to create a duplicate entity."""

  def __init__(self, identifier: str, entity_type: str = "Entity"):
    super().__init__(
      f"{entity_type} with identifier '{identifier}' already exists",
      error_code="DUPLICATE_ENTITY",
      details={"identifier": identifier, "entity_type": entity_type},
    )


# ============================================================================
# Authentication and Authorization Exceptions
# ============================================================================


class AuthError(RoboSystemsError):
  """Base exception for authentication/authorization errors."""

  pass


class AuthenticationError(AuthError):
  """Raised when authentication fails."""

  def __init__(self, reason: str = "Invalid credentials"):
    super().__init__(
      reason,
      error_code="AUTHENTICATION_FAILED",
      details={"auth_type": "credentials"},
    )


class TokenExpiredError(AuthError):
  """Raised when a token has expired."""

  def __init__(self, token_type: str = "access"):
    super().__init__(
      f"The {token_type} token has expired",
      error_code="TOKEN_EXPIRED",
      details={"token_type": token_type},
    )


class InsufficientPermissionsError(AuthError):
  """Raised when user lacks required permissions."""

  def __init__(
    self,
    required_permission: str,
    resource: Optional[str] = None,
    user_id: Optional[str] = None,
  ):
    details = {"required_permission": required_permission}
    if resource:
      details["resource"] = resource
    if user_id:
      details["user_id"] = user_id
    super().__init__(
      f"Insufficient permissions: {required_permission} required",
      error_code="INSUFFICIENT_PERMISSIONS",
      details=details,
    )


class RateLimitExceededError(AuthError):
  """Raised when rate limits are exceeded."""

  def __init__(
    self,
    limit: int,
    window: str,
    retry_after: Optional[int] = None,
  ):
    details = {"limit": limit, "window": window}
    if retry_after:
      details["retry_after_seconds"] = retry_after
    super().__init__(
      f"Rate limit exceeded: {limit} requests per {window}",
      error_code="RATE_LIMIT_EXCEEDED",
      details=details,
    )


# ============================================================================
# Credit and Billing Exceptions
# ============================================================================


class CreditError(RoboSystemsError):
  """Base exception for credit-related operations."""

  pass


class InsufficientCreditsError(CreditError):
  """Raised when there are insufficient credits for an operation."""

  def __init__(
    self,
    required: int,
    available: int,
    operation: str,
    graph_id: Optional[str] = None,
  ):
    details = {
      "required_credits": required,
      "available_credits": available,
      "operation": operation,
    }
    if graph_id:
      details["graph_id"] = graph_id
    super().__init__(
      f"Insufficient credits: {required} required, {available} available",
      error_code="INSUFFICIENT_CREDITS",
      details=details,
    )


class CreditAllocationError(CreditError):
  """Raised when credit allocation fails."""

  def __init__(self, reason: str, graph_id: Optional[str] = None):
    details = {"reason": reason}
    if graph_id:
      details["graph_id"] = graph_id
    super().__init__(
      f"Failed to allocate credits: {reason}",
      error_code="CREDIT_ALLOCATION_FAILED",
      details=details,
    )


# ============================================================================
# Data Processing Exceptions
# ============================================================================


class DataProcessingError(RoboSystemsError):
  """Base exception for data processing operations."""

  pass


class DataIngestionError(DataProcessingError):
  """Raised when data ingestion fails."""

  def __init__(
    self,
    source: str,
    reason: str,
    file_path: Optional[str] = None,
    **kwargs,
  ):
    details = {"source": source, "reason": reason}
    if file_path:
      details["file_path"] = file_path
    details.update(kwargs)
    super().__init__(
      f"Data ingestion from {source} failed: {reason}",
      error_code="DATA_INGESTION_ERROR",
      details=details,
    )


class DataValidationError(DataProcessingError):
  """Raised when data validation fails during processing."""

  def __init__(
    self,
    validation_type: str,
    errors: List[str],
    data_sample: Optional[Dict[str, Any]] = None,
  ):
    details = {
      "validation_type": validation_type,
      "errors": errors[:10],  # Limit to first 10 errors
      "total_errors": len(errors),
    }
    if data_sample:
      details["data_sample"] = str(data_sample)[:200]
    super().__init__(
      f"Data validation failed: {len(errors)} error(s) found",
      error_code="DATA_VALIDATION_ERROR",
      details=details,
    )


class PipelineError(DataProcessingError):
  """Raised when a data pipeline fails."""

  def __init__(
    self,
    pipeline_name: str,
    stage: str,
    reason: str,
    pipeline_id: Optional[str] = None,
  ):
    details = {
      "pipeline_name": pipeline_name,
      "stage": stage,
      "reason": reason,
    }
    if pipeline_id:
      details["pipeline_id"] = pipeline_id
    super().__init__(
      f"Pipeline '{pipeline_name}' failed at stage '{stage}': {reason}",
      error_code="PIPELINE_ERROR",
      details=details,
    )


# ============================================================================
# External Service Exceptions
# ============================================================================


class ExternalServiceError(RoboSystemsError):
  """Base exception for external service failures."""

  def __init__(
    self,
    service: str,
    message: str,
    status_code: Optional[int] = None,
    **kwargs,
  ):
    details = {"service": service}
    if status_code:
      details["status_code"] = str(status_code)
    details.update(kwargs)
    super().__init__(
      message,
      error_code="EXTERNAL_SERVICE_ERROR",
      details=details,
    )


class SECAPIError(ExternalServiceError):
  """Raised when SEC EDGAR API operations fail."""

  def __init__(self, message: str, cik: Optional[str] = None, **kwargs):
    details = {}
    if cik:
      details["cik"] = cik
    details.update(kwargs)
    super().__init__(
      service="SEC_EDGAR",
      message=f"SEC API error: {message}",
      **details,
    )


class S3Error(ExternalServiceError):
  """Raised when S3 operations fail."""

  def __init__(
    self,
    operation: str,
    bucket: str,
    key: Optional[str] = None,
    reason: Optional[str] = None,
  ):
    details = {"operation": operation, "bucket": bucket}
    if key:
      details["key"] = key
    message = f"S3 {operation} failed for bucket '{bucket}'"
    if reason:
      message += f": {reason}"
    super().__init__(
      service="AWS_S3",
      message=message,
      status_code=None,
      **details,
    )


# ============================================================================
# Configuration and Environment Exceptions
# ============================================================================


class ConfigurationError(RoboSystemsError):
  """Raised when there are configuration issues."""

  def __init__(self, config_key: str, reason: str):
    super().__init__(
      f"Configuration error for '{config_key}': {reason}",
      error_code="CONFIGURATION_ERROR",
      details={"config_key": config_key, "reason": reason},
    )


class EnvironmentError(RoboSystemsError):
  """Raised when there are environment-specific issues."""

  def __init__(self, environment: str, issue: str):
    super().__init__(
      f"Environment '{environment}' issue: {issue}",
      error_code="ENVIRONMENT_ERROR",
      details={"environment": environment, "issue": issue},
    )


# ============================================================================
# Retry and Circuit Breaker Exceptions
# ============================================================================


class RetryableError(RoboSystemsError):
  """Base class for errors that can be retried."""

  def __init__(
    self,
    message: str,
    retry_after: Optional[int] = None,
    max_retries: Optional[int] = None,
    **kwargs,
  ):
    details = {}
    if retry_after:
      details["retry_after_seconds"] = str(retry_after)
    if max_retries:
      details["max_retries"] = str(max_retries)
    details.update(kwargs)
    super().__init__(
      message,
      error_code="RETRYABLE_ERROR",
      details=details,
    )


class CircuitBreakerOpenError(RoboSystemsError):
  """Raised when a circuit breaker is open."""

  def __init__(self, service: str, reset_time: Optional[datetime] = None):
    details = {"service": service}
    if reset_time:
      details["reset_time"] = reset_time.isoformat()
    super().__init__(
      f"Circuit breaker is open for service '{service}'",
      error_code="CIRCUIT_BREAKER_OPEN",
      details=details,
    )


# ============================================================================
# Validation Helper Functions
# ============================================================================


def validate_graph_id(graph_id: str) -> None:
  """
  Validate graph ID format.

  Raises:
      GraphValidationError: If graph ID is invalid
  """
  if not graph_id:
    raise GraphError("Graph ID cannot be empty", error_code="INVALID_GRAPH_ID")

  if not graph_id.startswith("kg"):
    raise GraphError(
      f"Invalid graph ID format: {graph_id}",
      error_code="INVALID_GRAPH_ID",
      details={"expected_prefix": "kg", "actual": graph_id},
    )


def validate_entity_identifier(identifier: str, entity_type: str = "Entity") -> None:
  """
  Validate entity identifier format.

  Raises:
      EntityValidationError: If identifier is invalid
  """
  if not identifier:
    raise EntityValidationError("identifier", identifier, "Cannot be empty")

  if len(identifier) > 255:
    raise EntityValidationError(
      "identifier",
      identifier,
      f"Exceeds maximum length of 255 characters (actual: {len(identifier)})",
    )
