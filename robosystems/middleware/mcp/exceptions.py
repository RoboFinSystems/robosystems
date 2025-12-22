"""
MCP exceptions for Graph client operations.

Custom exceptions for graph databases MCP client error handling with enhanced hierarchy.
"""


class GraphAPIError(Exception):
  """Base exception for Graph API errors."""

  def __init__(
    self, message: str, error_code: str | None = None, details: dict | None = None
  ):
    super().__init__(message)
    self.error_code = error_code
    self.details = details or {}


class GraphQueryTimeoutError(GraphAPIError):
  """Exception raised when query execution times out."""

  def __init__(
    self,
    message: str = "Query execution timed out",
    timeout_seconds: int | None = None,
  ):
    super().__init__(message, error_code="QUERY_TIMEOUT")
    if timeout_seconds:
      self.details["timeout_seconds"] = timeout_seconds


class GraphQueryComplexityError(GraphAPIError):
  """Exception raised when query is too complex or risky."""

  def __init__(
    self, message: str = "Query is too complex", complexity_score: int | None = None
  ):
    super().__init__(message, error_code="QUERY_COMPLEXITY")
    if complexity_score:
      self.details["complexity_score"] = complexity_score


class GraphValidationError(GraphAPIError):
  """Exception raised when query validation fails."""

  def __init__(
    self,
    message: str = "Query validation failed",
    validation_errors: list | None = None,
  ):
    super().__init__(message, error_code="QUERY_VALIDATION")
    if validation_errors:
      self.details["validation_errors"] = validation_errors


class GraphAuthenticationError(GraphAPIError):
  """Exception raised when authentication fails."""

  def __init__(self, message: str = "Authentication failed"):
    super().__init__(message, error_code="AUTH_FAILED")


class GraphAuthorizationError(GraphAPIError):
  """Exception raised when authorization fails."""

  def __init__(
    self,
    message: str = "Authorization failed",
    required_permission: str | None = None,
  ):
    super().__init__(message, error_code="AUTH_DENIED")
    if required_permission:
      self.details["required_permission"] = required_permission


class GraphConnectionError(GraphAPIError):
  """Exception raised when connection to Graph API fails."""

  def __init__(self, message: str = "Connection failed", endpoint: str | None = None):
    super().__init__(message, error_code="CONNECTION_FAILED")
    if endpoint:
      self.details["endpoint"] = endpoint


class GraphResourceNotFoundError(GraphAPIError):
  """Exception raised when requested resource is not found."""

  def __init__(
    self,
    message: str = "Resource not found",
    resource_type: str | None = None,
    resource_id: str | None = None,
  ):
    super().__init__(message, error_code="RESOURCE_NOT_FOUND")
    if resource_type:
      self.details["resource_type"] = resource_type
    if resource_id:
      self.details["resource_id"] = resource_id


class GraphRateLimitError(GraphAPIError):
  """Exception raised when API rate limit is exceeded."""

  def __init__(
    self,
    message: str = "Rate limit exceeded",
    retry_after_seconds: int | None = None,
  ):
    super().__init__(message, error_code="RATE_LIMIT")
    if retry_after_seconds:
      self.details["retry_after_seconds"] = retry_after_seconds


class GraphSchemaError(GraphAPIError):
  """Exception raised when there's a schema-related error."""

  def __init__(self, message: str = "Schema error", schema_item: str | None = None):
    super().__init__(message, error_code="SCHEMA_ERROR")
    if schema_item:
      self.details["schema_item"] = schema_item
