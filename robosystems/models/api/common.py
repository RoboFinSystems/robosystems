"""Response and error shapes shared across every router."""

from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
  """Error body returned by every endpoint."""

  detail: str = Field(
    ...,
    description="Human-readable error message explaining what went wrong",
    examples=["Insufficient credits for operation"],
  )
  code: str | None = Field(
    None,
    description="Machine-readable error code for programmatic handling",
    examples=["INSUFFICIENT_CREDITS"],
  )
  request_id: str | None = Field(
    None,
    description="Unique request ID for tracking and debugging",
    examples=["req_1234567890abcdef"],
  )
  timestamp: datetime | None = Field(
    None,
    description="Timestamp when the error occurred",
    examples=["2024-01-01T00:00:00Z"],
  )

  class Config:
    json_encoders = {datetime: lambda v: v.isoformat()}
    json_schema_extra = {
      "example": {
        "detail": "Resource not found",
        "code": "RESOURCE_NOT_FOUND",
        "request_id": "req_1234567890abcdef",
        "timestamp": "2024-01-01T00:00:00Z",
      }
    }


class SuccessResponse(BaseModel):
  """Standard success response for operations without specific return data."""

  success: bool = Field(
    True, description="Indicates the operation completed successfully"
  )
  message: str = Field(
    ...,
    description="Human-readable success message",
    examples=["Operation completed successfully"],
  )
  data: dict[str, Any] | None = Field(
    None, description="Optional additional data related to the operation"
  )

  class Config:
    json_schema_extra = {
      "example": {
        "success": True,
        "message": "Resource deleted successfully",
        "data": {"deleted_count": 1},
      }
    }


class PaginationInfo(BaseModel):
  """Pagination information for list responses."""

  total: int = Field(..., description="Total number of items available", examples=[100])
  limit: int = Field(
    ..., description="Maximum number of items returned in this response", examples=[20]
  )
  offset: int = Field(..., description="Number of items skipped", examples=[0])
  has_more: bool = Field(
    ..., description="Whether more items are available", examples=[True]
  )

  class Config:
    json_schema_extra = {
      "example": {"total": 100, "limit": 20, "offset": 0, "has_more": True}
    }


class HealthStatus(BaseModel):
  """Health check status information."""

  status: str = Field(
    ...,
    description="Current health status",
    examples=["healthy"],
    pattern="^(healthy|degraded|unhealthy)$",
  )
  timestamp: datetime = Field(
    ..., description="Time of health check", examples=["2024-01-01T00:00:00Z"]
  )
  details: dict[str, Any] | None = Field(
    None, description="Additional health check details"
  )


class DeleteResult(BaseModel):
  """Shared response shape for delete / soft-delete operations.

  ``deleted=True`` means the operation succeeded (a row was deleted or
  flipped). A row that never existed gets a 404 — this shape never carries
  "not found".

  Defined once here, and used by both roboledger and roboinvestor, so the
  OpenAPI components key resolves to a single schema.
  """

  deleted: bool = Field(
    ...,
    description=(
      "`true` when the row was deleted in this call. Always `true` "
      "today — 404 covers the not-found case at the HTTP layer rather "
      "than via this field."
    ),
  )


class CreditCostInfo(BaseModel):
  """Information about credit costs for an operation."""

  base_cost: float = Field(
    ..., description="Base credit cost before multipliers", examples=[1.0]
  )
  multiplier: float = Field(
    ..., description="Cost multiplier based on graph tier", examples=[1.5]
  )
  total_cost: float = Field(
    ..., description="Total credits that will be consumed", examples=[1.5]
  )
  cached: bool = Field(
    False, description="Whether this is a cached operation (free)", examples=[False]
  )


def create_error_response(
  status_code: int,
  detail: str,
  code: str | None = None,
  request_id: str | None = None,
) -> HTTPException:
  """Build an ``HTTPException`` whose detail is an ``ErrorResponse`` body."""
  error = ErrorResponse(
    detail=detail,
    code=code,
    request_id=request_id,
    timestamp=datetime.now(UTC),
  )
  error_dict = error.model_dump(exclude_none=True)
  if "timestamp" in error_dict and isinstance(error_dict["timestamp"], datetime):
    error_dict["timestamp"] = error_dict["timestamp"].isoformat()
  return HTTPException(status_code=status_code, detail=error_dict)


def create_pagination_info(total: int, limit: int, offset: int) -> PaginationInfo:
  """Build a ``PaginationInfo``, deriving ``has_more`` from the window."""
  return PaginationInfo(
    total=total, limit=limit, offset=offset, has_more=(offset + limit) < total
  )


# Shared OpenAPI response dicts for consistent Swagger documentation.
#
# Use by spreading into a per-endpoint `responses=` kwarg:
#
#   responses={**RESOURCE_ERROR_RESPONSES, 200: {"model": MyResponse}}
#
# Levels are additive — pick the narrowest one that fits the endpoint:
#   COMMON           → 400, 429, 500 (any endpoint; covers unauthenticated)
#   AUTHENTICATED    → COMMON + 401, 403 (any endpoint behind auth)
#   RESOURCE         → AUTHENTICATED + 404 (endpoint resolves a specific resource)
#   OPERATION        → RESOURCE + 409 (CQRS operations with idempotency-key conflict)

COMMON_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
  400: {"model": ErrorResponse, "description": "Invalid request"},
  429: {"model": ErrorResponse, "description": "Rate limit exceeded"},
  500: {"model": ErrorResponse, "description": "Internal server error"},
}

AUTHENTICATED_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
  **COMMON_ERROR_RESPONSES,
  401: {"model": ErrorResponse, "description": "Authentication required"},
  403: {"model": ErrorResponse, "description": "Access denied"},
}

RESOURCE_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
  **AUTHENTICATED_ERROR_RESPONSES,
  404: {"model": ErrorResponse, "description": "Resource not found"},
}

OPERATION_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
  **RESOURCE_ERROR_RESPONSES,
  409: {
    "model": ErrorResponse,
    "description": "Idempotency-Key conflict — key reused with different body",
  },
  # FastAPI's default 422 schema is `HTTPValidationError` (request-body
  # pydantic validation), but our routes also raise
  # `HTTPException(422, "...")` for business-validation failures with a
  # plain string detail. The runtime `RequestValidationError` handler in
  # ``main.py`` normalizes both shapes into the `ErrorResponse` shape;
  # this declaration aligns the OpenAPI spec so SDK generators get the
  # correct response model.
  422: {"model": ErrorResponse, "description": "Validation error"},
}


class ErrorCode:
  """Standard error codes for common scenarios."""

  # Authentication & Authorization
  UNAUTHORIZED = "UNAUTHORIZED"
  FORBIDDEN = "FORBIDDEN"
  INVALID_CREDENTIALS = "INVALID_CREDENTIALS"
  TOKEN_EXPIRED = "TOKEN_EXPIRED"

  # Resource errors
  NOT_FOUND = "NOT_FOUND"
  ALREADY_EXISTS = "ALREADY_EXISTS"

  # Validation errors
  INVALID_INPUT = "INVALID_INPUT"
  MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
  INVALID_FORMAT = "INVALID_FORMAT"

  # Credit errors
  INSUFFICIENT_CREDITS = "INSUFFICIENT_CREDITS"
  CREDIT_LIMIT_EXCEEDED = "CREDIT_LIMIT_EXCEEDED"

  # Rate limiting
  RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"

  # Database errors
  DATABASE_ERROR = "DATABASE_ERROR"
  TRANSACTION_FAILED = "TRANSACTION_FAILED"

  # External service errors
  EXTERNAL_SERVICE_ERROR = "EXTERNAL_SERVICE_ERROR"
  PROVIDER_ERROR = "PROVIDER_ERROR"

  # Generic errors
  INTERNAL_ERROR = "INTERNAL_ERROR"
  OPERATION_FAILED = "OPERATION_FAILED"
