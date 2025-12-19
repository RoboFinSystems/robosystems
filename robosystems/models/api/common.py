"""
Common API models shared across multiple routers.

This module contains shared Pydantic models used throughout the API
for consistent response structures and error handling.
"""

from datetime import UTC, datetime
from typing import Any

from fastapi import HTTPException
from pydantic import BaseModel, Field


class ErrorResponse(BaseModel):
  """
  Standard error response format used across all API endpoints.

  This model ensures consistent error responses for SDK generation
  and client error handling.
  """

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


# Helper functions for consistent error handling


def create_error_response(
  status_code: int,
  detail: str,
  code: str | None = None,
  request_id: str | None = None,
) -> HTTPException:
  """
  Create a consistent error response using ErrorResponse model.

  Args:
      status_code: HTTP status code
      detail: Human-readable error message
      code: Machine-readable error code
      request_id: Request tracking ID

  Returns:
      HTTPException with ErrorResponse content
  """
  error = ErrorResponse(
    detail=detail,
    code=code,
    request_id=request_id,
    timestamp=datetime.now(UTC),
  )
  # Convert the error model to a dict with JSON-compatible values
  error_dict = error.model_dump(exclude_none=True)
  # Ensure timestamp is converted to ISO format string
  if "timestamp" in error_dict and isinstance(error_dict["timestamp"], datetime):
    error_dict["timestamp"] = error_dict["timestamp"].isoformat()
  return HTTPException(status_code=status_code, detail=error_dict)


def create_pagination_info(total: int, limit: int, offset: int) -> PaginationInfo:
  """
  Create pagination information for list responses.

  Args:
      total: Total number of items available
      limit: Maximum items per page
      offset: Number of items skipped

  Returns:
      PaginationInfo with calculated has_more flag
  """
  return PaginationInfo(
    total=total, limit=limit, offset=offset, has_more=(offset + limit) < total
  )


# Common error codes for consistency
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
