"""Credit system API models.

This module contains Pydantic models for credit-related API operations including
credit balance checks, transaction history, storage limits, and usage summaries.
"""

from enum import Enum

from pydantic import BaseModel, Field


class CreditSummaryResponse(BaseModel):
  """Credit summary response model."""

  graph_id: str
  graph_tier: str
  current_balance: float
  monthly_allocation: float
  consumed_this_month: float
  transaction_count: int
  usage_percentage: float
  last_allocation_date: str | None = None


class CreditTransactionResponse(BaseModel):
  """Credit transaction response model."""

  id: str
  type: str
  amount: float
  description: str
  metadata: dict[str, object]
  created_at: str


class CreditCheckRequest(BaseModel):
  """Request to check credit balance."""

  operation_type: str = Field(..., description="Type of operation to check")
  base_cost: float | None = Field(
    None, description="Custom base cost (uses default if not provided)"
  )


class StorageLimitResponse(BaseModel):
  """Storage limit information response."""

  graph_id: str
  current_storage_gb: float
  effective_limit_gb: float
  usage_percentage: float
  within_limit: bool
  approaching_limit: bool
  needs_warning: bool
  has_override: bool
  recommendations: list[str] | None = None


class EnhancedCreditTransactionResponse(BaseModel):
  """Enhanced credit transaction response with more details."""

  id: str
  type: str
  amount: float
  description: str
  metadata: dict[str, object]
  created_at: str
  operation_id: str | None = None
  idempotency_key: str | None = None
  request_id: str | None = None
  user_id: str | None = None


class TransactionSummaryResponse(BaseModel):
  """Summary of transactions by operation type."""

  operation_type: str
  total_amount: float
  transaction_count: int
  average_amount: float
  first_transaction: str | None = None
  last_transaction: str | None = None


class DetailedTransactionsResponse(BaseModel):
  """Detailed response for transaction queries."""

  transactions: list[EnhancedCreditTransactionResponse]
  summary: dict[str, TransactionSummaryResponse]
  total_count: int
  filtered_count: int
  date_range: dict[str, str]


class CreditErrorCode(str, Enum):
  """Error codes for credit system operations."""

  INSUFFICIENT_CREDITS = "insufficient_credits"
  ACCESS_DENIED = "access_denied"
  CREDIT_SYSTEM_ERROR = "credit_system_error"
  INVALID_OPERATION = "invalid_operation"


class StandardizedCreditError(Exception):
  """Standardized error for credit system operations."""

  message: str
  error_code: CreditErrorCode
  metadata: dict[str, object]

  def __init__(
    self,
    message: str,
    error_code: CreditErrorCode,
    metadata: dict[str, object] | None = None,
  ):
    self.message = message
    self.error_code = error_code
    self.metadata = metadata or {}
    super().__init__(message)

  def to_http_detail(self) -> dict[str, object]:
    """Convert to HTTP error detail format."""
    return {
      "message": self.message,
      "error_code": self.error_code.value,
      "metadata": self.metadata,
    }


def create_insufficient_credits_error(
  required_credits: float, available_credits: float, operation_description: str
) -> StandardizedCreditError:
  """Create standardized insufficient credits error."""
  return StandardizedCreditError(
    message=f"Insufficient credits for {operation_description}. Required: {required_credits}, Available: {available_credits}",
    error_code=CreditErrorCode.INSUFFICIENT_CREDITS,
    metadata={
      "required_credits": required_credits,
      "available_credits": available_credits,
      "operation": operation_description,
    },
  )


def create_access_denied_error(
  operation_description: str, resource_name: str, reason: str | None = None
) -> StandardizedCreditError:
  """Create standardized access denied error."""
  message = f"Access denied for {operation_description} on resource '{resource_name}'"
  if reason:
    message += f": {reason}"

  return StandardizedCreditError(
    message=message,
    error_code=CreditErrorCode.ACCESS_DENIED,
    metadata={
      "operation": operation_description,
      "resource": resource_name,
      "reason": reason,
    },
  )
