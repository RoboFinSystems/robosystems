"""Credit API models for admin endpoints."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


class CreditPoolResponse(BaseModel):
  """Response with credit pool details."""

  graph_id: str
  user_id: str | None
  graph_tier: str
  current_balance: float
  monthly_allocation: float
  credit_multiplier: float
  storage_limit_override_gb: float | None
  created_at: datetime
  updated_at: datetime


class BonusCreditsRequest(BaseModel):
  """Request to add bonus credits."""

  amount: float = Field(..., gt=0, description="Amount of credits to add")
  description: str = Field(..., min_length=1, description="Reason for bonus credits")
  metadata: dict[str, Any] | None = Field(
    default=None, description="Additional metadata for the transaction"
  )


class CreditAnalyticsResponse(BaseModel):
  """Response with system-wide credit analytics."""

  graph_credits: dict[str, Any]
  repository_credits: dict[str, Any]
  total_pools: int
  total_allocated_monthly: float
  total_current_balance: float
  total_consumed_month: float


class CreditHealthResponse(BaseModel):
  """Response with credit system health status."""

  status: str
  graph_health: dict[str, Any]
  repository_health: dict[str, Any]
  total_pools: int
  pools_with_issues: int
  last_checked: datetime


class RepositoryCreditPoolResponse(BaseModel):
  """Response with repository credit pool details."""

  user_repository_id: str
  user_id: str
  repository_type: str
  repository_plan: str
  current_balance: float
  monthly_allocation: float
  consumed_this_month: float
  allows_rollover: bool
  rollover_credits: float
  is_active: bool
  last_allocation_date: datetime | None
  next_allocation_date: datetime | None
  created_at: datetime
  updated_at: datetime
