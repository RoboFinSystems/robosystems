"""Subscription API models for admin endpoints."""

from datetime import datetime
from typing import Optional
from pydantic import BaseModel, Field

from ...billing import SubscriptionStatus


class SubscriptionCreateRequest(BaseModel):
  """Request to create a new subscription."""

  resource_type: str = "graph"
  resource_id: str
  org_id: str
  plan_name: str
  billing_interval: str = "monthly"


class SubscriptionUpdateRequest(BaseModel):
  """Request to update a subscription."""

  status: Optional[SubscriptionStatus] = None
  plan_name: Optional[str] = None
  base_price_cents: Optional[int] = Field(None, ge=0)
  cancel_at_period_end: Optional[bool] = None


class SubscriptionResponse(BaseModel):
  """Response with subscription details."""

  id: str
  org_id: str
  org_name: Optional[str]
  owner_email: Optional[str]
  owner_name: Optional[str]
  has_payment_method: bool
  invoice_billing_enabled: bool
  resource_type: str
  resource_id: str
  plan_name: str
  billing_interval: str
  base_price_cents: int
  stripe_subscription_id: Optional[str]
  status: str
  started_at: Optional[datetime]
  current_period_start: Optional[datetime]
  current_period_end: Optional[datetime]
  canceled_at: Optional[datetime]
  ends_at: Optional[datetime]
  created_at: datetime
  updated_at: datetime
