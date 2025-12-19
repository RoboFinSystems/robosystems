"""Subscription API models for admin endpoints."""

from datetime import datetime

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

  status: SubscriptionStatus | None = None
  plan_name: str | None = None
  base_price_cents: int | None = Field(None, ge=0)
  cancel_at_period_end: bool | None = None


class SubscriptionResponse(BaseModel):
  """Response with subscription details."""

  id: str
  org_id: str
  org_name: str | None
  owner_email: str | None
  owner_name: str | None
  has_payment_method: bool
  invoice_billing_enabled: bool
  resource_type: str
  resource_id: str
  plan_name: str
  billing_interval: str
  base_price_cents: int
  stripe_subscription_id: str | None
  status: str
  started_at: datetime | None
  current_period_start: datetime | None
  current_period_end: datetime | None
  canceled_at: datetime | None
  ends_at: datetime | None
  created_at: datetime
  updated_at: datetime
