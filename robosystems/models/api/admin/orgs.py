"""Organization API models for admin endpoints."""

from datetime import datetime

from pydantic import BaseModel, Field


class OrgUserInfo(BaseModel):
  """User info within an organization."""

  user_id: str
  email: str
  name: str
  role: str
  created_at: datetime


class OrgGraphInfo(BaseModel):
  """Graph info within an organization."""

  graph_id: str
  name: str
  tier: str
  created_at: datetime


class OrgResponse(BaseModel):
  """Response with organization details."""

  org_id: str
  name: str
  org_type: str
  user_count: int
  graph_count: int
  max_graphs: int
  total_credits: float
  stripe_customer_id: str | None
  has_payment_method: bool
  default_payment_method_id: str | None
  invoice_billing_enabled: bool
  billing_email: str | None
  billing_contact_name: str | None
  payment_terms: str
  created_at: datetime
  updated_at: datetime
  users: list[OrgUserInfo]
  graphs: list[OrgGraphInfo]


class OrgUpdateRequest(BaseModel):
  """Request to update organization billing settings and limits."""

  invoice_billing_enabled: bool | None = None
  billing_email: str | None = None
  billing_contact_name: str | None = None
  payment_terms: str | None = None
  max_graphs: int | None = Field(
    default=None,
    ge=-1,
    description="Maximum graphs the org may create (-1 for unlimited)",
  )
