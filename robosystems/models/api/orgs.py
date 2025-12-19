"""Organization API models for request/response validation."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, EmailStr, Field

from ..iam import OrgRole, OrgType


# Base models
class OrgBase(BaseModel):
  """Base organization model."""

  name: str = Field(..., min_length=1, max_length=100)
  org_type: OrgType = Field(default=OrgType.TEAM)


# Request models
class CreateOrgRequest(OrgBase):
  """Request to create an organization."""

  pass


class UpdateOrgRequest(BaseModel):
  """Request to update organization details."""

  name: str | None = Field(None, min_length=1, max_length=100)
  org_type: OrgType | None = None


class InviteMemberRequest(BaseModel):
  """Request to invite a member to an organization."""

  email: EmailStr
  role: OrgRole | None = Field(default=OrgRole.MEMBER)


class UpdateMemberRoleRequest(BaseModel):
  """Request to update a member's role."""

  role: OrgRole


# Response models
class OrgResponse(BaseModel):
  """Organization summary response."""

  id: str
  name: str
  org_type: OrgType
  role: OrgRole  # User's role in this org
  member_count: int
  graph_count: int
  created_at: datetime
  joined_at: datetime  # When the user joined


class OrgListResponse(BaseModel):
  """List of organizations response."""

  orgs: list[OrgResponse]
  total: int


class OrgMemberResponse(BaseModel):
  """Organization member response."""

  user_id: str
  name: str
  email: str
  role: OrgRole
  joined_at: datetime
  is_active: bool


class OrgMemberListResponse(BaseModel):
  """List of organization members response."""

  members: list[OrgMemberResponse]
  total: int
  org_id: str


class OrgDetailResponse(BaseModel):
  """Detailed organization response."""

  id: str
  name: str
  org_type: OrgType
  user_role: OrgRole
  members: list[dict[str, Any]]
  graphs: list[dict[str, Any]]
  limits: dict[str, Any] | None
  created_at: datetime
  updated_at: datetime


# Limits and usage models
class OrgLimitsResponse(BaseModel):
  """Organization limits response."""

  org_id: str
  max_graphs: int
  current_usage: dict[str, Any]
  warnings: list[str]
  can_create_graph: bool


class OrgUsageSummary(BaseModel):
  """Organization usage summary."""

  total_credits_used: float
  total_ai_operations: int
  total_storage_gb: float
  total_api_calls: int
  daily_avg_credits: float
  daily_avg_api_calls: float
  projected_monthly_credits: float
  projected_monthly_api_calls: int
  credits_limit: int | None
  api_calls_limit: int | None
  storage_limit_gb: int | None


class OrgUsageResponse(BaseModel):
  """Organization usage response."""

  org_id: str
  period_days: int
  start_date: datetime
  end_date: datetime
  summary: OrgUsageSummary
  graph_details: list[dict[str, Any]]
  daily_trend: list[dict[str, Any]]


# Billing models for orgs
class OrgBillingStatus(BaseModel):
  """Organization billing status."""

  org_id: str
  has_active_subscription: bool
  subscription_tier: str | None
  billing_admin_id: str | None
  payment_method_on_file: bool
  current_period_start: datetime | None
  current_period_end: datetime | None
  monthly_credit_allocation: float | None
  credits_remaining: float | None
  next_invoice_amount: float | None
  past_due: bool


class OrgInvoice(BaseModel):
  """Organization invoice."""

  invoice_id: str
  org_id: str
  amount_due: float
  amount_paid: float
  status: str  # draft, open, paid, void, uncollectible
  period_start: datetime
  period_end: datetime
  due_date: datetime | None
  paid_at: datetime | None
  invoice_url: str | None
  pdf_url: str | None


class OrgCheckoutSession(BaseModel):
  """Checkout session for organization billing."""

  session_id: str
  checkout_url: str
  expires_at: datetime
  org_id: str
  plan_id: str
  monthly_price: float
  setup_fee: float | None
