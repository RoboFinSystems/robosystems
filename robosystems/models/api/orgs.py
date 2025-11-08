"""Organization API models for request/response validation."""

from typing import List, Optional, Dict, Any
from datetime import datetime
from pydantic import BaseModel, Field, EmailStr

from ..iam import OrgType, OrgRole


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

  name: Optional[str] = Field(None, min_length=1, max_length=100)
  org_type: Optional[OrgType] = None


class InviteMemberRequest(BaseModel):
  """Request to invite a member to an organization."""

  email: EmailStr
  role: Optional[OrgRole] = Field(default=OrgRole.MEMBER)


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

  orgs: List[OrgResponse]
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

  members: List[OrgMemberResponse]
  total: int
  org_id: str


class OrgDetailResponse(BaseModel):
  """Detailed organization response."""

  id: str
  name: str
  org_type: OrgType
  user_role: OrgRole
  members: List[Dict[str, Any]]
  graphs: List[Dict[str, Any]]
  limits: Optional[Dict[str, Any]]
  created_at: datetime
  updated_at: datetime


# Limits and usage models
class OrgLimitsResponse(BaseModel):
  """Organization limits response."""

  org_id: str
  max_graphs: int
  current_usage: Dict[str, Any]
  warnings: List[str]
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
  credits_limit: Optional[int]
  api_calls_limit: Optional[int]
  storage_limit_gb: Optional[int]


class OrgUsageResponse(BaseModel):
  """Organization usage response."""

  org_id: str
  period_days: int
  start_date: datetime
  end_date: datetime
  summary: OrgUsageSummary
  graph_details: List[Dict[str, Any]]
  daily_trend: List[Dict[str, Any]]


# Billing models for orgs
class OrgBillingStatus(BaseModel):
  """Organization billing status."""

  org_id: str
  has_active_subscription: bool
  subscription_tier: Optional[str]
  billing_admin_id: Optional[str]
  payment_method_on_file: bool
  current_period_start: Optional[datetime]
  current_period_end: Optional[datetime]
  monthly_credit_allocation: Optional[float]
  credits_remaining: Optional[float]
  next_invoice_amount: Optional[float]
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
  due_date: Optional[datetime]
  paid_at: Optional[datetime]
  invoice_url: Optional[str]
  pdf_url: Optional[str]


class OrgCheckoutSession(BaseModel):
  """Checkout session for organization billing."""

  session_id: str
  checkout_url: str
  expires_at: datetime
  org_id: str
  plan_id: str
  monthly_price: float
  setup_fee: Optional[float]
