"""Subscription API models: repository plans, credit allocation, tier and status."""

from pydantic import BaseModel, Field

from robosystems.models.core.user.user_repository import RepositoryType


class RepositoryPlanInfo(BaseModel):
  """Information about a repository plan."""

  plan: str = Field(..., description="Repository plan name")
  name: str = Field(..., description="Display name of the tier")
  monthly_price: float = Field(..., description="Monthly price in USD")
  monthly_credits: int = Field(..., description="Monthly credit allocation")
  features: list[str] = Field(..., description="List of features included")


class AvailableRepository(BaseModel):
  """Available shared repository information."""

  type: str = Field(..., description="Repository type identifier")
  name: str = Field(..., description="Display name of the repository")
  description: str = Field(..., description="Repository description")
  enabled: bool = Field(
    ..., description="Whether repository is available for subscription"
  )
  coming_soon: bool | None = Field(
    False, description="Whether repository is coming soon"
  )
  plans: list[RepositoryPlanInfo] = Field(..., description="Available repository plans")


class AvailableRepositoriesResponse(BaseModel):
  """Response for available shared repositories."""

  available_repositories: list[AvailableRepository] = Field(
    ..., description="List of available repositories"
  )
  total_types: int = Field(..., description="Total number of repository types")


class SubscriptionInfo(BaseModel):
  """User subscription information."""

  id: str = Field(..., description="Subscription ID")
  user_id: str = Field(..., description="User ID")
  addon_type: str = Field(..., description="Add-on type")
  addon_tier: str = Field(..., description="Subscription tier")
  is_active: bool = Field(..., description="Whether subscription is active")
  activated_at: str = Field(..., description="Activation date (ISO format)")
  expires_at: str | None = Field(None, description="Expiration date (ISO format)")
  monthly_price_cents: int = Field(..., description="Monthly price in cents")
  features: list[str] = Field(..., description="List of features")
  metadata: dict[str, object] = Field(..., description="Additional metadata")


class UserSubscriptionsResponse(BaseModel):
  """Response for user subscriptions."""

  subscriptions: list[SubscriptionInfo] = Field(
    ..., description="List of user subscriptions"
  )
  total_count: int = Field(..., description="Total number of subscriptions")
  active_count: int = Field(..., description="Number of active subscriptions")


class SubscriptionRequest(BaseModel):
  """Request to create a new subscription."""

  repository_type: RepositoryType = Field(
    ..., description="Type of repository to subscribe to"
  )
  repository_plan: str = Field("starter", description="Repository plan")


class SubscriptionResponse(BaseModel):
  """Response for subscription creation."""

  message: str = Field(..., description="Success message")
  subscription: SubscriptionInfo = Field(
    ..., description="Created subscription details"
  )


class TierUpgradeRequest(BaseModel):
  """Request to upgrade subscription tier."""

  new_plan: str = Field(..., description="New repository plan")


class CreditSummary(BaseModel):
  """Credit balance summary."""

  current_balance: float = Field(..., description="Current credit balance")
  monthly_allocation: float = Field(..., description="Monthly credit allocation")
  consumed_this_month: float = Field(..., description="Credits consumed this month")
  usage_percentage: float = Field(
    ..., description="Usage percentage of monthly allocation"
  )
  rollover_credits: float = Field(
    ..., description="Credits rolled over from previous month"
  )
  allows_rollover: bool = Field(..., description="Whether rollover is allowed")
  last_allocation_date: str | None = Field(
    None, description="Last allocation date (ISO format)"
  )
  next_allocation_date: str | None = Field(
    None, description="Next allocation date (ISO format)"
  )
  is_active: bool = Field(..., description="Whether credit pool is active")


class AddOnCreditInfo(BaseModel):
  """Credit information for a specific add-on."""

  subscription_id: str = Field(..., description="Subscription ID")
  addon_type: str = Field(..., description="Add-on type (e.g., sec_data)")
  name: str = Field(..., description="Display name of the add-on")
  tier: str = Field(..., description="Subscription tier")
  credits_remaining: float = Field(..., description="Credits remaining")
  credits_allocated: float = Field(..., description="Monthly credit allocation")
  credits_consumed: float = Field(..., description="Credits consumed this month")
  rollover_amount: float = Field(
    0, description="Credits rolled over from previous month"
  )


class CreditsSummaryResponse(BaseModel):
  """Response for credits summary."""

  add_ons: list[AddOnCreditInfo] = Field(..., description="Credits breakdown by add-on")
  total_credits: float = Field(
    ..., description="Total credits remaining across all subscriptions"
  )
  credits_by_addon: list[dict[str, object]] | None = Field(
    None, description="Legacy field - Credits breakdown by add-on", deprecated=True
  )
  addon_count: int = Field(..., description="Number of active add-ons")


class RepositoryCreditsResponse(BaseModel):
  """Response for repository-specific credits."""

  repository: str = Field(..., description="Repository identifier")
  has_access: bool = Field(..., description="Whether user has access")
  message: str | None = Field(None, description="Access message")
  credits: CreditSummary | None = Field(
    None, description="Credit summary if access available"
  )


class AllocationResult(BaseModel):
  """Result of credit allocation."""

  addon_type: str = Field(..., description="Add-on type")
  addon_tier: str = Field(..., description="Add-on tier")
  was_allocated: bool = Field(..., description="Whether allocation was performed")
  current_balance: float = Field(..., description="Current balance after allocation")


class AllocationResponse(BaseModel):
  """Response for credit allocation."""

  message: str = Field(..., description="Allocation result message")
  allocations: list[AllocationResult] = Field(
    ..., description="Allocation results by add-on"
  )
  total_subscriptions: int = Field(..., description="Total number of subscriptions")


class GraphSubscriptionResponse(BaseModel):
  """Response for graph or repository subscription details."""

  id: str = Field(..., description="Subscription ID")
  resource_type: str = Field(..., description="Resource type (graph or repository)")
  resource_id: str = Field(..., description="Resource identifier")
  user_id: str | None = Field(
    None,
    description=(
      "Organization member the subscription belongs to. Set for repository "
      "subscriptions, which are billed to the org but granted per user."
    ),
  )
  plan_name: str = Field(..., description="Current plan name")
  plan_display_name: str = Field(
    ..., description="Human-readable plan name for UI display"
  )
  billing_interval: str = Field(..., description="Billing interval")
  status: str = Field(..., description="Subscription status")
  base_price_cents: int = Field(..., description="Base price in cents")
  current_period_start: str | None = Field(
    None, description="Current billing period start"
  )
  current_period_end: str | None = Field(None, description="Current billing period end")
  started_at: str | None = Field(None, description="Subscription start date")
  canceled_at: str | None = Field(None, description="Cancellation date")
  ends_at: str | None = Field(
    None,
    description="Subscription end date (when access will be revoked, especially relevant for cancelled subscriptions)",
  )
  created_at: str = Field(..., description="Creation timestamp")
  operation_id: str | None = Field(
    None,
    description="Operation ID for tracking async tier changes via SSE",
  )


class CreateRepositorySubscriptionRequest(BaseModel):
  """Request to create a repository subscription."""

  plan_name: str = Field(
    ...,
    description="Plan name for the repository subscription",
    examples=["sec-starter"],
  )
  user_id: str | None = Field(
    default=None,
    description=(
      "Subscribe this user instead of yourself. Org owners and admins only, "
      "and the target must belong to the same organization. Omit to subscribe "
      "yourself. Repository access is per-user while billing is org-level, so "
      "the subscriber is what determines who gets access."
    ),
    examples=["user_01ABC123"],
  )


class UpgradeSubscriptionRequest(BaseModel):
  """Request to upgrade a subscription."""

  new_plan_name: str = Field(
    ..., description="New plan name to change to", examples=["advanced"]
  )
  user_id: str | None = Field(
    None,
    description=(
      "Organization member whose subscription to change. Defaults to the "
      "caller; targeting another member requires org owner or admin."
    ),
  )


class CancelSubscriptionRequest(BaseModel):
  """Request to cancel a subscription.

  Default behavior cancels at period end (soft cancel). Pass `immediate=True`
  to terminate the subscription right away — this requires `confirm` to equal
  the subscription's `resource_id` (e.g. the graph_id) as a guard against
  accidental destructive calls.
  """

  immediate: bool = Field(
    False,
    description=(
      "If true, cancel immediately and trigger fast-path deprovisioning of "
      "the underlying resource (within ~10 minutes). If false (default), "
      "cancel at the end of the current billing period."
    ),
  )
  confirm: str | None = Field(
    None,
    description=(
      "Required when immediate=True. Must equal the subscription's "
      "resource_id (e.g. graph_id) to confirm intent."
    ),
  )
  user_id: str | None = Field(
    None,
    description=(
      "Organization member whose subscription to cancel. Defaults to the "
      "caller; canceling another member's requires org owner or admin."
    ),
  )
