"""Service offering API response models.

This module contains Pydantic models for service offering responses including
graph subscriptions, repository subscriptions, and operation costs.
"""

from pydantic import BaseModel, ConfigDict, Field


class GraphSubscriptionTier(BaseModel):
  """Information about a graph infrastructure tier.

  Each tier represents a per-graph subscription option with specific
  infrastructure, performance, and pricing characteristics.
  """

  name: str = Field(
    ..., description="Infrastructure tier identifier (e.g., ladybug-standard)"
  )
  display_name: str = Field(..., description="Display name for UI")
  description: str = Field(..., description="Tier description")
  monthly_price_per_graph: float = Field(
    ..., description="Monthly price in USD per graph"
  )
  monthly_credits_per_graph: int = Field(
    ..., description="Monthly AI credits per graph"
  )
  storage_included_gb: int = Field(..., description="Storage included in GB")
  storage_overage_per_gb: float = Field(
    ..., description="Overage cost per GB per month"
  )
  infrastructure: str = Field(..., description="Infrastructure description")
  features: list[str] = Field(..., description="List of features")
  backup_retention_days: int = Field(..., description="Backup retention in days")
  priority_support: bool = Field(
    ..., description="Whether priority support is included"
  )
  max_queries_per_hour: int | None = Field(None, description="Maximum queries per hour")
  max_subgraphs: int = Field(0, description="Maximum subgraphs supported")
  api_rate_multiplier: float = Field(..., description="API rate multiplier")
  backend: str = Field(..., description="Database backend (ladybug or neo4j)")
  instance_type: str | None = Field(None, description="Instance type")


class StorageInfo(BaseModel):
  """Storage pricing information."""

  included_per_tier: dict[str, int] = Field(
    ..., description="Storage included per tier in GB"
  )
  overage_pricing: dict[str, float] = Field(
    ..., description="Overage pricing per GB per tier"
  )


class GraphSubscriptions(BaseModel):
  """Graph subscription offerings.

  Graph subscriptions are per-graph, not per-organization. Each graph
  created by an organization has its own subscription with its own
  infrastructure tier, pricing, and credit allocation.
  """

  description: str = Field(..., description="Description of graph subscriptions")
  pricing_model: str = Field(
    ..., description="Pricing model type (per_graph or per_organization)"
  )
  tiers: list[GraphSubscriptionTier] = Field(
    ..., description="Available infrastructure tiers"
  )
  storage: StorageInfo = Field(..., description="Storage information")
  notes: list[str] = Field(..., description="Important notes")


class OfferingRepositoryPlan(BaseModel):
  """Information about a repository plan."""

  plan: str = Field(..., description="Plan identifier")
  name: str = Field(..., description="Plan name")
  monthly_price: float = Field(..., description="Monthly price in USD")
  monthly_credits: int = Field(..., description="Monthly credit allocation")
  access_level: str = Field(..., description="Access level")
  features: list[str] = Field(..., description="List of features")
  rate_limits: dict[str, int | None] | None = Field(
    None, description="Rate limits for this plan"
  )


class RepositoryInfo(BaseModel):
  """Information about a shared repository."""

  type: str = Field(..., description="Repository type identifier")
  name: str = Field(..., description="Repository name")
  description: str = Field(..., description="Repository description")
  enabled: bool = Field(..., description="Whether repository is enabled")
  coming_soon: bool = Field(..., description="Whether repository is coming soon")
  plans: list[OfferingRepositoryPlan] = Field(..., description="Available plans")


class RepositorySubscriptions(BaseModel):
  """Repository subscription offerings.

  Repository subscriptions are per-organization, not per-graph. All members
  of an organization share access to subscribed repositories.
  """

  model_config = ConfigDict(json_schema_mode_override="serialization")

  description: str = Field(..., description="Description of repository subscriptions")
  pricing_model: str = Field(
    ..., description="Pricing model type (per_graph or per_organization)"
  )
  repositories: list[RepositoryInfo] = Field(..., description="Available repositories")
  notes: list[str] = Field(..., description="Important notes")


class TokenPricing(BaseModel):
  """AI token pricing for a specific model."""

  input_per_1k_tokens: float = Field(..., description="Credits per 1K input tokens")
  output_per_1k_tokens: float = Field(..., description="Credits per 1K output tokens")


class OperationCosts(BaseModel):
  """Operation cost information."""

  description: str = Field(..., description="Description of operation costs")
  ai_operations: dict[str, float] = Field(
    ..., description="Base costs for AI operations"
  )
  token_pricing: dict[str, TokenPricing] = Field(
    ..., description="Token pricing by model"
  )
  included_operations: list[str] = Field(
    ..., description="Operations that don't consume credits"
  )
  notes: list[str] = Field(..., description="Important notes about costs")


class ServiceOfferingSummary(BaseModel):
  """Summary of service offerings."""

  total_graph_tiers: int = Field(..., description="Total number of graph tiers")
  total_repositories: int = Field(..., description="Total number of repositories")
  enabled_repositories: int = Field(..., description="Number of enabled repositories")
  coming_soon_repositories: int = Field(
    ..., description="Number of coming soon repositories"
  )


class ServiceOfferingsResponse(BaseModel):
  """Complete service offerings response."""

  billing_enabled: bool = Field(
    ..., description="Whether billing and payments are enabled"
  )
  graph_subscriptions: GraphSubscriptions = Field(
    ..., description="Graph subscription offerings"
  )
  repository_subscriptions: RepositorySubscriptions = Field(
    ..., description="Repository subscription offerings"
  )
  operation_costs: OperationCosts = Field(..., description="Operation cost information")
  summary: ServiceOfferingSummary = Field(..., description="Summary information")
