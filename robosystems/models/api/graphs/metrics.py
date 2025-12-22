"""Graph query API models."""

from typing import Any

from pydantic import BaseModel, Field


class GraphMetricsResponse(BaseModel):
  """Response model for graph metrics."""

  graph_id: str = Field(..., description="Graph database identifier")
  graph_name: str | None = Field(None, description="Display name for the graph")
  user_role: str | None = Field(None, description="User's role in this graph")
  timestamp: str = Field(..., description="Metrics collection timestamp")
  total_nodes: int = Field(..., description="Total number of nodes")
  total_relationships: int = Field(..., description="Total number of relationships")
  node_counts: dict[str, int] = Field(..., description="Node counts by label")
  relationship_counts: dict[str, int] = Field(
    ..., description="Relationship counts by type"
  )
  estimated_size: dict[str, Any] = Field(..., description="Database size estimates")
  health_status: dict[str, Any] = Field(..., description="Database health information")


class StorageSummary(BaseModel):
  """Storage usage summary."""

  graph_tier: str = Field(..., description="Subscription tier")
  avg_storage_gb: float = Field(..., description="Average storage in GB")
  max_storage_gb: float = Field(..., description="Peak storage in GB")
  min_storage_gb: float = Field(..., description="Minimum storage in GB")
  total_gb_hours: float = Field(..., description="Total GB-hours for billing")
  measurement_count: int = Field(..., description="Number of measurements taken")


class CreditSummary(BaseModel):
  """Credit consumption summary."""

  graph_tier: str = Field(..., description="Subscription tier")
  total_credits_consumed: float = Field(..., description="Total credits consumed")
  total_base_cost: float = Field(..., description="Total base cost before multipliers")
  operation_breakdown: dict[str, Any] = Field(
    ..., description="Credit usage by operation type"
  )
  cached_operations: int = Field(..., description="Number of cached operations")
  billable_operations: int = Field(..., description="Number of billable operations")
  transaction_count: int = Field(..., description="Total transaction count")


class PerformanceInsights(BaseModel):
  """Performance analytics."""

  analysis_period_days: int = Field(..., description="Analysis period in days")
  total_operations: int = Field(..., description="Total operations analyzed")
  operation_stats: dict[str, Any] = Field(
    ..., description="Performance stats by operation type"
  )
  slow_queries: list[dict[str, Any]] = Field(
    ..., description="Top slow queries (over 5 seconds)"
  )
  performance_score: int = Field(..., description="Performance score (0-100)")


class GraphUsageResponse(BaseModel):
  """Response model for graph usage statistics."""

  graph_id: str = Field(..., description="Graph database identifier")
  time_range: str = Field(..., description="Time range for usage data")
  storage_summary: StorageSummary | None = Field(
    None, description="Storage usage summary"
  )
  credit_summary: CreditSummary | None = Field(
    None, description="Credit consumption summary"
  )
  performance_insights: PerformanceInsights | None = Field(
    None, description="Performance analytics"
  )
  recent_events: list[dict[str, Any]] = Field(
    default_factory=list, description="Recent usage events"
  )
  timestamp: str = Field(..., description="Usage collection timestamp")
