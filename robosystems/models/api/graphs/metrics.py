"""Graph query API models."""

from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


class GraphMetricsResponse(BaseModel):
  """Response model for graph metrics."""

  graph_id: str = Field(..., description="Graph database identifier")
  graph_name: Optional[str] = Field(None, description="Display name for the graph")
  user_role: Optional[str] = Field(None, description="User's role in this graph")
  timestamp: str = Field(..., description="Metrics collection timestamp")
  total_nodes: int = Field(..., description="Total number of nodes")
  total_relationships: int = Field(..., description="Total number of relationships")
  node_counts: Dict[str, int] = Field(..., description="Node counts by label")
  relationship_counts: Dict[str, int] = Field(
    ..., description="Relationship counts by type"
  )
  estimated_size: Dict[str, Any] = Field(..., description="Database size estimates")
  health_status: Dict[str, Any] = Field(..., description="Database health information")


class GraphUsageResponse(BaseModel):
  """Response model for graph usage statistics."""

  graph_id: str = Field(..., description="Graph database identifier")
  storage_usage: Dict[str, Any] = Field(..., description="Storage usage information")
  query_statistics: Dict[str, Any] = Field(..., description="Query statistics")
  recent_activity: Dict[str, Any] = Field(..., description="Recent activity summary")
  timestamp: str = Field(..., description="Usage collection timestamp")
