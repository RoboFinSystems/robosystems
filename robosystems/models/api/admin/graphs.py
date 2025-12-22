"""Graph API models for admin endpoints."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class GraphResponse(BaseModel):
  """Response with graph details."""

  graph_id: str
  user_id: str
  org_id: str
  name: str
  description: str | None
  graph_tier: str
  backend: str
  status: str
  storage_gb: float | None
  storage_limit_gb: float | None
  subgraph_count: int | None
  subgraph_limit: int | None
  created_at: datetime
  updated_at: datetime


class GraphStorageResponse(BaseModel):
  """Response with graph storage details."""

  graph_id: str
  current_storage_gb: float
  storage_limit_gb: float
  usage_percentage: float
  within_limit: bool
  approaching_limit: bool
  recent_growth_gb: float | None
  estimated_days_to_limit: int | None


class GraphBackupResponse(BaseModel):
  """Response with graph backup status."""

  graph_id: str
  last_backup_at: datetime | None
  backup_count: int
  total_backup_size_gb: float
  backup_enabled: bool
  backup_status: str


class GraphInfrastructureResponse(BaseModel):
  """Response with graph infrastructure details."""

  graph_id: str
  tier: str
  instance_type: str | None
  cluster_type: str | None
  writer_endpoint: str | None
  reader_endpoint: str | None
  connection_status: str
  health_status: str


class GraphAnalyticsResponse(BaseModel):
  """Response with cross-graph analytics."""

  total_graphs: int
  by_tier: dict[str, int]
  by_backend: dict[str, int]
  by_status: dict[str, int]
  total_storage_gb: float
  largest_graphs: list[dict[str, Any]]
  most_active_graphs: list[dict[str, Any]]
