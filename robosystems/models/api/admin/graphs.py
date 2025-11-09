"""Graph API models for admin endpoints."""

from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel


class GraphResponse(BaseModel):
  """Response with graph details."""

  graph_id: str
  user_id: str
  org_id: str
  name: str
  description: Optional[str]
  graph_tier: str
  backend: str
  status: str
  storage_gb: Optional[float]
  storage_limit_gb: Optional[float]
  subgraph_count: Optional[int]
  subgraph_limit: Optional[int]
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
  recent_growth_gb: Optional[float]
  estimated_days_to_limit: Optional[int]


class GraphBackupResponse(BaseModel):
  """Response with graph backup status."""

  graph_id: str
  last_backup_at: Optional[datetime]
  backup_count: int
  total_backup_size_gb: float
  backup_enabled: bool
  backup_status: str


class GraphInfrastructureResponse(BaseModel):
  """Response with graph infrastructure details."""

  graph_id: str
  tier: str
  instance_type: Optional[str]
  cluster_type: Optional[str]
  writer_endpoint: Optional[str]
  reader_endpoint: Optional[str]
  connection_status: str
  health_status: str


class GraphAnalyticsResponse(BaseModel):
  """Response with cross-graph analytics."""

  total_graphs: int
  by_tier: Dict[str, int]
  by_backend: Dict[str, int]
  by_status: Dict[str, int]
  total_storage_gb: float
  largest_graphs: list[Dict[str, Any]]
  most_active_graphs: list[Dict[str, Any]]
