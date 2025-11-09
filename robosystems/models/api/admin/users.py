"""User API models for admin endpoints."""

from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel


class UserResponse(BaseModel):
  """Response with user details."""

  id: str
  email: str
  name: Optional[str]
  email_verified: bool
  org_id: str
  org_role: str
  created_at: datetime
  updated_at: datetime
  last_login_at: Optional[datetime]


class UserGraphAccessResponse(BaseModel):
  """Response with user's graph access details."""

  graph_id: str
  graph_name: str
  role: str
  graph_tier: str
  storage_gb: Optional[float]
  created_at: datetime


class UserRepositoryAccessResponse(BaseModel):
  """Response with user's repository access details."""

  repository_name: str
  access_level: str
  granted_at: datetime
  expires_at: Optional[datetime]


class UserAPIKeyResponse(BaseModel):
  """Response with user's API key metadata (not values)."""

  key_id: str
  name: str
  prefix: str
  scopes: list[str]
  last_used_at: Optional[datetime]
  created_at: datetime
  expires_at: Optional[datetime]


class UserActivityResponse(BaseModel):
  """Response with user's recent activity."""

  user_id: str
  recent_logins: list[Dict[str, Any]]
  recent_api_calls: int
  graphs_accessed: list[str]
  repositories_accessed: list[str]
  credit_usage_month: float
  storage_usage_gb: float
