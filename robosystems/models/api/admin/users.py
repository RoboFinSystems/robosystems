"""User API models for admin endpoints."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class UserResponse(BaseModel):
  """Response with user details."""

  id: str
  email: str
  name: str | None
  email_verified: bool
  org_id: str
  org_role: str
  created_at: datetime
  updated_at: datetime
  last_login_at: datetime | None


class UserGraphAccessResponse(BaseModel):
  """Response with user's graph access details."""

  graph_id: str
  graph_name: str
  role: str
  graph_tier: str
  storage_gb: float | None
  created_at: datetime


class UserRepositoryAccessResponse(BaseModel):
  """Response with user's repository access details."""

  repository_name: str
  access_level: str
  granted_at: datetime
  expires_at: datetime | None


class UserAPIKeyResponse(BaseModel):
  """Response with user's API key metadata (not values)."""

  key_id: str
  name: str
  prefix: str
  scopes: list[str]
  last_used_at: datetime | None
  created_at: datetime
  expires_at: datetime | None


class UserActivityResponse(BaseModel):
  """Response with user's recent activity."""

  user_id: str
  recent_logins: list[dict[str, Any]]
  recent_api_calls: int
  graphs_accessed: list[str]
  repositories_accessed: list[str]
  credit_usage_month: float
  storage_usage_gb: float
