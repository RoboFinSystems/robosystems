"""User management API models."""

from pydantic import BaseModel, EmailStr, Field


class AccountInfo(BaseModel):
  """Provider account information."""

  provider: str = Field(
    ..., description="Authentication provider ID (e.g., 'github', 'google')"
  )
  provider_type: str = Field(..., description="Type of provider")
  provider_account_id: str = Field(..., description="Account ID at the provider")

  class Config:
    json_schema_extra: dict[str, object] = {
      "example": {
        "provider": "github",
        "provider_type": "oauth",
        "provider_account_id": "12345",
      }
    }


class UserResponse(BaseModel):
  """User information response model."""

  id: str = Field(..., description="Unique identifier for the user")
  name: str | None = Field(None, description="User's display name")
  email: str | None = Field(None, description="User's email address")
  accounts: list[AccountInfo] = Field(
    default_factory=list, description="User's authentication accounts"
  )

  class Config:
    json_schema_extra: dict[str, object] = {
      "example": {
        "id": "user-123",
        "name": "johndoe",
        "email": "john@example.com",
        "accounts": [
          {
            "provider": "github",
            "provider_type": "oauth",
            "provider_account_id": "12345",
          },
          {
            "provider": "google",
            "provider_type": "oauth",
            "provider_account_id": "67890",
          },
        ],
      }
    }


class UpdateUserRequest(BaseModel):
  """Request model for updating user profile."""

  name: str | None = Field(
    None, min_length=1, max_length=100, description="User's display name"
  )
  email: EmailStr | None = Field(None, description="User's email address")


class UpdatePasswordRequest(BaseModel):
  """Request model for updating user password."""

  current_password: str = Field(..., min_length=8, description="Current password")
  new_password: str = Field(..., min_length=8, description="New password")
  confirm_password: str = Field(..., min_length=8, description="Confirm new password")


class GraphInfo(BaseModel):
  """Graph information for user."""

  graphId: str = Field(..., description="Graph database identifier")
  graphName: str = Field(..., description="Display name for the graph")
  role: str = Field(..., description="User's role in this graph")
  isSelected: bool = Field(
    ..., description="Whether this is the currently selected graph"
  )
  createdAt: str = Field(..., description="Creation timestamp")


class UserGraphsResponse(BaseModel):
  """User graphs response model."""

  graphs: list[GraphInfo] = Field(..., description="List of accessible graphs")
  selectedGraphId: str | None = Field(None, description="Currently selected graph ID")


class CreateAPIKeyRequest(BaseModel):
  """Request model for creating a new API key."""

  name: str = Field(
    ..., min_length=1, max_length=100, description="Name for the API key"
  )
  description: str | None = Field(
    None, max_length=500, description="Optional description"
  )
  expires_at: str | None = Field(
    None,
    description="Optional expiration date in ISO format (e.g. 2024-12-31T23:59:59Z)",
  )


class APIKeyInfo(BaseModel):
  """API key information response model."""

  id: str = Field(..., description="API key ID")
  name: str = Field(..., description="API key name")
  description: str | None = Field(None, description="API key description")
  prefix: str = Field(..., description="API key prefix for identification")
  is_active: bool = Field(..., description="Whether the key is active")
  last_used_at: str | None = Field(None, description="Last used timestamp")
  expires_at: str | None = Field(None, description="Expiration timestamp")
  created_at: str = Field(..., description="Creation timestamp")


class CreateAPIKeyResponse(BaseModel):
  """Response model for creating a new API key."""

  api_key: APIKeyInfo = Field(..., description="API key information")
  key: str = Field(..., description="The actual API key (only shown once)")


class APIKeysResponse(BaseModel):
  """Response model for listing API keys."""

  api_keys: list[APIKeyInfo] = Field(..., description="List of user's API keys")


class UpdateAPIKeyRequest(BaseModel):
  """Request model for updating an API key."""

  name: str | None = Field(
    None, min_length=1, max_length=100, description="New name for the API key"
  )
  description: str | None = Field(None, max_length=500, description="New description")


class UserLimitsResponse(BaseModel):
  """Response model for user limits information.

  UserLimits is now a simple safety valve to prevent runaway graph creation.
  Subscription tiers and rate limits are handled at the graph level.
  """

  id: str = Field(..., description="Unique limits identifier")
  user_id: str = Field(..., description="Associated user ID")
  max_user_graphs: int = Field(
    ..., description="Maximum number of user graphs allowed (safety limit)"
  )
  created_at: str = Field(..., description="Limits creation timestamp")
  updated_at: str = Field(..., description="Last update timestamp")


class UserUsageResponse(BaseModel):
  """Response model for user usage statistics.

  Simplified to only show graph usage as UserLimits is now just a safety valve.
  Other usage tracking (MCP, Agent calls) happens at the graph level.
  """

  user_id: str = Field(..., description="User identifier")
  graphs: dict[str, object] = Field(
    ..., description="Graph usage statistics (current/limit/remaining)"
  )
  limits: UserLimitsResponse = Field(..., description="Current user limits")


class UserGraphSummary(BaseModel):
  """Summary of a single graph for user analytics."""

  graph_id: str = Field(..., description="Graph database identifier")
  graph_name: str | None = Field(None, description="Display name for the graph")
  role: str = Field(..., description="User's role in this graph")
  total_nodes: int = Field(..., description="Total number of nodes")
  total_relationships: int = Field(..., description="Total number of relationships")
  estimated_size_mb: float = Field(..., description="Estimated database size in MB")
  last_accessed: str | None = Field(None, description="Last access timestamp")


class UserUsageSummaryResponse(BaseModel):
  """Response model for user usage summary."""

  user_id: str = Field(..., description="User identifier")
  graph_count: int = Field(..., description="Number of accessible graphs")
  total_nodes: int = Field(..., description="Total nodes across all graphs")
  total_relationships: int = Field(
    ..., description="Total relationships across all graphs"
  )
  usage_vs_limits: dict[str, object] = Field(
    ..., description="Usage compared to limits"
  )
  graphs: list[UserGraphSummary] = Field(..., description="Summary of each graph")
  timestamp: str = Field(..., description="Summary generation timestamp")


class UserAnalyticsResponse(BaseModel):
  """Response model for comprehensive user analytics."""

  user_info: dict[str, object] = Field(..., description="User information")
  graph_usage: dict[str, object] = Field(..., description="Graph usage statistics")
  api_usage: dict[str, object] = Field(..., description="API usage statistics")
  limits: dict[str, object] = Field(..., description="Current limits and restrictions")
  recent_activity: list[dict[str, object]] = Field(
    ..., description="Recent user activity"
  )
  timestamp: str = Field(..., description="Analytics generation timestamp")
