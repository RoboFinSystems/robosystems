"""User management API models."""

from typing import Optional
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

  graphId: str = Field(
    ..., description="Graph database identifier", examples=["kg1a2b3c4d5", "sec"]
  )
  graphName: str = Field(
    ...,
    description="Display name for the graph",
    examples=["Acme Consulting LLC", "SEC EDGAR Filings"],
  )
  role: str = Field(
    ..., description="User's role/access level", examples=["admin", "member", "read"]
  )
  isSelected: bool = Field(
    ...,
    description="Whether this is the currently selected graph",
    examples=[True, False],
  )
  createdAt: str = Field(
    ..., description="Creation timestamp", examples=["2024-01-15T10:00:00Z"]
  )
  isRepository: bool = Field(
    default=False,
    description="Whether this is a shared repository (vs user graph)",
    examples=[False, True],
  )
  repositoryType: Optional[str] = Field(
    default=None,
    description="Repository type if isRepository=true",
    examples=[None, "sec", "industry", "economic"],
  )

  class Config:
    json_schema_extra = {
      "examples": [
        {
          "graphId": "kg1a2b3c4d5",
          "graphName": "Acme Consulting LLC",
          "role": "admin",
          "isSelected": True,
          "createdAt": "2024-01-15T10:00:00Z",
        },
        {
          "graphId": "kg9z8y7x6w5",
          "graphName": "TechCorp Enterprises",
          "role": "member",
          "isSelected": False,
          "createdAt": "2024-02-20T14:30:00Z",
        },
      ]
    }


class UserGraphsResponse(BaseModel):
  """User graphs response model."""

  graphs: list[GraphInfo] = Field(..., description="List of accessible graphs")
  selectedGraphId: str | None = Field(
    None,
    description="Currently selected graph ID",
    examples=["kg1a2b3c4d5", None],
  )

  class Config:
    json_schema_extra = {
      "examples": [
        {
          "graphs": [
            {
              "graphId": "kg1a2b3c4d5",
              "graphName": "Acme Consulting LLC",
              "role": "admin",
              "isSelected": True,
              "createdAt": "2024-01-15T10:00:00Z",
            },
            {
              "graphId": "kg9z8y7x6w5",
              "graphName": "TechCorp Enterprises",
              "role": "member",
              "isSelected": False,
              "createdAt": "2024-02-20T14:30:00Z",
            },
          ],
          "selectedGraphId": "kg1a2b3c4d5",
        },
        {
          "graphs": [],
          "selectedGraphId": None,
        },
      ]
    }


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
