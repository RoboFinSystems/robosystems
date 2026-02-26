"""Shared connection models for all providers."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, ValidationInfo, field_validator

# Provider types
ProviderType = Literal["sec", "quickbooks"]


class ConnectionBase(BaseModel):
  """Base connection model."""

  provider: ProviderType = Field(..., description="Connection provider type")
  entity_id: str | None = Field(
    None,
    min_length=1,
    description="Entity identifier. Required for QuickBooks, optional for SEC "
    "(SEC creates the entity from filing data).",
  )


class SECConnectionConfig(BaseModel):
  """SEC-specific connection configuration."""

  cik: str = Field(
    ..., min_length=1, max_length=10, description="SEC Central Index Key"
  )

  @field_validator("cik")
  @classmethod
  def validate_cik(cls, v: str) -> str:
    """Validate and normalize CIK format. Auto-pads with leading zeros to 10 digits."""
    clean_cik = "".join(filter(str.isdigit, v))
    if not clean_cik:
      raise ValueError("CIK must contain digits")
    if len(clean_cik) > 10:
      raise ValueError("CIK cannot be longer than 10 digits")
    return clean_cik.zfill(10)


class QuickBooksConnectionConfig(BaseModel):
  """QuickBooks-specific connection configuration."""

  # QuickBooks connections are handled through OAuth flow
  # Config will be populated after OAuth completion
  realm_id: str | None = Field(None, description="QuickBooks Realm ID")
  refresh_token: str | None = Field(None, description="OAuth refresh token")


class CreateConnectionRequest(ConnectionBase):
  """Request to create a new connection."""

  sec_config: SECConnectionConfig | None = None
  quickbooks_config: QuickBooksConnectionConfig | None = None

  @field_validator("entity_id")
  @classmethod
  def validate_entity_id_for_provider(
    cls, v: str | None, info: ValidationInfo
  ) -> str | None:
    """Require entity_id for providers that need a pre-existing entity."""
    provider = info.data.get("provider")
    if provider == "quickbooks" and not v:
      raise ValueError("entity_id is required for QuickBooks connections")
    return v

  @field_validator("sec_config", "quickbooks_config")
  @classmethod
  def validate_provider_config(
    cls,
    v: SECConnectionConfig | QuickBooksConnectionConfig | None,
    info: ValidationInfo,
  ) -> SECConnectionConfig | QuickBooksConnectionConfig | None:
    """Ensure only the matching provider config is provided."""
    provider = info.data.get("provider")
    field_name = info.field_name
    if field_name is None:
      return v  # Should not happen in practice

    # Map field names to provider types
    field_to_provider = {
      "sec_config": "sec",
      "quickbooks_config": "quickbooks",
    }

    expected_provider = field_to_provider.get(field_name)

    if provider == expected_provider and v is None:
      raise ValueError(f"{field_name} is required for {provider} connections")
    elif provider != expected_provider and v is not None:
      raise ValueError(
        f"{field_name} should not be provided for {provider} connections"
      )

    return v


class ConnectionResponse(BaseModel):
  """Connection response model."""

  model_config = {"from_attributes": True}

  connection_id: str = Field(..., description="Unique connection identifier")
  provider: str = Field(..., description="Connection provider type")
  entity_id: str | None = Field(None, description="Entity identifier")
  status: str = Field(..., description="Connection status")
  created_at: datetime | str = Field(..., description="Creation timestamp")
  updated_at: datetime | str | None = Field(None, description="Last update timestamp")
  last_sync: datetime | str | None = Field(None, description="Last sync timestamp")
  metadata: dict[str, object] = Field(..., description="Provider-specific metadata")


class SyncConnectionRequest(BaseModel):
  """Request to sync a connection."""

  full_sync: bool = Field(False, description="Perform full sync vs incremental")
  sync_options: dict[str, object] | None = Field(
    None, description="Provider-specific sync options"
  )


class LinkTokenRequest(BaseModel):
  """Request to create a link token for embedded authentication."""

  entity_id: str = Field(..., min_length=1, description="Entity identifier")
  user_id: str = Field(..., min_length=1, description="User identifier")
  provider: ProviderType | None = Field(
    None, description="Provider type (defaults based on connection)"
  )
  products: list[str] | None = Field(
    None, description="Data products to request (provider-specific)"
  )
  options: dict[str, object] | None = Field(
    None, description="Provider-specific options"
  )


class ExchangeTokenRequest(BaseModel):
  """Exchange temporary token for permanent credentials."""

  connection_id: str = Field(..., description="Connection ID to update")
  public_token: str = Field(
    ..., min_length=1, description="Temporary token from embedded auth"
  )
  metadata: dict[str, object] | None = Field(
    None, description="Provider-specific metadata"
  )


class ConnectionProviderInfo(BaseModel):
  """Information about a connection provider."""

  provider: ProviderType = Field(..., description="Provider identifier")
  display_name: str = Field(..., description="Human-readable provider name")
  description: str = Field(..., description="Provider description")
  auth_type: Literal["none", "oauth", "link", "api_key"] = Field(
    ..., description="Authentication type"
  )
  auth_flow: str | None = Field(None, description="Description of authentication flow")
  required_config: list[str] = Field(..., description="Required configuration fields")
  optional_config: list[str] = Field(
    default_factory=list, description="Optional configuration fields"
  )
  features: list[str] = Field(..., description="Supported features")
  sync_frequency: str | None = Field(None, description="Typical sync frequency")
  data_types: list[str] = Field(..., description="Types of data available")
  setup_instructions: str | None = Field(None, description="Setup instructions")
  documentation_url: str | None = Field(None, description="Link to documentation")


class ConnectionOptionsResponse(BaseModel):
  """Response with all available connection options."""

  providers: list[ConnectionProviderInfo] = Field(
    ..., description="Available connection providers"
  )
  total_providers: int = Field(..., description="Total number of providers")
