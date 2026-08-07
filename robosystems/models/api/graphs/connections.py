"""Shared connection models for all providers."""

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, ValidationInfo, field_validator

# Provider types
ProviderType = Literal["sec", "quickbooks", "external"]

# Source names an external connection can never claim: platform-emitted
# values, platform adapter providers (current and reserved), and the
# provider discriminators themselves.
RESERVED_SOURCE_NAMES = frozenset(
  {
    "manual",
    "system",
    "schedule",
    "quickbooks",
    "xero",
    "plaid",
    "sec",
    "external",
  }
)


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


class ExternalConnectionConfig(BaseModel):
  """External-integration connection configuration.

  Registers a source namespace for an integration the platform does not
  run: the connection is registration + telemetry, not execution config.
  The platform holds no credentials for the external source — the
  integration authenticates to its own source and writes here through
  the public API, stamping ``source_name`` on everything it emits.
  """

  source_name: str = Field(
    ...,
    min_length=2,
    max_length=64,
    pattern=r"^[a-z][a-z0-9_-]{1,63}$",
    description=(
      "Source slug the integration stamps on the events it emits "
      "(lowercase letters, digits, '-', '_'; must start with a letter). "
      "Unique per graph among live connections."
    ),
  )
  display_name: str | None = Field(
    None, max_length=128, description="Human-readable label for the connections UI."
  )

  @field_validator("source_name")
  @classmethod
  def validate_not_reserved(cls, v: str) -> str:
    if v in RESERVED_SOURCE_NAMES:
      raise ValueError(
        f"source_name {v!r} is reserved (platform sources and providers: "
        f"{sorted(RESERVED_SOURCE_NAMES)})"
      )
    return v


class CreateConnectionRequest(ConnectionBase):
  """Request to create a new connection."""

  sec_config: SECConnectionConfig | None = None
  quickbooks_config: QuickBooksConnectionConfig | None = None
  external_config: ExternalConnectionConfig | None = None

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

  @field_validator("sec_config", "quickbooks_config", "external_config")
  @classmethod
  def validate_provider_config(
    cls,
    v: SECConnectionConfig
    | QuickBooksConnectionConfig
    | ExternalConnectionConfig
    | None,
    info: ValidationInfo,
  ) -> (
    SECConnectionConfig | QuickBooksConnectionConfig | ExternalConnectionConfig | None
  ):
    """Ensure only the matching provider config is provided."""
    provider = info.data.get("provider")
    field_name = info.field_name
    if field_name is None:
      return v  # Should not happen in practice

    field_to_provider = {
      "sec_config": "sec",
      "quickbooks_config": "quickbooks",
      "external_config": "external",
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
  write_policy: str | None = Field(
    None,
    description=(
      "Source-of-truth write policy: 'native' (RoboSystems is authoritative; "
      "no outbound write-back) or 'qb_authoritative' (QuickBooks is "
      "authoritative; RoboSystems-originated entries publish to QB). "
      "Set via the write-policy endpoint."
    ),
  )
  source_name: str | None = Field(
    None,
    description=(
      "External-provider registered source slug — the value the "
      "integration stamps on the events it emits. Null for platform "
      "providers."
    ),
  )
  metadata: dict[str, object] = Field(..., description="Provider-specific metadata")


class SetWritePolicyRequest(BaseModel):
  """Request to set a connection's source-of-truth write policy.

  The explicit operator opt-in for outbound write-back. `hybrid` is omitted
  until its code path ships."""

  write_policy: Literal["native", "qb_authoritative"] = Field(
    ...,
    description=(
      "'native' = RoboSystems authoritative, no write-back; "
      "'qb_authoritative' = QuickBooks authoritative, entries publish to QB."
    ),
  )


class SyncConnectionRequest(BaseModel):
  """Request to sync a connection."""

  full_rebuild: bool = Field(
    False,
    description="Pull complete history from the provider, ignoring lookback window. Takes precedence over since_date.",
  )
  since_date: date | None = Field(
    None,
    description="Sync data from this date forward (ISO 8601). Ignored if full_rebuild=True. If neither set, provider default applies (e.g., QuickBooks: 60 days).",
  )
  sync_options: dict[str, object] | None = Field(
    None,
    description="Provider-specific sync options (escape hatch for fields not exposed at the top level).",
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
