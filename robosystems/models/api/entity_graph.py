"""Pydantic schemas for entity-graph creation operations."""

from pydantic import BaseModel, Field, field_validator

from robosystems.models.api.common import OptionalLegalForm


class EntityCreate(BaseModel):
  name: str = Field(..., min_length=1, max_length=255)
  uri: str | None = Field(
    None,
    description=(
      "The entity's website, kept when it is an http(s) address. The entity's "
      "URI is assigned by RoboSystems."
    ),
  )
  ticker: str | None = None
  cik: str | None = None
  database: str | None = None
  sic: str | None = None
  sic_description: str | None = None
  category: str | None = None
  state_of_incorporation: str | None = None
  ein: str | None = None
  entity_type: OptionalLegalForm = Field(
    default=None,
    description=(
      "Entity legal form (corporation, llc, partnership, sole_proprietorship, "
      "non_profit)."
    ),
  )
  tier: str | None = Field(
    default=None,
    description="Graph tier to create (ladybug-standard, ladybug-large, ladybug-xlarge). If not specified, defaults to ladybug-standard.",
    examples=["ladybug-standard"],
  )
  extensions: list[str] | None = Field(
    default=None,
    description="Schema extensions to enable in the entity graph. If not specified, base schema only will be loaded for stability. Available extensions: roboledger, roboinvestor",
    examples=[["roboledger", "roboinvestor"]],
  )

  @field_validator("uri", mode="before")
  @classmethod
  def validate_uri(cls, v: str | None) -> str | None:
    """A blank website is none."""
    if isinstance(v, str) and not v.strip():
      return None
    return v

  @field_validator("extensions")
  @classmethod
  def validate_extensions(cls, v: list[str] | None) -> list[str] | None:
    """Validate that extensions are known and compatible."""
    if v is None:
      return v

    from robosystems.schemas.runtime.manager import ENTITY_GRAPH_EXTENSIONS

    available_extensions = ENTITY_GRAPH_EXTENSIONS

    # Check for unknown extensions
    unknown_extensions = set(v) - available_extensions
    if unknown_extensions:
      raise ValueError(
        f"Unknown extensions: {', '.join(unknown_extensions)}. Available extensions: {', '.join(sorted(available_extensions))}"
      )

    return v


class EntityUpdate(BaseModel):
  name: str | None = Field(None, min_length=1, max_length=255)
  uri: str | None = Field(None, min_length=1)
  cik: str | None = None
  sic: str | None = None
  sic_description: str | None = None
  category: str | None = None
  state_of_incorporation: str | None = None
  fiscal_year_end: str | None = None
  ein: str | None = None
  entity_type: str | None = None

  @field_validator("uri")
  @classmethod
  def validate_uri(cls, v: str | None) -> str | None:
    """Validate that URI is not an empty string."""
    if v is not None and len(v.strip()) == 0:
      raise ValueError("URI cannot be empty string")
    return v


class EntityResponse(BaseModel):
  id: str
  name: str
  uri: str
  cik: str | None = None
  database: str | None = None
  sic: str | None = None
  sic_description: str | None = None
  category: str | None = None
  state_of_incorporation: str | None = None
  fiscal_year_end: str | None = None
  ein: str | None = None
  entity_type: str | None = None
  created_at: str
  updated_at: str
  tier: str | None = None
  extensions: list[str] | None = None


class EntityListResponse(BaseModel):
  entities: list[EntityResponse]
  total: int


class EntityWithGraphResponse(BaseModel):
  entity: EntityResponse
  graph_id: str
  graph_status: str
  graph_tier: str
  graph_extensions: list[str]


class AvailableExtension(BaseModel):
  name: str
  # The name to show a user. `name` is the schema slug the create path takes
  # ("roboledger"), which is not what a tier or extension picker should be
  # rendering — clients fall back to the slug only when this is absent.
  display_name: str | None = None
  description: str
  enabled: bool = False


class AvailableExtensionsResponse(BaseModel):
  extensions: list[AvailableExtension]
