"""Pydantic schemas for entity-graph creation operations."""

from pydantic import BaseModel, Field, field_validator


class EntityCreate(BaseModel):
  name: str = Field(..., min_length=1, max_length=255)
  uri: str | None = Field(
    None,
    min_length=1,
    description="Entity URI. If not provided, will be auto-generated as an RDF-style URI based on the graph ID.",
  )
  cik: str | None = None
  database: str | None = None
  sic: str | None = None
  sic_description: str | None = None
  category: str | None = None
  state_of_incorporation: str | None = None
  fiscal_year_end: str | None = None
  ein: str | None = None
  tier: str | None = Field(
    default=None,
    description="Graph tier to create (kuzu-standard, kuzu-large, kuzu-xlarge). If not specified, defaults to kuzu-standard.",
    examples=["kuzu-standard"],
  )
  extensions: list[str] | None = Field(
    default=None,
    description="Schema extensions to enable in the entity graph. If not specified, base schema only will be loaded for stability. Available extensions: roboledger, roboinvestor, roboscm, robofo, robohrm, roboepm, roboreport",
    examples=[["roboledger", "roboinvestor"]],
  )

  @field_validator("extensions")
  @classmethod
  def validate_extensions(cls, v: list[str] | None) -> list[str] | None:
    """Validate that extensions are known and compatible."""
    if v is None:
      return v

    # List of available extensions
    available_extensions = {
      "roboledger",
      "roboinvestor",
      "roboscm",
      "robofo",
      "robohrm",
      "roboepm",
      "roboreport",
    }

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
  description: str
  enabled: bool = False


class AvailableExtensionsResponse(BaseModel):
  extensions: list[AvailableExtension]
