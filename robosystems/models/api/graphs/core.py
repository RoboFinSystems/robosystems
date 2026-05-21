"""Graph core API models - graph creation and metadata."""

from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .schema import CustomSchemaDefinition


class GraphMetadata(BaseModel):
  """Metadata for graph creation."""

  graph_name: str = Field(
    ..., description="Display name for the graph", examples=["Acme Consulting LLC"]
  )
  description: str | None = Field(
    None,
    description="Optional description",
    examples=["Professional consulting services with full accounting integration"],
  )
  schema_extensions: list[str] = Field(
    default_factory=list,
    description="Schema extensions to enable",
    examples=[["roboledger"]],
  )
  tags: list[str] = Field(
    default_factory=list,
    description="Tags for organizing graphs",
    examples=[["consulting", "professional-services"]],
  )


class InitialEntityData(BaseModel):
  """Initial entity data for entity-focused graph creation.

  When creating an entity graph with an initial entity node, this model defines
  the entity's identifying information and metadata.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Public company entity",
          "description": "Initial entity data for a publicly-traded company with SEC information",
          "value": {
            "name": "Apple Inc.",
            "uri": "https://www.apple.com",
            "cik": "0000320193",
            "ein": "94-2404110",
            "sic": "3571",
            "sic_description": "Electronic Computers",
            "state_of_incorporation": "California",
            "fiscal_year_end": "0930",
          },
        },
        {
          "summary": "Private company entity",
          "description": "Initial entity data for a private company with minimal information",
          "value": {
            "name": "Acme Consulting LLC",
            "uri": "https://acmeconsulting.com",
            "ein": "12-3456789",
            "state_of_incorporation": "Delaware",
            "category": "Professional Services",
          },
        },
        {
          "summary": "Minimal entity",
          "description": "Simplest entity with just required fields",
          "value": {
            "name": "Startup Inc",
            "uri": "https://startup.io",
          },
        },
      ]
    }
  )

  name: str = Field(..., min_length=1, max_length=255, description="Entity name")
  uri: str = Field(..., min_length=1, description="Entity website or URI")
  ticker: str | None = Field(
    None,
    min_length=1,
    max_length=10,
    description="Entity symbol/ticker (e.g., 'HARB', 'NVDA'). Auto-generated from name if not provided.",
  )
  cik: str | None = Field(None, description="CIK number for SEC filings")
  sic: str | None = Field(None, description="SIC code")
  sic_description: str | None = Field(None, description="SIC description")
  category: str | None = Field(None, description="Business category")
  state_of_incorporation: str | None = Field(None, description="State of incorporation")
  fiscal_year_end: str | None = Field(None, description="Fiscal year end (MMDD)")
  ein: str | None = Field(None, description="Employer Identification Number")
  entity_type: str | None = Field(
    None,
    description=(
      "Entity legal form (e.g. 'corporation', 'llc' / "
      "'limited_liability_company', 'partnership', 'sole_proprietorship', "
      "'non_profit'). Drives the graph's default "
      "Reporting Style at creation — partnership and llc get dedicated "
      "equity-form Styles; everything else defaults to corporate. Blank "
      "falls back to corporate."
    ),
  )
  reporting_style_id: str | None = Field(
    None,
    description=(
      "Optional explicit Reporting Style Structure id to pin on the graph, "
      "overriding the entity_type-derived default. Leave blank to derive "
      "from entity_type. Change later via the change-reporting-style "
      "operation."
    ),
  )


class CreateGraphRequest(BaseModel):
  """Request model for creating a new graph.

  Use this to create either:
  - **Entity graphs**: Standard graphs with entity schema. Requires `initial_entity`.
  - **Custom graphs**: Generic graphs with a fully custom schema. Requires `custom_schema`; `initial_entity` is not used.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Entity graph with initial entity",
          "description": "Create a graph with initial entity data for a specific company/organization",
          "value": {
            "metadata": {
              "graph_name": "Acme Consulting LLC",
              "description": "Professional consulting services with full accounting integration",
              "schema_extensions": ["roboledger"],
            },
            "instance_tier": "ladybug-standard",
            "initial_entity": {
              "name": "Acme Consulting LLC",
              "uri": "https://acmeconsulting.com",
              "ein": "12-3456789",
              "cik": "0001234567",
              "state_of_incorporation": "Delaware",
            },
            "create_entity": True,
            "tags": ["consulting", "professional-services", "production"],
          },
        },
        {
          "summary": "Custom graph with people and companies",
          "description": "Create a generic graph with custom schema from custom_graph_demo",
          "value": {
            "metadata": {
              "graph_name": "custom_graph_demo_1234",
              "description": "Custom graph demo with people, companies, and projects",
              "schema_extensions": [],
            },
            "instance_tier": "ladybug-standard",
            "custom_schema": {
              "name": "custom_graph_demo",
              "version": "1.0.0",
              "description": "People, companies, and projects schema",
              "extends": "base",
              "nodes": [
                {
                  "name": "Person",
                  "properties": [
                    {"name": "identifier", "type": "STRING", "is_primary_key": True},
                    {"name": "name", "type": "STRING", "is_required": True},
                    {"name": "title", "type": "STRING"},
                  ],
                },
                {
                  "name": "Company",
                  "properties": [
                    {"name": "identifier", "type": "STRING", "is_primary_key": True},
                    {"name": "name", "type": "STRING", "is_required": True},
                    {"name": "industry", "type": "STRING"},
                  ],
                },
              ],
              "relationships": [
                {
                  "name": "PERSON_WORKS_FOR_COMPANY",
                  "from_node": "Person",
                  "to_node": "Company",
                  "properties": [{"name": "role", "type": "STRING"}],
                }
              ],
              "metadata": {"domain": "custom_graph_demo"},
            },
            "tags": ["custom", "demo", "generic"],
          },
        },
        {
          "summary": "Inventory management custom graph",
          "description": "Create a custom graph for inventory tracking",
          "value": {
            "metadata": {
              "graph_name": "warehouse_inventory",
              "description": "Inventory management system",
              "schema_extensions": [],
            },
            "instance_tier": "ladybug-standard",
            "custom_schema": {
              "name": "inventory_management",
              "version": "1.0.0",
              "description": "Inventory tracking with products and warehouses",
              "nodes": [
                {
                  "name": "Product",
                  "properties": [
                    {"name": "sku", "type": "STRING", "is_primary_key": True},
                    {"name": "name", "type": "STRING", "is_required": True},
                    {"name": "price", "type": "DOUBLE"},
                    {"name": "quantity", "type": "INT64"},
                  ],
                },
                {
                  "name": "Warehouse",
                  "properties": [
                    {"name": "identifier", "type": "STRING", "is_primary_key": True},
                    {"name": "location", "type": "STRING", "is_required": True},
                  ],
                },
              ],
              "relationships": [
                {
                  "name": "STORED_IN",
                  "from_node": "Product",
                  "to_node": "Warehouse",
                  "properties": [{"name": "quantity", "type": "INT64"}],
                }
              ],
              "metadata": {"industry": "retail"},
            },
            "tags": ["inventory", "retail"],
          },
        },
        {
          "summary": "Minimal custom graph",
          "description": "Create a generic custom graph with a minimal schema",
          "value": {
            "metadata": {
              "graph_name": "Customer Analytics Graph",
              "description": "Graph database for customer relationship and behavior analysis",
              "schema_extensions": [],
            },
            "instance_tier": "ladybug-standard",
            "custom_schema": {
              "name": "customer_analytics",
              "version": "1.0.0",
              "nodes": [],
              "relationships": [],
            },
            "tags": ["analytics", "customers", "marketing"],
          },
        },
      ]
    }
  )

  metadata: GraphMetadata = Field(
    ..., description="Graph metadata including name, description, and schema extensions"
  )
  instance_tier: str = Field(
    "ladybug-standard",
    description="Instance tier: ladybug-standard, ladybug-large, ladybug-xlarge",
    pattern="^(ladybug-standard|ladybug-large|ladybug-xlarge)$",
  )
  custom_schema: CustomSchemaDefinition | None = Field(
    None,
    description="Custom schema definition to apply. If provided, creates a generic custom graph. If omitted, creates an entity graph using schema_extensions.",
  )
  initial_entity: InitialEntityData | None = Field(
    None,
    description="Initial entity for the graph. Required for entity graphs (when custom_schema is omitted). Omit only when providing custom_schema for a generic graph.",
  )
  create_entity: bool = Field(
    default=True,
    description="Whether to create the entity node and upload initial data. Only applies when initial_entity is provided. Set to False to create graph without populating entity data (useful for file-based ingestion workflows).",
  )
  tags: list[str] = Field(
    default_factory=list,
    description="Optional tags for organization",
    max_length=10,
  )

  @model_validator(mode="after")
  def require_entity_for_entity_graphs(self) -> Self:
    if self.custom_schema is None and self.initial_entity is None:
      raise ValueError(
        "initial_entity is required when creating an entity graph. "
        "Provide initial_entity, or provide custom_schema to create a generic graph instead."
      )
    return self


class CreateGraphResponse(BaseModel):
  """Response model for graph creation."""

  graph_id: str = Field(..., description="Created graph identifier")
  status: str = Field(..., description="Creation status")
  message: str = Field(..., description="Status message")
