"""Graph core API models - graph creation and metadata."""

from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict

from .schema import CustomSchemaDefinition


class GraphMetadata(BaseModel):
  """Metadata for graph creation."""

  graph_name: str = Field(
    ..., description="Display name for the graph", examples=["Acme Consulting LLC"]
  )
  description: Optional[str] = Field(
    None,
    description="Optional description",
    examples=["Professional consulting services with full accounting integration"],
  )
  schema_extensions: List[str] = Field(
    default_factory=list,
    description="Schema extensions to enable",
    examples=[["roboledger"]],
  )
  tags: List[str] = Field(
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
  cik: Optional[str] = Field(None, description="CIK number for SEC filings")
  sic: Optional[str] = Field(None, description="SIC code")
  sic_description: Optional[str] = Field(None, description="SIC description")
  category: Optional[str] = Field(None, description="Business category")
  state_of_incorporation: Optional[str] = Field(
    None, description="State of incorporation"
  )
  fiscal_year_end: Optional[str] = Field(None, description="Fiscal year end (MMDD)")
  ein: Optional[str] = Field(None, description="Employer Identification Number")


class CreateGraphRequest(BaseModel):
  """Request model for creating a new graph.

  Use this to create either:
  - **Entity graphs**: Standard graphs with entity schema and optional extensions
  - **Custom graphs**: Generic graphs with fully custom schema definitions
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
            "instance_tier": "kuzu-standard",
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
          "summary": "Entity graph without initial entity",
          "description": "Create an entity graph structure without populating initial data (useful for bulk data imports)",
          "value": {
            "metadata": {
              "graph_name": "Investment Portfolio Graph",
              "description": "Knowledge graph for tracking investment portfolios and holdings",
              "schema_extensions": ["roboinvestor"],
            },
            "instance_tier": "kuzu-standard",
            "initial_entity": None,
            "create_entity": False,
            "tags": ["investments", "portfolio-management"],
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
            "instance_tier": "kuzu-standard",
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
            "instance_tier": "kuzu-standard",
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
          "summary": "Generic empty graph",
          "description": "Create an empty graph for general-purpose use without custom schema",
          "value": {
            "metadata": {
              "graph_name": "Customer Analytics Graph",
              "description": "Graph database for customer relationship and behavior analysis",
              "schema_extensions": [],
            },
            "instance_tier": "kuzu-standard",
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
    "kuzu-standard",
    description="Instance tier: kuzu-standard, kuzu-large, kuzu-xlarge, neo4j-community-large, neo4j-enterprise-xlarge",
    pattern="^(kuzu-standard|kuzu-large|kuzu-xlarge|neo4j-community-large|neo4j-enterprise-xlarge)$",
  )
  custom_schema: Optional[CustomSchemaDefinition] = Field(
    None,
    description="Custom schema definition to apply. If provided, creates a generic custom graph. If omitted, creates an entity graph using schema_extensions.",
  )
  initial_entity: Optional[InitialEntityData] = Field(
    None,
    description="Optional initial entity to create in the graph. If provided with entity graph, populates the first entity node.",
  )
  create_entity: bool = Field(
    default=True,
    description="Whether to create the entity node and upload initial data. Only applies when initial_entity is provided. Set to False to create graph without populating entity data (useful for file-based ingestion workflows).",
  )
  tags: List[str] = Field(
    default_factory=list,
    description="Optional tags for organization",
    max_length=10,
  )


class CreateGraphResponse(BaseModel):
  """Response model for graph creation."""

  graph_id: str = Field(..., description="Created graph identifier")
  status: str = Field(..., description="Creation status")
  message: str = Field(..., description="Status message")
