"""Graph query API models."""

from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field, ConfigDict


class CustomSchemaDefinition(BaseModel):
  """Custom schema definition for generic graphs.

  This model allows you to define custom node types, relationship types, and properties
  for graphs that don't fit the standard entity-based schema. Perfect for domain-specific
  applications like inventory systems, org charts, project management, etc.
  """

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "People, companies, and projects schema",
          "description": "Custom schema from custom_graph_demo showing organizational structure",
          "value": {
            "name": "custom_graph_demo",
            "version": "1.0.0",
            "description": "People, companies, and projects schema for the custom graph demo",
            "extends": "base",
            "nodes": [
              {
                "name": "Company",
                "properties": [
                  {"name": "identifier", "type": "STRING", "is_primary_key": True},
                  {"name": "name", "type": "STRING", "is_required": True},
                  {"name": "industry", "type": "STRING"},
                  {"name": "location", "type": "STRING"},
                  {"name": "founded_year", "type": "INT64"},
                ],
              },
              {
                "name": "Project",
                "properties": [
                  {"name": "identifier", "type": "STRING", "is_primary_key": True},
                  {"name": "name", "type": "STRING", "is_required": True},
                  {"name": "status", "type": "STRING"},
                  {"name": "budget", "type": "DOUBLE"},
                  {"name": "start_date", "type": "STRING"},
                  {"name": "end_date", "type": "STRING"},
                ],
              },
              {
                "name": "Person",
                "properties": [
                  {"name": "identifier", "type": "STRING", "is_primary_key": True},
                  {"name": "name", "type": "STRING", "is_required": True},
                  {"name": "age", "type": "INT64"},
                  {"name": "title", "type": "STRING"},
                  {"name": "interests", "type": "STRING"},
                ],
              },
            ],
            "relationships": [
              {
                "name": "PERSON_WORKS_FOR_COMPANY",
                "from_node": "Person",
                "to_node": "Company",
                "properties": [
                  {"name": "role", "type": "STRING"},
                  {"name": "started_on", "type": "STRING"},
                ],
              },
              {
                "name": "PERSON_WORKS_ON_PROJECT",
                "from_node": "Person",
                "to_node": "Project",
                "properties": [
                  {"name": "hours_per_week", "type": "INT64"},
                  {"name": "contribution", "type": "STRING"},
                ],
              },
              {
                "name": "COMPANY_SPONSORS_PROJECT",
                "from_node": "Company",
                "to_node": "Project",
                "properties": [
                  {"name": "sponsorship_level", "type": "STRING"},
                  {"name": "budget_committed", "type": "DOUBLE"},
                ],
              },
            ],
            "metadata": {"domain": "custom_graph_demo"},
          },
        },
        {
          "summary": "Inventory management schema",
          "description": "Simple schema for tracking products and warehouses",
          "value": {
            "name": "inventory_management",
            "version": "1.0.0",
            "description": "Inventory tracking system with products, warehouses, and suppliers",
            "nodes": [
              {
                "name": "Product",
                "properties": [
                  {"name": "sku", "type": "STRING", "is_primary_key": True},
                  {"name": "name", "type": "STRING", "is_required": True},
                  {"name": "price", "type": "DOUBLE"},
                  {"name": "quantity", "type": "INT64"},
                  {"name": "category", "type": "STRING"},
                ],
              },
              {
                "name": "Warehouse",
                "properties": [
                  {"name": "identifier", "type": "STRING", "is_primary_key": True},
                  {"name": "location", "type": "STRING", "is_required": True},
                  {"name": "capacity", "type": "INT64"},
                ],
              },
              {
                "name": "Supplier",
                "properties": [
                  {"name": "id", "type": "STRING", "is_primary_key": True},
                  {"name": "name", "type": "STRING", "is_required": True},
                  {"name": "contact", "type": "STRING"},
                ],
              },
            ],
            "relationships": [
              {
                "name": "STORED_IN",
                "from_node": "Product",
                "to_node": "Warehouse",
                "properties": [
                  {"name": "since", "type": "DATE"},
                  {"name": "quantity", "type": "INT64"},
                ],
              },
              {
                "name": "SUPPLIED_BY",
                "from_node": "Product",
                "to_node": "Supplier",
                "properties": [{"name": "cost", "type": "DOUBLE"}],
              },
            ],
            "metadata": {"created_by": "inventory_team", "industry": "retail"},
          },
        },
        {
          "summary": "Minimal schema",
          "description": "Simplest custom schema with just two node types",
          "value": {
            "name": "simple_graph",
            "version": "1.0.0",
            "description": "Basic graph with just two node types",
            "nodes": [
              {
                "name": "Item",
                "properties": [
                  {"name": "id", "type": "STRING", "is_primary_key": True},
                  {"name": "name", "type": "STRING", "is_required": True},
                ],
              },
              {
                "name": "Category",
                "properties": [
                  {"name": "id", "type": "STRING", "is_primary_key": True},
                  {"name": "name", "type": "STRING", "is_required": True},
                ],
              },
            ],
            "relationships": [
              {
                "name": "BELONGS_TO",
                "from_node": "Item",
                "to_node": "Category",
                "properties": [],
              }
            ],
            "metadata": {},
          },
        },
      ]
    }
  )

  name: str = Field(..., description="Schema name")
  version: str = Field("1.0.0", description="Schema version")
  description: Optional[str] = Field(None, description="Schema description")
  extends: Optional[str] = Field(
    None, description="Base schema to extend (e.g., 'base' for common utilities)"
  )
  nodes: List[Dict[str, Any]] = Field(
    default_factory=list, description="List of node definitions with properties"
  )
  relationships: List[Dict[str, Any]] = Field(
    default_factory=list, description="List of relationship definitions"
  )
  metadata: Dict[str, Any] = Field(
    default_factory=dict, description="Additional schema metadata"
  )


class SchemaValidationRequest(BaseModel):
  """Request model for schema validation."""

  schema_definition: Union[Dict[str, Any], str] = Field(
    ...,
    description="Schema definition as JSON dict or JSON/YAML string",
    examples=[
      {
        "name": "financial_analysis",
        "version": "1.0.0",
        "description": "Schema for financial data with companies and filings",
        "nodes": [
          {
            "name": "Company",
            "properties": [
              {"name": "cik", "type": "STRING", "is_primary_key": True},
              {"name": "name", "type": "STRING", "is_required": True},
              {"name": "ticker", "type": "STRING"},
              {"name": "market_cap", "type": "INT64"},
            ],
          },
          {
            "name": "Filing",
            "properties": [
              {"name": "accession_number", "type": "STRING", "is_primary_key": True},
              {"name": "form_type", "type": "STRING", "is_required": True},
              {"name": "filing_date", "type": "DATE"},
            ],
          },
        ],
        "relationships": [
          {
            "name": "FILED",
            "from_node": "Company",
            "to_node": "Filing",
            "properties": [{"name": "filing_count", "type": "INT32"}],
          }
        ],
      },
      """name: inventory_management
version: '1.0.0'
description: Inventory tracking schema
nodes:
  - name: Product
    properties:
      - name: sku
        type: STRING
        is_primary_key: true
      - name: name
        type: STRING
        is_required: true
      - name: quantity
        type: INT32
  - name: Warehouse
    properties:
      - name: location_id
        type: STRING
        is_primary_key: true
      - name: name
        type: STRING
relationships:
  - name: STORED_IN
    from_node: Product
    to_node: Warehouse""",
      {
        "name": "invalid_schema_example",
        "version": "1.0.0",
        "nodes": [
          {
            "name": "Company",
            "properties": [
              {"name": "name", "type": "INVALID_TYPE"},
            ],
          }
        ],
        "relationships": [
          {
            "name": "RELATED_TO",
            "from_node": "Company",
            "to_node": "NonExistentNode",
          }
        ],
      },
    ],
  )
  format: str = Field(
    "json",
    description="Schema format: json, yaml, or dict",
    examples=["json", "yaml", "dict"],
  )
  check_compatibility: Optional[List[str]] = Field(
    None,
    description="List of existing schema extensions to check compatibility with",
    examples=[None, ["roboledger"], ["sec_base", "industry"]],
  )


class SchemaValidationResponse(BaseModel):
  """Response model for schema validation."""

  valid: bool = Field(
    ...,
    description="Whether the schema is valid",
    examples=[True, False, True],
  )
  message: str = Field(
    ...,
    description="Validation message",
    examples=[
      "Schema is valid",
      "Schema validation failed",
      "Schema is valid with 2 warning(s)",
    ],
  )
  errors: Optional[List[str]] = Field(
    None,
    description="List of validation errors (only present when valid=false)",
    examples=[
      None,
      [
        "Invalid data type 'INVALID_TYPE' for property 'name' in node 'Company'",
        "Relationship 'RELATED_TO' references non-existent node 'NonExistentNode'",
        "Node 'Company' has no primary key defined",
      ],
    ],
  )
  warnings: Optional[List[str]] = Field(
    None,
    description="List of validation warnings (schema is still valid but has potential issues)",
    examples=[
      None,
      [
        "Isolated nodes with no relationships: ['Warehouse', 'Location']",
        "Schema has no relationships defined",
        "Property 'deprecated_field' uses deprecated type DECIMAL",
      ],
    ],
  )
  stats: Optional[Dict[str, int]] = Field(
    None,
    description="Schema statistics (only present when valid=true)",
    examples=[
      {"nodes": 2, "relationships": 1, "total_properties": 7, "primary_keys": 2},
      {"nodes": 5, "relationships": 8, "total_properties": 32, "primary_keys": 5},
    ],
  )
  compatibility: Optional[Dict[str, Any]] = Field(
    None,
    description="Compatibility check results (only when check_compatibility specified)",
    examples=[
      {
        "compatible": True,
        "conflicts": [],
        "checked_extensions": ["roboledger"],
      },
      {
        "compatible": False,
        "conflicts": [
          "Node 'Transaction' conflicts with existing definition in 'roboledger'",
          "Property 'amount' type mismatch: INT64 vs DOUBLE",
        ],
        "checked_extensions": ["roboledger", "sec_base"],
      },
    ],
  )


class SchemaExportRequest(BaseModel):
  """Request model for exporting graph schema."""

  graph_id: str = Field(..., description="Graph ID to export schema from")
  format: str = Field("json", description="Export format: json, yaml, or cypher")
  include_data_stats: bool = Field(
    False, description="Include statistics about actual data in the graph"
  )


class SchemaExportResponse(BaseModel):
  """Response model for schema export."""

  graph_id: str = Field(..., description="Graph ID", examples=["sec", "kg1a2b3c4d5"])
  schema_definition: Union[Dict[str, Any], str] = Field(
    ...,
    description="Exported schema definition (format depends on 'format' parameter)",
    examples=[
      {
        "name": "financial_analysis_schema",
        "version": "1.0.0",
        "type": "custom",
        "description": "Schema for SEC financial data analysis",
        "nodes": [
          {
            "name": "Company",
            "properties": [
              {"name": "cik", "type": "STRING", "is_primary_key": True},
              {"name": "name", "type": "STRING", "is_required": True},
              {"name": "ticker", "type": "STRING"},
              {"name": "market_cap", "type": "INT64"},
              {"name": "sector", "type": "STRING"},
            ],
          },
          {
            "name": "Filing",
            "properties": [
              {"name": "accession_number", "type": "STRING", "is_primary_key": True},
              {"name": "form_type", "type": "STRING", "is_required": True},
              {"name": "filing_date", "type": "DATE"},
              {"name": "fiscal_year", "type": "INT32"},
            ],
          },
        ],
        "relationships": [
          {
            "name": "FILED",
            "from_node": "Company",
            "to_node": "Filing",
            "properties": [{"name": "filing_count", "type": "INT32"}],
          }
        ],
      },
      """name: financial_analysis_schema
version: '1.0.0'
type: custom
description: Schema for SEC financial data analysis
nodes:
  - name: Company
    properties:
      - name: cik
        type: STRING
        is_primary_key: true
      - name: name
        type: STRING
        is_required: true
      - name: ticker
        type: STRING
      - name: market_cap
        type: INT64
relationships:
  - name: FILED
    from_node: Company
    to_node: Filing""",
      """CREATE NODE TABLE Company (
  cik STRING PRIMARY KEY,
  name STRING NOT NULL,
  ticker STRING,
  market_cap INT64,
  sector STRING
);

CREATE NODE TABLE Filing (
  accession_number STRING PRIMARY KEY,
  form_type STRING NOT NULL,
  filing_date DATE,
  fiscal_year INT32
);

CREATE REL TABLE FILED (
  FROM Company TO Filing,
  filing_count INT32
);""",
    ],
  )
  format: str = Field(
    ...,
    description="Export format used",
    examples=["json", "yaml", "cypher"],
  )
  exported_at: str = Field(
    ..., description="Export timestamp", examples=["2025-10-29T10:30:00Z"]
  )
  data_stats: Optional[Dict[str, Any]] = Field(
    None,
    description="Data statistics if requested (only when include_data_stats=true)",
    examples=[
      {
        "node_labels_count": 4,
        "relationship_types_count": 3,
        "node_properties_count": 12,
        "node_counts": {"Company": 8500, "Filing": 125000, "Industry": 1200},
        "total_nodes": 134700,
      }
    ],
  )


class SchemaInfoResponse(BaseModel):
  """Response model for runtime schema introspection.

  This model represents the actual current state of the graph database,
  showing what node labels, relationship types, and properties exist right now.
  """

  model_config = {
    "populate_by_name": True,
    "json_schema_extra": {
      "examples": [
        {
          "summary": "SEC shared repository schema",
          "description": "Runtime schema from SEC shared repository showing XBRL structures",
          "value": {
            "graph_id": "sec",
            "schema": {
              "node_labels": [
                "Entity",
                "Report",
                "Fact",
                "Element",
                "Period",
                "Unit",
                "FactDimension",
                "Taxonomy",
                "Structure",
                "Association",
              ],
              "relationship_types": [
                "ENTITY_HAS_REPORT",
                "REPORT_HAS_FACT",
                "FACT_HAS_ELEMENT",
                "FACT_HAS_PERIOD",
                "FACT_HAS_UNIT",
                "FACT_HAS_DIMENSION",
                "REPORT_USES_TAXONOMY",
                "STRUCTURE_HAS_TAXONOMY",
                "STRUCTURE_HAS_ASSOCIATION",
                "ASSOCIATION_HAS_FROM_ELEMENT",
                "ASSOCIATION_HAS_TO_ELEMENT",
              ],
              "node_properties": {
                "Entity": {
                  "cik": "STRING",
                  "name": "STRING",
                  "ticker": "STRING",
                  "entity_type": "STRING",
                  "industry": "STRING",
                  "state_of_incorporation": "STRING",
                  "fiscal_year_end": "STRING",
                },
                "Report": {
                  "form": "STRING",
                  "report_date": "STRING",
                  "filing_date": "STRING",
                  "accession_number": "STRING",
                  "name": "STRING",
                },
                "Fact": {
                  "numeric_value": "DOUBLE",
                  "decimals": "INT64",
                  "fact_type": "STRING",
                },
              },
            },
          },
        },
        {
          "summary": "Accounting graph schema",
          "description": "Runtime schema from accounting demo showing double-entry bookkeeping structure",
          "value": {
            "graph_id": "kg1a2b3c4d5",
            "schema": {
              "node_labels": [
                "Transaction",
                "LineItem",
                "Element",
                "Report",
                "Entity",
                "Fact",
                "Period",
                "Unit",
              ],
              "relationship_types": [
                "TRANSACTION_HAS_LINE_ITEM",
                "LINE_ITEM_RELATES_TO_ELEMENT",
                "ENTITY_HAS_REPORT",
                "REPORT_HAS_FACT",
                "FACT_HAS_ELEMENT",
                "FACT_HAS_PERIOD",
                "FACT_HAS_UNIT",
              ],
              "node_properties": {
                "Transaction": {
                  "date": "STRING",
                  "description": "STRING",
                  "type": "STRING",
                },
                "LineItem": {
                  "debit_amount": "DOUBLE",
                  "credit_amount": "DOUBLE",
                },
                "Element": {
                  "name": "STRING",
                  "classification": "STRING",
                  "balance": "STRING",
                },
              },
            },
          },
        },
        {
          "summary": "Custom graph schema",
          "description": "Runtime schema from custom graph showing people, companies, and projects",
          "value": {
            "graph_id": "kg9f8e7d6c5",
            "schema": {
              "node_labels": ["Person", "Company", "Project"],
              "relationship_types": [
                "PERSON_WORKS_FOR_COMPANY",
                "PERSON_WORKS_ON_PROJECT",
                "COMPANY_SPONSORS_PROJECT",
              ],
              "node_properties": {
                "Person": {
                  "identifier": "STRING",
                  "name": "STRING",
                  "title": "STRING",
                  "interests": "STRING",
                },
                "Company": {
                  "identifier": "STRING",
                  "name": "STRING",
                  "industry": "STRING",
                  "location": "STRING",
                },
                "Project": {
                  "name": "STRING",
                  "status": "STRING",
                  "budget": "DOUBLE",
                },
              },
            },
          },
        },
        {
          "summary": "Empty graph schema",
          "description": "Runtime schema from a newly created graph with no data yet",
          "value": {
            "graph_id": "kg2x3y4z5a6",
            "schema": {
              "node_labels": [],
              "relationship_types": [],
              "node_properties": {},
            },
          },
        },
      ]
    },
  }

  graph_id: str = Field(
    ...,
    description="Graph database identifier",
  )
  schema_data: Dict[str, Any] = Field(
    ...,
    description="Runtime schema information showing actual database structure",
    alias="schema",
  )


# New API models for LadybugDB database operations
