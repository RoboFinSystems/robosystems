"""Graph management API models."""

from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field, field_validator, ConfigDict
import re

# Import secure write operation detection

# Neo4j to Kuzu query translation patterns
NEO4J_DB_COMMANDS = re.compile(
  r"CALL\s+db\.(schema|labels|relationships|relationshipTypes|propertyKeys|indexes|constraints)\s*\(\s*\)",
  re.IGNORECASE | re.MULTILINE,
)

# Mapping of Neo4j commands to Kuzu equivalents
NEO4J_TO_KUZU_MAPPING = {
  "db.schema": "SHOW_TABLES()",
  "db.labels": "SHOW_TABLES()",
  "db.relationships": "SHOW_TABLES()",
  "db.relationshipTypes": "SHOW_TABLES()",
  "db.propertyKeys": "TABLE_INFO",
  "db.indexes": "SHOW_TABLES()",  # Kuzu doesn't have explicit indexes like Neo4j
  "db.constraints": "SHOW_TABLES()",  # Kuzu doesn't have constraints like Neo4j
}

# Constants
MAX_QUERY_LENGTH = 50000
DEFAULT_QUERY_TIMEOUT = 60


def translate_neo4j_to_kuzu(query: str) -> str:
  """
  Translate Neo4j-style db.* commands to Kuzu equivalents.

  Args:
      query: The original Cypher query

  Returns:
      Translated query compatible with Kuzu
  """
  # Check if query contains Neo4j db.* commands
  match = NEO4J_DB_COMMANDS.search(query)
  if not match:
    return query

  # Extract the command type
  command = match.group(1).lower()

  # Handle different command types
  if command in ["schema", "labels", "relationships", "relationshiptypes"]:
    # Replace with Kuzu SHOW_TABLES
    translated = NEO4J_DB_COMMANDS.sub("CALL SHOW_TABLES()", query)

    # If there's no RETURN statement after the CALL, add it
    if not re.search(r"RETURN\s+", translated, re.IGNORECASE):
      # Handle case where CALL is at the end of the query
      if translated.strip().endswith("SHOW_TABLES()"):
        translated = translated.strip() + " RETURN *"
      # Handle case where there might be other clauses after CALL
      else:
        translated = re.sub(
          r"(CALL\s+SHOW_TABLES\(\s*\))",
          r"\1 RETURN *",
          translated,
          flags=re.IGNORECASE,
        )

    return translated
  elif command == "propertykeys":
    # For property keys, we need to return table info
    # Since we can't query all tables at once, we'll just show node tables
    # Users can then use TABLE_INFO on specific tables
    return "CALL SHOW_TABLES() RETURN *"
  else:
    # For other commands, default to SHOW_TABLES
    translated = NEO4J_DB_COMMANDS.sub("CALL SHOW_TABLES()", query)

    # If there's no RETURN statement after the CALL, add it
    if not re.search(r"RETURN\s+", translated, re.IGNORECASE):
      if translated.strip().endswith("SHOW_TABLES()"):
        translated = translated.strip() + " RETURN *"
      else:
        translated = re.sub(
          r"(CALL\s+SHOW_TABLES\(\s*\))",
          r"\1 RETURN *",
          translated,
          flags=re.IGNORECASE,
        )

    return translated


class GraphMetricsResponse(BaseModel):
  """Response model for graph metrics."""

  graph_id: str = Field(..., description="Graph database identifier")
  graph_name: Optional[str] = Field(None, description="Display name for the graph")
  user_role: Optional[str] = Field(None, description="User's role in this graph")
  timestamp: str = Field(..., description="Metrics collection timestamp")
  total_nodes: int = Field(..., description="Total number of nodes")
  total_relationships: int = Field(..., description="Total number of relationships")
  node_counts: Dict[str, int] = Field(..., description="Node counts by label")
  relationship_counts: Dict[str, int] = Field(
    ..., description="Relationship counts by type"
  )
  estimated_size: Dict[str, Any] = Field(..., description="Database size estimates")
  health_status: Dict[str, Any] = Field(..., description="Database health information")


class GraphUsageResponse(BaseModel):
  """Response model for graph usage statistics."""

  graph_id: str = Field(..., description="Graph database identifier")
  storage_usage: Dict[str, Any] = Field(..., description="Storage usage information")
  query_statistics: Dict[str, Any] = Field(..., description="Query statistics")
  recent_activity: Dict[str, Any] = Field(..., description="Recent activity summary")
  timestamp: str = Field(..., description="Usage collection timestamp")


class CypherQueryRequest(BaseModel):
  """Request model for Cypher query execution."""

  query: str = Field(
    ...,
    description="The Cypher query to execute. Use parameters ($param_name) for all dynamic values to prevent injection attacks.",
    min_length=1,
    max_length=MAX_QUERY_LENGTH,
    examples=[
      "MATCH (n:Entity {type: $entity_type}) RETURN n LIMIT $limit",
      "MATCH (e:Entity)-[r:TRANSACTION]->(t:Entity) WHERE r.amount >= $min_amount AND e.name = $entity_name RETURN e, r, t LIMIT $limit",
      "MATCH (n:Entity) WHERE n.identifier = $identifier RETURN n",
      "MATCH (n) RETURN n LIMIT 10",
    ],
  )
  parameters: Optional[Dict[str, Any]] = Field(
    default=None,
    description="Query parameters for safe value substitution. ALWAYS use parameters instead of string interpolation.",
    examples=[
      {"entity_type": "Company", "limit": 100},
      {"min_amount": 1000, "entity_name": "Acme Corp", "limit": 50},
      {"identifier": "ENT123456"},
      None,
    ],
  )
  timeout: Optional[int] = Field(
    default=DEFAULT_QUERY_TIMEOUT,
    ge=1,
    le=300,
    description="Query timeout in seconds (1-300)",
    examples=[30, 60, 120, 300],
  )

  class Config:
    extra = "forbid"
    json_schema_extra = {
      "examples": [
        {
          "summary": "Simple entity lookup",
          "description": "Find entities by type with parameterized values",
          "value": {
            "query": "MATCH (n:Entity {type: $entity_type}) RETURN n LIMIT $limit",
            "parameters": {"entity_type": "Company", "limit": 100},
            "timeout": 60,
          },
        },
        {
          "summary": "Relationship traversal query",
          "description": "Find transactions between entities with amount filtering",
          "value": {
            "query": "MATCH (e:Entity)-[r:TRANSACTION]->(t:Entity) WHERE r.amount >= $min_amount AND e.name = $entity_name RETURN e, r, t LIMIT $limit",
            "parameters": {"min_amount": 1000, "entity_name": "Acme Corp", "limit": 50},
            "timeout": 120,
          },
        },
        {
          "summary": "Lookup by identifier",
          "description": "Find specific entity using unique identifier",
          "value": {
            "query": "MATCH (n:Entity) WHERE n.identifier = $identifier RETURN n",
            "parameters": {"identifier": "ENT123456"},
            "timeout": 30,
          },
        },
        {
          "summary": "Company financial query",
          "description": "Aggregation query for financial metrics",
          "value": {
            "query": "MATCH (c:Company)-[:FILED]->(f:Filing) WHERE f.form_type = $form RETURN c.ticker, c.name, COUNT(f) as filing_count ORDER BY filing_count DESC LIMIT $limit",
            "parameters": {"form": "10-K", "limit": 20},
            "timeout": 60,
          },
        },
        {
          "summary": "Explore all nodes",
          "description": "Simple query without parameters to explore graph structure",
          "value": {"query": "MATCH (n) RETURN n LIMIT 10", "timeout": 30},
        },
      ]
    }

  @field_validator("query")
  def validate_query_length(cls, v):
    """Validate query is not empty and within length limits."""
    if not v or not v.strip():
      raise ValueError("Query cannot be empty")
    return v


class CypherQueryResponse(BaseModel):
  """Response model for Cypher query results."""

  success: bool = Field(..., description="Whether the query executed successfully")
  data: Optional[List[Dict[str, Any]]] = Field(
    default=None, description="Query results as a list of dictionaries"
  )
  columns: Optional[List[str]] = Field(
    default=None, description="Column names from the query result"
  )
  row_count: int = Field(..., description="Number of rows returned")
  execution_time_ms: float = Field(
    ..., description="Query execution time in milliseconds"
  )
  graph_id: str = Field(..., description="Graph database identifier")
  timestamp: str = Field(..., description="Query execution timestamp")
  error: Optional[str] = Field(
    default=None, description="Error message if query failed"
  )

  class Config:
    json_schema_extra = {
      "examples": [
        {
          "summary": "Successful entity query",
          "description": "Query returned multiple entity nodes",
          "value": {
            "success": True,
            "data": [
              {
                "n": {
                  "type": "Company",
                  "name": "Apple Inc.",
                  "ticker": "AAPL",
                  "identifier": "ENT123456",
                }
              },
              {
                "n": {
                  "type": "Company",
                  "name": "Microsoft Corporation",
                  "ticker": "MSFT",
                  "identifier": "ENT789012",
                }
              },
            ],
            "columns": ["n"],
            "row_count": 2,
            "execution_time_ms": 45.3,
            "graph_id": "kg1a2b3c4d5",
            "timestamp": "2024-01-15T10:30:45Z",
          },
        },
        {
          "summary": "Aggregation query result",
          "description": "Financial metrics aggregation with multiple columns",
          "value": {
            "success": True,
            "data": [
              {"ticker": "AAPL", "name": "Apple Inc.", "filing_count": 42},
              {"ticker": "MSFT", "name": "Microsoft Corporation", "filing_count": 38},
              {"ticker": "GOOGL", "name": "Alphabet Inc.", "filing_count": 35},
            ],
            "columns": ["ticker", "name", "filing_count"],
            "row_count": 3,
            "execution_time_ms": 128.7,
            "graph_id": "kg1a2b3c4d5",
            "timestamp": "2024-01-15T10:35:22Z",
          },
        },
        {
          "summary": "Empty result set",
          "description": "Query executed successfully but returned no results",
          "value": {
            "success": True,
            "data": [],
            "columns": ["n"],
            "row_count": 0,
            "execution_time_ms": 12.5,
            "graph_id": "kg1a2b3c4d5",
            "timestamp": "2024-01-15T10:40:15Z",
          },
        },
        {
          "summary": "Query error",
          "description": "Query failed due to syntax error",
          "value": {
            "success": False,
            "data": None,
            "columns": None,
            "row_count": 0,
            "execution_time_ms": 5.2,
            "graph_id": "kg1a2b3c4d5",
            "timestamp": "2024-01-15T10:45:30Z",
            "error": "Syntax error: Expected MATCH, CREATE, or RETURN at line 1",
          },
        },
      ]
    }


class BackupCreateRequest(BaseModel):
  """Request model for creating a backup."""

  backup_format: str = Field(
    "full_dump",
    description="Backup format - only 'full_dump' is supported (complete .kuzu database file)",
    pattern="^full_dump$",  # Only allow full_dump
  )
  backup_type: str = Field(
    "full",
    description="Backup type - only 'full' is supported",
    pattern="^full$",  # Only allow full backups
  )
  retention_days: int = Field(90, ge=1, le=2555, description="Retention period in days")
  compression: bool = Field(
    True, description="Enable compression (always enabled for optimal storage)"
  )
  encryption: bool = Field(
    False, description="Enable encryption (encrypted backups cannot be downloaded)"
  )
  schedule: Optional[str] = Field(
    None, description="Optional cron schedule for automated backups"
  )

  @field_validator("compression")
  @classmethod
  def compression_must_be_true(cls, v):
    """Ensure compression is always enabled."""
    if v is not True:
      raise ValueError("Compression must be enabled (always True)")
    return True


class BackupResponse(BaseModel):
  """Response model for backup information."""

  backup_id: str
  graph_id: str
  backup_format: str
  backup_type: str
  status: str
  # s3_bucket and s3_key removed for security - infrastructure details not needed by users
  original_size_bytes: int
  compressed_size_bytes: int
  compression_ratio: float
  node_count: int
  relationship_count: int
  backup_duration_seconds: float
  encryption_enabled: bool
  compression_enabled: bool
  allow_export: bool
  created_at: str
  completed_at: Optional[str]
  expires_at: Optional[str]


class BackupListResponse(BaseModel):
  """Response model for backup list."""

  backups: List[BackupResponse]
  total_count: int
  graph_id: str


class BackupStatsResponse(BaseModel):
  """Response model for backup statistics."""

  graph_id: str
  total_backups: int
  successful_backups: int
  failed_backups: int
  success_rate: float
  total_original_size_bytes: int
  total_compressed_size_bytes: int
  storage_saved_bytes: int
  average_compression_ratio: float
  latest_backup_date: Optional[str]
  backup_formats: Dict[str, int]


class BackupExportRequest(BaseModel):
  """Request model for exporting a backup."""

  backup_id: str = Field(..., description="ID of backup to export")
  export_format: str = Field(
    "original",
    description="Export format - only 'original' is supported (compressed .kuzu file)",
    pattern="^original$",  # Only allow original format
  )


class BackupRestoreRequest(BaseModel):
  """Request model for restoring from a backup."""

  create_system_backup: bool = Field(
    True, description="Create a system backup of existing database before restore"
  )
  verify_after_restore: bool = Field(
    True, description="Verify database integrity after restore"
  )


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


# New API models for Kuzu database operations


class DatabaseHealthResponse(BaseModel):
  """Response model for database health check."""

  graph_id: str = Field(
    ..., description="Graph database identifier", examples=["kg1a2b3c4d5"]
  )
  status: str = Field(..., description="Overall health status", examples=["healthy"])
  connection_status: str = Field(
    ..., description="Database connection status", examples=["connected"]
  )
  uptime_seconds: float = Field(
    ..., description="Database uptime in seconds", examples=[3600.5]
  )
  last_query_time: Optional[str] = Field(
    None,
    description="Timestamp of last query execution",
    examples=["2024-01-15T10:30:00Z"],
  )
  query_count_24h: int = Field(
    ..., description="Number of queries executed in last 24 hours", examples=[150]
  )
  avg_query_time_ms: float = Field(
    ..., description="Average query execution time in milliseconds", examples=[45.2]
  )
  error_rate_24h: float = Field(
    ..., description="Error rate in last 24 hours (percentage)", examples=[0.5]
  )
  memory_usage_mb: Optional[float] = Field(
    None, description="Memory usage in MB", examples=[512.3]
  )
  storage_usage_mb: Optional[float] = Field(
    None, description="Storage usage in MB", examples=[1024.7]
  )
  alerts: List[str] = Field(
    default_factory=list,
    description="Active alerts or warnings",
    examples=[["High memory usage detected"]],
  )


class DatabaseInfoResponse(BaseModel):
  """Response model for database information and statistics."""

  graph_id: str = Field(
    ..., description="Graph database identifier", examples=["kg1a2b3c4d5"]
  )
  database_name: str = Field(..., description="Database name", examples=["kg1a2b3c4d5"])
  # database_path removed for security - no need to expose file system paths
  database_size_bytes: int = Field(
    ..., description="Database size in bytes", examples=[1048576]
  )
  database_size_mb: float = Field(
    ..., description="Database size in MB", examples=[1.0]
  )
  node_count: int = Field(..., description="Total number of nodes", examples=[1250])
  relationship_count: int = Field(
    ..., description="Total number of relationships", examples=[2340]
  )
  node_labels: List[str] = Field(
    ..., description="List of node labels", examples=[["Entity", "Report", "Fact"]]
  )
  relationship_types: List[str] = Field(
    ...,
    description="List of relationship types",
    examples=[["HAS_REPORT", "REPORTED_IN", "HAS_ELEMENT"]],
  )
  created_at: str = Field(
    ..., description="Database creation timestamp", examples=["2024-01-15T10:00:00Z"]
  )
  last_modified: str = Field(
    ..., description="Last modification timestamp", examples=["2024-01-15T10:30:00Z"]
  )
  schema_version: Optional[str] = Field(
    None, description="Schema version", examples=["1.0.0"]
  )
  read_only: bool = Field(
    ..., description="Whether database is read-only", examples=[False]
  )
  backup_count: int = Field(
    ..., description="Number of available backups", examples=[5]
  )
  last_backup_date: Optional[str] = Field(
    None, description="Date of last backup", examples=["2024-01-15T09:00:00Z"]
  )


class StorageLimits(BaseModel):
  """Storage limits information."""

  current_usage_gb: Optional[float] = Field(
    None, description="Current storage usage in GB"
  )
  max_storage_gb: float = Field(..., description="Maximum storage limit in GB")
  approaching_limit: bool = Field(
    ..., description="Whether approaching storage limit (>80%)"
  )


class QueryLimits(BaseModel):
  """Query operation limits."""

  max_timeout_seconds: int = Field(..., description="Maximum query timeout in seconds")
  chunk_size: int = Field(..., description="Maximum chunk size for result streaming")
  max_rows_per_query: int = Field(..., description="Maximum rows returned per query")
  concurrent_queries: int = Field(..., description="Maximum concurrent queries allowed")


class CopyOperationLimits(BaseModel):
  """Copy/ingestion operation limits."""

  max_file_size_gb: float = Field(..., description="Maximum file size in GB")
  timeout_seconds: int = Field(..., description="Operation timeout in seconds")
  concurrent_operations: int = Field(..., description="Maximum concurrent operations")
  max_files_per_operation: int = Field(..., description="Maximum files per operation")
  daily_copy_operations: int = Field(..., description="Daily operation limit")
  supported_formats: List[str] = Field(..., description="Supported file formats")


class BackupLimits(BaseModel):
  """Backup operation limits."""

  max_backup_size_gb: float = Field(..., description="Maximum backup size in GB")
  backup_retention_days: int = Field(..., description="Backup retention period in days")
  max_backups_per_day: int = Field(..., description="Maximum backups per day")


class RateLimits(BaseModel):
  """API rate limits."""

  requests_per_minute: int = Field(..., description="Requests per minute limit")
  requests_per_hour: int = Field(..., description="Requests per hour limit")
  burst_capacity: int = Field(..., description="Burst capacity for short spikes")


class CreditLimits(BaseModel):
  """AI credit limits (optional)."""

  monthly_ai_credits: int = Field(..., description="Monthly AI credits allocation")
  current_balance: int = Field(..., description="Current credit balance")
  storage_billing_enabled: bool = Field(
    ..., description="Whether storage billing is enabled"
  )
  storage_rate_per_gb_per_day: int = Field(
    ..., description="Storage billing rate per GB per day"
  )


class GraphLimitsResponse(BaseModel):
  """Response model for comprehensive graph operational limits."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Standard tier user graph limits",
          "description": "Operational limits for a kuzu-standard tier user graph with full details",
          "value": {
            "graph_id": "kg1a2b3c4d5",
            "subscription_tier": "standard",
            "graph_tier": "kuzu-standard",
            "is_shared_repository": False,
            "storage": {
              "current_usage_gb": 2.45,
              "max_storage_gb": 500,
              "approaching_limit": False,
            },
            "queries": {
              "max_timeout_seconds": 60,
              "chunk_size": 1000,
              "max_rows_per_query": 10000,
              "concurrent_queries": 1,
            },
            "copy_operations": {
              "max_file_size_gb": 1.0,
              "timeout_seconds": 300,
              "concurrent_operations": 1,
              "max_files_per_operation": 100,
              "daily_copy_operations": 10,
              "supported_formats": ["parquet", "csv", "json", "delta", "iceberg"],
            },
            "backups": {
              "max_backup_size_gb": 10,
              "backup_retention_days": 7,
              "max_backups_per_day": 2,
            },
            "rate_limits": {
              "requests_per_minute": 60,
              "requests_per_hour": 1000,
              "burst_capacity": 10,
            },
            "credits": {
              "monthly_ai_credits": 10000,
              "current_balance": 7500,
              "storage_billing_enabled": True,
              "storage_rate_per_gb_per_day": 10,
            },
          },
        },
        {
          "summary": "Shared repository limits (SEC)",
          "description": "Operational limits for SEC shared repository (read-only, no credits)",
          "value": {
            "graph_id": "sec",
            "subscription_tier": "standard",
            "graph_tier": "kuzu-shared",
            "is_shared_repository": True,
            "storage": {
              "current_usage_gb": 125.3,
              "max_storage_gb": 1000,
              "approaching_limit": False,
            },
            "queries": {
              "max_timeout_seconds": 120,
              "chunk_size": 2000,
              "max_rows_per_query": 10000,
              "concurrent_queries": 1,
            },
            "copy_operations": {
              "max_file_size_gb": 5.0,
              "timeout_seconds": 600,
              "concurrent_operations": 2,
              "max_files_per_operation": 200,
              "daily_copy_operations": 50,
              "supported_formats": ["parquet", "csv", "json", "delta", "iceberg"],
            },
            "backups": {
              "max_backup_size_gb": 50,
              "backup_retention_days": 30,
              "max_backups_per_day": 4,
            },
            "rate_limits": {
              "requests_per_minute": 120,
              "requests_per_hour": 2000,
              "burst_capacity": 20,
            },
          },
        },
        {
          "summary": "Enterprise tier limits",
          "description": "Operational limits for kuzu-large tier with enhanced capabilities",
          "value": {
            "graph_id": "kg9f8e7d6c5",
            "subscription_tier": "enterprise",
            "graph_tier": "kuzu-large",
            "is_shared_repository": False,
            "storage": {
              "current_usage_gb": 450.8,
              "max_storage_gb": 2000,
              "approaching_limit": False,
            },
            "queries": {
              "max_timeout_seconds": 300,
              "chunk_size": 5000,
              "max_rows_per_query": 10000,
              "concurrent_queries": 1,
            },
            "copy_operations": {
              "max_file_size_gb": 10.0,
              "timeout_seconds": 900,
              "concurrent_operations": 5,
              "max_files_per_operation": 500,
              "daily_copy_operations": 100,
              "supported_formats": ["parquet", "csv", "json", "delta", "iceberg"],
            },
            "backups": {
              "max_backup_size_gb": 100,
              "backup_retention_days": 90,
              "max_backups_per_day": 10,
            },
            "rate_limits": {
              "requests_per_minute": 300,
              "requests_per_hour": 5000,
              "burst_capacity": 50,
            },
            "credits": {
              "monthly_ai_credits": 50000,
              "current_balance": 42300,
              "storage_billing_enabled": True,
              "storage_rate_per_gb_per_day": 10,
            },
          },
        },
      ]
    }
  )

  graph_id: str = Field(..., description="Graph database identifier")
  subscription_tier: str = Field(..., description="User's subscription tier")
  graph_tier: str = Field(..., description="Graph's database tier")
  is_shared_repository: bool = Field(
    ..., description="Whether this is a shared repository"
  )
  storage: StorageLimits = Field(..., description="Storage limits and usage")
  queries: QueryLimits = Field(..., description="Query operation limits")
  copy_operations: CopyOperationLimits = Field(
    ..., description="Copy/ingestion operation limits"
  )
  backups: BackupLimits = Field(..., description="Backup operation limits")
  rate_limits: RateLimits = Field(..., description="API rate limits")
  credits: Optional[CreditLimits] = Field(
    None, description="AI credit limits (if applicable)"
  )


class BackupDownloadUrlResponse(BaseModel):
  """Response model for backup download URL generation."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "summary": "Download URL for recent backup",
          "description": "Generated pre-signed URL for downloading a backup with 1 hour expiration",
          "value": {
            "download_url": "https://s3.amazonaws.com/robosystems-backups/kg1a2b3c4d5/backup_20240115_100000.kuzu.tar.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...",
            "expires_in": 3600,
            "expires_at": 1705315200.0,
            "backup_id": "bk1a2b3c4d5",
            "graph_id": "kg1a2b3c4d5",
          },
        },
        {
          "summary": "Extended expiration download URL",
          "description": "Download URL with 24-hour expiration for large backup files",
          "value": {
            "download_url": "https://s3.amazonaws.com/robosystems-backups/kg9f8e7d6c5/backup_20240114_183000.kuzu.tar.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...",
            "expires_in": 86400,
            "expires_at": 1705401600.0,
            "backup_id": "bk9f8e7d6c5",
            "graph_id": "kg9f8e7d6c5",
          },
        },
        {
          "summary": "Short-lived download URL",
          "description": "Download URL with minimum 5-minute expiration for immediate download",
          "value": {
            "download_url": "https://s3.amazonaws.com/robosystems-backups/sec/backup_20240115_120000.kuzu.tar.gz?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=...",
            "expires_in": 300,
            "expires_at": 1705314900.0,
            "backup_id": "bksec123456",
            "graph_id": "sec",
          },
        },
      ]
    }
  )

  download_url: str = Field(
    ...,
    description="Pre-signed S3 URL for downloading the backup file",
    examples=[
      "https://s3.amazonaws.com/robosystems-backups/kg1a2b3c4d5/backup.kuzu.tar.gz?X-Amz-Credential=..."
    ],
  )
  expires_in: int = Field(
    ...,
    description="URL expiration time in seconds from now",
    examples=[3600],
    ge=300,
    le=86400,
  )
  expires_at: float = Field(
    ...,
    description="Unix timestamp when the URL expires",
    examples=[1705315200.0],
  )
  backup_id: str = Field(..., description="Backup identifier", examples=["bk1a2b3c4d5"])
  graph_id: str = Field(
    ..., description="Graph database identifier", examples=["kg1a2b3c4d5"]
  )
