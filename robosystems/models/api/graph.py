"""Graph management API models."""

from typing import Optional, Dict, Any, List, Union
from pydantic import BaseModel, Field, field_validator
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
          "query": "MATCH (n:Entity {type: $entity_type}) RETURN n LIMIT $limit",
          "parameters": {"entity_type": "Company", "limit": 100},
          "timeout": 60,
        },
        {
          "query": "MATCH (e:Entity)-[r:TRANSACTION]->(t:Entity) WHERE r.amount >= $min_amount AND e.name = $entity_name RETURN e, r, t LIMIT $limit",
          "parameters": {"min_amount": 1000, "entity_name": "Acme Corp", "limit": 50},
          "timeout": 120,
        },
        {
          "query": "MATCH (n:Entity) WHERE n.identifier = $identifier RETURN n",
          "parameters": {"identifier": "ENT123456"},
          "timeout": 30,
        },
        {
          "query": "MATCH (n) RETURN n LIMIT 10",
          "parameters": {},
          "timeout": 30,
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
  """Custom schema definition for custom graphs."""

  name: str = Field(..., description="Schema name", examples=["inventory_management"])
  version: str = Field("1.0.0", description="Schema version", examples=["1.0.0"])
  description: Optional[str] = Field(
    None,
    description="Schema description",
    examples=["Inventory management system schema"],
  )
  extends: Optional[str] = Field(
    None,
    description="Base schema to extend (e.g., 'base')",
    examples=["base"],
  )
  nodes: List[Dict[str, Any]] = Field(
    default_factory=list,
    description="List of node definitions with properties",
    examples=[
      [
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
            {"name": "location", "type": "STRING"},
          ],
        },
      ]
    ],
  )
  relationships: List[Dict[str, Any]] = Field(
    default_factory=list,
    description="List of relationship definitions",
    examples=[
      [
        {
          "name": "STORED_IN",
          "from_node": "Product",
          "to_node": "Warehouse",
          "properties": [{"name": "since", "type": "DATE"}],
        }
      ]
    ],
  )
  metadata: Dict[str, Any] = Field(
    default_factory=dict,
    description="Additional schema metadata",
    examples=[{"created_by": "inventory_team", "industry": "retail"}],
  )


class CreateGraphRequest(BaseModel):
  """Request model for creating a new graph."""

  metadata: GraphMetadata = Field(..., description="Graph metadata")
  instance_tier: str = Field(
    "kuzu-standard",
    description="Instance tier: kuzu-standard, kuzu-large, or kuzu-xlarge",
    examples=["kuzu-standard"],
  )
  custom_schema: Optional[CustomSchemaDefinition] = Field(
    None,
    description="Optional custom schema definition. If provided, overrides schema_extensions",
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

  model_config = {"populate_by_name": True}

  graph_id: str = Field(
    ...,
    description="Graph database identifier",
    examples=["sec", "kg1a2b3c4d5"],
  )
  schema_data: Dict[str, Any] = Field(
    ...,
    description="Runtime schema information showing actual database structure",
    alias="schema",
    examples=[
      {
        "node_labels": ["Company", "Filing", "Industry", "Executive"],
        "relationship_types": ["FILED", "PART_OF", "EMPLOYED_BY", "COMPETES_WITH"],
        "node_properties": {
          "Company": {
            "cik": "STRING",
            "name": "STRING",
            "ticker": "STRING",
            "market_cap": "INT64",
            "founded_year": "INT32",
            "sector": "STRING",
          },
          "Filing": {
            "accession_number": "STRING",
            "form_type": "STRING",
            "filing_date": "DATE",
            "fiscal_year": "INT32",
            "period_of_report": "DATE",
          },
          "Industry": {
            "sic_code": "STRING",
            "name": "STRING",
            "division": "STRING",
          },
          "Executive": {
            "name": "STRING",
            "title": "STRING",
            "compensation": "DOUBLE",
          },
        },
      }
    ],
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
