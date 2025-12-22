"""Graph management API models."""

import re
from typing import Any

from pydantic import BaseModel, Field, field_validator

# Import secure write operation detection

# Neo4j to LadybugDB query translation patterns
NEO4J_DB_COMMANDS = re.compile(
  r"CALL\s+db\.(schema|labels|relationships|relationshipTypes|propertyKeys|indexes|constraints)\s*\(\s*\)",
  re.IGNORECASE | re.MULTILINE,
)

# Mapping of Neo4j commands to LadybugDB equivalents
NEO4J_TO_LADYBUG_MAPPING = {
  "db.schema": "SHOW_TABLES()",
  "db.labels": "SHOW_TABLES()",
  "db.relationships": "SHOW_TABLES()",
  "db.relationshipTypes": "SHOW_TABLES()",
  "db.propertyKeys": "TABLE_INFO",
  "db.indexes": "SHOW_TABLES()",  # LadybugDB doesn't have explicit indexes like Neo4j
  "db.constraints": "SHOW_TABLES()",  # LadybugDB doesn't have constraints like Neo4j
}

# Constants
MAX_QUERY_LENGTH = 50000
DEFAULT_QUERY_TIMEOUT = 60


def translate_neo4j_to_lbug(query: str) -> str:
  """
  Translate Neo4j-style db.* commands to LadybugDB equivalents.

  Args:
      query: The original Cypher query

  Returns:
      Translated query compatible with LadybugDB
  """
  # Check if query contains Neo4j db.* commands
  match = NEO4J_DB_COMMANDS.search(query)
  if not match:
    return query

  # Extract the command type
  command = match.group(1).lower()

  # Handle different command types
  if command in ["schema", "labels", "relationships", "relationshiptypes"]:
    # Replace with LadybugDB SHOW_TABLES
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
  graph_name: str | None = Field(None, description="Display name for the graph")
  user_role: str | None = Field(None, description="User's role in this graph")
  timestamp: str = Field(..., description="Metrics collection timestamp")
  total_nodes: int = Field(..., description="Total number of nodes")
  total_relationships: int = Field(..., description="Total number of relationships")
  node_counts: dict[str, int] = Field(..., description="Node counts by label")
  relationship_counts: dict[str, int] = Field(
    ..., description="Relationship counts by type"
  )
  estimated_size: dict[str, Any] = Field(..., description="Database size estimates")
  health_status: dict[str, Any] = Field(..., description="Database health information")


class GraphUsageResponse(BaseModel):
  """Response model for graph usage statistics."""

  graph_id: str = Field(..., description="Graph database identifier")
  storage_usage: dict[str, Any] = Field(..., description="Storage usage information")
  query_statistics: dict[str, Any] = Field(..., description="Query statistics")
  recent_activity: dict[str, Any] = Field(..., description="Recent activity summary")
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
  parameters: dict[str, Any] | None = Field(
    default=None,
    description="Query parameters for safe value substitution. ALWAYS use parameters instead of string interpolation.",
    examples=[
      {"entity_type": "Company", "limit": 100},
      {"min_amount": 1000, "entity_name": "Acme Corp", "limit": 50},
      {"identifier": "ENT123456"},
      None,
    ],
  )
  timeout: int | None = Field(
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
  data: list[dict[str, Any]] | None = Field(
    default=None, description="Query results as a list of dictionaries"
  )
  columns: list[str] | None = Field(
    default=None, description="Column names from the query result"
  )
  row_count: int = Field(..., description="Number of rows returned")
  execution_time_ms: float = Field(
    ..., description="Query execution time in milliseconds"
  )
  graph_id: str = Field(..., description="Graph database identifier")
  timestamp: str = Field(..., description="Query execution timestamp")
  error: str | None = Field(default=None, description="Error message if query failed")

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
