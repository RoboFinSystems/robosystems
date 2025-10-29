"""MCP (Model Context Protocol) API models."""

from pydantic import BaseModel, Field, ConfigDict


class MCPToolCall(BaseModel):
  """Request model for MCP tool execution."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "name": "read-graph-cypher",
          "arguments": {
            "query": "MATCH (c:Company) WHERE c.ticker = $ticker RETURN c",
            "parameters": {"ticker": "AAPL"},
          },
        },
        {
          "name": "get-graph-schema",
          "arguments": {},
        },
        {
          "name": "get-graph-info",
          "arguments": {},
        },
      ]
    }
  )

  name: str = Field(..., description="Name of the MCP tool to execute", min_length=1)
  arguments: dict[str, object] = Field(
    default_factory=dict, description="Arguments to pass to the tool"
  )


class MCPQueryRequest(BaseModel):
  """Request model for MCP Cypher query execution with safety limits."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "query": "MATCH (c:Company {ticker: $ticker})-[:FILED]->(f:Filing) RETURN c.name, f.form_type, f.filing_date LIMIT 10",
          "params": {"ticker": "AAPL"},
        },
        {
          "query": "MATCH (c:Company) WHERE c.market_cap > 1000000000 RETURN c.name, c.ticker, c.market_cap ORDER BY c.market_cap DESC LIMIT 20",
          "params": {},
        },
        {
          "query": "MATCH (c:Company {cik: $cik})-[:PART_OF]->(i:Industry) RETURN c, i",
          "params": {"cik": "0000320193"},
          "timeout_override": 60,
        },
      ]
    }
  )

  query: str = Field(
    ...,
    description="Cypher query to execute (read-only)",
    min_length=1,
    max_length=50000,
  )
  params: dict[str, object] = Field(
    default_factory=dict, description="Query parameters"
  )
  timeout_override: int | None = Field(
    None, description="Override default query timeout (max 300 seconds)", ge=1, le=300
  )


class MCPToolsResponse(BaseModel):
  """Response model for MCP tools listing."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "tools": [
            {
              "name": "read-graph-cypher",
              "description": "Execute a read-only Cypher query against the graph database",
              "inputSchema": {
                "type": "object",
                "properties": {
                  "query": {"type": "string", "description": "Cypher query to execute"},
                  "parameters": {
                    "type": "object",
                    "description": "Query parameters",
                  },
                },
                "required": ["query"],
              },
              "capabilities": {
                "streaming": True,
                "progress": True,
                "cacheable": False,
                "timeout_seconds": 300,
              },
            },
            {
              "name": "get-graph-schema",
              "description": "Get the complete schema of the graph database",
              "inputSchema": {"type": "object", "properties": {}},
              "capabilities": {
                "streaming": False,
                "progress": True,
                "cacheable": True,
                "cache_ttl_seconds": 3600,
                "timeout_seconds": 60,
              },
            },
          ]
        }
      ]
    }
  )

  tools: list[dict[str, object]] = Field(
    ..., description="List of available MCP tools with their schemas"
  )


class MCPToolResult(BaseModel):
  """Response model for MCP tool execution result."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "result": {
            "content": [
              {
                "type": "text",
                "text": "Found 3 companies matching criteria:\n- Apple Inc. (AAPL)\n- Microsoft Corporation (MSFT)\n- Alphabet Inc. (GOOGL)",
              }
            ],
            "rows_returned": 3,
            "execution_time_ms": 145,
          }
        },
        {
          "result": {
            "schema": {
              "nodes": ["Company", "Filing", "Industry"],
              "relationships": ["FILED", "PART_OF", "COMPETES_WITH"],
            }
          }
        },
      ]
    }
  )

  result: dict[str, object] = Field(..., description="Result of the MCP tool execution")


class MCPSchemaResponse(BaseModel):
  """Response model for graph database schema information."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "schema": [
            {
              "label": "Company",
              "properties": [
                {"name": "cik", "type": "STRING"},
                {"name": "name", "type": "STRING"},
                {"name": "ticker", "type": "STRING"},
                {"name": "market_cap", "type": "INT64"},
              ],
            },
            {
              "label": "Filing",
              "properties": [
                {"name": "accession_number", "type": "STRING"},
                {"name": "form_type", "type": "STRING"},
                {"name": "filing_date", "type": "DATE"},
              ],
            },
          ]
        }
      ]
    }
  )

  schema_data: list[dict[str, object]] = Field(
    ...,
    description="Graph database schema with node types, properties, and relationships",
    alias="schema",
  )


class MCPQueryResponse(BaseModel):
  """Response model for MCP query execution."""

  model_config = ConfigDict(
    json_schema_extra={
      "examples": [
        {
          "results": [
            {
              "c.name": "Apple Inc.",
              "c.ticker": "AAPL",
              "c.market_cap": 2800000000000,
            },
            {
              "c.name": "Microsoft Corporation",
              "c.ticker": "MSFT",
              "c.market_cap": 2600000000000,
            },
          ]
        }
      ]
    }
  )

  results: list[dict[str, object]] = Field(
    ..., description="Query results from graph database"
  )
