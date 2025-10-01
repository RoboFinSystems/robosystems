"""MCP (Model Context Protocol) API models."""

from pydantic import BaseModel, Field


class MCPToolCall(BaseModel):
  """Request model for MCP tool execution."""

  name: str = Field(..., description="Name of the MCP tool to execute", min_length=1)
  arguments: dict[str, object] = Field(
    default_factory=dict, description="Arguments to pass to the tool"
  )


class MCPQueryRequest(BaseModel):
  """Request model for MCP Cypher query execution with safety limits."""

  query: str = Field(
    ...,
    description="Cypher query to execute (read-only)",
    min_length=1,
    max_length=50000,  # Increased to match adapter limits
  )
  params: dict[str, object] = Field(
    default_factory=dict, description="Query parameters"
  )
  timeout_override: int | None = Field(
    None, description="Override default query timeout (max 300 seconds)", ge=1, le=300
  )


class MCPToolsResponse(BaseModel):
  """Response model for MCP tools listing."""

  tools: list[dict[str, object]] = Field(
    ..., description="List of available MCP tools with their schemas"
  )


class MCPToolResult(BaseModel):
  """Response model for MCP tool execution result."""

  result: dict[str, object] = Field(..., description="Result of the MCP tool execution")


class MCPSchemaResponse(BaseModel):
  """Response model for graph database schema information."""

  schema_data: list[dict[str, object]] = Field(
    ...,
    description="Graph database schema with node types, properties, and relationships",
    alias="schema",
  )


class MCPQueryResponse(BaseModel):
  """Response model for MCP query execution."""

  results: list[dict[str, object]] = Field(
    ..., description="Query results from graph database"
  )
