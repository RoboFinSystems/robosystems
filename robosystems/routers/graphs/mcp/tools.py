"""
MCP tools listing endpoint.

This module provides the tool discovery endpoint for MCP clients.
"""

from fastapi import APIRouter, Depends, Path
from sqlalchemy.orm import Session

from robosystems.database import get_db_session
from robosystems.middleware.auth.dependencies import get_current_user_with_graph
from robosystems.middleware.rate_limits import (
  subscription_aware_rate_limit_dependency,
)
from robosystems.middleware.graph import get_graph_repository
from robosystems.models.iam import User
from robosystems.models.api.graphs.mcp import MCPToolsResponse
from robosystems.models.api.common import ErrorResponse
from robosystems.middleware.graph.utils import MultiTenantUtils
from robosystems.middleware.otel.metrics import (
  endpoint_metrics_decorator,
  get_endpoint_metrics,
)
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.routers.graphs.mcp.handlers import MCPHandler
from robosystems.logger import logger
from robosystems.middleware.graph.types import GRAPH_OR_SUBGRAPH_ID_PATTERN

# Import MCP components
from .handlers import validate_mcp_access

router = APIRouter()

# Circuit breaker instance
circuit_breaker = CircuitBreakerManager()


def _get_mcp_operation_type(graph_id: str) -> str:
  """
  Determine the correct operation type for MCP operations based on graph type.

  For consistency with distributed LadybugDB architecture:
  - User graphs: Always use 'write' to ensure writer cluster routing
  - Shared repositories: Use 'read' for reader cluster routing
  """
  if MultiTenantUtils.is_shared_repository(graph_id):
    return "read"
  else:
    return "write"


@router.get(
  "/tools",
  response_model=MCPToolsResponse,
  summary="List MCP Tools",
  description="""Get available Model Context Protocol tools for graph analysis.

This endpoint returns a comprehensive list of MCP tools optimized for AI agents:
- Tool schemas with detailed parameter documentation
- Context-aware descriptions based on graph type
- Capability indicators for streaming and progress

The tool list is customized based on:
- Graph type (shared repository vs user graph)
- User permissions and subscription tier
- Backend capabilities (LadybugDB, Neo4j, etc.)

**Subgraph Support:**
This endpoint accepts both parent graph IDs and subgraph IDs.
- Parent graph: Use `graph_id` like `kg0123456789abcdef`
- Subgraph: Use full subgraph ID like `kg0123456789abcdef_dev`
The returned tool list is identical for parent graphs and subgraphs, as all
MCP tools work uniformly across graph boundaries.

**Note:**
MCP tool listing is included - no credit consumption required.""",
  operation_id="listMcpTools",
  responses={
    200: {"description": "MCP tools retrieved successfully", "model": MCPToolsResponse},
    403: {"description": "Access denied to graph", "model": ErrorResponse},
    500: {"description": "Failed to retrieve MCP tools", "model": ErrorResponse},
  },
)
@endpoint_metrics_decorator(
  "/v1/graphs/{graph_id}/mcp/tools", business_event_type="mcp_tools_listed"
)
async def list_mcp_tools(
  graph_id: str = Path(
    ...,
    description="Graph database identifier",
    pattern=GRAPH_OR_SUBGRAPH_ID_PATTERN,
  ),
  current_user: User = Depends(get_current_user_with_graph),
  db: Session = Depends(get_db_session),
  _rate_limit: None = Depends(subscription_aware_rate_limit_dependency),
) -> MCPToolsResponse:
  """
  List all available Model Context Protocol (MCP) tools.

  This endpoint provides tool discovery for AI agents, returning detailed
  schemas and capabilities for each available tool. The tool list is
  dynamically generated based on graph type and user permissions.

  **Tool Categories:**
  - **Query Tools**: Execute Cypher queries with streaming support
  - **Schema Tools**: Retrieve and explore graph structure
  - **Info Tools**: Get statistics and metadata
  - **Analysis Tools**: Specialized financial and graph analysis

  **AI Agent Integration:**
  The returned tool definitions are designed for seamless integration
  with AI agents through the MCP protocol. Each tool includes:
  - Complete input schema for parameter validation
  - Capability hints (streaming, caching, progress)
  - Context-aware descriptions

  **Returns:**
  Complete list of available tools with schemas and metadata.
  """
  # Validate access based on graph type
  await validate_mcp_access(graph_id, current_user, db, "read")

  # Use proper operation type based on graph type for consistent routing
  operation_type = _get_mcp_operation_type(graph_id)
  repository = await get_graph_repository(graph_id, operation_type)

  # Check circuit breaker for list_tools operation
  circuit_breaker.check_circuit(graph_id, "list_tools")

  try:
    # Create handler directly for tool discovery
    handler = MCPHandler(repository, graph_id, current_user)
    tools = await handler.get_tools()

    # Enhance tool definitions with capability hints for AI agents
    enhanced_tools = []
    for tool in tools:
      # Add capability indicators
      tool_name = tool["name"]

      # Query tools support streaming
      if "cypher" in tool_name:
        tool["capabilities"] = {
          "streaming": True,
          "progress": True,
          "cacheable": False,
          "timeout_seconds": 300,
        }
      # Schema tools support caching
      elif "schema" in tool_name:
        tool["capabilities"] = {
          "streaming": False,
          "progress": True,
          "cacheable": True,
          "cache_ttl_seconds": 3600,
          "timeout_seconds": 60,
        }
      # Info tools are fast and cacheable
      elif "info" in tool_name or "describe" in tool_name:
        tool["capabilities"] = {
          "streaming": False,
          "progress": False,
          "cacheable": True,
          "cache_ttl_seconds": 300,
          "timeout_seconds": 30,
        }
      else:
        tool["capabilities"] = {
          "streaming": False,
          "progress": False,
          "cacheable": False,
          "timeout_seconds": 60,
        }

      enhanced_tools.append(tool)

    # Record successful operation
    circuit_breaker.record_success(graph_id, "list_tools")

    # Record metrics
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/mcp/tools",
      method="GET",
      event_type="mcp_tools_listed",
      event_data={
        "graph_id": graph_id,
        "tool_count": len(enhanced_tools),
        "is_shared_repo": MultiTenantUtils.is_shared_repository(graph_id),
      },
      user_id=current_user.id,
    )

    logger.info(
      f"Listed {len(enhanced_tools)} MCP tools for graph {graph_id}",
      extra={
        "graph_id": graph_id,
        "user_id": str(current_user.id),
        "tool_count": len(enhanced_tools),
      },
    )

    return MCPToolsResponse(tools=enhanced_tools)

  except Exception as e:
    # Record failed operation
    circuit_breaker.record_failure(graph_id, "list_tools")

    logger.error(
      f"Failed to list MCP tools for graph {graph_id}: {e}",
      extra={
        "graph_id": graph_id,
        "user_id": str(current_user.id),
        "error": str(e),
      },
    )

    # Record error metrics
    metrics_instance = get_endpoint_metrics()
    metrics_instance.record_business_event(
      endpoint="/v1/graphs/{graph_id}/mcp/tools",
      method="GET",
      event_type="mcp_tools_list_failed",
      event_data={
        "graph_id": graph_id,
        "error_type": type(e).__name__,
      },
      user_id=current_user.id,
    )

    # Handle exception securely
    from robosystems.security import handle_exception_securely

    handle_exception_securely(
      e, additional_context={"operation": "list_mcp_tools", "graph_id": graph_id}
    )
    raise  # This should never be reached, but satisfies type checker
