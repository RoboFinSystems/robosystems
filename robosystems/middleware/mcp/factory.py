"""
Factory module for creating Kuzu MCP clients.

This module provides factory functions for creating KuzuMCPClient instances
with proper configuration and environment discovery.
"""

from typing import Optional
from contextlib import asynccontextmanager

from robosystems.logger import logger
from robosystems.config import env
from .client import KuzuMCPClient
from .pool import get_connection_pool


async def create_kuzu_mcp_client(
  graph_id: str = "sec", api_base_url: Optional[str] = None
) -> KuzuMCPClient:
  """
  Create a Kuzu MCP client with environment-based configuration and timeout controls.

  Args:
      graph_id: Graph database identifier
      api_base_url: Override API URL (uses env var if None)

  Returns:
      Configured KuzuMCPClient instance with appropriate timeouts
  """
  # If URL not provided, use GraphClientFactory to discover the proper endpoint
  if not api_base_url:
    from robosystems.graph_api.client.factory import GraphClientFactory
    from robosystems.middleware.graph.multitenant_utils import MultiTenantUtils

    # Determine operation type based on graph
    # Shared repositories are read-only from the application perspective
    operation_type = (
      "read" if MultiTenantUtils.is_shared_repository(graph_id) else "write"
    )

    # Get a client from the factory which will discover the proper endpoint
    # The factory handles routing appropriately:
    # - Shared repos: Routes to shared_master/shared_replica infrastructure
    # - User graphs: Looks up the tier from the database
    kuzu_client = await GraphClientFactory.create_client(
      graph_id=graph_id, operation_type=operation_type
    )

    # Extract the base URL from the client
    if hasattr(kuzu_client, "config") and hasattr(kuzu_client.config, "base_url"):
      api_base_url = kuzu_client.config.base_url
    elif hasattr(kuzu_client, "_base_url"):
      api_base_url = kuzu_client._base_url
    elif hasattr(kuzu_client, "base_url"):
      api_base_url = kuzu_client.base_url
    else:
      # Fallback to environment variable
      api_base_url = env.GRAPH_API_URL or "http://localhost:8001"

    # Ensure we have a valid URL
    if not api_base_url:
      api_base_url = "http://localhost:8001"

    logger.info(
      f"GraphClientFactory discovered endpoint: {api_base_url} for graph {graph_id}"
    )

  # Configure timeouts based on environment and query type
  timeout = env.GRAPH_HTTP_TIMEOUT
  query_timeout = env.GRAPH_QUERY_TIMEOUT  # 30 seconds to prevent resource exhaustion
  max_query_length = int(
    env.GRAPH_MAX_QUERY_LENGTH if hasattr(env, "GRAPH_MAX_QUERY_LENGTH") else 50000
  )  # 50KB queries max

  return KuzuMCPClient(
    api_base_url=api_base_url,
    graph_id=graph_id,
    timeout=timeout,
    query_timeout=query_timeout,
    max_query_length=max_query_length,
  )


@asynccontextmanager
async def acquire_kuzu_mcp_client(
  graph_id: str = "sec", api_base_url: Optional[str] = None, use_pool: bool = True
):
  """
  Acquire a Kuzu MCP client from the connection pool.

  This is the preferred method for getting MCP clients as it reuses
  connections to reduce initialization overhead.

  Args:
      graph_id: Graph database identifier
      api_base_url: Override API URL (uses env var if None)
      use_pool: Whether to use connection pooling (default: True)

  Yields:
      Configured KuzuMCPClient instance

  Example:
      async with acquire_kuzu_mcp_client("sec") as client:
          result = await client.execute_query("MATCH (n) RETURN n LIMIT 1")
  """
  if use_pool:
    # Use connection pool for better performance
    pool = get_connection_pool()
    async with pool.acquire(graph_id, api_base_url) as client:
      yield client
  else:
    # Create a new client without pooling (for testing or special cases)
    client = await create_kuzu_mcp_client(graph_id, api_base_url)
    try:
      yield client
    finally:
      # Clean up if client has a close method
      if hasattr(client, "close"):
        try:
          await client.close()
        except Exception as e:
          logger.error(f"Error closing MCP client: {e}")
