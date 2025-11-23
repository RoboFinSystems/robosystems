"""
Connection pooling for MCP clients.

This module provides connection pooling to reuse MCP client instances
and improve performance by reducing initialization overhead.
"""

import asyncio
from typing import Dict, Optional
from datetime import datetime
from contextlib import asynccontextmanager

from robosystems.logger import logger


class MCPConnectionPool:
  """
  Connection pool for MCP clients.

  Maintains a pool of reusable client connections per graph_id
  to reduce initialization overhead and improve performance.
  """

  def __init__(
    self,
    max_connections_per_graph: int = 10,
    max_idle_time: int = 300,  # 5 minutes
    max_lifetime: int = 3600,  # 1 hour
  ):
    """
    Initialize the connection pool.

    Args:
        max_connections_per_graph: Maximum connections per graph_id
        max_idle_time: Maximum idle time in seconds before closing
        max_lifetime: Maximum lifetime in seconds before recycling
    """
    self.max_connections_per_graph = max_connections_per_graph
    self.max_idle_time = max_idle_time
    self.max_lifetime = max_lifetime

    # Pool storage: graph_id -> list of (client, last_used, created_at)
    self._pools: Dict[str, list] = {}
    self._locks: Dict[str, asyncio.Lock] = {}
    self._cleanup_task: Optional[asyncio.Task] = None
    self._running = False

  async def start(self):
    """Start the connection pool and cleanup task."""
    if not self._running:
      self._running = True
      self._cleanup_task = asyncio.create_task(self._cleanup_loop())
      logger.info("MCP connection pool started")

  async def stop(self):
    """Stop the connection pool and close all connections."""
    self._running = False

    if self._cleanup_task:
      self._cleanup_task.cancel()
      try:
        await self._cleanup_task
      except asyncio.CancelledError:
        pass

    # Close all connections
    for graph_id in list(self._pools.keys()):
      await self._close_pool(graph_id)

    logger.info("MCP connection pool stopped")

  async def _cleanup_loop(self):
    """Background task to cleanup idle connections."""
    while self._running:
      try:
        await asyncio.sleep(60)  # Run cleanup every minute
        await self._cleanup_idle_connections()
      except asyncio.CancelledError:
        break
      except Exception as e:
        logger.error(f"Error in cleanup loop: {e}")

  async def _cleanup_idle_connections(self):
    """Remove idle and expired connections from pools."""
    now = datetime.now()

    for graph_id in list(self._pools.keys()):
      async with self._get_lock(graph_id):
        pool = self._pools.get(graph_id, [])
        active_connections = []

        for client, last_used, created_at in pool:
          idle_time = (now - last_used).total_seconds()
          lifetime = (now - created_at).total_seconds()

          # Check if connection should be removed
          if idle_time > self.max_idle_time or lifetime > self.max_lifetime:
            logger.debug(
              f"Closing idle connection for {graph_id} "
              f"(idle: {idle_time}s, lifetime: {lifetime}s)"
            )
            # Close client if it has a close method
            if hasattr(client, "close"):
              try:
                await client.close()
              except Exception as e:
                logger.error(f"Error closing client: {e}")
          else:
            active_connections.append((client, last_used, created_at))

        # Update pool with active connections only
        if active_connections:
          self._pools[graph_id] = active_connections
        elif graph_id in self._pools:
          del self._pools[graph_id]
          if graph_id in self._locks:
            del self._locks[graph_id]

  def _get_lock(self, graph_id: str) -> asyncio.Lock:
    """Get or create a lock for a graph_id."""
    if graph_id not in self._locks:
      self._locks[graph_id] = asyncio.Lock()
    return self._locks[graph_id]

  async def _close_pool(self, graph_id: str):
    """Close all connections in a pool."""
    async with self._get_lock(graph_id):
      pool = self._pools.get(graph_id, [])

      for client, _, _ in pool:
        if hasattr(client, "close"):
          try:
            await client.close()
          except Exception as e:
            logger.error(f"Error closing client: {e}")

      if graph_id in self._pools:
        del self._pools[graph_id]

  @asynccontextmanager
  async def acquire(self, graph_id: str, api_base_url: Optional[str] = None):
    """
    Acquire a client from the pool.

    Args:
        graph_id: Graph database identifier
        api_base_url: Override API URL (uses env var if None)

    Yields:
        GraphMCPClient instance

    Example:
        async with pool.acquire("sec") as client:
            result = await client.execute_query(query)
    """
    client = None
    created_at = None

    async with self._get_lock(graph_id):
      pool = self._pools.get(graph_id, [])

      # Try to get a client from the pool
      if pool:
        client, _, created_at = pool.pop(0)
        logger.debug(f"Reusing pooled connection for {graph_id}")

    # Create new client if none available
    if not client:
      # Import here to avoid circular dependency
      from .factory import create_graph_mcp_client

      logger.debug(f"Creating new connection for {graph_id}")
      client = await create_graph_mcp_client(graph_id, api_base_url)
      created_at = datetime.now()

    try:
      yield client
    finally:
      # Return client to pool if there's room
      async with self._get_lock(graph_id):
        pool = self._pools.get(graph_id, [])

        if len(pool) < self.max_connections_per_graph:
          # Add back to pool
          # Use current time if created_at is somehow None (shouldn't happen)
          pool.append((client, datetime.now(), created_at or datetime.now()))
          self._pools[graph_id] = pool
          logger.debug(f"Returned connection to pool for {graph_id}")
        else:
          # Pool is full, close the client
          logger.debug(f"Pool full for {graph_id}, closing connection")
          if hasattr(client, "close"):
            try:
              await client.close()
            except Exception as e:
              logger.error(f"Error closing client: {e}")

  async def get_stats(self) -> Dict[str, Dict]:
    """
    Get statistics about the connection pool.

    Returns:
        Dictionary with pool statistics per graph_id
    """
    stats = {}
    now = datetime.now()

    for graph_id, pool in self._pools.items():
      pool_stats = {
        "total_connections": len(pool),
        "max_connections": self.max_connections_per_graph,
        "connections": [],
      }

      for client, last_used, created_at in pool:
        idle_time = (now - last_used).total_seconds()
        lifetime = (now - created_at).total_seconds()

        pool_stats["connections"].append(
          {
            "idle_seconds": idle_time,
            "lifetime_seconds": lifetime,
            "will_expire_in": max(
              self.max_idle_time - idle_time, self.max_lifetime - lifetime, 0
            ),
          }
        )

      stats[graph_id] = pool_stats

    return stats


# Global connection pool instance
_global_pool: Optional[MCPConnectionPool] = None


def get_connection_pool() -> MCPConnectionPool:
  """
  Get the global connection pool instance.

  Returns:
      Global MCPConnectionPool instance
  """
  global _global_pool

  if _global_pool is None:
    _global_pool = MCPConnectionPool()
    # Note: The pool should be started by the application during startup
    # asyncio.create_task(_global_pool.start())

  return _global_pool


async def initialize_pool():
  """Initialize and start the global connection pool."""
  pool = get_connection_pool()
  await pool.start()
  logger.info("MCP connection pool initialized")


async def shutdown_pool():
  """Shutdown the global connection pool."""
  pool = get_connection_pool()
  await pool.stop()
  logger.info("MCP connection pool shutdown")
