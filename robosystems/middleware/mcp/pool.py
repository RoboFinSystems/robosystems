"""Connection pooling for MCP clients, so a client's setup cost is paid once."""

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime

from robosystems.config.tuning import TuningConfig
from robosystems.logger import logger


class MCPConnectionPool:
  """Pool of reusable MCP clients, one bucket per graph_id.

  A background sweep closes clients that have sat idle too long or outlived
  `max_lifetime`, so a pooled client never outlives the endpoint it was
  built against.
  """

  def __init__(
    self,
    max_connections_per_graph: int = 10,
    max_idle_time: int | None = None,
    max_lifetime: int | None = None,
  ):
    """Times are in seconds; None reads the SSM-tunable TuningConfig default."""
    self.max_connections_per_graph = max_connections_per_graph
    self.max_idle_time = (
      max_idle_time
      if max_idle_time is not None
      else TuningConfig.get_mcp_pool_idle_timeout()
    )
    self.max_lifetime = (
      max_lifetime
      if max_lifetime is not None
      else TuningConfig.get_mcp_pool_max_lifetime()
    )

    # graph_id -> [(client, last_used, created_at)]
    self._pools: dict[str, list] = {}
    self._locks: dict[str, asyncio.Lock] = {}
    self._cleanup_task: asyncio.Task | None = None
    self._running = False

  async def start(self):
    if not self._running:
      self._running = True
      self._cleanup_task = asyncio.create_task(self._cleanup_loop())
      logger.info("MCP connection pool started")

  async def stop(self):
    self._running = False

    if self._cleanup_task:
      self._cleanup_task.cancel()
      try:
        await self._cleanup_task
      except asyncio.CancelledError:
        pass

    for graph_id in list(self._pools.keys()):
      await self._close_pool(graph_id)

    logger.info("MCP connection pool stopped")

  async def _cleanup_loop(self):
    while self._running:
      try:
        await asyncio.sleep(60)
        await self._cleanup_idle_connections()
      except asyncio.CancelledError:
        break
      except Exception as e:
        logger.error(f"Error in cleanup loop: {e}")

  async def _cleanup_idle_connections(self):
    now = datetime.now()

    for graph_id in list(self._pools.keys()):
      async with self._get_lock(graph_id):
        pool = self._pools.get(graph_id, [])
        active_connections = []

        for client, last_used, created_at in pool:
          idle_time = (now - last_used).total_seconds()
          lifetime = (now - created_at).total_seconds()

          if idle_time > self.max_idle_time or lifetime > self.max_lifetime:
            logger.debug(
              f"Closing idle connection for {graph_id} "
              f"(idle: {idle_time}s, lifetime: {lifetime}s)"
            )
            if hasattr(client, "close"):
              try:
                await client.close()
              except Exception as e:
                logger.error(f"Error closing client: {e}")
          else:
            active_connections.append((client, last_used, created_at))

        if active_connections:
          self._pools[graph_id] = active_connections
        elif graph_id in self._pools:
          del self._pools[graph_id]
          if graph_id in self._locks:
            del self._locks[graph_id]

  def _get_lock(self, graph_id: str) -> asyncio.Lock:
    if graph_id not in self._locks:
      self._locks[graph_id] = asyncio.Lock()
    return self._locks[graph_id]

  async def _close_pool(self, graph_id: str):
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
  async def acquire(self, graph_id: str, api_base_url: str | None = None):
    """Yield a pooled or new client; it returns to the pool on exit if there's room."""
    client = None
    created_at = None

    async with self._get_lock(graph_id):
      pool = self._pools.get(graph_id, [])

      if pool:
        client, _, created_at = pool.pop(0)
        logger.debug(f"Reusing pooled connection for {graph_id}")

    if not client:
      from .factory import create_graph_mcp_client

      logger.debug(f"Creating new connection for {graph_id}")
      client = await create_graph_mcp_client(graph_id, api_base_url)
      created_at = datetime.now()

    try:
      yield client
    finally:
      async with self._get_lock(graph_id):
        pool = self._pools.get(graph_id, [])

        if len(pool) < self.max_connections_per_graph:
          pool.append((client, datetime.now(), created_at or datetime.now()))
          self._pools[graph_id] = pool
          logger.debug(f"Returned connection to pool for {graph_id}")
        else:
          logger.debug(f"Pool full for {graph_id}, closing connection")
          if hasattr(client, "close"):
            try:
              await client.close()
            except Exception as e:
              logger.error(f"Error closing client: {e}")

  async def get_stats(self) -> dict[str, dict]:
    """Return per-graph pool statistics."""
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
              0,
              min(self.max_idle_time - idle_time, self.max_lifetime - lifetime),
            ),
          }
        )

      stats[graph_id] = pool_stats

    return stats


_global_pool: MCPConnectionPool | None = None


def get_connection_pool() -> MCPConnectionPool:
  """Return the process-wide pool, creating it on first use.

  The pool is created stopped; the application starts it during startup via
  `initialize_pool`.
  """
  global _global_pool

  if _global_pool is None:
    _global_pool = MCPConnectionPool()

  return _global_pool


async def initialize_pool():
  pool = get_connection_pool()
  await pool.start()
  logger.info("MCP connection pool initialized")


async def shutdown_pool():
  pool = get_connection_pool()
  await pool.stop()
  logger.info("MCP connection pool shutdown")
