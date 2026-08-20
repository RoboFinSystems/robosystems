"""
Graph client factory — routes a graph ID to the instance that holds it.

Builds :class:`~robosystems.graph_api.client.GraphClient` instances aimed at
one of three targets, chosen from the graph ID, the operation type and the
tier:

1. User graph writers — per-graph LadybugDB instances (Standard/Large/XLarge),
   located through the allocation manager and cached in Valkey.
2. Shared repository master — source of truth for shared data (all writes, and
   reads when no replica ALB is configured), discovered from DynamoDB.
3. Shared repository replica ALB — read-only replicas for high-volume reads.
"""

import asyncio
import json
import random
import threading
import time
from enum import Enum
from typing import Any

import httpx
import redis.asyncio as redis

from robosystems.config import env
from robosystems.config.constants import (
  GRAPH_CONNECT_TIMEOUT,
  GRAPH_INSTANCE_CACHE_TTL,
  GRAPH_READ_TIMEOUT,
)
from robosystems.config.graph_tier import GraphTier
from robosystems.config.shared_repositories import is_shared_repository
from robosystems.config.valkey_registry import ValkeyDatabase
from robosystems.graph_api.client import GraphClient
from robosystems.logger import logger
from robosystems.middleware.graph.allocation_manager import LadybugAllocationManager
from robosystems.middleware.graph.utils import parse_subgraph_id


class GraphClientError(Exception):
  """Base exception for graph client errors."""

  pass


class ServiceUnavailableError(GraphClientError):
  """Raised when a required service is not available."""

  pass


class ConfigurationError(GraphClientError):
  """Raised when there's a configuration issue."""

  pass


class RouteError(GraphClientError):
  """Raised when routing cannot be determined."""

  pass


class RouteTarget(Enum):
  """Routing target types."""

  USER_GRAPH = "user_graph"
  SHARED_MASTER = "shared_master"
  SHARED_REPLICA = "shared_replica"  # Read replicas via ALB


class CircuitBreaker:
  """Async-safe circuit breaker for managing service availability."""

  def __init__(self, failure_threshold: int = 5, timeout: int = 60):
    """Open after ``failure_threshold`` failures; retry after ``timeout`` seconds."""
    self._lock = asyncio.Lock()
    self.failure_threshold = failure_threshold
    self.timeout = timeout
    self.failure_count = 0
    self.last_failure_time: float | None = None
    self.is_open = False

  async def record_success(self):
    """Record a successful operation."""
    async with self._lock:
      self.failure_count = 0
      self.is_open = False
      self.last_failure_time = None

  async def record_failure(self):
    """Record a failed operation."""
    async with self._lock:
      self.failure_count += 1
      self.last_failure_time = time.time()

      if self.failure_count >= self.failure_threshold:
        self.is_open = True
        logger.warning(f"Circuit breaker opened after {self.failure_count} failures")

  async def should_attempt(self) -> bool:
    """Check whether the operation should be attempted, half-opening on timeout."""
    async with self._lock:
      if not self.is_open:
        return True

      if (
        self.last_failure_time and (time.time() - self.last_failure_time) > self.timeout
      ):
        logger.info("Circuit breaker attempting to close after timeout")
        self.is_open = False
        self.failure_count = 0
        return True

      return False


def with_retry(
  max_attempts: int = 3,
  base_delay: float = 1.0,
  max_delay: float = 30.0,
  exponential_base: float = 2.0,
  jitter: bool = True,
):
  """Retry an async function with exponential backoff.

  Only retries connection-level failures (``httpx.TimeoutException``,
  ``httpx.ConnectError``, ``ServiceUnavailableError``); everything else
  propagates on the first raise. ``jitter`` spreads the delay over 0.5x-1.5x to
  avoid a synchronized retry storm across callers. Disabled wholesale by
  ``GRAPH_RETRY_LOGIC_ENABLED=false``.
  """

  def decorator(func):
    async def wrapper(*args, **kwargs):
      if not env.GRAPH_RETRY_LOGIC_ENABLED:
        return await func(*args, **kwargs)

      last_exception = None

      for attempt in range(max_attempts):
        try:
          return await func(*args, **kwargs)
        except (
          httpx.TimeoutException,
          httpx.ConnectError,
          ServiceUnavailableError,
        ) as e:
          last_exception = e

          if attempt == max_attempts - 1:
            logger.error(
              f"Failed after {max_attempts} attempts in {func.__name__}: {e}"
            )
            break

          delay = min(base_delay * (exponential_base**attempt), max_delay)

          if jitter:
            delay = delay * (0.5 + random.random())

          logger.warning(
            f"Attempt {attempt + 1}/{max_attempts} failed for {func.__name__}, "
            f"retrying in {delay:.2f}s: {e}"
          )
          await asyncio.sleep(delay)

      raise last_exception or Exception(f"Failed after {max_attempts} attempts")

    return wrapper

  return decorator


class GraphClientFactory:
  """Creates graph clients routed to the backend that holds the graph.

  All state is class-level and shared process-wide: the connection pools, the
  Valkey client used for route caching, and the shared-master circuit breaker.
  """

  _instance_cache_ttl = GRAPH_INSTANCE_CACHE_TTL

  _connect_timeout = GRAPH_CONNECT_TIMEOUT
  _read_timeout = GRAPH_READ_TIMEOUT

  _connection_pools: dict[str, httpx.AsyncClient] = {}
  _pool_stats: dict[str, dict[str, Any]] = {}

  # Guarded by a threading lock rather than an asyncio one: the pool is shared
  # across event loops, and each loop builds its own client from it.
  _redis_pool: redis.ConnectionPool | None = None
  _redis_client_lock = threading.Lock()

  _master_circuit_breaker = CircuitBreaker(
    failure_threshold=env.GRAPH_CIRCUIT_BREAKER_THRESHOLD,
    timeout=env.GRAPH_CIRCUIT_BREAKER_TIMEOUT,
  )

  @classmethod
  def _get_cache_key(cls, key_type: str, identifier: str = "") -> str:
    """Build a cache key, prefixed by environment so envs never share entries."""
    env_prefix = env.ENVIRONMENT or "dev"
    if identifier:
      return f"graph:{env_prefix}:{key_type}:{identifier}"
    return f"graph:{env_prefix}:{key_type}"

  @classmethod
  async def _get_redis(cls) -> redis.Redis | None:
    """Get a Redis client for route caching, or None when caching is unavailable.

    A fresh client is built per call rather than reused: background tasks run
    on their own event loops, and a client bound to a closed loop raises. Every
    failure path returns None so routing degrades to uncached lookups instead
    of failing.
    """
    if not env.GRAPH_REDIS_CACHE_ENABLED:
      return None

    try:
      import socket

      from robosystems.config.valkey_registry import create_async_redis_client

      # Constants are looked up on `socket` rather than hardcoded; they are
      # Linux-only and absent on macOS.
      keepalive_options = {}
      if env.ENVIRONMENT in ["prod", "staging"]:
        if hasattr(socket, "TCP_KEEPIDLE"):
          keepalive_options[socket.TCP_KEEPIDLE] = 30
        if hasattr(socket, "TCP_KEEPINTVL"):
          keepalive_options[socket.TCP_KEEPINTVL] = 10
        if hasattr(socket, "TCP_KEEPCNT"):
          keepalive_options[socket.TCP_KEEPCNT] = 6

      client = create_async_redis_client(
        ValkeyDatabase.GRAPH_ROUTING,
        decode_responses=True,
        max_connections=10,
        socket_keepalive=True,
        socket_keepalive_options=keepalive_options if keepalive_options else None,
      )

      try:
        await client.ping()
        return client
      except AttributeError as ae:
        # A changed event-loop context surfaces as an AttributeError on the
        # parser rather than a connection error.
        logger.debug(f"Redis connection state issue, skipping cache: {ae}")
        return None

    except Exception as e:
      logger.warning(f"Redis not available for caching: {e}")
      return None

  @classmethod
  async def create_client(
    cls,
    graph_id: str,
    operation_type: str = "read",
    environment: str | None = None,
    tier: GraphTier | None = None,
  ) -> GraphClient:
    """Create a graph client routed to whichever backend holds ``graph_id``.

    Shared repositories — and subgraphs of them, such as ``sec_historical`` —
    go to the shared master or a replica depending on ``operation_type``;
    everything else routes to the user graph's allocated instance.

    Raises ``ServiceUnavailableError`` when no endpoint can be resolved, and
    ``RouteError`` when the graph is not allocated to any instance.
    """
    if environment is None:
      environment = env.ENVIRONMENT or "dev"

    logger.info(
      f"Creating graph client for graph={graph_id}, "
      f"operation={operation_type}, tier={tier}, env={environment}"
    )

    try:
      if is_shared_repository(graph_id.lower()):
        return await cls._create_shared_repository_client(graph_id, operation_type)

      subgraph_info = parse_subgraph_id(graph_id)
      if subgraph_info and is_shared_repository(subgraph_info.parent_graph_id.lower()):
        return await cls._create_shared_repository_client(graph_id, operation_type)

      return await cls._create_user_graph_client(graph_id, environment, tier)
    except ServiceUnavailableError as e:
      raise ServiceUnavailableError(
        f"Failed to create client for {graph_id} ({operation_type} operation): {e}. "
        f"Environment={environment}, Tier={tier}"
      )
    except Exception as e:
      logger.error(
        f"Unexpected error creating client for {graph_id}: {e}",
        extra={
          "graph_id": graph_id,
          "operation_type": operation_type,
          "environment": environment,
          "tier": tier,
        },
      )
      raise

  @classmethod
  async def _create_shared_repository_client(
    cls, graph_id: str, operation_type: str
  ) -> GraphClient:
    """Create a client for a shared repository.

    Dev collapses to the single local instance. In prod/staging writes always
    go to the shared master — replicas are read-only copies synced from it —
    and reads prefer the replica ALB, falling back to the master.
    """

    if env.is_development():
      api_url = env.GRAPH_API_URL or "http://localhost:8001"
      logger.info(
        f"Dev environment: Routing {graph_id} {operation_type} to LadybugDB at {api_url}"
      )

      api_key = env.GRAPH_API_KEY
      target = RouteTarget.SHARED_MASTER

    elif operation_type == "write":
      target = RouteTarget.SHARED_MASTER
      api_url = await cls._get_shared_master_url()
      api_key = env.GRAPH_API_KEY

      logger.info(f"Routing {graph_id} WRITE to shared master at {api_url}")

    else:
      target, api_url, api_key = await cls._determine_read_target(graph_id)

    if not api_url:
      raise ConfigurationError(f"No endpoint configured for {target.value}")

    if not api_key:
      if not env.is_development() and not env.is_test():
        logger.warning(f"No API key configured for {target.value}, using default")
      api_key = env.GRAPH_API_KEY

    client = GraphClient(base_url=api_url, api_key=api_key)

    client._route_target = target.value
    client._graph_id = graph_id

    return client

  @classmethod
  async def _determine_read_target(cls, graph_id: str) -> tuple[RouteTarget, str, str]:
    """Pick the read endpoint: replica ALB if configured, else the master.

    Falling through to the master requires ``SHARED_MASTER_READS_ENABLED``, so
    a deployment can keep read load off the writer entirely. Returns
    ``(target, api_url, api_key)``.
    """
    if env.SHARED_REPLICA_ALB_URL:
      logger.info(f"Routing {graph_id} READ to shared replica ALB")
      return (
        RouteTarget.SHARED_REPLICA,
        env.SHARED_REPLICA_ALB_URL,
        env.GRAPH_API_KEY,
      )

    if env.SHARED_MASTER_READS_ENABLED:
      logger.info(
        f"Routing {graph_id} READ to shared master (no replica ALB configured)"
      )
      return (
        RouteTarget.SHARED_MASTER,
        await cls._get_shared_master_url(),
        env.GRAPH_API_KEY,
      )

    raise ServiceUnavailableError(
      f"No read endpoint available for shared repository {graph_id}. "
      "Configure SHARED_REPLICA_ALB_URL or enable SHARED_MASTER_READS_ENABLED."
    )

  @classmethod
  @with_retry(max_attempts=3, base_delay=1.0)
  async def _get_shared_master_url(cls) -> str:
    """Discover the shared master's URL.

    The shared master runs the same graph-ladybug stack with ``tier="shared"``
    and registers itself in the DynamoDB instance registry under
    ``node_type="shared_master"``, so there is no static endpoint to
    configure. Resolution order: Valkey cache, then a paginated registry scan
    for a healthy master (cached for 5 minutes), then ``GRAPH_API_URL`` — but
    that last fallback applies in development only, never prod/staging.
    """
    if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
      if not await cls._master_circuit_breaker.should_attempt():
        logger.warning("Shared master circuit breaker is open, using fallback")
        if env.GRAPH_API_URL:
          return env.GRAPH_API_URL
        raise ServiceUnavailableError(
          "Shared master unavailable (circuit breaker open)"
        )

    try:
      redis_client = await cls._get_redis()
      cache_key = cls._get_cache_key("shared_master", "url")

      if redis_client:
        try:
          cached_url = await redis_client.get(cache_key)
          if cached_url:
            logger.debug(f"Using cached shared master URL: {cached_url}")
            if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
              await cls._master_circuit_breaker.record_success()
            return cached_url
        except Exception as e:
          logger.warning(f"Redis cache read failed: {e}")

      import boto3

      dynamodb = boto3.client("dynamodb", region_name=env.AWS_REGION or "us-east-1")

      paginator = dynamodb.get_paginator("scan")

      page_iterator = paginator.paginate(
        TableName=env.INSTANCE_REGISTRY_TABLE,
        FilterExpression="#status = :status AND node_type = :node_type",
        ExpressionAttributeNames={
          "#status": "status",  # 'status' is a reserved keyword in DynamoDB
        },
        ExpressionAttributeValues={
          ":status": {"S": "healthy"},
          ":node_type": {"S": "shared_master"},
        },
        PaginationConfig={
          "MaxItems": 100,  # Maximum items to scan
          "PageSize": 25,  # Items per page
        },
      )

      for page in page_iterator:
        if page.get("Items"):
          item = page["Items"][0]
          private_ip = item.get("private_ip", {}).get("S")
          instance_id = item.get("instance_id", {}).get("S", "unknown")

          if private_ip:
            url = f"http://{private_ip}:8001"
            logger.info(f"Discovered shared master at {url} (instance: {instance_id})")

            if redis_client:
              try:
                await redis_client.setex(cache_key, 300, url)
              except Exception as e:
                logger.warning(f"Redis cache write failed: {e}")

            if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
              await cls._master_circuit_breaker.record_success()
            return url

      # The registry is the only source of master liveness here. In-flight
      # destructive operations are tracked by the DynamoDB busy counter on
      # instance-registry (see instance_busy.py), which the GHA pre-refresh
      # workflows read directly — routing does not consult it.
      logger.warning(
        f"No healthy shared master found in DynamoDB after scanning "
        f"{env.ENVIRONMENT} environment"
      )
      if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
        await cls._master_circuit_breaker.record_failure()

    except Exception as e:
      if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
        await cls._master_circuit_breaker.record_failure()
      logger.error(
        f"Failed to discover shared master: {e}",
        extra={
          "environment": env.ENVIRONMENT,
          "table_name": env.INSTANCE_REGISTRY_TABLE,
          "error_type": type(e).__name__,
          "error_details": str(e),
        },
      )

    # Gated on development: a localhost fallback in prod/staging would send
    # shared-repository writes to whatever happens to answer on port 8001.
    if env.is_development() and env.GRAPH_API_URL:
      logger.warning(
        f"Dev environment: Using API URL fallback for shared master: {env.GRAPH_API_URL}"
      )
      return env.GRAPH_API_URL

    raise ServiceUnavailableError(
      f"Cannot find shared master in {env.ENVIRONMENT} environment. "
      f"Ensure shared master is running and registered in DynamoDB with node_type='shared_master'"
    )

  @classmethod
  async def _create_user_graph_client(
    cls, graph_id: str, environment: str | None, tier: GraphTier | None
  ) -> GraphClient:
    """Create a client for a user graph, resolving its allocated instance.

    Dev routes to the single local instance. Prod/staging asks the allocation
    manager which instance holds the graph. A subgraph routes to its *parent's*
    instance but targets the subgraph's own database, which is why
    ``_database_name`` is set separately from ``_graph_id``.
    """

    subgraph_info = parse_subgraph_id(graph_id)
    actual_graph_id = subgraph_info.parent_graph_id if subgraph_info else graph_id
    database_name = subgraph_info.database_name if subgraph_info else graph_id

    if subgraph_info:
      logger.info(
        f"Detected subgraph {graph_id} - routing via parent {actual_graph_id}, "
        f"database: {database_name}"
      )

    if env.is_development():
      api_url = env.GRAPH_API_URL or "http://localhost:8001"
      api_key = env.GRAPH_API_KEY

      logger.info(
        f"Dev environment: Routing user graph {graph_id} to local graph at {api_url}"
      )

      client = GraphClient(base_url=api_url, api_key=api_key)
      client._route_target = RouteTarget.USER_GRAPH.value
      client._graph_id = graph_id
      client._database_name = database_name

      return client

    # Keyed on actual_graph_id, so a parent and its subgraphs share one entry.
    cache_key = cls._get_cache_key("location", actual_graph_id)
    redis_client = await cls._get_redis()

    # Routing needs only the instance's private IP and ID, cached as plain
    # scalars. Do not round-trip a DatabaseLocation through this cache: its
    # required fields (graph_id, availability_zone, status, created_at) are not
    # stored, so reconstruction raises and silently disables caching.
    private_ip: str | None = None
    instance_id: str | None = None
    if redis_client:
      try:
        cached = await redis_client.get(cache_key)
        if cached:
          location_data = json.loads(cached)
          if time.time() - location_data.get("timestamp", 0) < cls._instance_cache_ttl:
            location = location_data["location"]
            private_ip = location["private_ip"]
            instance_id = location["instance_id"]
            logger.debug(f"Using cached location for {graph_id}")
      except Exception as e:
        logger.warning(f"Redis cache read failed for location: {e}")

    if private_ip is None:
      allocation_manager = LadybugAllocationManager(
        environment=environment or env.ENVIRONMENT
      )
      db_location = await allocation_manager.find_database_location(actual_graph_id)

      if not db_location:
        error_msg = (
          f"Database {actual_graph_id} not found in any instance. "
          f"It may need to be created first."
        )
        if subgraph_info:
          error_msg = (
            f"Parent graph {actual_graph_id} not found for subgraph {graph_id}. "
            f"The parent graph must be created before creating subgraphs."
          )
        raise RouteError(error_msg)

      private_ip = db_location.private_ip
      instance_id = db_location.instance_id

      if redis_client:
        try:
          location_data = {
            "location": {
              "instance_id": instance_id,
              "private_ip": private_ip,
            },
            "timestamp": time.time(),
          }
          await redis_client.setex(
            cache_key, cls._instance_cache_ttl, json.dumps(location_data)
          )
        except Exception as e:
          logger.warning(f"Redis cache write failed for location: {e}")

    api_url = f"http://{private_ip}:8001"
    api_key = env.GRAPH_API_KEY

    logger.info(f"Routing user graph {graph_id} to instance {instance_id} at {api_url}")

    if not api_key:
      logger.error("GRAPH_API_KEY is not set in environment!")

    client = GraphClient(base_url=api_url, api_key=api_key)

    client._route_target = RouteTarget.USER_GRAPH.value
    client._graph_id = graph_id
    # Differs from _graph_id for subgraphs — the client targets this database.
    client._database_name = database_name
    client._instance_id = instance_id

    return client

  @classmethod
  def create_client_sync(
    cls,
    graph_id: str,
    operation_type: str = "read",
    environment: str | None = None,
    tier: GraphTier | None = None,
  ) -> GraphClient:
    """Synchronous wrapper for :meth:`create_client`.

    Only valid outside a running event loop; called from async code it raises
    rather than deadlocking on a nested ``asyncio.run``.
    """
    try:
      asyncio.get_running_loop()
      raise RuntimeError(
        "create_client_sync() cannot be called from an async context. "
        "Use 'await get_graph_client(graph_id)' or "
        "'await GraphClientFactory.create_client(graph_id)' instead. "
        "For sync contexts, wrap with asyncio.run()."
      )
    except RuntimeError as e:
      if "no running event loop" in str(e).lower():
        return asyncio.run(
          cls.create_client(graph_id, operation_type, environment, tier)
        )
      else:
        raise

  @classmethod
  def get_pool_statistics(cls) -> dict[str, Any]:
    """Get per-pool request counts and failure rates plus circuit-breaker state."""
    stats = {
      "pools": {},
      "circuit_breakers": {
        "master": {
          "is_open": cls._master_circuit_breaker.is_open,
          "failure_count": cls._master_circuit_breaker.failure_count,
          "last_failure": cls._master_circuit_breaker.last_failure_time,
        },
      },
      "total_pools": len(cls._connection_pools),
    }

    for url, pool_stat in cls._pool_stats.items():
      failure_rate = 0
      if pool_stat.get("requests", 0) > 0:
        failure_rate = pool_stat.get("failures", 0) / pool_stat["requests"]

      stats["pools"][url] = {
        "created_at": pool_stat.get("created_at"),
        "requests": pool_stat.get("requests", 0),
        "failures": pool_stat.get("failures", 0),
        "failure_rate": round(failure_rate, 3),
        "is_active": url in cls._connection_pools,
      }

    return stats

  @classmethod
  async def cleanup(cls):
    """Close all pools and reset the circuit breakers.

    Every step swallows its own errors so one bad pool cannot abort shutdown.
    """
    try:
      stats = cls.get_pool_statistics()
      logger.info(
        f"Cleaning up GraphClientFactory: {stats['total_pools']} pools active",
        extra={"pool_stats": stats},
      )
    except Exception as e:
      logger.warning(f"Error getting pool statistics during cleanup: {e}")

    for url, client in cls._connection_pools.items():
      try:
        await client.aclose()
        logger.debug(f"Closed connection pool for {url}")
      except Exception as e:
        logger.warning(f"Error closing connection pool for {url}: {e}")

    cls._connection_pools.clear()
    cls._pool_stats.clear()

    with cls._redis_client_lock:
      if cls._redis_pool:
        try:
          await cls._redis_pool.disconnect()
          logger.debug("Closed Redis connection pool")
        except Exception as e:
          logger.warning(f"Error closing Redis connection pool: {e}")
        finally:
          cls._redis_pool = None

    cls._master_circuit_breaker = CircuitBreaker(
      failure_threshold=env.GRAPH_CIRCUIT_BREAKER_THRESHOLD,
      timeout=env.GRAPH_CIRCUIT_BREAKER_TIMEOUT,
    )

    logger.info("GraphClientFactory cleanup completed")


# Convenience functions


async def get_graph_client(
  graph_id: str,
  operation_type: str = "read",
  environment: str | None = None,
  tier: GraphTier | None = None,
) -> GraphClient:
  """Get a routed graph client. Preferred entry point in async contexts.

  Example:
      async with await get_graph_client("sec", "read") as client:
          result = await client.query("MATCH (c:Company) RETURN c LIMIT 10")
  """
  return await GraphClientFactory.create_client(
    graph_id, operation_type, environment, tier
  )


def get_graph_client_sync(
  graph_id: str,
  operation_type: str = "read",
  environment: str | None = None,
  tier: GraphTier | None = None,
) -> GraphClient:
  """Get a routed graph client from sync code (no event loop running).

  Example:
      with get_graph_client_sync("kg1a2b3c") as client:
          result = client.query("MATCH (c:Entity) RETURN c")
  """
  return GraphClientFactory.create_client_sync(
    graph_id, operation_type, environment, tier
  )


async def get_graph_client_for_instance(
  instance_ip: str, api_key: str | None = None
) -> GraphClient:
  """Get a client pinned to one instance IP, bypassing routing entirely.

  Used by allocation, where the target instance is chosen before any database
  exists on it for the router to find.

  Example:
      client = await get_graph_client_for_instance("10.0.1.123")
      await client.create_database("entity_456")
  """
  if api_key is None:
    api_key = env.GRAPH_API_KEY

  api_url = f"http://{instance_ip}:8001"
  logger.info(f"Creating direct graph client for instance at {api_url}")

  return GraphClient(base_url=api_url, api_key=api_key)


async def get_graph_client_for_sec_ingestion() -> GraphClient:
  """Get a client pinned to the shared master, for SEC ingestion.

  SEC ingestion must land on the shared master: that instance is the one
  snapshotted to S3 and synced out to the read replicas, so a write anywhere
  else would be invisible to readers and lost on the next sync.
  """
  logger.info("Creating graph client for SEC ingestion (direct to shared master)")

  if env.is_development():
    api_url = env.GRAPH_API_URL or "http://localhost:8001"
    logger.info(f"Dev environment: SEC ingestion to local graph at {api_url}")
  else:
    try:
      api_url = await GraphClientFactory._get_shared_master_url()
      logger.info(f"Discovered shared master for SEC ingestion: {api_url}")
    except Exception as e:
      logger.warning(f"Failed to discover shared master: {e}")
      if env.GRAPH_API_URL:
        api_url = env.GRAPH_API_URL
        logger.warning("Using API Gateway fallback for SEC ingestion")
      else:
        raise ServiceUnavailableError(
          "Cannot find shared master for SEC ingestion and no fallback configured"
        )

  api_key = env.GRAPH_API_KEY

  client = GraphClient(base_url=api_url, api_key=api_key)
  client._route_target = RouteTarget.SHARED_MASTER.value
  client._purpose = "sec_ingestion"

  return client


async def boost_graph_memory(graph_id: str, target: str = "both") -> dict[str, Any]:
  """Raise memory limits for staging (``duckdb``) or materialization (``ladybug``).

  The boost stays active until :func:`release_graph_memory`. Never raises — a
  failed boost degrades to default limits rather than blocking the operation.

  Example:
      await boost_graph_memory("sec", target="duckdb")
      # ... run staging ...
      await boost_graph_memory("sec", target="ladybug")
      # ... run materialization ...
      await release_graph_memory("sec", target="both")
  """
  client = await get_graph_client(graph_id, operation_type="write")

  try:
    result = await client.boost_memory(graph_id, target=target)
    logger.info(f"Memory boost for {graph_id}: {result.get('message', 'done')}")
    return result
  except Exception as e:
    logger.warning(f"Failed to boost memory for {graph_id}: {e}")
    return {
      "graph_id": graph_id,
      "target": target,
      "duckdb_boosted": False,
      "ladybug_boosted": False,
      "message": f"Boost failed: {e}",
    }


async def release_graph_memory(
  graph_id: str, target: str = "both", aggressive: bool = True
) -> dict[str, Any]:
  """Close connections and return buffer memory to the OS.

  Closing connections is what forces the engines to give buffers back; the
  graph API's ``restore-memory`` endpoint (:meth:`GraphClient.restore_memory`)
  only lowers the configured ceiling, which is why every boost in the SEC
  pipeline pairs with *this* function. ``aggressive`` additionally runs GC and
  ``malloc_trim`` on the LadybugDB side. Never raises; this is cleanup.

  Example:
      await release_graph_memory("sec", target="duckdb")
      await release_graph_memory("sec", target="both", aggressive=True)
  """
  client = await get_graph_client(graph_id, operation_type="write")

  try:
    result = await client.release_memory(graph_id, target=target, aggressive=aggressive)
    logger.info(f"Memory release for {graph_id}: {result.get('message', 'done')}")
    return result
  except Exception as e:
    logger.warning(f"Failed to release memory for {graph_id}: {e}")
    return {
      "graph_id": graph_id,
      "target": target,
      "duckdb_connections_closed": 0,
      "duckdb_released": False,
      "ladybug_released": False,
      "message": f"Release failed: {e}",
    }
