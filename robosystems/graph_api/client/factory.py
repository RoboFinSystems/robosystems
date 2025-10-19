"""
Graph Client Factory - Intelligent routing for all graph database backends.

This module provides a factory for creating GraphClient instances that properly
route to the correct graph database instance (Kuzu or Neo4j) based on the graph ID,
operation type, and tier.

Routing targets:
1. User Graph Writers - Tier-based routing (Standard/Enterprise/Premium)
   - Standard: Kuzu backend
   - Enterprise: Neo4j Community backend
   - Premium: Neo4j Enterprise backend
2. Shared Repository Master - Primary source of truth for shared data (writes + fallback reads)
3. Shared Repository Replica ALB - Read-only replicas for high-volume reads
"""

import asyncio
import httpx
import time
import json
import random
import threading
from typing import Dict, Any
from enum import Enum
import redis.asyncio as redis
from robosystems.graph_api.client import GraphClient
from robosystems.config import env
from robosystems.config.valkey_registry import ValkeyDatabase
from robosystems.logger import logger
from robosystems.middleware.graph.allocation_manager import KuzuAllocationManager
from robosystems.middleware.graph.types import InstanceTier, GraphTypeRegistry
from robosystems.middleware.graph.subgraph_utils import parse_subgraph_id


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
  SHARED_REPLICA_ALB = "shared_replica_alb"


class CircuitBreaker:
  """Async-safe circuit breaker for managing service availability."""

  def __init__(self, failure_threshold: int = 5, timeout: int = 60):
    """
    Initialize circuit breaker.

    Args:
        failure_threshold: Number of failures before opening circuit
        timeout: Seconds to wait before attempting to close circuit
    """
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
    """Check if we should attempt the operation."""
    async with self._lock:
      if not self.is_open:
        return True

      # Check if timeout has passed
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
  """
  Decorator for retry logic with exponential backoff.

  Args:
      max_attempts: Maximum number of retry attempts
      base_delay: Initial delay between retries in seconds
      max_delay: Maximum delay between retries
      exponential_base: Base for exponential backoff
      jitter: Add random jitter to delays
  """

  def decorator(func):
    async def wrapper(*args, **kwargs):
      # Check if retry logic is enabled via feature flag
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

          # Calculate delay with exponential backoff
          delay = min(base_delay * (exponential_base**attempt), max_delay)

          # Add jitter if enabled
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
  """
  Factory for creating properly routed graph database clients.

  Handles intelligent routing to different backends:
  - User graph writers (tier-based routing to Kuzu or Neo4j)
    - Standard tier: Kuzu
    - Enterprise tier: Neo4j Community
    - Premium tier: Neo4j Enterprise
  - Shared repository master (writes + fallback reads)
  - Shared repository replica ALB (primary reads)
  """

  # Shared repositories from the graph type registry
  SHARED_REPOSITORIES = list(GraphTypeRegistry.SHARED_REPOSITORIES.keys())

  # Cache TTLs from constants
  _alb_health_cache_ttl = env.GRAPH_ALB_HEALTH_CACHE_TTL
  _instance_cache_ttl = env.GRAPH_INSTANCE_CACHE_TTL

  # Timeout configurations from constants
  _connect_timeout = env.GRAPH_CONNECT_TIMEOUT
  _read_timeout = env.GRAPH_READ_TIMEOUT

  # Connection pools for reuse (HTTP/2 enabled for efficiency)
  _connection_pools: Dict[str, httpx.AsyncClient] = {}
  _pool_stats: Dict[str, Dict[str, Any]] = {}  # Track pool statistics

  # Redis connection pool for caching (thread-local to avoid event loop issues)
  _redis_pool: redis.ConnectionPool | None = None
  _redis_client_lock = threading.Lock()

  # Circuit breakers for different services
  _alb_circuit_breaker = CircuitBreaker(
    failure_threshold=env.GRAPH_CIRCUIT_BREAKER_THRESHOLD,
    timeout=env.GRAPH_CIRCUIT_BREAKER_TIMEOUT,
  )
  _master_circuit_breaker = CircuitBreaker(
    failure_threshold=env.GRAPH_CIRCUIT_BREAKER_THRESHOLD,
    timeout=env.GRAPH_CIRCUIT_BREAKER_TIMEOUT,
  )

  @classmethod
  def _get_cache_key(cls, key_type: str, identifier: str = "") -> str:
    """
    Generate environment-prefixed cache key to prevent collisions.

    Args:
        key_type: Type of cache key (e.g., "master", "alb", "location")
        identifier: Optional identifier (e.g., graph_id)

    Returns:
        Environment-prefixed cache key
    """
    env_prefix = env.ENVIRONMENT or "dev"
    if identifier:
      return f"graph:{env_prefix}:{key_type}:{identifier}"
    return f"graph:{env_prefix}:{key_type}"

  @classmethod
  async def _get_redis(cls) -> redis.Redis | None:
    """Get or create Redis connection for caching with event loop safety."""
    # Check if Redis caching is enabled via feature flag
    if not env.GRAPH_REDIS_CACHE_ENABLED:
      return None

    try:
      # Create a new Redis client for each event loop to avoid "Event loop is closed" errors
      # This is necessary because Celery tasks create new event loops
      # Use async factory method to handle SSL params correctly
      from robosystems.config.valkey_registry import create_async_redis_client

      client = create_async_redis_client(
        ValkeyDatabase.KUZU_CACHE,
        decode_responses=True,
        max_connections=10,
        socket_keepalive=True,
        socket_keepalive_options={
          1: 30,  # TCP_KEEPIDLE
          2: 10,  # TCP_KEEPINTVL
          3: 6,  # TCP_KEEPCNT
        }
        if env.ENVIRONMENT in ["prod", "staging"]
        else {},
      )

      # Test the connection - handle connection state issues gracefully
      try:
        await client.ping()
        return client
      except AttributeError as ae:
        # Handle '_AsyncRESP2Parser' object has no attribute '_connected' error
        # This happens when the event loop context changes (e.g., in Celery tasks)
        logger.debug(f"Redis connection state issue, skipping cache: {ae}")
        return None

    except Exception as e:
      # Don't cache failures - might be transient
      logger.warning(f"Redis not available for caching: {e}")
      return None

  @classmethod
  async def create_client(
    cls,
    graph_id: str,
    operation_type: str = "read",
    environment: str | None = None,
    tier: InstanceTier | None = None,
  ) -> GraphClient:
    """
    Create a graph database client with intelligent routing.

    Routes to the appropriate backend based on tier:
    - Standard tier: Kuzu
    - Enterprise tier: Neo4j Community
    - Premium tier: Neo4j Enterprise

    Args:
        graph_id: Graph database identifier
        operation_type: "read" or "write"
        environment: Environment (defaults to env.ENVIRONMENT)
        tier: Instance tier for user graphs (Standard/Enterprise/Premium)

    Returns:
        Configured GraphClient instance (works with all backends via Graph API)

    Raises:
        ValueError: If graph not found or invalid configuration
        ServiceUnavailableError: If required services are unavailable
    """
    if environment is None:
      environment = env.ENVIRONMENT or "dev"

    # Log routing decision with context
    logger.info(
      f"Creating graph client for graph={graph_id}, "
      f"operation={operation_type}, tier={tier}, env={environment}"
    )

    try:
      # Determine routing based on graph_id
      if graph_id.lower() in GraphClientFactory.SHARED_REPOSITORIES:
        # Route to shared repository infrastructure
        return await cls._create_shared_repository_client(graph_id, operation_type)
      else:
        # Route to user graph writers
        return await cls._create_user_graph_client(graph_id, environment, tier)
    except ServiceUnavailableError as e:
      # Enhance error with routing context
      raise ServiceUnavailableError(
        f"Failed to create client for {graph_id} ({operation_type} operation): {e}. "
        f"Environment={environment}, Tier={tier}"
      )
    except Exception as e:
      # Add context to unexpected errors
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
    """
    Create client for shared repository with intelligent routing.

    Routing logic:
    - Dev: Everything goes to single local graph instance
    - Prod/Staging:
      - Writes: Always go to shared master
      - Reads: Try replica ALB first, fallback to master if allowed
    """

    # In dev environment, route to appropriate local instance based on configuration
    if env.is_development():
      # Priority hierarchy for backend selection in dev:
      # 1. GRAPH_SHARED_REPOSITORY_BACKEND env var (explicit override)
      # 2. graph.yml tier config (fallback for consistency with AWS)
      # 3. Default to kuzu

      backend = "kuzu"  # Default

      # Check env var first (highest priority in dev)
      if env.GRAPH_SHARED_REPOSITORY_BACKEND:
        backend = env.GRAPH_SHARED_REPOSITORY_BACKEND
        logger.info(f"Using backend from GRAPH_SHARED_REPOSITORY_BACKEND: {backend}")
      else:
        # Fall back to tier config (for consistency with AWS environments)
        from robosystems.config.tier_config import TierConfig
        tier_config = TierConfig.get_tier_config("shared", "staging")
        if tier_config.get("backend"):
          backend = tier_config.get("backend")
          logger.info(f"Using backend from graph.yml tier config: {backend}")
        else:
          logger.info(f"Using default backend: {backend}")

      # Route to appropriate local instance based on backend
      if backend == "neo4j":
        api_url = "http://neo4j-api:8002"  # Neo4j instance
        logger.info(
          f"Dev environment: Routing {graph_id} {operation_type} to Neo4j at {api_url}"
        )
      else:
        api_url = env.GRAPH_API_URL or "http://localhost:8001"  # Kuzu instance
        logger.info(
          f"Dev environment: Routing {graph_id} {operation_type} to Kuzu at {api_url}"
        )

      api_key = env.GRAPH_API_KEY
      target = RouteTarget.SHARED_MASTER  # Treat as master in dev

    # Production/staging routing
    elif operation_type == "write":
      # All writes MUST go to shared master
      target = RouteTarget.SHARED_MASTER
      api_url = await cls._get_shared_master_url()
      api_key = env.GRAPH_API_KEY

      logger.info(f"Routing {graph_id} WRITE to shared master at {api_url}")

    else:  # read operation in prod/staging
      # Determine read target with fallback logic
      target, api_url, api_key = await cls._determine_read_target(graph_id)

    # Validate we have necessary configuration
    if not api_url:
      raise ConfigurationError(f"No endpoint configured for {target.value}")

    if not api_key:
      # Only warn in production/staging environments
      if not env.is_development() and not env.is_test():
        logger.warning(f"No API key configured for {target.value}, using default")
      api_key = env.GRAPH_API_KEY

    # Create client with appropriate configuration
    client = GraphClient(base_url=api_url, api_key=api_key)

    # Add metadata for debugging
    client._route_target = target.value
    client._graph_id = graph_id

    return client

  @classmethod
  async def _determine_read_target(cls, graph_id: str) -> tuple[RouteTarget, str, str]:
    """
    Determine the best target for read operations with fallback logic.

    Returns:
        Tuple of (target, api_url, api_key)
    """

    # Check if replica ALB is enabled
    if env.SHARED_REPLICA_ALB_ENABLED and env.GRAPH_REPLICA_ALB_URL:
      # Check ALB health if health checks are enabled
      alb_is_healthy = True
      if env.GRAPH_HEALTH_CHECKS_ENABLED:
        alb_is_healthy = await cls._check_alb_health()

      if alb_is_healthy:
        logger.info(f"Routing {graph_id} READ to replica ALB")
        return (
          RouteTarget.SHARED_REPLICA_ALB,
          env.GRAPH_REPLICA_ALB_URL,
          env.GRAPH_API_KEY,
        )
      else:
        logger.warning(f"Replica ALB unhealthy for {graph_id}")

        # Check if fallback to master is allowed
        if env.ALLOW_SHARED_MASTER_READS:
          logger.warning(f"Falling back to shared master for {graph_id} READ")
          return (
            RouteTarget.SHARED_MASTER,
            await cls._get_shared_master_url(),
            env.GRAPH_API_KEY,
          )
        else:
          raise ServiceUnavailableError(
            f"Replica ALB unavailable and master reads disabled for {graph_id}"
          )

    # No replica ALB configured, check if master reads allowed
    elif env.ALLOW_SHARED_MASTER_READS:
      logger.info(f"Routing {graph_id} READ to shared master (no ALB configured)")
      return (
        RouteTarget.SHARED_MASTER,
        await cls._get_shared_master_url(),
        env.GRAPH_API_KEY,
      )

    else:
      raise ServiceUnavailableError(
        f"No read endpoint available for shared repository {graph_id}"
      )

  @classmethod
  @with_retry(max_attempts=3, base_delay=1.0)
  async def _get_shared_master_url(cls) -> str:
    """
    Get the shared master URL by discovering from DynamoDB.

    The shared master is deployed using the same kuzu-writers stack with tier="shared".
    It registers in DynamoDB with node_type="shared_master" and is auto-discoverable.

    Priority:
    1. Check Redis cache for recently discovered master
    2. Discover from DynamoDB by looking for node_type = shared_master (with pagination)
    3. Fallback to standard API URL if discovery fails
    """
    # Check circuit breaker if enabled
    if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
      if not await cls._master_circuit_breaker.should_attempt():
        logger.warning("Shared master circuit breaker is open, using fallback")
        if env.GRAPH_API_URL:
          return env.GRAPH_API_URL
        raise ServiceUnavailableError(
          "Shared master unavailable (circuit breaker open)"
        )

    try:
      # Check Redis cache first
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

      # Query DynamoDB for shared master instance with pagination support
      import boto3

      dynamodb = boto3.client("dynamodb", region_name=env.AWS_REGION or "us-east-1")

      # Scan instance registry for healthy shared master with pagination
      paginator = dynamodb.get_paginator("scan")

      # Configure pagination
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

      # Process pages
      for page in page_iterator:
        if page.get("Items"):
          # Use the first healthy shared master found
          item = page["Items"][0]
          private_ip = item.get("private_ip", {}).get("S")
          instance_id = item.get("instance_id", {}).get("S", "unknown")

          if private_ip:
            url = f"http://{private_ip}:8001"
            logger.info(f"Discovered shared master at {url} (instance: {instance_id})")

            # Cache the discovery
            if redis_client:
              try:
                await redis_client.setex(cache_key, 300, url)  # Cache for 5 minutes
              except Exception as e:
                logger.warning(f"Redis cache write failed: {e}")

            if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
              await cls._master_circuit_breaker.record_success()
            return url

      # No healthy shared master found - check if there's one marked unhealthy due to ingestion
      logger.warning(
        "No healthy shared master found in DynamoDB, checking for instances in ingestion state"
      )

      # Check Redis for any instances with active ingestion flags
      # These might be marked unhealthy but are actually processing
      if redis_client:
        try:
          # Scan for ingestion flags
          pattern = "kuzu:ingestion:active:*"
          async for key in redis_client.scan_iter(match=pattern, count=100):
            # Extract instance ID from key
            instance_id = (
              key.decode().split(":")[-1]
              if isinstance(key, bytes)
              else key.split(":")[-1]
            )

            # Check if this is the shared master instance
            response = dynamodb.get_item(
              TableName=env.INSTANCE_REGISTRY_TABLE,
              Key={"instance_id": {"S": instance_id}},
            )

            if response.get("Item"):
              item = response["Item"]
              node_type = item.get("node_type", {}).get("S")
              private_ip = item.get("private_ip", {}).get("S")

              if node_type == "shared_master" and private_ip:
                url = f"http://{private_ip}:8001"
                logger.warning(
                  f"Found shared master {instance_id} with active ingestion flag, "
                  f"using despite unhealthy status: {url}"
                )

                # Cache this discovery with shorter TTL
                await redis_client.setex(cache_key, 60, url)  # Only 1 minute cache

                if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
                  await cls._master_circuit_breaker.record_success()
                return url

        except Exception as redis_error:
          logger.warning(f"Could not check Redis for ingestion flags: {redis_error}")

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

    # Don't use localhost as fallback in production/staging
    # This should only be used in development
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
  async def _check_alb_health(cls) -> bool:
    """
    Check if the replica ALB is healthy.

    Uses Redis caching to avoid excessive health checks.
    Implements circuit breaker pattern for fault tolerance.
    """
    # Check circuit breaker first if enabled
    if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
      if not await cls._alb_circuit_breaker.should_attempt():
        logger.debug("ALB circuit breaker is open, returning unhealthy")
        return False

    cache_key = cls._get_cache_key("alb", "health")

    # Try Redis cache first
    redis_client = await cls._get_redis()
    if redis_client:
      try:
        cached = await redis_client.get(cache_key)
        if cached:
          health_data = json.loads(cached)
          age = time.time() - health_data["timestamp"]
          if age < cls._alb_health_cache_ttl:
            is_healthy = health_data["healthy"]
            if is_healthy and env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
              await cls._alb_circuit_breaker.record_success()
            return is_healthy
      except Exception as e:
        logger.warning(f"Redis cache read failed: {e}")

    # Perform health check with timeout
    alb_url = env.GRAPH_REPLICA_ALB_URL
    try:
      if not alb_url:
        return False

      # Get or create connection pool with HTTP/2
      if alb_url not in cls._connection_pools:
        cls._connection_pools[alb_url] = httpx.AsyncClient(
          timeout=httpx.Timeout(connect=cls._connect_timeout, read=cls._read_timeout),
          http2=True,  # Enable HTTP/2 for better performance
          limits=httpx.Limits(max_keepalive_connections=10),
        )
        cls._pool_stats[alb_url] = {
          "created_at": time.time(),
          "requests": 0,
          "failures": 0,
        }

      client = cls._connection_pools[alb_url]

      # Track pool statistics
      cls._pool_stats[alb_url]["requests"] += 1

      response = await client.get(f"{alb_url}/health")
      healthy = response.status_code == 200

      if healthy:
        if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
          await cls._alb_circuit_breaker.record_success()
        logger.debug(f"ALB health check successful for {alb_url}")
      else:
        if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
          await cls._alb_circuit_breaker.record_failure()
        cls._pool_stats[alb_url]["failures"] += 1
        logger.warning(
          f"ALB health check returned {response.status_code} for {alb_url}"
        )

      # Cache result
      health_data = {"healthy": healthy, "timestamp": time.time()}

      # Try Redis first, fall back to memory
      if redis_client:
        try:
          await redis_client.setex(
            cache_key, cls._alb_health_cache_ttl, json.dumps(health_data)
          )
        except Exception as e:
          logger.warning(f"Redis cache write failed: {e}")

      return healthy

    except Exception as e:
      if env.GRAPH_CIRCUIT_BREAKERS_ENABLED:
        await cls._alb_circuit_breaker.record_failure()
      cls._pool_stats.get(alb_url, {}).setdefault("failures", 0)
      cls._pool_stats.get(alb_url, {})["failures"] += 1

      logger.error(f"ALB health check failed: {e}", extra={"alb_url": alb_url})

      # Cache negative result
      health_data = {"healthy": False, "timestamp": time.time()}

      if redis_client:
        try:
          await redis_client.setex(
            cache_key, cls._alb_health_cache_ttl, json.dumps(health_data)
          )
        except Exception:
          pass

      return False

  @classmethod
  async def _create_user_graph_client(
    cls, graph_id: str, environment: str | None, tier: InstanceTier | None
  ) -> GraphClient:
    """
    Create client for user graph with tier-based routing.

    - Dev: Routes to single local graph instance
    - Prod/Staging: Uses allocation manager to find the appropriate instance
      based on the graph's tier:
      - Standard: Kuzu backend
      - Enterprise: Neo4j Community backend
      - Premium: Neo4j Enterprise backend
    - Subgraphs: Routes to parent's instance but uses subgraph database
    """

    # Check if this is a subgraph
    subgraph_info = parse_subgraph_id(graph_id)
    actual_graph_id = subgraph_info.parent_graph_id if subgraph_info else graph_id
    database_name = subgraph_info.database_name if subgraph_info else graph_id

    if subgraph_info:
      logger.info(
        f"Detected subgraph {graph_id} - routing via parent {actual_graph_id}, "
        f"database: {database_name}"
      )

    # In dev environment, route everything to the single graph instance
    if env.is_development():
      api_url = env.GRAPH_API_URL or "http://localhost:8001"
      api_key = env.GRAPH_API_KEY

      logger.info(
        f"Dev environment: Routing user graph {graph_id} to local graph at {api_url}"
      )

      client = GraphClient(base_url=api_url, api_key=api_key)
      client._route_target = RouteTarget.USER_GRAPH.value
      client._graph_id = graph_id
      client._database_name = database_name  # Actual database to use

      return client

    # Production/staging: Use allocation manager to find the right instance
    # Check cache first - use actual_graph_id for routing
    cache_key = cls._get_cache_key("location", actual_graph_id)
    redis_client = await cls._get_redis()

    db_location = None
    if redis_client:
      try:
        cached = await redis_client.get(cache_key)
        if cached:
          location_data = json.loads(cached)
          # Verify cache is still fresh (1 minute TTL)
          if time.time() - location_data.get("timestamp", 0) < cls._instance_cache_ttl:
            from robosystems.middleware.graph.allocation_manager import DatabaseLocation

            db_location = DatabaseLocation(**location_data["location"])
            logger.debug(f"Using cached location for {graph_id}")
      except Exception as e:
        logger.warning(f"Redis cache read failed for location: {e}")

    # If not cached, look it up - use actual_graph_id for routing
    if not db_location:
      allocation_manager = KuzuAllocationManager(
        environment=environment or env.ENVIRONMENT
      )
      db_location = await allocation_manager.find_database_location(actual_graph_id)

      if not db_location:
        # Database doesn't exist
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

      # Cache the location
      if redis_client:
        try:
          location_data = {
            "location": {
              "instance_id": db_location.instance_id,
              "private_ip": db_location.private_ip,
              "database_name": db_location.graph_id,
            },
            "timestamp": time.time(),
          }
          await redis_client.setex(
            cache_key, cls._instance_cache_ttl, json.dumps(location_data)
          )
        except Exception as e:
          logger.warning(f"Redis cache write failed for location: {e}")

    # Create client with the allocated instance's endpoint
    api_url = f"http://{db_location.private_ip}:8001"
    api_key = env.GRAPH_API_KEY

    logger.info(
      f"Routing user graph {graph_id} to instance "
      f"{db_location.instance_id} at {api_url}"
    )

    if not api_key:
      logger.error("GRAPH_API_KEY is not set in environment!")

    client = GraphClient(base_url=api_url, api_key=api_key)

    # Add metadata
    client._route_target = RouteTarget.USER_GRAPH.value
    client._graph_id = graph_id
    client._database_name = (
      database_name  # Actual database to use (important for subgraphs)
    )
    client._instance_id = db_location.instance_id

    return client

  @classmethod
  def create_client_sync(
    cls,
    graph_id: str,
    operation_type: str = "read",
    environment: str | None = None,
    tier: InstanceTier | None = None,
  ) -> GraphClient:
    """
    Synchronous wrapper for create_client.

    Args:
        graph_id: Graph database identifier
        operation_type: "read" or "write"
        environment: Environment (defaults to env.ENVIRONMENT)
        tier: Instance tier for user graphs

    Returns:
        Configured GraphClient instance
    """
    # Check if we're already in an async context
    try:
      asyncio.get_running_loop()
      # We're in an async context, can't use asyncio.run()
      # This is a design flaw - sync methods shouldn't be called from async context
      raise RuntimeError(
        "create_client_sync() cannot be called from an async context. "
        "Use 'await get_graph_client(graph_id)' or "
        "'await GraphClientFactory.create_client(graph_id)' instead. "
        "If you're in a Celery task, wrap with asyncio.run()."
      )
    except RuntimeError as e:
      if "no running event loop" in str(e).lower():
        # No running loop, we can use asyncio.run()
        return asyncio.run(
          cls.create_client(graph_id, operation_type, environment, tier)
        )
      else:
        # Re-raise the error about being in async context
        raise

  @classmethod
  def get_pool_statistics(cls) -> Dict[str, Any]:
    """
    Get connection pool statistics for monitoring.

    Returns:
        Dictionary with pool statistics including request counts,
        failure rates, and circuit breaker status.
    """
    stats = {
      "pools": {},
      "circuit_breakers": {
        "alb": {
          "is_open": cls._alb_circuit_breaker.is_open,
          "failure_count": cls._alb_circuit_breaker.failure_count,
          "last_failure": cls._alb_circuit_breaker.last_failure_time,
        },
        "master": {
          "is_open": cls._master_circuit_breaker.is_open,
          "failure_count": cls._master_circuit_breaker.failure_count,
          "last_failure": cls._master_circuit_breaker.last_failure_time,
        },
      },
      "total_pools": len(cls._connection_pools),
    }

    # Add pool-specific statistics
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
    """Clean up connection pools and Redis connection with proper error handling."""
    # Log final statistics before cleanup
    try:
      stats = cls.get_pool_statistics()
      logger.info(
        f"Cleaning up GraphClientFactory: {stats['total_pools']} pools active",
        extra={"pool_stats": stats},
      )
    except Exception as e:
      logger.warning(f"Error getting pool statistics during cleanup: {e}")

    # Close HTTP connection pools with error handling
    for url, client in cls._connection_pools.items():
      try:
        await client.aclose()
        logger.debug(f"Closed connection pool for {url}")
      except Exception as e:
        logger.warning(f"Error closing connection pool for {url}: {e}")

    cls._connection_pools.clear()
    cls._pool_stats.clear()

    # Close Redis connection pool with error handling
    with cls._redis_client_lock:
      if cls._redis_pool:
        try:
          await cls._redis_pool.disconnect()
          logger.debug("Closed Redis connection pool")
        except Exception as e:
          logger.warning(f"Error closing Redis connection pool: {e}")
        finally:
          cls._redis_pool = None

    # Reset circuit breakers
    cls._alb_circuit_breaker = CircuitBreaker(
      failure_threshold=env.GRAPH_CIRCUIT_BREAKER_THRESHOLD,
      timeout=env.GRAPH_CIRCUIT_BREAKER_TIMEOUT,
    )
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
  tier: InstanceTier | None = None,
) -> GraphClient:
  """
  Convenience function to get a properly routed graph database client.

  This is the preferred method for getting a graph client in async contexts.
  Routes to appropriate backend (Kuzu or Neo4j) based on tier.

  Args:
      graph_id: Graph database identifier
      operation_type: "read" or "write"
      environment: Environment (defaults to env.ENVIRONMENT)
      tier: Instance tier for user graphs (Standard/Enterprise/Premium)

  Returns:
      Configured GraphClient instance (works with all backends via Graph API)

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
  tier: InstanceTier | None = None,
) -> GraphClient:
  """
  Convenience function to get a properly routed graph database client (sync version).

  This is the preferred method for getting a graph client in sync contexts.
  Routes to appropriate backend (Kuzu or Neo4j) based on tier.

  Args:
      graph_id: Graph database identifier
      operation_type: "read" or "write"
      environment: Environment (defaults to env.ENVIRONMENT)
      tier: Instance tier for user graphs (Standard/Enterprise/Premium)

  Returns:
      Configured GraphClient instance (works with all backends via Graph API)

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
  """
  Get a graph database client for direct instance access.

  This bypasses all routing and connects directly to a specific instance.
  Used for allocation operations where we need to target a specific instance.
  Works with both Kuzu and Neo4j backends via Graph API.

  Args:
      instance_ip: Private IP address of the graph database instance
      api_key: API key (defaults to env.GRAPH_API_KEY)

  Returns:
      Configured GraphClient instance for direct access (works with all backends)

  Example:
      client = await get_graph_client_for_instance("10.0.1.123")
      await client.create_database("entity_456")
  """
  if api_key is None:
    api_key = env.GRAPH_API_KEY

  api_url = f"http://{instance_ip}:8001"
  logger.info(f"Creating direct graph client for instance at {api_url}")

  return GraphClient(base_url=api_url, api_key=api_key)


# Special factory method for SEC ingestion
async def get_graph_client_for_sec_ingestion() -> GraphClient:
  """
  Get a graph database client specifically for SEC data ingestion.

  CRITICAL: SEC ingestion MUST always go to the shared master instance.
  This bypasses normal routing logic to ensure data is loaded to the
  correct instance that will be snapshotted for replicas.

  Returns:
      GraphClient configured for shared master
  """
  logger.info("Creating graph client for SEC ingestion (direct to shared master)")

  # In dev, use the single local graph instance
  if env.is_development():
    api_url = env.GRAPH_API_URL or "http://localhost:8001"
    logger.info(f"Dev environment: SEC ingestion to local graph at {api_url}")
  else:
    # In prod/staging, discover shared master from DynamoDB
    try:
      api_url = await GraphClientFactory._get_shared_master_url()
      logger.info(f"Discovered shared master for SEC ingestion: {api_url}")
    except Exception as e:
      # Fallback during migration or if discovery fails
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
