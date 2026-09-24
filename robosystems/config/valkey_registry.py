"""Single source of truth for Valkey/Redis database-number allocation.

Never hardcode a database number anywhere else — two subsystems that pick the
same integer share a keyspace and silently clobber each other. Always select a
member of :class:`ValkeyDatabase`, and get a client from
:func:`create_redis_client` / :func:`create_async_redis_client` so auth and TLS
are configured for the environment.

To add a connection: call :meth:`ValkeyDatabase.get_next_available` for a free
slot, add a member here, and describe it in :func:`get_database_purpose`.
"""

import logging
import os
from enum import IntEnum
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml

logger = logging.getLogger(__name__)


class ValkeyDatabase(IntEnum):
  """Every Valkey/Redis database allocation, one member per purpose.

  Redis exposes databases 0-15; the unused tail is free for new subsystems.
  """

  AUTH = 0  # JWT tokens, API key cache, sessions
  RATE_LIMITS = 1  # Burst protection, download limits
  GRAPH_ROUTING = 2  # Graph client factory (URLs, health)
  SSE = 3  # Real-time event pub/sub and task state tracking
  LOCKS = 4  # Distributed locks (SSO, materialize)
  MCP_CACHE = 5  # MCP tool result cache (schema, info, report models)
  WORKER_QUEUE = 6  # Background task queue (BRPOP consumer)
  OPERATION_IDEMPOTENCY = 7  # Extensions operation idempotency envelope cache

  @classmethod
  def get_next_available(cls) -> int:
    """Get the lowest unallocated database number, or raise if 0-15 are full."""
    used_numbers = {db.value for db in cls}
    for i in range(16):
      if i not in used_numbers:
        return i
    raise ValueError("No database slots available (all 0-15 are allocated)")

  @classmethod
  def get_url(cls, database: "ValkeyDatabase", base_url: str) -> str:
    """Build the URL for a database against an explicit base URL."""
    return ValkeyURLBuilder.build_url(base_url, database)


class ValkeyURLBuilder:
  """Builds Valkey/Redis URLs with database numbers, auth and TLS."""

  _cached_base_url: str | None = None
  _cache_environment: str | None = None
  _cached_auth_token: str | None = None
  _auth_token_environment: str | None = None

  @staticmethod
  def _get_valkey_url_from_cloudformation() -> str | None:
    """Get the Valkey URL from CloudFormation stack outputs, or None."""
    try:
      import boto3

      config_paths = [
        Path("/app/configs/stacks.yml"),
        Path(__file__).parent.parent.parent / ".github/configs/stacks.yml",
      ]

      stack_name = None
      for config_path in config_paths:
        if config_path.exists():
          try:
            with open(config_path) as f:
              config = yaml.safe_load(f)
              env_key = os.getenv("ENVIRONMENT", "dev").lower()
              if env_key in config and "valkey" in config[env_key]:
                stack_name = config[env_key]["valkey"].get("stack_name")
                break
          except Exception as e:
            logger.debug(f"Failed to load stack config from {config_path}: {e}")

      if not stack_name:
        return None

      region = os.getenv("AWS_REGION", "us-east-1")
      cf_client = boto3.client("cloudformation", region_name=region)
      response = cf_client.describe_stacks(StackName=stack_name)

      if "Stacks" in response and len(response["Stacks"]) > 0:
        stack = response["Stacks"][0]
        if "Outputs" in stack:
          for output in stack["Outputs"]:
            if output.get("OutputKey") == "ValkeyUrl":
              return output.get("OutputValue")

      return None

    except (ImportError, Exception) as e:
      logger.debug(f"Could not fetch Valkey URL from CloudFormation: {e}")
      return None

  @staticmethod
  def get_base_url() -> str:
    """Base URL, cached per environment: CloudFormation output (prod/staging),
    then ``VALKEY_URL``, then localhost.
    """
    current_env = os.getenv("ENVIRONMENT", "dev").lower()

    if (
      ValkeyURLBuilder._cached_base_url
      and ValkeyURLBuilder._cache_environment == current_env
    ):
      return ValkeyURLBuilder._cached_base_url

    if current_env in ["prod", "staging"]:
      url = ValkeyURLBuilder._get_valkey_url_from_cloudformation()
      if url:
        ValkeyURLBuilder._cached_base_url = url
        ValkeyURLBuilder._cache_environment = current_env
        return url

    url = os.getenv("VALKEY_URL", "redis://localhost:6379")
    ValkeyURLBuilder._cached_base_url = url
    ValkeyURLBuilder._cache_environment = current_env
    return url

  @staticmethod
  def get_auth_token() -> str | None:
    """Auth token, cached per environment: Secrets Manager (prod/staging), then
    ``VALKEY_AUTH_TOKEN``, else None (unauthenticated).
    """
    current_env = os.getenv("ENVIRONMENT", "dev").lower()

    if (
      ValkeyURLBuilder._cached_auth_token is not None
      and ValkeyURLBuilder._auth_token_environment == current_env
    ):
      return ValkeyURLBuilder._cached_auth_token

    if current_env in ["prod", "staging"]:
      try:
        from robosystems.config.secrets_manager import get_secret_value

        token = get_secret_value("VALKEY_AUTH_TOKEN", "")
        if token:
          ValkeyURLBuilder._cached_auth_token = token
          ValkeyURLBuilder._auth_token_environment = current_env
          return token
      except (ImportError, Exception):
        pass

    token = os.getenv("VALKEY_AUTH_TOKEN", "")
    if token:
      ValkeyURLBuilder._cached_auth_token = token
      ValkeyURLBuilder._auth_token_environment = current_env
      return token

    ValkeyURLBuilder._cached_auth_token = None
    ValkeyURLBuilder._auth_token_environment = current_env
    return None

  @staticmethod
  def build_url(
    base_url: str | None = None,
    database: ValkeyDatabase = ValkeyDatabase.AUTH,
    use_valkey_prefix: bool = False,
    auth_token: str | None = None,
    use_tls: bool | None = None,
    include_ssl_params: bool = True,
  ) -> str:
    """Build a Valkey/Redis URL for ``database``.

    A None ``base_url`` auto-discovers via :meth:`get_base_url`. A None
    ``use_tls`` enables TLS only with an auth token in prod/staging. Any
    database number or credentials already on ``base_url`` are replaced.
    """
    if base_url is None:
      base_url = ValkeyURLBuilder.get_base_url()

    if use_tls is None:
      environment = os.getenv("ENVIRONMENT", "dev").lower()
      use_tls = auth_token is not None and environment in ["prod", "staging"]

    base_url = base_url.rstrip("/")

    if "/" in base_url.split("://")[-1]:
      base_url = base_url.rsplit("/", 1)[0]

    if "://" in base_url:
      protocol, host_part = base_url.split("://", 1)

      if "@" in host_part:
        host_part = host_part.split("@")[-1]

      if use_tls:
        protocol = "rediss"
      elif use_valkey_prefix:
        protocol = "valkey"
      else:
        protocol = "redis"

      if auth_token:
        encoded_token = quote(auth_token, safe="")
        base_url = f"{protocol}://default:{encoded_token}@{host_part}"
      else:
        base_url = f"{protocol}://{host_part}"
    else:
      if use_tls:
        prefix = "rediss://"
      elif use_valkey_prefix:
        prefix = "valkey://"
      else:
        prefix = "redis://"

      if auth_token:
        encoded_token = quote(auth_token, safe="")
        base_url = f"{prefix}default:{encoded_token}@{base_url}"
      else:
        base_url = f"{prefix}{base_url}"

    url = f"{base_url}/{database.value}"

    if use_tls and include_ssl_params:
      url += "?ssl_cert_reqs=CERT_NONE"

    return url

  @staticmethod
  def build_authenticated_url(
    database: ValkeyDatabase = ValkeyDatabase.AUTH,
    base_url: str | None = None,
    include_ssl_params: bool = True,
  ) -> str:
    """:meth:`build_url` with the environment's auth token."""
    auth_token = ValkeyURLBuilder.get_auth_token()
    return ValkeyURLBuilder.build_url(
      base_url=base_url,
      database=database,
      auth_token=auth_token,
      include_ssl_params=include_ssl_params,
    )

  @staticmethod
  def parse_url(url: str) -> tuple[str, int | None]:
    """Split a URL into ``(base_url, database_number or None)``."""
    if "/" in url.split("://")[-1]:
      base_url, db_part = url.rsplit("/", 1)
      try:
        db_num = int(db_part.split("?")[0])
        return base_url, db_num
      except ValueError:
        return url, None
    return url, None


def get_database_purpose(database: ValkeyDatabase) -> str:
  """Human-readable purpose of a database."""
  descriptions = {
    ValkeyDatabase.AUTH: "JWT tokens, API key cache, and sessions",
    ValkeyDatabase.RATE_LIMITS: "Burst protection and download rate limits",
    ValkeyDatabase.SSE: "Real-time event pub/sub for SSE streams and task state tracking",
    ValkeyDatabase.LOCKS: "Distributed locks for SSO and materialize coordination",
    ValkeyDatabase.GRAPH_ROUTING: "Graph client factory routing (URLs, health, discovery)",
    ValkeyDatabase.MCP_CACHE: (
      "MCP tool result cache (schema, info, information-block report models — "
      "compressed, ~1 MB per 10-K, 6h/5min TTL; allkeys-lru bounds it)"
    ),
    ValkeyDatabase.WORKER_QUEUE: "Background task queue (BRPOP consumer)",
    ValkeyDatabase.OPERATION_IDEMPOTENCY: (
      "Extensions operation idempotency envelope cache (24h TTL)"
    ),
  }

  return descriptions.get(
    database, f"Reserved for future use (database {database.value})"
  )


def print_database_registry():
  """Print the current database registry for documentation."""
  print("=" * 70)
  print("VALKEY/REDIS DATABASE REGISTRY")
  print("=" * 70)
  print()

  for db in ValkeyDatabase:
    purpose = get_database_purpose(db)
    status = "✓ IN USE"
    print(f"DB {db.value:2d} [{status}]: {db.name:20s} - {purpose}")

  print()
  print("=" * 70)
  print("USAGE EXAMPLE:")
  print("=" * 70)
  print("""
from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
import redis.asyncio as redis

# RECOMMENDED: Use factory methods that handle SSL correctly
# For async operations:
redis_client = create_async_redis_client(ValkeyDatabase.AUTH, decode_responses=True)

# MANUAL: Build URL with explicit auth token
auth_token = ValkeyURLBuilder.get_auth_token()  # Gets from Secrets Manager in prod
manual_url = ValkeyURLBuilder.build_url(
    database=ValkeyDatabase.AUTH,
    auth_token=auth_token
)

# DEV: build a URL without authentication (development only)
dev_url = ValkeyURLBuilder.build_url(database=ValkeyDatabase.AUTH)
""")


# =============================================================================
# Redis Client Creation Utilities
# =============================================================================


def get_redis_connection_params(environment: str | None = None) -> dict[str, Any]:
  """Get Redis client parameters for an environment.

  Adds the ElastiCache-specific SSL/TLS settings in prod/staging.
  """
  import redis.exceptions as redis_exceptions

  if environment is None:
    environment = os.getenv("ENVIRONMENT", "dev").lower()

  params: dict[str, Any] = {
    "decode_responses": True,
    "socket_connect_timeout": 5,
    "socket_timeout": 5,
    "retry_on_timeout": True,
    "retry_on_error": [
      redis_exceptions.ConnectionError,
      redis_exceptions.TimeoutError,
    ],
    "health_check_interval": 30,
  }

  if environment in ["prod", "staging"]:
    # ElastiCache TLS certificates cannot be validated against a CA; accepted
    # because traffic stays in the VPC behind security groups. redis-py async
    # takes no ssl_context, and wants lowercase 'none'.
    params["ssl_cert_reqs"] = "none"
    params["ssl_check_hostname"] = False

  return params


def create_redis_client(
  database: ValkeyDatabase, decode_responses: bool = True, **kwargs
) -> Any:  # Returns redis.Redis but avoid import here
  """Create a Redis client with the environment's auth and TLS."""
  import redis

  # SSL goes in connection params, not the query string.
  url = ValkeyURLBuilder.build_authenticated_url(database, include_ssl_params=False)

  params = get_redis_connection_params()
  params["decode_responses"] = decode_responses

  params.update(kwargs)

  return redis.Redis.from_url(url, **params)


def create_async_redis_client(
  database: ValkeyDatabase, decode_responses: bool = True, **kwargs
) -> Any:  # Returns redis.asyncio.Redis but avoid import here
  """Async counterpart of :func:`create_redis_client`."""
  import redis.asyncio as redis_async

  url = ValkeyURLBuilder.build_authenticated_url(database, include_ssl_params=False)

  params = get_redis_connection_params()
  params["decode_responses"] = decode_responses

  params.update(kwargs)

  return redis_async.from_url(url, **params)


def create_redis_client_from_url(
  url: str, decode_responses: bool = True, **kwargs
) -> Any:  # Returns redis.Redis
  """Create a Redis client from a pre-built URL (e.g. one read from env).

  Applies the same ElastiCache connection parameters as
  :func:`create_redis_client`.
  """
  import redis

  params = get_redis_connection_params()
  params["decode_responses"] = decode_responses

  params.update(kwargs)

  return redis.from_url(url, **params)
