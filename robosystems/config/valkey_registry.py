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

  # =========================================================================
  # APPLICATION DATABASES (0-4, ordered by system criticality)
  # =========================================================================
  AUTH = 0  # JWT tokens, API key cache, sessions
  RATE_LIMITS = 1  # Burst protection, download limits
  GRAPH_ROUTING = 2  # Graph client factory (URLs, health)
  SSE = 3  # Real-time event pub/sub and task state tracking
  LOCKS = 4  # Distributed locks (SSO, materialize)
  MCP_CACHE = 5  # MCP tool result cache (schema, info)
  WORKER_QUEUE = 6  # Background task queue (BRPOP consumer)
  OPERATION_IDEMPOTENCY = 7  # Extensions operation idempotency envelope cache

  @classmethod
  def get_next_available(cls) -> int:
    """Get the lowest unallocated database number, or raise if 0-15 are full."""
    used_numbers = {db.value for db in cls}
    # Redis supports databases 0-15
    for i in range(16):
      if i not in used_numbers:
        return i
    raise ValueError("No database slots available (all 0-15 are allocated)")

  @classmethod
  def get_url(cls, database: "ValkeyDatabase", base_url: str) -> str:
    """Build the URL for a database against an explicit base URL."""
    return ValkeyURLBuilder.build_url(base_url, database)


class ValkeyURLBuilder:
  """Helper class to build Valkey/Redis URLs with proper database numbers."""

  # Cache for the base Valkey URL and auth token
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

      # Fetch from CloudFormation
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
    """
    Get the base Valkey URL for the current environment.

    Resolution order, cached per environment:

    1. prod/staging: CloudFormation stack outputs
    2. the ``VALKEY_URL`` environment variable
    3. ``redis://localhost:6379``
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

    # Fall back to environment variable
    url = os.getenv("VALKEY_URL", "redis://localhost:6379")
    ValkeyURLBuilder._cached_base_url = url
    ValkeyURLBuilder._cache_environment = current_env
    return url

  @staticmethod
  def get_auth_token() -> str | None:
    """
    Get the Valkey auth token for the current environment.

    Resolution order, cached per environment:

    1. prod/staging: AWS Secrets Manager
    2. the ``VALKEY_AUTH_TOKEN`` environment variable
    3. None, meaning unauthenticated
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
        # Secrets Manager not available or error occurred, fall back to env var
        pass

    # Fall back to environment variable
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
    """
    Build a complete Valkey/Redis URL with the specified database.

    A None ``base_url`` auto-discovers via :meth:`get_base_url`. A None
    ``use_tls`` turns TLS on only when an auth token is present in
    prod/staging. Any database number or credentials already on ``base_url``
    are stripped and replaced.

    Examples:
        >>> # Auto-discover base URL (recommended for prod/staging)
        >>> ValkeyURLBuilder.build_url(database=ValkeyDatabase.AUTH)
        'redis://valkey.us-east-1.cache.amazonaws.com:6379/0'

        >>> # With authentication (production)
        >>> ValkeyURLBuilder.build_url(
        ...     database=ValkeyDatabase.AUTH,
        ...     auth_token="secret_token_here"
        ... )
        'rediss://default:secret_token_here@valkey.us-east-1.cache.amazonaws.com:6379/0?ssl_cert_reqs=CERT_NONE'

        >>> # Explicit base URL (for testing or dev)
        >>> ValkeyURLBuilder.build_url("redis://localhost:6379", ValkeyDatabase.AUTH)
        'redis://localhost:6379/0'
    """
    if base_url is None:
      base_url = ValkeyURLBuilder.get_base_url()

    # Auto-detect TLS if not specified - only use TLS in prod/staging with auth
    if use_tls is None:
      environment = os.getenv("ENVIRONMENT", "dev").lower()
      use_tls = auth_token is not None and environment in ["prod", "staging"]

    base_url = base_url.rstrip("/")

    # Remove any existing database number
    if "/" in base_url.split("://")[-1]:
      # Has a database number already, remove it
      base_url = base_url.rsplit("/", 1)[0]

    # Parse the URL to handle authentication injection
    if "://" in base_url:
      protocol, host_part = base_url.split("://", 1)

      # Handle existing authentication in URL
      if "@" in host_part:
        # URL already has auth, strip it to avoid conflicts
        host_part = host_part.split("@")[-1]

      # Determine protocol
      if use_tls:
        protocol = "rediss"
      elif use_valkey_prefix:
        protocol = "valkey"
      else:
        protocol = "redis"

      # Build the URL with optional authentication
      if auth_token:
        # Use 'default' as username for Redis/Valkey AUTH
        # URL-encode the auth token to handle special characters
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
        # URL-encode the auth token to handle special characters
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
    """
    Build a Valkey URL with auto-detected authentication.

    Looks up the environment's auth token, then picks ``rediss://`` when one
    exists and ``redis://`` when it does not.

    Examples:
        >>> # Production (with auth)
        >>> ValkeyURLBuilder.build_authenticated_url(ValkeyDatabase.AUTH)
        'rediss://default:secret_token@valkey.us-east-1.cache.amazonaws.com:6379/0?ssl_cert_reqs=CERT_NONE'

        >>> # Development (no auth)
        >>> ValkeyURLBuilder.build_authenticated_url(ValkeyDatabase.AUTH)
        'redis://localhost:6379/0'
    """
    auth_token = ValkeyURLBuilder.get_auth_token()
    return ValkeyURLBuilder.build_url(
      base_url=base_url,
      database=database,
      auth_token=auth_token,
      include_ssl_params=include_ssl_params,
    )

  @staticmethod
  def parse_url(url: str) -> tuple[str, int | None]:
    """
    Split a Valkey/Redis URL into ``(base_url, database_number)``.

    The database number is None when the URL carries none.

    Example:
        >>> ValkeyURLBuilder.parse_url("redis://localhost:6379/2")
        ('redis://localhost:6379', 2)
    """
    if "/" in url.split("://")[-1]:
      base_url, db_part = url.rsplit("/", 1)
      try:
        db_num = int(db_part.split("?")[0])  # Handle query params
        return base_url, db_num
      except ValueError:
        return url, None
    return url, None


# =============================================================================
# USAGE DOCUMENTATION
# =============================================================================


def get_database_purpose(database: ValkeyDatabase) -> str:
  """Get a human-readable description of what a database is used for."""
  descriptions = {
    ValkeyDatabase.AUTH: "JWT tokens, API key cache, and sessions",
    ValkeyDatabase.RATE_LIMITS: "Burst protection and download rate limits",
    ValkeyDatabase.SSE: "Real-time event pub/sub for SSE streams and task state tracking",
    ValkeyDatabase.LOCKS: "Distributed locks for SSO and materialize coordination",
    ValkeyDatabase.GRAPH_ROUTING: "Graph client factory routing (URLs, health, discovery)",
    ValkeyDatabase.MCP_CACHE: "MCP tool result cache (schema, info)",
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

# LEGACY: Build URL without authentication (development only)
legacy_url = ValkeyURLBuilder.build_url(database=ValkeyDatabase.AUTH)
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
    "socket_connect_timeout": 5,  # 5 second connection timeout
    "socket_timeout": 5,  # 5 second operation timeout
    "retry_on_timeout": True,
    "retry_on_error": [
      redis_exceptions.ConnectionError,
      redis_exceptions.TimeoutError,
    ],
    "health_check_interval": 30,  # Health check every 30 seconds
  }

  # In production/staging with ElastiCache TLS
  if environment in ["prod", "staging"]:
    # SECURITY NOTE: ElastiCache uses self-signed certificates that cannot be validated
    # against a CA. This is AWS's design for ElastiCache. The connection is still
    # encrypted with TLS, but we cannot verify the certificate authenticity.
    # This is acceptable because:
    # 1. Connection is within AWS VPC (not over public internet)
    # 2. ElastiCache endpoint DNS is managed by AWS
    # 3. Network security groups restrict access

    # Use individual SSL parameters (redis-py async doesn't support ssl_context)
    # Note: ssl_cert_reqs must be lowercase 'none' for redis-py (not ssl.CERT_NONE)
    params["ssl_cert_reqs"] = "none"
    params["ssl_check_hostname"] = False

  return params


def create_redis_client(
  database: ValkeyDatabase, decode_responses: bool = True, **kwargs
) -> Any:  # Returns redis.Redis but avoid import here
  """Create a Redis client, wiring up auth and TLS for the environment.

  Prefer this over building a client by hand — it is what keeps database
  numbers, credentials, and ElastiCache TLS consistent.

  Example:
      >>> from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
      >>> client = create_redis_client(ValkeyDatabase.AUTH)
      >>> client.set("key", "value")
  """
  import redis

  # Build authenticated URL WITHOUT SSL params in query string
  # (SSL params will be passed as connection parameters instead)
  url = ValkeyURLBuilder.build_authenticated_url(database, include_ssl_params=False)

  params = get_redis_connection_params()
  params["decode_responses"] = decode_responses

  params.update(kwargs)

  return redis.Redis.from_url(url, **params)


def create_async_redis_client(
  database: ValkeyDatabase, decode_responses: bool = True, **kwargs
) -> Any:  # Returns redis.asyncio.Redis but avoid import here
  """Create an async Redis client, wiring up auth and TLS for the environment.

  Example:
      >>> from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
      >>> client = create_async_redis_client(ValkeyDatabase.AUTH)
      >>> await client.set("key", "value")
  """
  import redis.asyncio as redis_async

  # Build authenticated URL WITHOUT SSL params in query string
  # (SSL params will be passed as connection parameters instead)
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
