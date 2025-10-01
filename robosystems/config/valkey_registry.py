"""
Centralized Valkey/Redis Database Number Registry.

This module provides a single source of truth for all Valkey/Redis database
allocations to prevent conflicts and make it easy to track usage.

IMPORTANT: When adding a new Redis connection, always check this registry first
and use the next available database number.
"""

import os
import ssl
from enum import IntEnum
from typing import Any, Dict, Optional
from urllib.parse import quote


class ValkeyDatabase(IntEnum):
  """
  Enumeration of all Valkey/Redis database allocations.

  Each database is dedicated to a specific purpose to ensure proper
  isolation and prevent key collisions.

  Current allocation:
  - 0-1: Celery task queue system
  - 2-8: Application services (auth, SSE, locks, pipelines, credits, rate limiting, kuzu)
  """

  # =========================================================================
  # CELERY DATABASES (0-1)
  # =========================================================================
  CELERY_BROKER = 0  # Celery task queue (messages)
  CELERY_RESULTS = 1  # Celery task results storage

  # =========================================================================
  # APPLICATION DATABASES (2-8)
  # =========================================================================
  AUTH_CACHE = 2  # Authentication tokens, sessions, API keys
  SSE_EVENTS = 3  # Server-Sent Events pub/sub and queue
  DISTRIBUTED_LOCKS = 4  # Distributed locks for coordination
  PIPELINE_TRACKING = 5  # SEC/data pipeline progress tracking
  CREDITS_CACHE = 6  # Credit balance and transaction caching
  RATE_LIMITING = 7  # Rate limiting counters and windows
  KUZU_CACHE = 8  # Kuzu client factory caching (URLs, health, locations)

  @classmethod
  def get_next_available(cls) -> int:
    """
    Get the next available database number.

    Returns:
        The next available database number (8-15)

    Raises:
        ValueError: If no database slots are available
    """
    used_numbers = {db.value for db in cls}
    # Redis supports databases 0-15
    for i in range(16):
      if i not in used_numbers:
        return i
    raise ValueError("No database slots available (all 0-15 are allocated)")

  @classmethod
  def get_url(cls, database: "ValkeyDatabase", base_url: str) -> str:
    """
    Convenience method to get URL for a specific database.

    Args:
        database: The database enum value
        base_url: Base URL (e.g., "redis://localhost:6379")

    Returns:
        Complete URL with database number
    """
    return ValkeyURLBuilder.build_url(base_url, database)


class ValkeyURLBuilder:
  """Helper class to build Valkey/Redis URLs with proper database numbers."""

  # Cache for the base Valkey URL and auth token
  _cached_base_url: Optional[str] = None
  _cache_environment: Optional[str] = None
  _cached_auth_token: Optional[str] = None
  _auth_token_environment: Optional[str] = None

  @staticmethod
  def get_base_url() -> str:
    """
    Get the base Valkey URL for the current environment.

    This method:
    1. In prod/staging: Fetches from CloudFormation (with caching)
    2. In dev: Uses VALKEY_URL environment variable
    3. Falls back to localhost if nothing is configured

    Returns:
        Base Valkey URL (e.g., "redis://valkey.us-east-1.cache.amazonaws.com:6379")
    """
    current_env = os.getenv("ENVIRONMENT", "dev").lower()

    # Check if we have a cached URL for this environment
    if (
      ValkeyURLBuilder._cached_base_url
      and ValkeyURLBuilder._cache_environment == current_env
    ):
      return ValkeyURLBuilder._cached_base_url

    # Try to get from CloudFormation for prod/staging
    if current_env in ["prod", "staging"]:
      try:
        from robosystems.config.aws import get_valkey_url_from_cloudformation

        url = get_valkey_url_from_cloudformation()
        if url:
          ValkeyURLBuilder._cached_base_url = url
          ValkeyURLBuilder._cache_environment = current_env
          return url
      except ImportError:
        # CloudFormation config not available, fall back to env var
        pass

    # Fall back to environment variable
    url = os.getenv("VALKEY_URL", "redis://localhost:6379")
    ValkeyURLBuilder._cached_base_url = url
    ValkeyURLBuilder._cache_environment = current_env
    return url

  @staticmethod
  def get_auth_token() -> Optional[str]:
    """
    Get the Valkey auth token for the current environment.

    This method:
    1. In prod/staging: Fetches from AWS Secrets Manager (with caching)
    2. In dev: Uses VALKEY_AUTH_TOKEN environment variable
    3. Returns None if no auth token is configured

    Returns:
        Auth token string or None if not configured
    """
    current_env = os.getenv("ENVIRONMENT", "dev").lower()

    # Check if we have a cached token for this environment
    if (
      ValkeyURLBuilder._cached_auth_token is not None
      and ValkeyURLBuilder._auth_token_environment == current_env
    ):
      return ValkeyURLBuilder._cached_auth_token

    # Try to get from Secrets Manager for prod/staging
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

    # No auth token configured
    ValkeyURLBuilder._cached_auth_token = None
    ValkeyURLBuilder._auth_token_environment = current_env
    return None

  @staticmethod
  def build_url(
    base_url: Optional[str] = None,
    database: ValkeyDatabase = ValkeyDatabase.CELERY_BROKER,
    use_valkey_prefix: bool = False,
    auth_token: Optional[str] = None,
    use_tls: Optional[bool] = None,
    include_ssl_params: bool = True,
  ) -> str:
    """
    Build a complete Valkey/Redis URL with the specified database.

    Args:
        base_url: Base Redis URL. If None, will auto-discover from CloudFormation or env vars
        database: Database number from ValkeyDatabase enum
        use_valkey_prefix: If True, use 'valkey://' prefix instead of 'redis://'
        auth_token: Optional auth token for authenticated connections
        use_tls: If True, use TLS (rediss://). If None, auto-detect based on auth_token
        include_ssl_params: If True, add SSL parameters to URL for rediss:// connections

    Returns:
        Complete URL with database number (e.g., "redis://localhost:6379/2")

    Examples:
        >>> # Auto-discover base URL (recommended for prod/staging)
        >>> ValkeyURLBuilder.build_url(database=ValkeyDatabase.AUTH_CACHE)
        'redis://valkey.us-east-1.cache.amazonaws.com:6379/2'

        >>> # With authentication (production)
        >>> ValkeyURLBuilder.build_url(
        ...     database=ValkeyDatabase.AUTH_CACHE,
        ...     auth_token="secret_token_here"
        ... )
        'rediss://default:secret_token_here@valkey.us-east-1.cache.amazonaws.com:6379/2?ssl_cert_reqs=CERT_NONE'

        >>> # Explicit base URL (for testing or dev)
        >>> ValkeyURLBuilder.build_url("redis://localhost:6379", ValkeyDatabase.AUTH_CACHE)
        'redis://localhost:6379/2'
    """
    # If no base URL provided, auto-discover it
    if base_url is None:
      base_url = ValkeyURLBuilder.get_base_url()

    # Auto-detect TLS if not specified - only use TLS in prod/staging with auth
    if use_tls is None:
      environment = os.getenv("ENVIRONMENT", "dev").lower()
      use_tls = auth_token is not None and environment in ["prod", "staging"]

    # Remove trailing slash if present
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
      # Add protocol if missing
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

    # Add database number
    url = f"{base_url}/{database.value}"

    # Add SSL parameters for rediss:// URLs (required by Celery/Kombu)
    if use_tls and include_ssl_params:
      url += "?ssl_cert_reqs=CERT_NONE"

    return url

  @staticmethod
  def build_authenticated_url(
    database: ValkeyDatabase = ValkeyDatabase.CELERY_BROKER,
    base_url: Optional[str] = None,
    include_ssl_params: bool = True,
  ) -> str:
    """
    Build a Valkey URL with auto-detected authentication for the current environment.

    This is a convenience method that automatically:
    1. Detects if auth token is available (prod/staging)
    2. Uses appropriate protocol (rediss:// for auth, redis:// otherwise)
    3. Includes auth token if available
    4. Adds SSL parameters for rediss:// connections

    Args:
        database: Database number from ValkeyDatabase enum
        base_url: Base Redis URL. If None, will auto-discover from CloudFormation or env vars
        include_ssl_params: If True, add SSL parameters to URL for rediss:// connections

    Returns:
        Complete URL with authentication if available

    Examples:
        >>> # Production (with auth)
        >>> ValkeyURLBuilder.build_authenticated_url(ValkeyDatabase.AUTH_CACHE)
        'rediss://default:secret_token@valkey.us-east-1.cache.amazonaws.com:6379/2?ssl_cert_reqs=CERT_NONE'

        >>> # Development (no auth)
        >>> ValkeyURLBuilder.build_authenticated_url(ValkeyDatabase.AUTH_CACHE)
        'redis://localhost:6379/2'
    """
    auth_token = ValkeyURLBuilder.get_auth_token()
    return ValkeyURLBuilder.build_url(
      base_url=base_url,
      database=database,
      auth_token=auth_token,
      include_ssl_params=include_ssl_params,
    )

  @staticmethod
  def parse_url(url: str) -> tuple[str, Optional[int]]:
    """
    Parse a Valkey/Redis URL to extract base URL and database number.

    Args:
        url: Full Redis/Valkey URL

    Returns:
        Tuple of (base_url, database_number)

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
  """
  Get a human-readable description of what a database is used for.

  Args:
      database: Database number from ValkeyDatabase enum

  Returns:
      Description of the database's purpose
  """
  descriptions = {
    ValkeyDatabase.CELERY_BROKER: "Celery task queue for async job processing",
    ValkeyDatabase.CELERY_RESULTS: "Storage for Celery task results and status",
    ValkeyDatabase.AUTH_CACHE: "Authentication tokens, sessions, and API key cache",
    ValkeyDatabase.SSE_EVENTS: "Server-Sent Events for real-time updates",
    ValkeyDatabase.DISTRIBUTED_LOCKS: "Distributed locks for multi-instance coordination",
    ValkeyDatabase.PIPELINE_TRACKING: "SEC data pipeline progress and status tracking",
    ValkeyDatabase.CREDITS_CACHE: "Credit balance and transaction caching",
    ValkeyDatabase.RATE_LIMITING: "API rate limiting counters and time windows",
    ValkeyDatabase.KUZU_CACHE: "Kuzu client factory caching for URLs, health, and instance locations",
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
redis_client = create_async_redis_client(ValkeyDatabase.AUTH_CACHE, decode_responses=True)

# MANUAL: Build URL with explicit auth token
auth_token = ValkeyURLBuilder.get_auth_token()  # Gets from Secrets Manager in prod
manual_url = ValkeyURLBuilder.build_url(
    database=ValkeyDatabase.AUTH_CACHE,
    auth_token=auth_token
)

# LEGACY: Build URL without authentication (development only)
legacy_url = ValkeyURLBuilder.build_url(database=ValkeyDatabase.AUTH_CACHE)
""")


# =============================================================================
# Redis Client Creation Utilities
# =============================================================================


def get_redis_connection_params(environment: Optional[str] = None) -> Dict[str, Any]:
  """
  Get Redis connection parameters based on environment.

  This handles ElastiCache-specific SSL/TLS configuration for production.

  Args:
      environment: Environment name (defaults to ENVIRONMENT env var)

  Returns:
      Dictionary of connection parameters for Redis client.
  """
  if environment is None:
    environment = os.getenv("ENVIRONMENT", "dev").lower()

  params = {
    "decode_responses": True,
    "socket_connect_timeout": 5,  # 5 second connection timeout
    "socket_timeout": 5,  # 5 second operation timeout
    "retry_on_timeout": True,
    "retry_on_error": [ConnectionError, TimeoutError],
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
    params["ssl_cert_reqs"] = ssl.CERT_NONE  # Don't verify certificate
    params["ssl_check_hostname"] = False  # Don't check hostname
    params["ssl_ca_certs"] = None  # No CA certificate validation

  return params


def create_redis_client(
  database: ValkeyDatabase, decode_responses: bool = True, **kwargs
) -> Any:  # Returns redis.Redis but avoid import here
  """
  Create a Redis client with proper configuration for the environment.

  This automatically handles authentication and TLS configuration.

  Args:
      database: The Valkey database to connect to
      decode_responses: Whether to decode responses as strings
      **kwargs: Additional Redis client parameters

  Returns:
      Configured Redis client

  Example:
      >>> from robosystems.config.valkey_registry import ValkeyDatabase, create_redis_client
      >>> client = create_redis_client(ValkeyDatabase.AUTH_CACHE)
      >>> client.set("key", "value")
  """
  import redis

  # Build authenticated URL WITHOUT SSL params in query string
  # (SSL params will be passed as connection parameters instead)
  url = ValkeyURLBuilder.build_authenticated_url(database, include_ssl_params=False)

  # Get connection parameters (includes SSL settings for prod/staging)
  params = get_redis_connection_params()
  params["decode_responses"] = decode_responses

  # Merge with any provided kwargs
  params.update(kwargs)

  # Create client
  return redis.Redis.from_url(url, **params)


def create_async_redis_client(
  database: ValkeyDatabase, decode_responses: bool = True, **kwargs
) -> Any:  # Returns redis.asyncio.Redis but avoid import here
  """
  Create an async Redis client with proper configuration for the environment.

  This automatically handles authentication and TLS configuration.

  Args:
      database: The Valkey database to connect to
      decode_responses: Whether to decode responses as strings
      **kwargs: Additional Redis client parameters

  Returns:
      Configured async Redis client

  Example:
      >>> from robosystems.config.valkey_registry import ValkeyDatabase, create_async_redis_client
      >>> client = create_async_redis_client(ValkeyDatabase.AUTH_CACHE)
      >>> await client.set("key", "value")
  """
  import redis.asyncio as redis_async

  # Build authenticated URL WITHOUT SSL params in query string
  # (SSL params will be passed as connection parameters instead)
  url = ValkeyURLBuilder.build_authenticated_url(database, include_ssl_params=False)

  # Get connection parameters (includes SSL settings for prod/staging)
  params = get_redis_connection_params()
  params["decode_responses"] = decode_responses

  # Merge with any provided kwargs
  params.update(kwargs)

  # Create async client
  return redis_async.from_url(url, **params)


def create_redis_client_from_url(
  url: str, decode_responses: bool = True, **kwargs
) -> Any:  # Returns redis.Redis
  """
  Create a Redis client from a URL with proper ElastiCache configuration.

  Use this when you have a pre-built URL (e.g., from env variables).

  Args:
      url: Redis URL (can include auth)
      decode_responses: Whether to decode responses as strings
      **kwargs: Additional Redis client parameters

  Returns:
      Configured Redis client
  """
  import redis

  # Get connection parameters
  params = get_redis_connection_params()
  params["decode_responses"] = decode_responses

  # Merge with any provided kwargs
  params.update(kwargs)

  # Create client
  return redis.from_url(url, **params)
