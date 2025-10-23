"""
Authentication middleware for Graph API with environment-based security.

Provides API key authentication for production/staging environments while
allowing unrestricted access in development and from bastion hosts.
Supports both Kuzu and Neo4j backends.
"""

import json
import time
from functools import lru_cache
from typing import Optional

import bcrypt
import boto3
from botocore.exceptions import ClientError
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from robosystems.config import env
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType


class GraphAuthMiddleware(BaseHTTPMiddleware):
  """
  Authentication middleware for Graph API.

  Features:
  - API key authentication in production/staging
  - Bypassed in development environment
  - Bypassed for requests from bastion hosts
  - Bypassed for health check endpoints
  - Rate limiting for failed auth attempts
  - Works with both Kuzu and Neo4j backends
  """

  # Endpoints that don't require authentication
  EXEMPT_PATHS = {
    "/health",
    "/status",
    "/info",
    "/metrics",
    "/openapi.json",
    "/docs",
    "/redoc",
    "/",
  }

  def __init__(self, app, api_key: Optional[str] = None, key_type: str = "writer"):
    super().__init__(app)
    self.environment = env.ENVIRONMENT
    self.auth_enabled = self.environment in ["prod", "staging"]
    self.key_type = key_type
    self.failed_attempts: dict[
      str, tuple[int, float]
    ] = {}  # IP -> (count, last_attempt_time)
    self.max_failed_attempts = 10
    self.lockout_duration = 300  # 5 minutes

    # Try to get API key from various sources
    # Use centralized config to ensure we get from Secrets Manager
    self.api_key = None
    if api_key:
      self.api_key = api_key
    elif env.GRAPH_API_KEY:
      # Get from centralized config (handles Secrets Manager)
      self.api_key = env.GRAPH_API_KEY
    elif self.auth_enabled:
      # Fallback to direct Secrets Manager lookup
      self.api_key = get_api_key_from_secrets_manager(key_type=self.key_type)

    if self.auth_enabled and not self.api_key:
      logger.error(
        f"Graph API key not configured for {self.key_type} in {self.environment} environment!"
      )
      raise ValueError(
        f"KUZU_API_KEY must be set for {self.key_type} in production/staging"
      )

    logger.info(
      f"Graph Auth Middleware initialized - Environment: {self.environment}, "
      f"Auth Enabled: {self.auth_enabled}, Key Type: {self.key_type}"
    )

  async def dispatch(self, request: Request, call_next):
    """Process request through authentication middleware."""
    # Skip auth for exempt paths
    if request.url.path in self.EXEMPT_PATHS:
      return await call_next(request)

    # Skip auth in development
    if not self.auth_enabled:
      logger.debug("Auth bypassed - development environment")
      return await call_next(request)

    # Check rate limiting
    client_ip = request.client.host if request.client else "unknown"
    if self._is_rate_limited(client_ip):
      logger.warning(f"Rate limited IP: {client_ip}")
      return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": "Too many failed authentication attempts"},
      )

    # Validate API key
    try:
      self._validate_api_key(request)
      # Reset failed attempts on success
      if client_ip in self.failed_attempts:
        del self.failed_attempts[client_ip]
      return await call_next(request)
    except HTTPException as e:
      # Track failed attempt
      self._record_failed_attempt(client_ip)
      logger.warning(f"Authentication failed from {client_ip} - {e.detail}")
      return JSONResponse(status_code=e.status_code, content={"detail": e.detail})

  def _validate_api_key(self, request: Request) -> None:
    """Validate API key from request headers."""
    # Check for API key in header (support both old and new header names)
    api_key = request.headers.get("X-Graph-API-Key") or request.headers.get(
      "X-Kuzu-API-Key"
    )
    if not api_key:
      # Also check Authorization header
      auth_header = request.headers.get("Authorization", "")
      if auth_header.startswith("Bearer "):
        api_key = auth_header[7:]

    if not api_key:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing API key"
      )

    # Constant-time comparison to prevent timing attacks
    if not self.api_key or not self._constant_time_compare(api_key, self.api_key):
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key"
      )

  def _constant_time_compare(self, a: str, b: str) -> bool:
    """Compare two strings in constant time to prevent timing attacks."""
    import hmac

    # Use hmac.compare_digest for cryptographically secure constant-time comparison
    return hmac.compare_digest(a, b)

  def _is_rate_limited(self, client_ip: str) -> bool:
    """Check if IP is rate limited due to failed attempts."""
    if client_ip not in self.failed_attempts:
      return False

    count, last_attempt = self.failed_attempts[client_ip]

    # Check if lockout period has expired
    if time.time() - last_attempt > self.lockout_duration:
      del self.failed_attempts[client_ip]
      return False

    return count >= self.max_failed_attempts

  def _record_failed_attempt(self, client_ip: str) -> None:
    """Record a failed authentication attempt."""
    current_time = time.time()

    if client_ip in self.failed_attempts:
      count, _ = self.failed_attempts[client_ip]
      self.failed_attempts[client_ip] = (count + 1, current_time)
    else:
      self.failed_attempts[client_ip] = (1, current_time)

    # Clean up old entries
    self._cleanup_failed_attempts()

  def _cleanup_failed_attempts(self) -> None:
    """Remove expired entries from failed attempts tracking."""
    current_time = time.time()
    expired_ips = [
      ip
      for ip, (_, last_time) in self.failed_attempts.items()
      if current_time - last_time > self.lockout_duration
    ]

    for ip in expired_ips:
      del self.failed_attempts[ip]


@lru_cache(maxsize=4)  # Cache for writer, shared_writer, shared_master, shared_replica
def get_api_key_from_secrets_manager(
  key_type: str = "writer", secret_name: Optional[str] = None, region: str = "us-east-1"
) -> Optional[str]:
  """
  Retrieve API key from AWS Secrets Manager with caching.

  Args:
      key_type: Type of key to retrieve ("writer", "shared_writer", "shared_master", "shared_replica")
      secret_name: Name of the secret (default: robosystems/{env}/kuzu)
      region: AWS region

  Returns:
      API key or None if not found
  """
  if not secret_name:
    secret_name = f"robosystems/{env.ENVIRONMENT}/kuzu"

  try:
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)

    # Parse the secret
    secret_data = json.loads(response["SecretString"])

    # All node types use the same unified API key
    api_key = secret_data.get("GRAPH_API_KEY")

    if api_key:
      logger.info("Successfully retrieved Graph API key from Secrets Manager")
      return api_key
    else:
      logger.error(f"No GRAPH_API_KEY found in secret: {secret_name[:50]}")
      return None

  except ClientError as e:
    if e.response["Error"]["Code"] == "ResourceNotFoundException":
      logger.warning(f"Secret not found: {secret_name[:50]}")
    else:
      logger.error(f"Error retrieving secret: {e}")
    return None
  except Exception as e:
    logger.error(f"Unexpected error retrieving secret: {e}")
    return None


def clear_api_key_cache():
  """Clear the cached API key (useful for rotation)."""
  get_api_key_from_secrets_manager.cache_clear()


def create_api_key(prefix: str = "kuzu") -> tuple[str, str]:
  """
  Generate a secure API key with bcrypt hashing.

  Returns:
      Tuple of (api_key, bcrypt_hash) where bcrypt_hash should be stored
  """
  import secrets

  # Generate 32 bytes of randomness
  key_bytes = secrets.token_bytes(32)
  api_key = f"{prefix}_{key_bytes.hex()}"

  # Create bcrypt hash for secure storage (never store plain key)
  salt = bcrypt.gensalt(rounds=12)
  key_hash = bcrypt.hashpw(api_key.encode("utf-8"), salt).decode("utf-8")

  # Log secure key generation
  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.AUTH_SUCCESS,
    details={
      "action": "secure_kuzu_api_key_generated",
      "prefix": prefix,
      "hash_algorithm": "bcrypt",
    },
    risk_level="low",
  )

  return api_key, key_hash


KuzuAuthMiddleware = GraphAuthMiddleware
