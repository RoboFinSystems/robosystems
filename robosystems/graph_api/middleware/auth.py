"""
Authentication middleware for Graph API with environment-based security.

Provides API key authentication for production/staging environments while
allowing unrestricted access in development.
"""

import time

import bcrypt
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from robosystems.config import env
from robosystems.logger import logger
from robosystems.security import SecurityAuditLogger, SecurityEventType


class GraphAuthMiddleware(BaseHTTPMiddleware):
  """API key authentication for the Graph API.

  Enforced in production and staging only; development runs unauthenticated.
  ``/health`` is the only exempt path so ALB and container probes do not
  need a key. Repeated failures from one IP are locked out.
  """

  EXEMPT_PATHS = frozenset({"/health"})

  def __init__(self, app, api_key: str | None = None, key_type: str = "writer"):
    super().__init__(app)
    self.environment = env.ENVIRONMENT
    self.auth_enabled = self.environment in ["prod", "staging"]
    self.key_type = key_type
    self.failed_attempts: dict[
      str, tuple[int, float]
    ] = {}  # IP -> (count, last_attempt_time)
    self.max_failed_attempts = 10
    self.lockout_duration = 300  # 5 minutes

    # Explicit argument, then centralized config, then Secrets Manager.
    self.api_key = None
    if api_key:
      self.api_key = api_key
    elif env.GRAPH_API_KEY:
      self.api_key = env.GRAPH_API_KEY
    elif self.auth_enabled:
      self.api_key = get_api_key_from_secrets_manager(key_type=self.key_type)

    if self.auth_enabled and not self.api_key:
      logger.error(
        f"Graph API key not configured for {self.key_type} in {self.environment} environment!"
      )
      raise ValueError(
        f"GRAPH_API_KEY must be set for {self.key_type} in production/staging"
      )

    logger.info(
      f"Graph Auth Middleware initialized - Environment: {self.environment}, "
      f"Auth Enabled: {self.auth_enabled}, Key Type: {self.key_type}"
    )

  async def dispatch(self, request: Request, call_next):
    """Authenticate the request, or pass it through when auth does not apply."""
    if request.url.path in self.EXEMPT_PATHS:
      return await call_next(request)

    if not self.auth_enabled:
      logger.debug("Auth bypassed - development environment")
      return await call_next(request)

    client_ip = request.client.host if request.client else "unknown"
    if self._is_rate_limited(client_ip):
      logger.warning(f"Rate limited IP: {client_ip}")
      return JSONResponse(
        status_code=status.HTTP_429_TOO_MANY_REQUESTS,
        content={"detail": "Too many failed authentication attempts"},
      )

    try:
      self._validate_api_key(request)
      if client_ip in self.failed_attempts:
        del self.failed_attempts[client_ip]
      return await call_next(request)
    except HTTPException as e:
      self._record_failed_attempt(client_ip)
      logger.warning(f"Authentication failed from {client_ip} - {e.detail}")
      return JSONResponse(status_code=e.status_code, content={"detail": e.detail})

  def _validate_api_key(self, request: Request) -> None:
    """Validate the key from ``X-Graph-API-Key`` or a Bearer Authorization header."""
    api_key = request.headers.get("X-Graph-API-Key")
    if not api_key:
      auth_header = request.headers.get("Authorization", "")
      if auth_header.startswith("Bearer "):
        api_key = auth_header[7:]

    if not api_key:
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing API key"
      )

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

    self._cleanup_failed_attempts()

  def _cleanup_failed_attempts(self) -> None:
    """Drop expired entries; the tracking dict is unbounded without this."""
    current_time = time.time()
    expired_ips = [
      ip
      for ip, (_, last_time) in self.failed_attempts.items()
      if current_time - last_time > self.lockout_duration
    ]

    for ip in expired_ips:
      del self.failed_attempts[ip]


def get_api_key_from_secrets_manager(
  key_type: str = "writer", secret_name: str | None = None, region: str = "us-east-1"
) -> str | None:
  """Read the Graph API key from Secrets Manager (``robosystems/{env}/graph-api``).

  ``key_type`` selects the key (``writer``, ``shared_writer``,
  ``shared_master``, ``shared_replica``). ``secret_name`` and ``region`` are
  resolved by the central secrets manager and ignored here. Returns None on
  any failure rather than raising.
  """
  try:
    from robosystems.config.secrets_manager import get_secret_value

    api_key = get_secret_value("GRAPH_API_KEY", "")

    if api_key:
      logger.info("Successfully retrieved Graph API key from Secrets Manager")
      return api_key
    else:
      logger.warning("No GRAPH_API_KEY found in secrets")
      return None

  except Exception as e:
    logger.error(f"Error retrieving Graph API key: {e}")
    return None


def clear_api_key_cache():
  """Drop the cached API key so the next read picks up a rotated secret."""
  try:
    from robosystems.config.secrets_manager import get_secrets_manager

    manager = get_secrets_manager()
    manager.refresh("graph-api")
    logger.info("Graph API key cache cleared successfully")
  except Exception as e:
    logger.warning(f"Failed to clear API key cache: {e}")


def create_api_key(prefix: str = "ladybug") -> tuple[str, str]:
  """Generate an API key and its bcrypt hash.

  Returns ``(api_key, bcrypt_hash)``. Store the hash; the key itself is
  returned once and is not recoverable afterwards.
  """
  import secrets

  key_bytes = secrets.token_bytes(32)
  api_key = f"{prefix}_{key_bytes.hex()}"

  salt = bcrypt.gensalt(rounds=12)
  key_hash = bcrypt.hashpw(api_key.encode("utf-8"), salt).decode("utf-8")

  SecurityAuditLogger.log_security_event(
    event_type=SecurityEventType.AUTH_SUCCESS,
    details={
      "action": "secure_graph_api_key_generated",
      "prefix": prefix,
      "hash_algorithm": "bcrypt",
    },
    risk_level="low",
  )

  return api_key, key_hash


LadybugAuthMiddleware = GraphAuthMiddleware
