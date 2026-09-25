"""API key authentication for the Graph API (prod/staging only)."""

import asyncio
import time
from collections.abc import Callable

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

from robosystems.config import env
from robosystems.logger import logger


class GraphAuthMiddleware(BaseHTTPMiddleware):
  """API key authentication for the Graph API.

  Enforced in production and staging only; development runs unauthenticated.
  ``/health`` is the only exempt path so ALB and container probes do not
  need a key. Repeated failures from one IP are locked out.
  """

  EXEMPT_PATHS = frozenset({"/health"})
  # A rotated key reaches every process at a different moment, so both the
  # current and the previous key are accepted, and re-read on this interval.
  KEY_REFRESH_SECONDS = 300
  # A rejected key triggers an early re-read, at most this often.
  KEY_MISS_REFRESH_SECONDS = 30

  def __init__(
    self,
    app,
    api_key: str | None = None,
    key_type: str = "writer",
    key_source: Callable[[], tuple[str | None, str | None]] | None = None,
  ):
    super().__init__(app)
    self.environment = env.ENVIRONMENT
    self.auth_enabled = self.environment in ["prod", "staging"]
    self.key_type = key_type
    self.failed_attempts: dict[
      str, tuple[int, float]
    ] = {}  # IP -> (count, last_attempt_time)
    self.max_failed_attempts = 10
    self.lockout_duration = 300  # 5 minutes

    # Explicit argument, then the rotating key source, then centralized
    # config, then Secrets Manager.
    self.api_key = api_key or None
    self.previous_api_key: str | None = None
    self.key_source = None if api_key else key_source
    self.keys_loaded_at = 0.0
    if self.key_source is not None and self.auth_enabled:
      self._load_keys()
    if not self.api_key and env.GRAPH_API_KEY:
      self.api_key = env.GRAPH_API_KEY
    if not self.api_key and self.auth_enabled:
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

    now = time.time()
    if (
      self.key_source is not None
      and now - self.keys_loaded_at > self.KEY_REFRESH_SECONDS
    ):
      # Stamped before the read so concurrent requests do not all reload.
      self.keys_loaded_at = now
      await asyncio.to_thread(self._load_keys)

    try:
      try:
        self._validate_api_key(request)
      except HTTPException:
        # A key rotated since the last read: re-read once, rate-limited.
        if (
          self.key_source is None
          or time.time() - self.keys_loaded_at < self.KEY_MISS_REFRESH_SECONDS
        ):
          raise
        self.keys_loaded_at = time.time()
        await asyncio.to_thread(self._load_keys)
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

    accepted = [k for k in (self.api_key, self.previous_api_key) if k]
    # Every key is compared, so the time taken does not say which one matched.
    matches = [self._constant_time_compare(api_key, k) for k in accepted]
    if not any(matches):
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key"
      )

  def _load_keys(self) -> None:
    """Re-read the current and previous keys; a failed read keeps the last."""
    self.keys_loaded_at = time.time()
    try:
      current, previous = self.key_source()  # type: ignore[misc]
    except Exception as e:
      logger.error(f"Reading the Graph API keys failed; keeping the last read: {e}")
      return
    if current:
      self.api_key = current
      self.previous_api_key = previous

  def _constant_time_compare(self, a: str, b: str) -> bool:
    """Compare two strings in constant time to prevent timing attacks."""
    import hmac

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

  All arguments are ignored: every node type reads the single
  ``GRAPH_API_KEY`` through the central secrets manager. Returns None on any
  failure rather than raising.
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


def read_graph_api_keys() -> tuple[str | None, str | None]:
  """The current and previous Graph API keys, read from Secrets Manager
  (``robosystems/{env}/graph-api``) past the process-wide secret cache."""
  import json

  import boto3
  from botocore.config import Config

  # Called on the request path: a slow Secrets Manager must fail fast.
  client = boto3.client(
    "secretsmanager",
    region_name=env.AWS_REGION,
    config=Config(connect_timeout=2, read_timeout=5, retries={"max_attempts": 2}),
  )
  secret_id = f"robosystems/{env.ENVIRONMENT}/graph-api"

  def _key(stage: str) -> str | None:
    try:
      value = client.get_secret_value(SecretId=secret_id, VersionStage=stage)
    except client.exceptions.ResourceNotFoundException:
      return None
    return json.loads(value["SecretString"]).get("GRAPH_API_KEY") or None

  return _key("AWSCURRENT"), _key("AWSPREVIOUS")


LadybugAuthMiddleware = GraphAuthMiddleware
