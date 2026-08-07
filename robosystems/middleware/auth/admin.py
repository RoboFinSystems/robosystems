"""Admin authentication middleware using AWS Secrets Manager."""

import hmac
from functools import wraps
from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from ...config.secrets_manager import get_secrets_manager
from ...logger import get_logger
from ...security.audit_logger import SecurityAuditLogger

logger = get_logger(__name__)

security = HTTPBearer()


class AdminAuthMiddleware:
  """Bearer-token authentication for the admin API, against the admin key
  held in AWS Secrets Manager.
  """

  def __init__(self):
    self.secrets_manager = get_secrets_manager()

  def _get_admin_key(self) -> str | None:
    """Fetch the admin key from Secrets Manager, or None if unavailable."""
    try:
      admin_key = self.secrets_manager.get_admin_key()

      if admin_key:
        logger.debug("Successfully retrieved admin key from Secrets Manager")
        return admin_key
      else:
        logger.warning("Admin key not found in Secrets Manager")
        return None

    except Exception as e:
      logger.error(f"Error fetching admin key: {e!s}")
      return None

  def verify_admin_key(self, api_key: str) -> dict[str, Any] | None:
    """Return key metadata when `api_key` matches the admin key, else None.

    The comparison is constant-time so a wrong key leaks no timing signal
    about how much of it was right.
    """
    admin_key = self._get_admin_key()

    if not admin_key:
      return None

    if hmac.compare_digest(api_key, admin_key):
      return {
        "key_id": "admin",
        "name": "Admin API Key",
        "permissions": ["*"],  # Admin has all permissions
        "created_by": "system",
      }

    return None

  async def __call__(
    self, request: Request, credentials: HTTPAuthorizationCredentials = None
  ):
    """Authenticate an admin request, raising 401 when the key is missing or
    wrong, and publishing the key metadata on `request.state.admin`.
    """
    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing or invalid authorization header",
        headers={"WWW-Authenticate": "Bearer"},
      )

    api_key = auth_header[7:]

    key_metadata = self.verify_admin_key(api_key)

    if not key_metadata:
      client_ip = request.client.host if request.client else None
      logger.warning(f"Invalid admin key attempted from IP: {client_ip}")
      SecurityAuditLogger.log_admin_auth_failure(
        reason="invalid_admin_key",
        ip_address=client_ip,
        endpoint=str(request.url.path),
        user_agent=request.headers.get("User-Agent"),
      )
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid admin API key",
        headers={"WWW-Authenticate": "Bearer"},
      )

    request.state.admin = key_metadata
    request.state.admin_key_id = key_metadata["key_id"]

    logger.info(
      f"Admin authenticated: {key_metadata['name']}",
      extra={
        "admin_key_id": key_metadata["key_id"],
        "admin_name": key_metadata["name"],
        "ip_address": request.client.host if request.client else None,
      },
    )


admin_auth = AdminAuthMiddleware()


def require_admin(permissions: list[str] | None = None):
  """Require admin authentication, and any of `permissions` when given."""

  def decorator(func):
    @wraps(func)
    async def wrapper(request: Request, *args, **kwargs):
      await admin_auth(request)

      if permissions:
        admin_permissions = request.state.admin.get("permissions", [])

        if "*" in admin_permissions:
          pass  # Has all permissions
        else:
          has_permission = any(perm in admin_permissions for perm in permissions)
          if not has_permission:
            raise HTTPException(
              status_code=status.HTTP_403_FORBIDDEN,
              detail=f"Insufficient permissions. Required: {permissions}",
            )

      return await func(request, *args, **kwargs)

    return wrapper

  return decorator
