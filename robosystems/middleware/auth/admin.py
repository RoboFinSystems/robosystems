"""Admin authentication middleware using AWS Secrets Manager."""

from functools import wraps
from typing import Optional, Dict, Any
from fastapi import Request, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from ...config.secrets_manager import get_secrets_manager
from ...logger import get_logger

logger = get_logger(__name__)

security = HTTPBearer()


class AdminAuthMiddleware:
  """Middleware for admin authentication using centralized Secrets Manager."""

  def __init__(self):
    """Initialize the admin auth middleware."""
    self.secrets_manager = get_secrets_manager()

  def _get_admin_key(self) -> Optional[str]:
    """Get admin API key from AWS Secrets Manager.

    Returns:
        The admin key string, or None if not found
    """
    try:
      # Use centralized secrets manager with built-in caching
      admin_key = self.secrets_manager.get_admin_key()

      if admin_key:
        logger.debug("Successfully retrieved admin key from Secrets Manager")
        return admin_key
      else:
        logger.warning("Admin key not found in Secrets Manager")
        return None

    except Exception as e:
      logger.error(f"Error fetching admin key: {str(e)}")
      return None

  def verify_admin_key(self, api_key: str) -> Optional[Dict[str, Any]]:
    """Verify an admin API key.

    Args:
        api_key: The API key to verify

    Returns:
        Key metadata if valid, None otherwise
    """
    admin_key = self._get_admin_key()

    if not admin_key:
      return None

    # Check if the provided key matches
    if api_key == admin_key:
      # Return metadata for the valid key
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
    """Authenticate admin requests.

    Args:
        request: FastAPI request object
        credentials: Bearer token credentials

    Raises:
        HTTPException: If authentication fails
    """
    # Get the authorization header
    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Missing or invalid authorization header",
        headers={"WWW-Authenticate": "Bearer"},
      )

    # Extract the token
    api_key = auth_header[7:]  # Remove "Bearer " prefix

    # Verify the admin key
    key_metadata = self.verify_admin_key(api_key)

    if not key_metadata:
      logger.warning(f"Invalid admin key attempted from IP: {request.client.host}")
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid admin API key",
        headers={"WWW-Authenticate": "Bearer"},
      )

    # Store admin metadata in request state
    request.state.admin = key_metadata
    request.state.admin_key_id = key_metadata["key_id"]

    logger.info(
      f"Admin authenticated: {key_metadata['name']}",
      extra={
        "admin_key_id": key_metadata["key_id"],
        "admin_name": key_metadata["name"],
        "ip_address": request.client.host,
      },
    )


# Create singleton instance
admin_auth = AdminAuthMiddleware()


def require_admin(permissions: Optional[list[str]] = None):
  """Decorator to require admin authentication and check permissions.

  Args:
      permissions: List of required permissions (default: any permission)

  Returns:
      Decorated function
  """

  def decorator(func):
    @wraps(func)
    async def wrapper(request: Request, *args, **kwargs):
      # Run authentication
      await admin_auth(request)

      # Check permissions if specified
      if permissions:
        admin_permissions = request.state.admin.get("permissions", [])

        # Check for wildcard permission
        if "*" in admin_permissions:
          pass  # Has all permissions
        else:
          # Check if user has required permissions
          has_permission = any(perm in admin_permissions for perm in permissions)
          if not has_permission:
            raise HTTPException(
              status_code=status.HTTP_403_FORBIDDEN,
              detail=f"Insufficient permissions. Required: {permissions}",
            )

      # Call the original function
      return await func(request, *args, **kwargs)

    return wrapper

  return decorator
