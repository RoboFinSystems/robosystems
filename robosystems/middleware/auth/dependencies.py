"""
Authentication dependencies for FastAPI.

Security Note:
- JWT tokens can be passed via Authorization header (preferred) or query parameter (for SSE)
- Query parameter tokens are automatically redacted in all logs via middleware/logging.py
- Never log full request URLs; always use request.url.path for logging
"""

from typing import Optional

from fastapi import HTTPException, Security, status, Request, Header, Query
from fastapi.security import APIKeyHeader

from ...models.iam import User
from ...database import session
from ...logger import logger
from ...security import SecurityAuditLogger, SecurityEventType
from .cache import api_key_cache
from .utils import (
  validate_api_key,
  validate_api_key_with_graph,
  validate_repository_access,
)

# Import JWT verification from local jwt module to avoid circular imports
from .jwt import verify_jwt_token as verify_jwt_token_from_auth


def _validate_cached_user_data(user_data: dict) -> bool:
  """Validate cached user data before creating User object."""
  if not isinstance(user_data, dict):
    return False

  # Validate required fields
  user_id = user_data.get("id")
  email = user_data.get("email")

  if not user_id or not isinstance(user_id, (int, str)):
    return False

  if not email or not isinstance(email, str) or "@" not in email:
    return False

  # Validate optional fields
  name = user_data.get("name")
  if name is not None and not isinstance(name, str):
    return False

  is_active = user_data.get("is_active")
  if is_active is not None and not isinstance(is_active, bool):
    return False

  return True


def _create_user_from_cache(user_data: dict) -> Optional[User]:
  """Safely create User object from validated cached data."""
  if not _validate_cached_user_data(user_data):
    logger.warning("Invalid cached user data detected, falling back to database")
    return None

  try:
    user = User(
      id=user_data.get("id"),
      email=user_data.get("email"),
      name=user_data.get("name"),
      is_active=user_data.get("is_active", True),
    )
    return user
  except (TypeError, ValueError) as e:
    logger.error(f"Invalid data type in cached user data: {e}")
    return None
  except Exception as e:
    logger.error(f"Unexpected error creating User from cached data: {e}")
    return None


# Define API key header
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)


def verify_jwt_token(token: str) -> Optional[str]:
  """Verify a JWT token and return the user_id if valid.

  This includes caching for performance.
  """
  # Try to get from cache first
  cached_data = api_key_cache.get_cached_jwt_validation(token)
  if cached_data:
    user_data = cached_data.get("user_data", {})
    return user_data.get("id")

  # Cache miss - use the auth verification
  user_id = verify_jwt_token_from_auth(token)

  if user_id:
    # Get user data and cache it
    user = User.get_by_id(user_id, session())
    if user and bool(user.is_active):
      user_data = {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "is_active": user.is_active,
      }
      api_key_cache.cache_jwt_validation(token, user_data)
      return user_id

  return None


async def get_optional_user(
  request: Request,
  api_key: str = Security(API_KEY_HEADER),
) -> Optional[User]:
  """
  Get the authenticated user if API key or JWT token is valid (optional authentication).

  Args:
      request: FastAPI request object
      api_key (str): The API key from the X-API-Key header.

  Returns:
      Optional[User]: The authenticated user or None if no valid authentication provided.
  """
  # Extract JWT token from Authorization header
  authorization = request.headers.get("authorization")
  jwt_token = None
  if authorization and authorization.startswith("Bearer "):
    jwt_token = authorization[7:]  # Remove "Bearer " prefix

  # Try JWT token authentication first (takes precedence)
  if jwt_token:
    user_id = verify_jwt_token(jwt_token)
    if user_id:
      # Try to get user data from cache first
      cached_data = api_key_cache.get_cached_jwt_validation(jwt_token)
      if cached_data:
        user_data = cached_data.get("user_data", {})
        # Safely create User object from validated cached data
        user = _create_user_from_cache(user_data)
        if user:
          return user
        # If validation failed, fall through to database query
      else:
        # Fallback to database query
        user = User.get_by_id(user_id, session())
        if user and bool(user.is_active):
          return user

  # Fall back to API key authentication
  if api_key:
    user = validate_api_key(api_key)
    return user

  return None


async def get_current_user(
  request: Request,
  api_key: str = Security(API_KEY_HEADER),
) -> User:
  """
  Get the authenticated user, raising an exception if authentication fails.

  Args:
      request: FastAPI request object for extracting client info
      api_key (str): The API key from the X-API-Key header.

  Returns:
      User: The authenticated user.

  Raises:
      HTTPException: If no valid authentication is provided.
  """
  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  endpoint = str(request.url.path)

  # Extract JWT token from Authorization header
  authorization = request.headers.get("authorization")
  jwt_token = None
  if authorization and authorization.startswith("Bearer "):
    jwt_token = authorization[7:]  # Remove "Bearer " prefix

  # Try JWT token authentication first (takes precedence)
  if jwt_token:
    user_id = verify_jwt_token(jwt_token)
    if user_id:
      # Try to get user data from cache first
      cached_data = api_key_cache.get_cached_jwt_validation(jwt_token)
      if cached_data:
        user_data = cached_data.get("user_data", {})
        # Safely create User object from validated cached data
        user = _create_user_from_cache(user_data)
        if user:
          SecurityAuditLogger.log_auth_success(
            user_id=str(user_id),
            ip_address=client_ip,
            user_agent=user_agent,
            auth_method="jwt_token",
          )
          return user
        # If validation failed, fall through to database query
      else:
        # Fallback to database query
        user = User.get_by_id(user_id, session())
        if user and bool(user.is_active):
          SecurityAuditLogger.log_auth_success(
            user_id=str(user_id),
            ip_address=client_ip,
            user_agent=user_agent,
            auth_method="jwt_token",
          )
          return user

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_TOKEN_INVALID,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      details={"token_type": "jwt"},
      risk_level="high",
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Invalid or expired token",
      headers={"WWW-Authenticate": "Bearer"},
    )

  # Fall back to API key authentication
  if api_key:
    user = validate_api_key(api_key)
    if user:
      SecurityAuditLogger.log_auth_success(
        user_id=str(user.id),
        ip_address=client_ip,
        user_agent=user_agent,
        auth_method="api_key",
      )
      return user
    else:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.API_KEY_INVALID,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint=endpoint,
        details={"api_key_prefix": api_key[:8] if api_key else ""},
        risk_level="high",
      )
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key",
        headers={"WWW-Authenticate": "ApiKey"},
      )

  # No authentication provided
  SecurityAuditLogger.log_auth_failure(
    reason="No authentication provided",
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint=endpoint,
  )
  raise HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Authentication required",
    headers={"WWW-Authenticate": "Bearer, ApiKey"},
  )


async def get_current_user_with_graph(
  request: Request,
  graph_id: str,
  api_key: str = Security(API_KEY_HEADER),
) -> User:
  """
  Get the authenticated user with graph authorization check and security audit logging.

  Args:
      request: FastAPI request object
      graph_id (str): The graph database ID to check access for.
      api_key (str): The API key from the X-API-Key header.

  Returns:
      User: The authenticated user.

  Raises:
      HTTPException: If no valid authentication is provided or user doesn't have graph access.
  """
  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  endpoint = str(request.url.path)

  # Extract JWT token from Authorization header
  authorization = request.headers.get("authorization")
  jwt_token = None
  if authorization and authorization.startswith("Bearer "):
    jwt_token = authorization[7:]  # Remove "Bearer " prefix

  # Try JWT token authentication first (takes precedence)
  if jwt_token:
    user_id = verify_jwt_token(jwt_token)
    if user_id:
      # Try to get user data from cache first
      cached_data = api_key_cache.get_cached_jwt_validation(jwt_token)
      user = None

      if cached_data:
        user_data = cached_data.get("user_data", {})
        # Create User object from cached data (avoid database query)
        user = User(
          id=user_data.get("id"),
          email=user_data.get("email"),
          name=user_data.get("name"),
          is_active=user_data.get("is_active", True),
        )
      else:
        # Fallback to database query
        user = User.get_by_id(user_id, session())

      if user and bool(user.is_active):
        # Check if user has access to the graph (try cache first)
        has_access = api_key_cache.get_cached_jwt_graph_access(str(user_id), graph_id)

        if has_access is None:
          # Cache miss - check database and cache result
          from ...models.iam import UserGraph

          has_access = UserGraph.user_has_access(user_id, graph_id, session())
          api_key_cache.cache_jwt_graph_access(str(user_id), graph_id, has_access)

        if has_access:
          SecurityAuditLogger.log_auth_success(
            user_id=str(user_id),
            ip_address=client_ip,
            user_agent=user_agent,
            auth_method="jwt_token",
          )
          return user
        else:
          SecurityAuditLogger.log_authorization_denied(
            user_id=str(user_id),
            resource=f"graph_database:{graph_id}",
            action="access",
            ip_address=client_ip,
            endpoint=endpoint,
          )
          raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Access denied to graph",
          )

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_TOKEN_INVALID,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      details={"token_type": "jwt"},
      risk_level="high",
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Invalid or expired token",
      headers={"WWW-Authenticate": "Bearer"},
    )

  # Fall back to API key authentication (with graph validation)
  if api_key:
    user = validate_api_key_with_graph(api_key, graph_id)
    if user:
      SecurityAuditLogger.log_auth_success(
        user_id=str(user.id),
        ip_address=client_ip,
        user_agent=user_agent,
        auth_method="api_key",
      )
      return user
    else:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.API_KEY_INVALID,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint=endpoint,
        details={
          "api_key_prefix": api_key[:8] if api_key else "",
          "graph_id": graph_id,
        },
        risk_level="high",
      )
      raise HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="Invalid API key or access denied to graph",
        headers={"WWW-Authenticate": "ApiKey"},
      )

  # No authentication provided
  SecurityAuditLogger.log_auth_failure(
    reason="No authentication provided",
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint=endpoint,
  )
  raise HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Authentication required",
    headers={"WWW-Authenticate": "Bearer, ApiKey"},
  )


async def get_current_user_with_repository_access(
  request: Request,
  repository_id: str,
  operation_type: str = "read",
  api_key: str = Security(API_KEY_HEADER),
) -> User:
  """
  Get the authenticated user with repository access validation.

  Args:
      request: FastAPI request object for extracting client info
      repository_id: Repository identifier (e.g., 'sec', 'industry')
      operation_type: Type of operation ("read", "write", "admin")
      api_key: The API key from the X-API-Key header

  Returns:
      User: The authenticated user with validated repository access

  Raises:
      HTTPException: If authentication fails or repository access denied
  """
  # First get the authenticated user
  current_user = await get_current_user(request, api_key)

  # Then validate repository access
  if not validate_repository_access(current_user, repository_id, operation_type):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"{repository_id.upper()} repository {operation_type} access denied",
    )

  return current_user


def get_repository_user_dependency(repository_id: str, operation_type: str = "read"):
  """
  Factory function to create repository access dependency with specific operation type.

  Args:
      repository_id: Repository identifier (e.g., 'sec', 'industry')
      operation_type: Type of operation required

  Returns:
      Dependency function for repository access validation
  """

  async def _get_repository_user(
    request: Request,
    api_key: str = Security(API_KEY_HEADER),
  ) -> User:
    return await get_current_user_with_repository_access(
      request, repository_id, operation_type, api_key
    )

  return _get_repository_user


# ============================================================================
# SSE-Specific Authentication Dependencies
# ============================================================================


async def get_current_user_sse(
  request: Request,
  api_key: str = Security(API_KEY_HEADER),
  authorization: Optional[str] = Header(None),
  token: Optional[str] = Query(None, description="JWT token for SSE authentication"),
) -> User:
  """
  Get the authenticated user for SSE endpoints (supports query parameter tokens).

  This is a specialized version of get_current_user that accepts JWT tokens
  via query parameters, which is necessary for Server-Sent Events since
  EventSource API doesn't support custom headers.

  Args:
      request: FastAPI request object for extracting client info
      api_key: The API key from the X-API-Key header
      authorization: The Authorization header (supports Bearer tokens)
      token: JWT token from query parameter (for SSE connections)

  Returns:
      User: The authenticated user

  Raises:
      HTTPException: If no valid authentication is provided
  """
  client_ip = request.client.host if request.client else None
  user_agent = request.headers.get("user-agent")
  endpoint = str(request.url.path)

  # Extract JWT token from Authorization header or query parameter
  jwt_token = None
  if authorization and authorization.startswith("Bearer "):
    jwt_token = authorization[7:]  # Remove "Bearer " prefix
  elif token:
    # Fallback to query parameter for SSE connections
    jwt_token = token

  # Try JWT token authentication first (takes precedence)
  if jwt_token:
    user_id = verify_jwt_token(jwt_token)
    if user_id:
      # Try to get user data from cache first
      cached_data = api_key_cache.get_cached_jwt_validation(jwt_token)
      if cached_data:
        user_data = cached_data.get("user_data", {})
        # Safely create User object from validated cached data
        user = _create_user_from_cache(user_data)
        if user:
          SecurityAuditLogger.log_auth_success(
            user_id=str(user_id),
            ip_address=client_ip,
            user_agent=user_agent,
            auth_method="jwt_token",
          )
          return user
        # If validation failed, fall through to database query
      else:
        # Fallback to database query
        user = User.get_by_id(user_id, session())
        if user and bool(user.is_active):
          SecurityAuditLogger.log_auth_success(
            user_id=str(user_id),
            ip_address=client_ip,
            user_agent=user_agent,
            auth_method="jwt_token",
          )
          return user

    SecurityAuditLogger.log_security_event(
      event_type=SecurityEventType.AUTH_TOKEN_INVALID,
      ip_address=client_ip,
      user_agent=user_agent,
      endpoint=endpoint,
      details={"token_type": "jwt"},
      risk_level="high",
    )
    raise HTTPException(
      status_code=status.HTTP_401_UNAUTHORIZED,
      detail="Invalid or expired token",
      headers={"WWW-Authenticate": "Bearer"},
    )

  # Fall back to API key authentication
  if api_key:
    user = validate_api_key(api_key)
    if user:
      SecurityAuditLogger.log_auth_success(
        user_id=str(user.id),
        ip_address=client_ip,
        user_agent=user_agent,
        auth_method="api_key",
      )
      return user
    else:
      SecurityAuditLogger.log_security_event(
        event_type=SecurityEventType.API_KEY_INVALID,
        ip_address=client_ip,
        user_agent=user_agent,
        endpoint=endpoint,
        details={"api_key_prefix": api_key[:8] if api_key else ""},
        risk_level="high",
      )
      raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid API key",
        headers={"WWW-Authenticate": "ApiKey"},
      )

  # No authentication provided
  SecurityAuditLogger.log_auth_failure(
    reason="No authentication provided",
    ip_address=client_ip,
    user_agent=user_agent,
    endpoint=endpoint,
  )
  raise HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail="Authentication required",
    headers={"WWW-Authenticate": "Bearer, ApiKey"},
  )
