"""Authentication utilities."""

import hashlib

from sqlalchemy.orm import Session

from ...database import session
from ...logger import logger
from ...models.iam import GraphUser, User, UserAPIKey
from ...security import SecurityAuditLogger
from .cache import api_key_cache


def _safe_cache_call(func_name: str, *args, **kwargs):
  """Safely call cache functions, handling None cache gracefully."""
  if api_key_cache is None:
    return None
  try:
    func = getattr(api_key_cache, func_name)
    return func(*args, **kwargs)
  except Exception as e:
    logger.error(f"Cache call failed ({func_name}): {e}")
    return None


def validate_api_key(api_key: str, db_session: Session | None = None) -> User | None:
  """
  Validate an API key and return the associated user if valid.
  Uses secure bcrypt verification with encrypted cache.

  Args:
      api_key (str): The API key to validate.
      db_session (Session, optional): Database session to use. Defaults to global session.

  Returns:
      Optional[User]: The user associated with the API key, or None if invalid.
  """
  if not api_key:
    return None

  # Generate cache key based on the API key
  import hashlib

  cache_key = hashlib.sha256(api_key.encode()).hexdigest()

  # Try cache first (cache now uses encrypted storage)
  cached_data = _safe_cache_call("get_cached_api_key_validation", cache_key)
  if cached_data:
    if not cached_data.get("is_active", False):
      logger.debug(f"Cached API key is inactive: {cache_key[:8]}...")
      return None

    # Reconstruct user from cached data
    user_data = cached_data.get("user_data", {})
    if user_data and user_data.get("id"):
      logger.debug(f"API key validation cache hit: {cache_key[:8]}...")

      # Create a minimal User object with cached data
      user = User()
      user.id = user_data["id"]
      user.name = user_data.get("name")
      user.email = user_data.get("email")
      user.is_active = user_data.get("is_active", True)

      return user

  # Cache miss - fall back to database with secure bcrypt verification
  logger.debug(f"API key cache miss, querying database: {cache_key[:8]}...")
  sess = db_session or session

  # Use secure bcrypt verification (handles both verification and last_used update)
  key_record = UserAPIKey.get_by_key(api_key, sess)
  if not key_record:
    # Cache negative result (with shorter TTL)
    try:
      _safe_cache_call("cache_api_key_validation", cache_key, {}, is_active=False)
    except (ConnectionError, TimeoutError) as e:
      logger.error(f"Cache service unavailable for negative API key result: {e}")
    except Exception as e:
      logger.warning(f"Unexpected error caching negative API key result: {e}")
    return None

  # Cache positive result with encrypted storage
  try:
    user_data = {
      "id": key_record.user.id,
      "name": key_record.user.name,
      "email": key_record.user.email,
      "is_active": key_record.user.is_active,
    }
    _safe_cache_call(
      "cache_api_key_validation", cache_key, user_data, is_active=key_record.is_active
    )
  except (ConnectionError, TimeoutError) as e:
    logger.error(f"Cache service unavailable for API key validation result: {e}")
  except Exception as e:
    logger.warning(f"Unexpected error caching API key validation result: {e}")

  # Log successful API key validation
  SecurityAuditLogger.log_auth_success(
    user_id=str(key_record.user.id), auth_method="api_key"
  )

  # Return the associated user
  return key_record.user


def validate_api_key_with_graph(
  api_key: str, graph_id: str, db_session: Session | None = None
) -> User | None:
  """
  Validate an API key with graph ID authorization and return the associated user.
  Uses Valkey cache with PostgreSQL fallback for performance.

  Args:
      api_key (str): The API key to validate.
      graph_id (str): The graph database ID to check access for.
      db_session (Session, optional): Database session to use. Defaults to global session.

  Returns:
      Optional[User]: The user associated with the API key, or None if invalid or unauthorized.
  """
  if not api_key or not graph_id:
    return None

  # Hash the API key for cache lookup
  api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()

  # Try to get cached API key validation first
  cached_api_key = _safe_cache_call("get_cached_api_key_validation", api_key_hash)
  cached_graph_access = _safe_cache_call(
    "get_cached_graph_access", api_key_hash, graph_id
  )

  # If we have both cached results, use them
  if cached_api_key and cached_graph_access is not None:
    if not cached_api_key.get("is_active", False):
      logger.debug(f"Cached API key is inactive: {api_key_hash[:8]}...")
      return None

    if not cached_graph_access:
      logger.debug(f"Cached graph access denied: {api_key_hash[:8]}... -> {graph_id}")
      return None

    # Reconstruct user from cached data
    user_data = cached_api_key.get("user_data", {})
    if user_data and user_data.get("id"):
      logger.debug(
        f"API key + graph validation cache hit: {api_key_hash[:8]}... -> {graph_id}"
      )

      # Create a minimal User object with cached data
      user = User()
      user.id = user_data["id"]
      user.name = user_data.get("name")
      user.email = user_data.get("email")
      user.is_active = user_data.get("is_active", True)

      # Update last_used_at in background (don't block on this)
      try:
        sess = db_session or session
        key_record = UserAPIKey.get_by_hash(api_key_hash, sess)
        if key_record:
          key_record.update_last_used(sess)
      except Exception as e:
        logger.error(f"Failed to update last_used_at for cached API key: {e}")

      return user

  # Cache miss - fall back to database
  logger.debug(
    f"API key + graph cache miss, querying database: {api_key_hash[:8]}... -> {graph_id}"
  )
  sess = db_session or session

  # Check if API key exists and is active
  key_record = UserAPIKey.get_by_key(api_key, sess)
  if not key_record:
    # Cache negative API key result
    try:
      _safe_cache_call("cache_api_key_validation", api_key_hash, {}, is_active=False)
    except (ConnectionError, TimeoutError) as e:
      logger.error(f"Cache service unavailable for negative API key result: {e}")
    except Exception as e:
      logger.warning(f"Unexpected error caching negative API key result: {e}")
    return None

  # Check if the user has access to the specified graph
  # Special handling for SEC database
  from ..graph.utils import MultiTenantUtils

  has_access = False  # Initialize variable
  if MultiTenantUtils.is_shared_repository(graph_id):
    # Use generic repository access validation
    has_access = MultiTenantUtils.validate_repository_access(
      graph_id,
      key_record.user_id,
      "read",
    )
  else:
    has_access = GraphUser.user_has_access(key_record.user_id, graph_id, sess)
  if not has_access:
    # Cache the API key validation (positive) but graph access (negative)
    try:
      user_data = {
        "id": key_record.user.id,
        "name": key_record.user.name,
        "email": key_record.user.email,
        "is_active": key_record.user.is_active,
      }
      _safe_cache_call(
        "cache_api_key_validation",
        api_key_hash,
        user_data,
        is_active=key_record.is_active,
      )
      _safe_cache_call("cache_graph_access", api_key_hash, graph_id, has_access=False)
    except Exception as e:
      logger.error(f"Failed to cache API key + graph validation result: {e}")
    return None

  # Update last used timestamp
  key_record.update_last_used(sess)

  # Cache both positive results
  try:
    user_data = {
      "id": key_record.user.id,
      "name": key_record.user.name,
      "email": key_record.user.email,
      "is_active": key_record.user.is_active,
    }
    _safe_cache_call(
      "cache_api_key_validation",
      api_key_hash,
      user_data,
      is_active=key_record.is_active,
    )
    _safe_cache_call("cache_graph_access", api_key_hash, graph_id, has_access=True)
  except Exception as e:
    logger.error(f"Failed to cache API key + graph validation result: {e}")

  # Log successful API key with graph validation
  SecurityAuditLogger.log_auth_success(
    user_id=str(key_record.user.id), auth_method="api_key"
  )

  # Return the associated user
  return key_record.user


def validate_repository_access(
  user: User,
  repository_id: str,
  operation_type: str = "read",
  db_session: Session | None = None,
) -> bool:
  """
  Validate repository access for a user using the generic repository access system.

  Args:
      user: User to validate access for
      repository_id: Repository identifier (e.g., 'sec', 'industry')
      operation_type: Type of operation ("read", "write", "admin")
      db_session: Database session to use

  Returns:
      bool: True if user has the required repository access
  """
  if not user or not bool(user.is_active):
    return False

  from ..graph.utils import MultiTenantUtils

  # Use the generic repository access validation
  return MultiTenantUtils.validate_repository_access(
    repository_id,
    user.id,
    operation_type,
  )
