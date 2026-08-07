"""Authentication utilities."""

import hashlib
import re

from sqlalchemy.orm import Session

from ...logger import logger
from ...models.core import GraphUser, User, UserAPIKey
from ...security import SecurityAuditLogger
from .cache import api_key_cache

# Format: "rfs" (account-wide) or "rfsc" (graph-scoped) prefix + 64 hex chars.
# Lowercase only — secrets.token_hex() always produces lowercase hex.
# Unambiguous despite the shared stem: total length differs (67 vs 68).
_API_KEY_FORMAT_RE = re.compile(r"^rfsc?[0-9a-f]{64}$")


def _is_valid_api_key_format(api_key: str) -> bool:
  """Check if the API key matches the expected format before any cache/DB work."""
  return bool(_API_KEY_FORMAT_RE.match(api_key))


def _key_scope_allows(key_graph_id: str | None, requested_graph_id: str) -> bool:
  """Whether a key's graph scope permits the requested graph.

  A NULL scope means an account-wide key, allowed everywhere. A scoped key
  covers exactly its graph and that graph's subgraphs
  (subgraph ids are ``{parent}_{name}`` and share the parent's access model).
  """
  if key_graph_id is None:
    return True
  return requested_graph_id == key_graph_id or requested_graph_id.startswith(
    f"{key_graph_id}_"
  )


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
  """Validate an account-wide API key and return its user.

  Bcrypt verification against the database, fronted by the encrypted cache.
  Graph-scoped keys are rejected here: without a graph context they would
  reach account-level surfaces, so they are only valid through
  `validate_api_key_with_graph`.
  """
  if not api_key:
    return None

  if not _is_valid_api_key_format(api_key):
    logger.debug("API key rejected: invalid format")
    return None

  cache_key = hashlib.sha256(api_key.encode()).hexdigest()

  cached_data = _safe_cache_call("get_cached_api_key_validation", cache_key)
  if cached_data:
    if not cached_data.get("is_active", False):
      logger.debug(f"Cached API key is inactive: {cache_key[:8]}...")
      return None

    user_data = cached_data.get("user_data", {})
    if user_data and user_data.get("id"):
      logger.debug(f"API key validation cache hit: {cache_key[:8]}...")

      # Graph-scoped keys are never valid without a graph context (least
      # privilege: a connector credential cannot reach account-level surfaces).
      if user_data.get("key_graph_id"):
        logger.debug(
          f"API key rejected: graph-scoped key without graph context: {cache_key[:8]}..."
        )
        return None

      user = User()
      user.id = user_data["id"]
      user.name = user_data.get("name")
      user.email = user_data.get("email")
      user.is_active = user_data.get("is_active", True)

      # Reject keys owned by a deactivated user, even when the key row and its
      # cache entry are still marked active. Deactivation invalidates the cache
      # entry, but this guard closes the window until that propagates.
      if not user.is_active:
        logger.debug(f"API key rejected: owning user inactive: {cache_key[:8]}...")
        return None

      return user

  # Cache miss. Use a short-lived session so the pool connection is returned
  # right after the DB work; the scoped ``session`` proxy would hold it until
  # middleware cleanup at end-of-request, which starves long-running endpoints
  # like MCP tool execution.
  from ...database import SessionFactory

  _owns_session = db_session is None
  sess = SessionFactory() if _owns_session else db_session

  try:
    logger.debug(f"API key cache miss, querying database: {cache_key[:8]}...")

    key_record = UserAPIKey.get_by_key(api_key, sess)
    if not key_record:
      try:
        _safe_cache_call("cache_api_key_validation", cache_key, {}, is_active=False)
      except (ConnectionError, TimeoutError) as e:
        logger.error(f"Cache service unavailable for negative API key result: {e}")
      except Exception as e:
        logger.warning(f"Unexpected error caching negative API key result: {e}")
      return None

    # Load while the session is live so the lazy relationship resolves once.
    user = key_record.user

    # Reject keys whose owning user is deactivated. UserAPIKey.get_by_key only
    # filters on the key's own is_active flag, not the user's — without this a
    # deactivated user retains full API access via any existing key.
    if not user.is_active:
      logger.debug(f"API key rejected: owning user inactive: {cache_key[:8]}...")
      return None

    try:
      user_data = {
        "id": user.id,
        "name": user.name,
        "email": user.email,
        "is_active": user.is_active,
        "key_graph_id": key_record.graph_id,
      }
      _safe_cache_call(
        "cache_api_key_validation", cache_key, user_data, is_active=key_record.is_active
      )
    except (ConnectionError, TimeoutError) as e:
      logger.error(f"Cache service unavailable for API key validation result: {e}")
    except Exception as e:
      logger.warning(f"Unexpected error caching API key validation result: {e}")

    # Graph-scoped keys are never valid without a graph context (least
    # privilege). Cached above so the graph-scoped path still benefits.
    if key_record.graph_id:
      logger.debug(
        f"API key rejected: graph-scoped key without graph context: {cache_key[:8]}..."
      )
      return None

    SecurityAuditLogger.log_auth_success(user_id=str(user.id), auth_method="api_key")

    # Detach so the pool connection is returned immediately.
    sess.expunge(user)
    return user
  finally:
    if _owns_session:
      sess.rollback()
      sess.close()


def validate_api_key_with_graph(
  api_key: str,
  graph_id: str,
  db_session: Session | None = None,
  require_scoped: bool = False,
) -> User | None:
  """Validate an API key against a graph and return its user.

  Graph-scoped keys (``graph_id`` set on the row) are honored only for their
  own graph and its subgraphs, regardless of how the key is carried. With
  ``require_scoped=True`` (the MCP URL-token path), account-wide keys are
  additionally rejected — the account credential must never be the one that
  travels in a URL.

  Returns None for an invalid key or an unauthorized graph, without
  distinguishing the two.
  """
  if not api_key or not graph_id:
    return None

  if not _is_valid_api_key_format(api_key):
    logger.debug("API key rejected: invalid format")
    return None

  api_key_hash = hashlib.sha256(api_key.encode()).hexdigest()

  cached_api_key = _safe_cache_call("get_cached_api_key_validation", api_key_hash)

  # Cached negative result: key is unknown or inactive.
  if cached_api_key and not cached_api_key.get("is_active", False):
    logger.debug(f"Cached API key is inactive: {api_key_hash[:8]}...")
    return None

  cached_graph_access = _safe_cache_call(
    "get_cached_graph_access", api_key_hash, graph_id
  )

  if cached_api_key and cached_graph_access is not None:
    if not cached_graph_access:
      logger.debug(f"Cached graph access denied: {api_key_hash[:8]}... -> {graph_id}")
      return None

    user_data = cached_api_key.get("user_data", {})
    if user_data and user_data.get("id"):
      logger.debug(
        f"API key + graph validation cache hit: {api_key_hash[:8]}... -> {graph_id}"
      )

      # Key-level scope restrictions (checked on top of the cached user-level
      # graph access, which is shared across carriage paths).
      key_graph_id = user_data.get("key_graph_id")
      if not _key_scope_allows(key_graph_id, graph_id):
        logger.debug(
          f"API key rejected: scoped to another graph: {api_key_hash[:8]}... -> {graph_id}"
        )
        return None
      if require_scoped and not key_graph_id:
        logger.debug(
          f"API key rejected: account-wide key where a graph-scoped key is required: {api_key_hash[:8]}..."
        )
        return None

      user = User()
      user.id = user_data["id"]
      user.name = user_data.get("name")
      user.email = user_data.get("email")
      user.is_active = user_data.get("is_active", True)

      # Reject keys owned by a deactivated user (see validate_api_key).
      if not user.is_active:
        logger.debug(f"API key rejected: owning user inactive: {api_key_hash[:8]}...")
        return None

      # last_used_at is only updated on the cache-miss path below; touching
      # it on every cache hit would put the DB pool back in the hot path, and
      # the cache TTL refreshes it every few minutes regardless.

      return user

  # Cache miss. Short-lived session, same rationale as validate_api_key().
  from ...database import SessionFactory

  _owns_session = db_session is None
  sess = SessionFactory() if _owns_session else db_session

  try:
    logger.debug(
      f"API key + graph cache miss, querying database: {api_key_hash[:8]}... -> {graph_id}"
    )

    key_record = UserAPIKey.get_by_key(api_key, sess)
    if not key_record:
      try:
        _safe_cache_call("cache_api_key_validation", api_key_hash, {}, is_active=False)
      except (ConnectionError, TimeoutError) as e:
        logger.error(f"Cache service unavailable for negative API key result: {e}")
      except Exception as e:
        logger.warning(f"Unexpected error caching negative API key result: {e}")
      return None

    # Load while the session is live so the lazy relationship resolves once.
    user = key_record.user

    # Reject keys whose owning user is deactivated (see validate_api_key).
    if not user.is_active:
      logger.debug(f"API key rejected: owning user inactive: {api_key_hash[:8]}...")
      return None

    # Key-level scope restriction, applied before the user-level access check.
    # A scope mismatch is a durable property of (key, graph), so the negative
    # access decision is safe to cache for every carriage path.
    if not _key_scope_allows(key_record.graph_id, graph_id):
      logger.debug(
        f"API key rejected: scoped to another graph: {api_key_hash[:8]}... -> {graph_id}"
      )
      try:
        user_data = {
          "id": user.id,
          "name": user.name,
          "email": user.email,
          "is_active": user.is_active,
          "key_graph_id": key_record.graph_id,
        }
        _safe_cache_call(
          "cache_api_key_validation",
          api_key_hash,
          user_data,
          is_active=key_record.is_active,
        )
        _safe_cache_call("cache_graph_access", api_key_hash, graph_id, has_access=False)
      except Exception as e:
        logger.error(f"Failed to cache scoped-key mismatch result: {e}")
      return None

    # require_scoped rejects account-wide keys WITHOUT caching a negative
    # access decision — the same key remains valid for this graph via headers,
    # and the graph-access cache is shared across carriage paths.
    if require_scoped and not key_record.graph_id:
      logger.debug(
        f"API key rejected: account-wide key where a graph-scoped key is required: {api_key_hash[:8]}..."
      )
      return None

    from ..graph.utils import MultiTenantUtils

    has_access = False

    from robosystems.config.shared_repositories import is_shared_repository_or_subgraph

    if is_shared_repository_or_subgraph(graph_id):
      # Resolves a subgraph to its parent repository before checking.
      has_access = MultiTenantUtils.validate_repository_access(
        graph_id,
        key_record.user_id,
        "read",
      )
    else:
      has_access = GraphUser.user_has_access(key_record.user_id, graph_id, sess)
    if not has_access:
      # Key is valid; only the graph decision is negative. Cache both.
      try:
        user_data = {
          "id": user.id,
          "name": user.name,
          "email": user.email,
          "is_active": user.is_active,
          "key_graph_id": key_record.graph_id,
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

    key_record.update_last_used(sess)

    try:
      user_data = {
        "id": user.id,
        "name": user.name,
        "email": user.email,
        "is_active": user.is_active,
        "key_graph_id": key_record.graph_id,
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

    SecurityAuditLogger.log_auth_success(user_id=str(user.id), auth_method="api_key")

    # Detach so the pool connection is returned immediately.
    sess.expunge(user)
    return user
  finally:
    if _owns_session:
      sess.rollback()
      sess.close()


def validate_repository_access(
  user: User,
  repository_id: str,
  operation_type: str = "read",
  db_session: Session | None = None,
) -> bool:
  """Whether `user` holds `operation_type` access to a shared repository."""
  if not user or not bool(user.is_active):
    return False

  from ..graph.utils import MultiTenantUtils

  return MultiTenantUtils.validate_repository_access(
    repository_id,
    user.id,
    operation_type,
  )
