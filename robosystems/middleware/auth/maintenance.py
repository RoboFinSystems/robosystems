"""Authentication maintenance and cleanup functions."""

from datetime import datetime, timezone
from sqlalchemy.orm import Session
from sqlalchemy import and_

from ...models.iam import UserAPIKey


def cleanup_expired_api_keys(session: Session) -> dict:
  """
  Clean up expired API keys and perform auth maintenance.

  This function handles:
  - Deactivating API keys that are past their expiration date
  - Providing cleanup statistics

  Args:
      session: Database session for performing operations

  Returns:
      Dict with cleanup statistics including:
      - expired_sessions_deleted: Always 0 (API key system, no sessions)
      - expired_user_keys_deactivated: Number of API keys deactivated
      - expired_by_date: Number of API keys expired by date
  """
  from ...logger import logger

  try:
    # Note: This system uses API key + JWT authentication, not session-based auth
    # No session cleanup is needed as there are no persistent session tables
    logger.debug("Skipping session cleanup - system uses API key + JWT authentication")
    expired_sessions = 0

    current_time = datetime.now(timezone.utc)

    # Clean up API keys that have reached their expiration date
    logger.debug("Cleaning up expired API keys (past expires_at date)")
    expired_keys = (
      session.query(UserAPIKey)
      .filter(
        and_(
          UserAPIKey.is_active,
          UserAPIKey.expires_at.isnot(None),
          UserAPIKey.expires_at <= current_time,
        )
      )
      .all()
    )

    expired_by_date = 0
    for api_key in expired_keys:
      logger.info(
        f"Deactivating expired API key {api_key.id} (expired: {api_key.expires_at})"
      )
      api_key.deactivate(session)
      expired_by_date += 1

    logger.debug(f"Deactivated {expired_by_date} expired API keys")

    return {
      "expired_sessions_deleted": expired_sessions,
      "expired_user_keys_deactivated": expired_by_date,  # For backward compatibility
      "expired_by_date": expired_by_date,
    }
  except Exception as exc:
    logger.error(f"Error in cleanup_expired_api_keys: {exc}", exc_info=True)
    raise


def cleanup_jwt_cache_expired() -> dict:
  """
  Clean up expired JWT cache entries.

  Note: This is handled automatically by Valkey TTL expiration,
  but this function provides manual cleanup capability if needed.

  Returns:
      Dict with cleanup statistics
  """
  from ...logger import logger
  from .cache import api_key_cache

  try:
    # Get current cache stats
    stats = api_key_cache.get_cache_stats()

    # JWT cache cleanup is automatic via TTL, but we can provide stats
    logger.debug("JWT cache cleanup handled automatically by Valkey TTL")

    return {
      "jwt_tokens_cached": stats.get("cache_counts", {}).get("jwt_tokens", 0),
      "jwt_blacklisted": stats.get("cache_counts", {}).get("jwt_blacklisted", 0),
      "cleanup_method": "automatic_ttl",
    }

  except Exception as exc:
    logger.error(f"Error checking JWT cache: {exc}", exc_info=True)
    return {
      "jwt_tokens_cached": 0,
      "jwt_blacklisted": 0,
      "cleanup_method": "error",
      "error": str(exc),
    }


# Legacy function name for backward compatibility
def cleanup_inactive_api_keys(session: Session) -> dict:
  """
  Legacy function name for backward compatibility.
  Now delegates to cleanup_expired_api_keys since we only handle expiration by date.

  Args:
      session: Database session for performing operations

  Returns:
      Dict with cleanup statistics
  """
  return cleanup_expired_api_keys(session)


# Legacy function name for backward compatibility
def cleanup_api_keys(session: Session) -> dict:
  """
  Legacy function name for backward compatibility.

  Args:
      session: Database session for performing operations

  Returns:
      Dict with cleanup statistics
  """
  return cleanup_expired_api_keys(session)
