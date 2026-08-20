"""Authentication maintenance and cleanup functions."""

from datetime import UTC, datetime

from sqlalchemy import and_
from sqlalchemy.orm import Session

from ...models.core import UserAPIKey


def cleanup_expired_api_keys(session: Session) -> dict:
  """Deactivate API keys past their `expires_at` and return counts.

  `expired_sessions_deleted` is always 0: authentication is API key + JWT,
  with no persistent session table to sweep.
  """
  from ...logger import logger

  try:
    logger.debug("Skipping session cleanup - system uses API key + JWT authentication")
    expired_sessions = 0

    current_time = datetime.now(UTC)

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
      "expired_user_keys_deactivated": expired_by_date,
      "expired_by_date": expired_by_date,
    }
  except Exception as exc:
    logger.error(f"Error in cleanup_expired_api_keys: {exc}", exc_info=True)
    raise


def cleanup_jwt_cache_expired() -> dict:
  """Report JWT cache occupancy.

  Valkey TTL expires these entries on its own; this reports counts rather
  than deleting anything.
  """
  from ...logger import logger
  from .cache import api_key_cache

  try:
    stats = api_key_cache.get_cache_stats()

    logger.debug("JWT cache cleanup handled automatically by Valkey TTL")

    return {
      "jwt_tokens_cached": stats.get("cache_counts", {}).get("jwt_tokens", 0),
      "jwt_revoked": stats.get("cache_counts", {}).get("jwt_revoked", 0),
      "cleanup_method": "automatic_ttl",
    }

  except Exception as exc:
    logger.error(f"Error checking JWT cache: {exc}", exc_info=True)
    return {
      "jwt_tokens_cached": 0,
      "jwt_revoked": 0,
      "cleanup_method": "error",
      "error": str(exc),
    }


def cleanup_inactive_api_keys(session: Session) -> dict:
  """Alias for `cleanup_expired_api_keys`."""
  return cleanup_expired_api_keys(session)


def cleanup_api_keys(session: Session) -> dict:
  """Alias for `cleanup_expired_api_keys`."""
  return cleanup_expired_api_keys(session)
