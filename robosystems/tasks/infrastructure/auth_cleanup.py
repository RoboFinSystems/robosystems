"""Background tasks for authentication maintenance and cleanup."""

from sqlalchemy.orm import sessionmaker

from ...middleware.auth.maintenance import cleanup_expired_api_keys
from ...database import engine
from ...logger import logger
from ...celery import celery_app


@celery_app.task(bind=True)
def cleanup_expired_api_keys_task(self):
  """Clean up expired API keys."""
  logger.info("Starting API key cleanup task")

  try:
    # Check if engine is available
    if not engine:
      logger.error("Database engine is not available")
      raise Exception("Database engine is not available")

    SessionLocal = sessionmaker(bind=engine)
    logger.debug("Created session maker")

    with SessionLocal() as session:
      logger.debug("Starting database session for cleanup")

      # Test database connection first
      try:
        from sqlalchemy import text

        session.execute(text("SELECT 1"))
        logger.debug("Database connection verified")
      except Exception as db_exc:
        logger.error(f"Database connection test failed: {db_exc}")
        raise

      result = cleanup_expired_api_keys(session)
      session.commit()

      logger.info(
        f"API key cleanup completed: {result['expired_sessions_deleted']} sessions deleted, "
        f"{result['expired_user_keys_deactivated']} API keys deactivated by expiration date "
        f"(system keys are protected from cleanup)"
      )

      return result

  except Exception as exc:
    logger.error(f"Failed to clean up expired API keys: {exc}", exc_info=True)
    # Don't retry if it's a database connection issue
    if "database" in str(exc).lower() or "connection" in str(exc).lower():
      logger.error("Database connection issue detected, not retrying")
      raise exc
    raise self.retry(exc=exc, countdown=60, max_retries=3)
