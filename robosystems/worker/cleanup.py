"""Connection cleanup between worker tasks.

Safety net to prevent session/connection leaks between tasks
processing different tenants. Properly written tasks use
extensions_session() and close platform sessions in finally blocks,
but this catches anything that slips through.

See: feedback_session_isolation.md — prior Celery incident where
leaked search_path caused cross-tenant data contamination.
"""

import logging

logger = logging.getLogger(__name__)


def cleanup_connections() -> None:
  """Dispose SQLAlchemy connection pools between tasks.

  Called in the consumer loop's finally block after every task,
  regardless of success or failure. Forces all pooled connections
  closed so the next task starts with a clean slate.
  """
  # Platform engine (module-level, always initialized)
  try:
    from robosystems.db.platform import engine as platform_engine

    platform_engine.dispose()
  except Exception as e:
    logger.warning(f"Failed to dispose platform engine: {e}")

  # Extensions engine (lazy global, may be None)
  try:
    from robosystems.db import extensions

    if extensions._engine is not None:
      extensions._engine.dispose()
  except Exception as e:
    logger.warning(f"Failed to dispose extensions engine: {e}")
