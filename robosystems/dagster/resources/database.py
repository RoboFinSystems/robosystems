"""PostgreSQL database resource for Dagster.

Provides database session management for Dagster jobs and assets,
matching the patterns used in the existing RoboSystems codebase.
"""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from dagster import ConfigurableResource, InitResourceContext
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from robosystems.config import env


class DatabaseResource(ConfigurableResource):
  """PostgreSQL database resource for Dagster operations.

  This resource provides database sessions that match the patterns
  used throughout RoboSystems, ensuring consistency between
  Dagster jobs and existing application code.
  """

  database_url: str = ""

  def setup_for_execution(self, context: InitResourceContext) -> None:
    """Initialize the database engine on resource setup."""
    url = self.database_url or env.DATABASE_URL
    self._engine = create_engine(url, pool_pre_ping=True)
    self._session_factory = sessionmaker(bind=self._engine)

  @contextmanager
  def get_session(self) -> Generator[Session]:
    """Get a database session context manager.

    Yields:
        SQLAlchemy session that auto-commits on success, rolls back on error.

    Example:
        ```python
        @op
        def my_op(context, db: DatabaseResource):
            with db.get_session() as session:
                users = session.query(User).all()
        ```
    """
    session = self._session_factory()
    try:
      yield session
      session.commit()
    except Exception:
      session.rollback()
      raise
    finally:
      session.close()

  def execute_query(self, query: str, params: dict[str, Any] | None = None) -> list:
    """Execute a raw SQL query and return results.

    Args:
        query: SQL query string
        params: Optional query parameters

    Returns:
        List of result rows
    """
    with self.get_session() as session:
      result = session.execute(query, params or {})
      return list(result.fetchall())
