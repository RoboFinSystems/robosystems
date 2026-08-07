"""PostgreSQL database resource for Dagster.

Provides platform-database session management for Dagster jobs and assets.
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

  Defaults to env.DATABASE_URL when ``database_url`` is left empty.
  """

  database_url: str = ""

  def setup_for_execution(self, context: InitResourceContext) -> None:
    """Initialize the database engine on resource setup."""
    url = self.database_url or env.DATABASE_URL
    self._engine = create_engine(url, pool_pre_ping=True)
    self._session_factory = sessionmaker(bind=self._engine)

  @contextmanager
  def get_session(self) -> Generator[Session]:
    """Yield a session that commits on success and rolls back on error.

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
    """Execute a raw SQL query and return all result rows."""
    with self.get_session() as session:
      result = session.execute(query, params or {})
      return list(result.fetchall())
