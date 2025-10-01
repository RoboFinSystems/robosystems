from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, DeclarativeBase
from robosystems.config import env


def get_database_url():
  """Get database URL with SSL configuration if needed."""
  database_url = env.DATABASE_URL

  # Add SSL parameters for staging/prod environments
  if (
    (env.is_staging() or env.is_production())
    and database_url
    and "?" not in database_url
  ):
    database_url += "?sslmode=require"
  elif (
    (env.is_staging() or env.is_production())
    and database_url
    and "sslmode" not in database_url
  ):
    database_url += "&sslmode=require"

  return database_url


engine = create_engine(
  get_database_url(),
  pool_size=env.DATABASE_POOL_SIZE,
  max_overflow=env.DATABASE_MAX_OVERFLOW,
  pool_timeout=env.DATABASE_POOL_TIMEOUT,
  pool_recycle=env.DATABASE_POOL_RECYCLE,
  pool_pre_ping=True,
  echo=env.DATABASE_ECHO,
)
session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


class Base(DeclarativeBase):
  """Base class for all models."""

  pass


# For backward compatibility
Model = Base
Model.query = session.query_property()


def get_db_session():
  """Get database session for FastAPI dependency injection."""
  db = session()
  try:
    yield db
  finally:
    db.close()


async def get_async_db_session():
  """
  Get database session for async FastAPI endpoints.

  This version is safer for async contexts as it ensures the session
  is properly closed after all async operations complete.
  """
  db = session()
  try:
    yield db
  finally:
    # Remove the session from the scoped session registry
    # This is safer than close() in async contexts
    session.remove()
