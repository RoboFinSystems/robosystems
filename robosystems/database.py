import asyncio
import contextvars
import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, scoped_session, sessionmaker

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


_request_scope = contextvars.ContextVar("db_request_scope", default=None)


def activate_request_scope():
  """
  Activate a request-scoped SQLAlchemy session context.

  Returns:
      ContextVar token if a new scope was set, otherwise None.
  """
  if _request_scope.get() is not None:
    return None
  return _request_scope.set(object())


def deactivate_request_scope(token):
  """Reset request scope context if it was set."""
  if token is None:
    return
  try:
    _request_scope.reset(token)
  except ValueError:
    # Context may differ if the dependency ran in a worker thread.
    _request_scope.set(None)


def _session_scope():
  """
  Return an identifier for the current execution context.

  FastAPI runs multiple requests in the same thread via asyncio tasks.
  Using the current task as the scope avoids sharing the same SQLAlchemy
  Session across concurrent requests while still supporting threaded usage.
  """
  scope_id = _request_scope.get()
  if scope_id is not None:
    return scope_id

  try:
    current_task = asyncio.current_task()
  except RuntimeError:
    current_task = None

  if current_task is not None:
    return current_task

  # Fallback to thread identifier for synchronous/background contexts
  return threading.get_ident()


engine = create_engine(
  get_database_url(),
  pool_size=env.DATABASE_POOL_SIZE,
  max_overflow=env.DATABASE_MAX_OVERFLOW,
  pool_timeout=env.DATABASE_POOL_TIMEOUT,
  pool_recycle=env.DATABASE_POOL_RECYCLE,
  pool_pre_ping=True,
  echo=env.DATABASE_ECHO,
)
SessionFactory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
session = scoped_session(SessionFactory, scopefunc=_session_scope)


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
    session.remove()


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
