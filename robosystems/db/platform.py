import asyncio
import contextvars
import threading

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, scoped_session, sessionmaker

from robosystems.config import env
from robosystems.config.tuning import TuningConfig


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


def _connect_args() -> dict[str, str]:
  # A per-statement ceiling set on the connection, so no single platform query
  # can hold the (synchronous, loop-bound) session unboundedly. Mirrors the
  # extensions engine; migrations run on their own engine and are unaffected.
  #
  # Engine-wide (workers included) is intentional and safe: `statement_timeout`
  # bounds a single statement, not a transaction, so a bulk job that issues
  # many small statements under one commit (e.g. bulk_allocate_monthly_credits)
  # is unaffected — only an individual query running longer than the ceiling is
  # cut. The platform DB has no such single bulk statement (that shape lives on
  # the extensions engine, which opts bulk paths out per-session). SSM-tunable;
  # `database/STATEMENT_TIMEOUT_MS = 0` disables it without a deploy.
  timeout_ms = TuningConfig.get_database_statement_timeout_ms()
  # `options` is a libpq connection parameter; only PostgreSQL drivers accept it.
  if timeout_ms <= 0 or not (get_database_url() or "").startswith("postgresql"):
    return {}
  return {"options": f"-c statement_timeout={timeout_ms}"}


engine = create_engine(
  get_database_url(),
  pool_size=TuningConfig.get_database_pool_size(),
  max_overflow=TuningConfig.get_database_max_overflow(),
  pool_timeout=TuningConfig.get_database_pool_timeout(),
  pool_recycle=TuningConfig.get_database_pool_recycle(),
  pool_pre_ping=True,
  connect_args=_connect_args(),
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


from contextlib import contextmanager  # noqa: E402


@contextmanager
def platform_session():
  """Independent platform-DB session for code OUTSIDE FastAPI's dependency
  machinery — GraphQL resolvers, MCP tools, runner threads, scripts, Dagster
  jobs.

  It opens a fresh ``SessionFactory()`` session, NOT the request-scoped
  registry, and that distinction is load-bearing. The scoped ``session`` is
  keyed by the request (``_session_scope``: request contextvar → task →
  thread), and a runner thread spawned by a request inherits the request's
  contextvars — so a scoped session opened *inside* a request, or inside a
  worker thread it started, resolves to the endpoint's own Session. Were this
  context manager to drive ``get_db_session()`` (the scoped path), its
  ``finally`` close would then tear down the endpoint's session mid-flight:
  objects expunged, pending ``add()``s silently discarded, the next use
  auto-beginning a fresh empty transaction. An independent session has its own
  lifecycle and cannot reach across into the request's. The scoped registry
  stays for FastAPI ``Depends`` (``get_db_session`` / ``get_async_db_session``)
  only.

  Usage:

      from robosystems.db.platform import platform_session

      with platform_session() as db:
          row = db.query(Connection).first()

  This replaces ad-hoc `gen = get_db_session(); next(gen); ...; gen.close()`
  patterns that used to live in the GraphQL resolvers and MCP tools.
  """
  db = SessionFactory()
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
