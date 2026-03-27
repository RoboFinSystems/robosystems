"""Extensions OLTP database engine and session management.

Separate from the platform database (database.py) because:
- Different DeclarativeBase (ExtensionsBase vs Base/Model)
- Schema-per-graph-id multi-tenancy (SET search_path per request)
- Independent connection pool and lifecycle

The extensions database stores domain data (entities, accounts, transactions,
positions, trades, etc.) shared across all extension modules (roboledger,
roboinvestor, robofo, robohrm, roboepm) with schema-per-graph tenancy.
"""

import re
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from robosystems.config import env

# Graph IDs are validated upstream (GRAPH_ID_PATTERN: kg + 16+ hex chars)
# but we re-validate here for defense in depth against SQL injection.
_VALID_SCHEMA_PATTERN = re.compile(r"^kg[0-9a-f]{16,}$")


def get_extensions_database_url() -> str:
  """Get extensions database URL with SSL for staging/prod."""
  database_url = env.EXTENSIONS_DATABASE_URL

  if (env.is_staging() or env.is_production()) and database_url:
    if "?" not in database_url:
      database_url += "?sslmode=require"
    elif "sslmode" not in database_url:
      database_url += "&sslmode=require"

  return database_url


def _create_extensions_engine():
  """Create the extensions database engine.

  Lazy creation to avoid connection attempts during import
  in contexts that don't need the extensions database (e.g., graph instances).
  """
  return create_engine(
    get_extensions_database_url(),
    pool_size=3,
    max_overflow=5,
    pool_timeout=30,
    pool_recycle=3600,
    pool_pre_ping=True,
    echo=env.DATABASE_ECHO,
  )


# Lazy engine — created on first use
_engine = None
_session_factory = None


def _get_engine():
  global _engine
  if _engine is None:
    if not env.EXTENSIONS_ENABLED:
      raise RuntimeError(
        "Extensions database access attempted but EXTENSIONS_ENABLED is false. "
        "Set EXTENSIONS_ENABLED=true to enable the extensions OLTP database "
        "(required for RoboLedger, RoboInvestor, and other extension modules)."
      )
    _engine = _create_extensions_engine()
  return _engine


def _get_session_factory():
  global _session_factory
  if _session_factory is None:
    _session_factory = sessionmaker(
      autocommit=False, autoflush=False, bind=_get_engine()
    )
  return _session_factory


class ExtensionsBase(DeclarativeBase):
  """Base class for all extension OLTP models."""

  pass


def _sanitize_schema(graph_id: str) -> str:
  """Validate graph_id is safe for use as a PostgreSQL schema name.

  Graph IDs follow the pattern kg + 16+ hex chars. This function
  validates that pattern to prevent SQL injection in SET search_path.

  Raises:
      ValueError: If graph_id doesn't match the expected pattern.
  """
  if not _VALID_SCHEMA_PATTERN.match(graph_id):
    raise ValueError(f"Invalid graph_id for schema name: {graph_id}")
  return graph_id


@contextmanager
def extensions_session(graph_id: str):
  """Context manager providing a schema-scoped session for a tenant.

  Sets search_path to '{graph_id}, public' so tenant tables resolve
  in the graph_id schema and shared tables (fiscal_periods) resolve
  from public.

  Usage:
      with extensions_session("kg0123456789abcdef") as session:
          accounts = session.execute(select(Account)).scalars().all()

  Args:
      graph_id: The graph ID that maps to a PostgreSQL schema.

  Yields:
      A SQLAlchemy Session scoped to the tenant schema.
  """
  schema = _sanitize_schema(graph_id)
  session: Session = _get_session_factory()()
  try:
    session.execute(text(f"SET search_path TO {schema}, public"))
    yield session
    session.commit()
  except Exception:
    session.rollback()
    raise
  finally:
    session.close()


def provision_tenant_schema(graph_id: str) -> None:
  """Create all tenant tables in a new PostgreSQL schema for this graph_id.

  Called lazily on first extension access for a graph. Creates the schema
  and all tenant-scoped tables (accounts, transactions, entries, etc.)
  using ExtensionsBase metadata.

  The public schema tables (fiscal_periods, generate_prefixed_id function)
  are managed by Alembic migrations, not this function.

  Args:
      graph_id: The graph ID to create a schema for.
  """
  schema = _sanitize_schema(graph_id)
  engine = _get_engine()

  # Import models to ensure they're registered on ExtensionsBase.metadata
  import robosystems.models.extensions  # noqa: F401

  # Collect tenant tables (those without an explicit schema = public schema objects)
  tenant_tables = [
    table for table in ExtensionsBase.metadata.sorted_tables if table.schema is None
  ]

  with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    # Use schema_translate_map to create tables in the tenant schema.
    # Without this, create_all finds existing tables in public and skips them.
    tenant_conn = conn.execution_options(schema_translate_map={None: schema})
    ExtensionsBase.metadata.create_all(bind=tenant_conn, tables=tenant_tables)

    conn.commit()
