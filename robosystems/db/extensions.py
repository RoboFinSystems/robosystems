"""Extensions OLTP engine and sessions: its own DeclarativeBase and pool,
with schema-per-graph-id tenancy via ``search_path``.
"""

import re
import threading
from contextlib import contextmanager

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import DeclarativeBase, Session, SessionTransaction, sessionmaker

from robosystems.config import env

# Re-validated here (defense in depth): graph ids are interpolated into SQL.
_VALID_SCHEMA_PATTERN = re.compile(r"^kg[0-9a-f]{16,}$")

# Longer than any legitimate pause inside a transaction (a close's QuickBooks
# publish commits between batches).
IDLE_IN_TRANSACTION_TIMEOUT_MS = 5 * 60 * 1000


# graph_id sentinel for the taxonomy library: binds search_path to `public`.
LIBRARY_GRAPH_ID = "library"

# Extensions whose graphs must have a tenant schema provisioned at creation;
# without one every session bind is refused (see `_bind_statement`).
TENANT_SCHEMA_EXTENSIONS: tuple[str, ...] = ("roboledger", "roboinvestor")


def needs_tenant_schema(schema_extensions) -> bool:
  """Whether a graph with these schema extensions gets a tenant schema."""
  return any(ext in TENANT_SCHEMA_EXTENSIONS for ext in (schema_extensions or ()))


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
  # Lazy import keeps the tuning/SSM layer off this module's import path.
  from robosystems.config.tuning import TuningConfig

  return create_engine(
    get_extensions_database_url(),
    pool_size=TuningConfig.get_extensions_pool_size(),
    max_overflow=TuningConfig.get_extensions_max_overflow(),
    pool_timeout=TuningConfig.get_database_pool_timeout(),
    pool_recycle=TuningConfig.get_database_pool_recycle(),
    pool_pre_ping=True,
    echo=env.DATABASE_ECHO,
    # A session idle inside a transaction (killed task, leaked connection)
    # would hold its row locks and block every writer; Postgres closes it.
    # No engine-level `statement_timeout`: loader syncs legitimately run long.
    connect_args={
      "options": (
        f"-c idle_in_transaction_session_timeout={IDLE_IN_TRANSACTION_TIMEOUT_MS}"
      )
    },
  )


# Lazy engine, locked: concurrent first use would otherwise build and leak pools.
_engine = None
_session_factory = None
_engine_lock = threading.Lock()


def _get_engine():
  global _engine
  if _engine is None:
    with _engine_lock:
      if _engine is None:
        if not env.EXTENSIONS_ENABLED:
          raise RuntimeError(
            "Extensions database access attempted but no extension domain is "
            "enabled. Set ROBOLEDGER_ENABLED=true or ROBOINVESTOR_ENABLED=true "
            "to enable the extensions OLTP database (the EXTENSIONS_ENABLED "
            "value is now derived from the per-domain flags)."
          )
        _engine = _create_extensions_engine()
  return _engine


def get_extensions_engine():
  """The shared extensions engine.

  Advisory locks that must survive ``Session.commit()`` need a dedicated
  connection from here (commit returns the session's connection to the pool,
  leaking the lock to the next borrower); unlock or invalidate before return.
  """
  return _get_engine()


def _get_session_factory():
  global _session_factory
  if _session_factory is None:
    engine = _get_engine()
    with _engine_lock:
      if _session_factory is None:
        _session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
  return _session_factory


class ExtensionsBase(DeclarativeBase):
  """Base class for all extension OLTP models."""

  pass


def _sanitize_schema(graph_id: str) -> str:
  """Return graph_id if it is safe as a schema name, else raise ValueError."""
  if not _VALID_SCHEMA_PATTERN.match(graph_id):
    raise ValueError(f"Invalid graph_id for schema name: {graph_id}")
  return graph_id


def _search_path_for(graph_id: str) -> str:
  """The ``search_path`` value a session for ``graph_id`` runs under."""
  if graph_id == LIBRARY_GRAPH_ID:
    return "public"
  return f"{_sanitize_schema(graph_id)}, public"


def _bind_statement(
  search_path: str,
  tenant_schema: str | None,
  statement_timeout_ms: int | None = None,
) -> str:
  """The SQL that binds one transaction to ``search_path``.

  Fail-closed for a tenant: PostgreSQL silently skips a missing first
  ``search_path`` entry, and ``public`` holds a copy of every tenant table,
  so a missing schema would send one tenant's rows where every tenant can
  read them. The guard raises ``invalid_schema_name`` (``3F000``), which the
  surfaces translate to "not initialized". The timeout precedes the guard so
  it is bounded too.
  """
  statements: list[str] = []
  if statement_timeout_ms:
    statements.append(f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}")
  if tenant_schema is not None:
    # Validated at the interpolation, independent of the caller.
    tenant_schema = _sanitize_schema(tenant_schema)
    statements.append(
      "DO $$ BEGIN "
      f"IF to_regnamespace('{tenant_schema}') IS NULL THEN "
      f"RAISE EXCEPTION 'tenant schema \"{tenant_schema}\" does not exist' "
      "USING ERRCODE = '3F000'; "
      "END IF; END $$"
    )
  statements.append(f"SET LOCAL search_path TO {search_path}")
  return "; ".join(statements)


def bind_search_path(
  session: Session,
  search_path: str,
  *,
  tenant_schema: str | None = None,
  statement_timeout_ms: int | None = None,
) -> None:
  """Bind ``session`` to ``search_path`` for every transaction it opens.

  ``search_path`` is connection state and ``commit()`` returns the connection
  to the pool, so a single ``SET`` covers only the first transaction. The
  bind is re-applied from ``after_begin`` as ``SET LOCAL``, so it never
  lingers on a pooled connection. Savepoints inherit it. ``None`` timeout
  leaves the server default.
  """
  stmt = text(_bind_statement(search_path, tenant_schema, statement_timeout_ms))

  @event.listens_for(session, "after_begin")
  def _stamp(_session: Session, transaction: SessionTransaction, connection) -> None:
    if transaction.nested:
      return
    connection.execute(stmt)


# Default: resolve the interactive ceiling at open time. `None` opts out.
_INTERACTIVE_TIMEOUT = -1


def interactive_statement_timeout_ms() -> int | None:
  """Per-statement ceiling for interactive sessions; tuning ``0`` disables it."""
  from robosystems.config.tuning import TuningConfig

  timeout_ms = TuningConfig.get_extensions_statement_timeout_ms()
  return timeout_ms if timeout_ms > 0 else None


@contextmanager
def extensions_session(
  graph_id: str, *, statement_timeout_ms: int | None = _INTERACTIVE_TIMEOUT
):
  """Session bound to ``{graph_id}, public`` (``public`` for ``"library"``),
  committed on exit.

  Fail-closed: a missing tenant schema raises ``invalid_schema_name`` on the
  first statement. Every statement is bounded by the interactive
  ``statement_timeout`` because one pool serves every tenant; bulk paths
  (loader syncs, backfills) pass ``statement_timeout_ms=None``. A cancelled
  statement raises SQLSTATE ``57014``; see `is_statement_timeout`.
  """
  search_path = _search_path_for(graph_id)
  tenant_schema = None if graph_id == LIBRARY_GRAPH_ID else graph_id
  if statement_timeout_ms == _INTERACTIVE_TIMEOUT:
    statement_timeout_ms = interactive_statement_timeout_ms()
  session: Session = _get_session_factory()()
  bind_search_path(
    session,
    search_path,
    tenant_schema=tenant_schema,
    statement_timeout_ms=statement_timeout_ms,
  )
  try:
    yield session
    session.commit()
  except Exception:
    session.rollback()
    raise
  finally:
    session.close()


STATEMENT_TIMEOUT_SQLSTATE = "57014"


def is_statement_timeout(exc: BaseException) -> bool:
  """Whether ``exc`` is a ``statement_timeout`` cancellation (``57014``)."""
  from sqlalchemy.exc import DBAPIError

  if not isinstance(exc, DBAPIError):
    return False
  return getattr(exc.orig, "pgcode", None) == STATEMENT_TIMEOUT_SQLSTATE


_LIBRARY_IMMUTABLE_TABLES = (
  "taxonomies",
  "elements",
  "element_labels",
  "element_references",
  "structures",
  "associations",
  # Must mirror migration 0002's _IMMUTABLE_TABLES.
  "traits",
  "element_traits",
  "classifications",
  "association_classifications",
  "rules",
  "reporting_style_networks",
)


def _install_library_immutability_triggers(conn, schema: str) -> None:
  """Attach the library-immutability triggers (mirrors migration 0003, which
  owns the trigger functions), plus the guard blocking tenant arc inserts
  into library-seeded structures.
  """
  for table in _LIBRARY_IMMUTABLE_TABLES:
    trigger = f"{table}_library_immutable"
    conn.execute(text(f'DROP TRIGGER IF EXISTS {trigger} ON "{schema}".{table}'))
    conn.execute(
      text(
        f"CREATE TRIGGER {trigger} "
        f'BEFORE UPDATE OR DELETE ON "{schema}".{table} '
        f"FOR EACH ROW EXECUTE FUNCTION public.raise_library_immutable()"
      )
    )
  conn.execute(
    text(
      f'DROP TRIGGER IF EXISTS raise_insert_into_library_structure ON "{schema}".associations'
    )
  )
  conn.execute(
    text(
      f"CREATE TRIGGER raise_insert_into_library_structure "
      f'BEFORE INSERT ON "{schema}".associations '
      f"FOR EACH ROW EXECUTE FUNCTION public.raise_insert_into_library_structure()"
    )
  )


def _widen_library_checks(conn, schema: str) -> None:
  """Align tenant CHECKs with the library vocabulary (mirrors migration 0002),
  using the models' value lists so the two cannot drift.
  """
  # Function-level import: the models import `ExtensionsBase` from here.
  from robosystems.models.extensions.association import ASSOCIATION_TYPE_VALUES
  from robosystems.models.extensions.element import ELEMENT_SOURCE_VALUES
  from robosystems.models.extensions.structure import BLOCK_TYPE_VALUES
  from robosystems.models.extensions.taxonomy import TAXONOMY_TYPE_VALUES

  def _in(column: str, values: tuple[str, ...]) -> str:
    return f"{column} IN (" + ", ".join(f"'{v}'" for v in values) + ")"

  widened_assoc = _in("association_type", ASSOCIATION_TYPE_VALUES)
  widened_source = _in("source", ELEMENT_SOURCE_VALUES)
  widened_taxonomy_type = _in("taxonomy_type", TAXONOMY_TYPE_VALUES)
  widened_block_type = _in("block_type", BLOCK_TYPE_VALUES)
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".associations '
      f"DROP CONSTRAINT IF EXISTS check_association_type"
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".associations '
      f"ADD CONSTRAINT check_association_type CHECK ({widened_assoc})"
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".elements DROP CONSTRAINT IF EXISTS check_element_source'
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".elements '
      f"ADD CONSTRAINT check_element_source CHECK ({widened_source})"
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".taxonomies DROP CONSTRAINT IF EXISTS check_taxonomy_type'
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".taxonomies '
      f"ADD CONSTRAINT check_taxonomy_type CHECK ({widened_taxonomy_type})"
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".structures DROP CONSTRAINT IF EXISTS check_block_type'
    )
  )
  conn.execute(
    text(
      f'ALTER TABLE "{schema}".structures '
      f"ADD CONSTRAINT check_block_type CHECK ({widened_block_type})"
    )
  )


class TenantDeprovisionedError(RuntimeError):
  """The graph is torn down; nothing may create or write its tenant schema."""

  def __init__(self, graph_id: str) -> None:
    self.graph_id = graph_id
    super().__init__(f"Graph {graph_id} is deprovisioned; refusing to provision")


def ensure_tenant_schema(graph_id: str) -> bool:
  """Provision the tenant schema only if missing; True when it did.

  Re-provisioning takes AccessExclusive locks on every tenant table and
  deadlocks against concurrent reads, so per-sync callers must use this.
  """
  if tenant_schema_exists(graph_id):
    return False
  provision_tenant_schema(graph_id)
  return True


def provision_tenant_schema(graph_id: str) -> None:
  """Create the tenant schema and tables, copy the taxonomy library per the
  graph's pin, and install the immutability triggers.

  Public-schema objects are owned by Alembic, not this function.
  """
  schema = _sanitize_schema(graph_id)
  engine = _get_engine()

  import robosystems.models.extensions  # noqa: F401

  # Tables without an explicit schema are tenant tables.
  tenant_tables = [
    table for table in ExtensionsBase.metadata.sorted_tables if table.schema is None
  ]

  from robosystems.database import platform_session
  from robosystems.models.core.graph.graph import Graph, GraphStatus
  from robosystems.taxonomy.pins import resolve_pin
  from robosystems.taxonomy.writer import copy_library_into_tenant

  with platform_session() as pdb:
    graph = pdb.get(Graph, graph_id)
    if graph is not None and (
      graph.status == GraphStatus.DEPROVISIONED.value or graph.deleted_at is not None
    ):
      # An in-flight sync must not re-create a torn-down schema. `deleted_at`
      # is committed before teardown drops anything, covering the gap before
      # the status flip.
      raise TenantDeprovisionedError(graph_id)
    pin = resolve_pin(graph)

  with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    # Without the translate map, create_all finds the public tables and skips.
    tenant_conn = conn.execution_options(schema_translate_map={None: schema})
    ExtensionsBase.metadata.create_all(bind=tenant_conn, tables=tenant_tables)

    # Copy before installing triggers. Once they exist a re-copy would be
    # refused by the insert guard, so an already-copied library is skipped;
    # the other two steps are idempotent.
    _widen_library_checks(conn, schema)
    library_copied = conn.execute(
      text(f"""
        SELECT 1 FROM {schema}.structures
        WHERE created_by = 'library-seeder'
        LIMIT 1
      """)
    ).scalar()
    if not library_copied:
      copy_library_into_tenant(conn, schema, pin)
    _install_library_immutability_triggers(conn, schema)

    conn.commit()


def list_tenant_schemas() -> list[str]:
  """Every ``kg…`` schema in the extensions DB (for the orphan sweep)."""
  if not env.EXTENSIONS_ENABLED:
    return []
  engine = _get_engine()
  with engine.connect() as conn:
    rows = conn.execute(
      text(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name ~ '^kg[0-9a-f]{16,}$' ORDER BY schema_name"
      )
    ).all()
  return [row[0] for row in rows]


def tenant_schema_exists(graph_id: str) -> bool:
  """Whether ``graph_id`` has a tenant schema.

  Lets cross-graph paths report a deprovisioned recipient as an outcome
  before a session would fail. False for non-tenant ids (subgraphs share
  their parent's schema).
  """
  if not env.EXTENSIONS_ENABLED:
    return False
  if not _VALID_SCHEMA_PATTERN.match(graph_id):
    return False

  engine = _get_engine()
  with engine.connect() as conn:
    return (
      conn.execute(
        text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :name"),
        {"name": graph_id},
      ).first()
      is not None
    )


def drop_tenant_schema(graph_id: str) -> bool:
  """``DROP SCHEMA … CASCADE`` for a deprovisioned tenant.

  Returns False (skipped) when extensions are off or ``graph_id`` is not a
  tenant id (subgraphs have no schema of their own).
  """
  if not env.EXTENSIONS_ENABLED:
    return False
  if not _VALID_SCHEMA_PATTERN.match(graph_id):
    return False

  engine = _get_engine()
  with engine.connect() as conn:
    # Pattern-validated above, safe to inline.
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{graph_id}" CASCADE'))
    conn.commit()
  return True
