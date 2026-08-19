"""Extensions OLTP database engine and session management.

Separate from the platform database (database.py) because:
- Different DeclarativeBase (ExtensionsBase vs Base/Model)
- Schema-per-graph-id multi-tenancy (SET search_path per request)
- Independent connection pool and lifecycle

The extensions database stores domain data (entities, elements, transactions,
positions, trades, etc.) shared across all extension modules (roboledger,
roboinvestor; future RoboX modules to be added when their design and OLTP
land) with schema-per-graph tenancy.
"""

import re
import threading
from contextlib import contextmanager

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import DeclarativeBase, Session, SessionTransaction, sessionmaker

from robosystems.config import env

# Graph IDs are validated upstream (GRAPH_ID_PATTERN: kg + 16+ hex chars)
# but we re-validate here for defense in depth against SQL injection.
_VALID_SCHEMA_PATTERN = re.compile(r"^kg[0-9a-f]{16,}$")

# Five minutes: longer than any legitimate pause inside an extensions
# transaction (a close's QuickBooks publish is the slowest, and it commits
# between batches), far shorter than "until the ECS task is replaced".
IDLE_IN_TRANSACTION_TIMEOUT_MS = 5 * 60 * 1000


# Sentinel used in place of a real graph_id when the caller is routing to
# the taxonomy library. `extensions_session(LIBRARY_GRAPH_ID)` binds the
# session's search_path to `public`; the GraphQL context stamps it as the
# graph_type + schema_extension name; `check_graph_access` short-circuits
# on it (any authenticated user can read the library). Defined here so
# all sentinel call sites share one string and renames don't drift.
LIBRARY_GRAPH_ID = "library"

# The schema extensions that give a graph an OLTP tenant schema. A graph
# carrying any of these must have its schema provisioned at creation — an
# extensions-flagged graph with no schema is not "empty", it is a session
# whose bind is refused (see `_bind_statement`).
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
  """Create the extensions database engine.

  Lazy creation to avoid connection attempts during import
  in contexts that don't need the extensions database (e.g., graph instances).
  """
  # Pool sizing is SSM-tunable (was hardcoded 3 + 5 = 8, which a single client
  # could exhaust and wedge the API). Lazy import keeps the tuning/SSM layer out
  # of this module's import path until an engine is actually created.
  from robosystems.config.tuning import TuningConfig

  return create_engine(
    get_extensions_database_url(),
    pool_size=TuningConfig.get_extensions_pool_size(),
    max_overflow=TuningConfig.get_extensions_max_overflow(),
    pool_timeout=TuningConfig.get_database_pool_timeout(),
    pool_recycle=TuningConfig.get_database_pool_recycle(),
    pool_pre_ping=True,
    echo=env.DATABASE_ECHO,
    # A session that opened a transaction, took row locks (`FOR UPDATE`, the
    # period fence's row locks) and then went idle — a task killed mid-request,
    # a leaked connection — blocked every writer of those rows for as long as
    # the connection lived. Postgres closes such a session after this long.
    # Idle *inside* a transaction only; a busy statement (a long loader sync)
    # is untouched, and so is a pooled connection idle between transactions.
    # No `statement_timeout` here on purpose: loader syncs and materialization
    # reads legitimately run long, and a wrong value there is an outage.
    connect_args={
      "options": (
        f"-c idle_in_transaction_session_timeout={IDLE_IN_TRANSACTION_TIMEOUT_MS}"
      )
    },
  )


# Lazy engine — created on first use. Guarded by a lock: operation runners,
# Dagster sensor threads and the MCP server can all reach a cold engine at
# once, and without the lock each loser of the race builds (and leaks) a pool.
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

  Session-scoped advisory locks that must survive ``Session.commit()``
  cannot ride the session's connection — commit returns that connection
  to the pool, and a pooled checkout of a connection still holding
  ``pg_advisory_lock`` would leak the lock to the next borrower. Those
  lockers check out a dedicated connection from this engine and unlock
  (or invalidate) before returning it.
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
  """Validate graph_id is safe for use as a PostgreSQL schema name.

  Graph IDs follow the pattern kg + 16+ hex chars. This function
  validates that pattern to prevent SQL injection in SET search_path.

  Raises:
      ValueError: If graph_id doesn't match the expected pattern.
  """
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

  For a tenant, the bind is guarded: PostgreSQL accepts a ``search_path``
  whose first entry does not exist and silently skips it, and ``public``
  holds a live copy of every tenant table (it is the template new tenants
  are copied from). A session bound to a schema that was never provisioned,
  or that teardown has already dropped, would therefore run every
  unqualified statement against the shared template tables — no error, no
  audit trail, one tenant's rows landing where every tenant's session can
  read them. So the schema is required to exist, in the same round trip as
  the ``SET LOCAL``, and its absence raises ``invalid_schema_name``
  (``3F000``): the same failure a genuinely missing schema produces, which
  the surfaces already translate to "not initialized" (404).

  ``statement_timeout_ms`` adds a ``SET LOCAL statement_timeout`` ahead of
  the guard, so every statement in the transaction — the guard included —
  is bounded. Also ``LOCAL``: it ends with the transaction and is never left
  on the pooled connection.
  """
  statements: list[str] = []
  if statement_timeout_ms:
    statements.append(f"SET LOCAL statement_timeout = {int(statement_timeout_ms)}")
  if tenant_schema is not None:
    # Validated here as well as by the caller: this is the interpolation, and
    # the guarantee should not depend on statement order in
    # `extensions_session`.
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

  ``SET search_path`` is connection state, and a ``Session`` does not keep
  its connection: ``commit()`` returns it to the pool and the next statement
  checks out whichever connection the pool hands back. A single ``SET`` at
  the top of the session therefore covers exactly one transaction. A command
  that commits mid-flow and keeps going — close does, around its QuickBooks
  publish — would run the rest on a connection last used by some other
  tenant, or on a fresh one bound to nothing.

  So the binding is re-applied on every transaction the session begins,
  from ``after_begin``, and it is ``SET LOCAL``: scoped to that transaction,
  gone at commit or rollback, never left on a pooled connection for the
  next borrower to inherit. Nested (savepoint) transactions inherit the
  outer transaction's setting and are skipped.

  When ``tenant_schema`` is given the bind is fail-closed: it refuses to
  begin a transaction on a schema that does not exist — see
  :func:`_bind_statement` for why falling through to ``public`` is the
  failure being prevented.

  ``statement_timeout_ms`` bounds every statement the session runs, per
  transaction and per statement (a session that commits mid-flow and keeps
  going gets the bound re-applied with the bind). ``None`` leaves the
  server default in force.
  """
  stmt = text(_bind_statement(search_path, tenant_schema, statement_timeout_ms))

  @event.listens_for(session, "after_begin")
  def _stamp(_session: Session, transaction: SessionTransaction, connection) -> None:
    if transaction.nested:
      return
    connection.execute(stmt)


# Sentinel default for `extensions_session(statement_timeout_ms=...)`: resolve
# the interactive ceiling from tuning at open time. Distinct from `None`,
# which is the explicit opt-out for bulk work.
_INTERACTIVE_TIMEOUT = -1


def interactive_statement_timeout_ms() -> int | None:
  """The per-statement ceiling an interactive extensions session runs under.

  Read from tuning on every call (the tuning layer caches), so an SSM change
  reaches new sessions without a restart. ``0`` disables the ceiling.
  """
  from robosystems.config.tuning import TuningConfig

  timeout_ms = TuningConfig.get_extensions_statement_timeout_ms()
  return timeout_ms if timeout_ms > 0 else None


@contextmanager
def extensions_session(
  graph_id: str, *, statement_timeout_ms: int | None = _INTERACTIVE_TIMEOUT
):
  """Context manager providing a schema-scoped session for a tenant.

  Binds the session's search_path to '{graph_id}, public' so tenant tables
  resolve in the graph_id schema and shared tables resolve from public. The
  binding holds for the life of the session, across any commit inside it —
  see :func:`bind_search_path` for why that is not the same as one ``SET``.
  The bind is fail-closed: a graph whose schema does not exist (never
  provisioned, or already torn down) raises ``invalid_schema_name`` on the
  session's first statement instead of falling through to ``public``.

  Every statement is bounded by a ``statement_timeout``. The engine's pool
  is one pool for every tenant, and a statement left to run unbounded holds
  a shared connection for the duration — a legal-but-expensive aggregate on
  one tenant's data becomes every other tenant's queue-pool wait. The
  default is the interactive ceiling (`interactive_statement_timeout_ms`,
  SSM-tunable, 30s); a bulk path whose single statements legitimately run
  longer — a loader's full sync, a migration-time backfill — passes
  ``statement_timeout_ms=None`` to opt out, or a larger explicit value. The
  ceiling is per statement, not per session: a command that commits between
  steps and continues is not cut short by it. A cancelled statement raises
  ``OperationalError`` with SQLSTATE ``57014``; see `is_statement_timeout`.

  Special case: `graph_id="library"` routes to the taxonomy library
  (read-only; library content currently lives in the `public` schema,
  but the sentinel name is the stable API identity). No tenant schema
  binding.

  Usage:
      with extensions_session("kg0123456789abcdef") as session:
          accounts = session.execute(select(Account)).scalars().all()

      with extensions_session("library") as session:
          taxonomies = session.execute(select(Taxonomy)).scalars().all()

  Args:
      graph_id: The graph ID that maps to a PostgreSQL schema, or the
          `"library"` sentinel for the taxonomy library.

  Yields:
      A SQLAlchemy Session scoped to the tenant schema (or library).
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
  """Whether ``exc`` is PostgreSQL cancelling a statement for exceeding
  ``statement_timeout`` (SQLSTATE ``57014``, ``query_canceled``) — the
  failure an interactive session's ceiling produces. Surfaces translate it
  to a timeout the client can act on rather than a generic database fault.
  """
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
  # Mirror 0002's _IMMUTABLE_TABLES so fresh tenant provisioning applies
  # triggers to the full library surface. Omitting rows here leaves new
  # graphs with weaker guarantees than migration-backfilled ones.
  "traits",
  "element_traits",
  "classifications",
  "association_classifications",
  "rules",
  # Reporting Style composition. Library-seeded rows
  # pin Networks per statement_type for each Style; tenant writes are
  # blocked so customer-authored Styles use their own non-seeded rows.
  "reporting_style_networks",
)


def _install_library_immutability_triggers(conn, schema: str) -> None:
  """Attach BEFORE UPDATE/DELETE triggers keyed on created_by='library-seeder'.

  Mirrors the migration at ``0003_library_immutability_and_tenant_backfill``
  so newly provisioned tenant schemas receive the same protection existing
  ones got during backfill. The PL/pgSQL function itself lives in the
  public schema and is created by that migration; provisioning only
  attaches triggers (no function DDL here).

  Also installs the BEFORE-INSERT guard on ``associations`` that blocks
  tenant arc inserts into library-seeded structures.
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
  """Align tenant-template CHECKs with the library's vocabulary.

  Matches the widening applied in the 0002 migration so a fresh provision
  lands in the same state as a backfilled tenant. The vocabularies come from
  the models — one source for the CHECK the model declares and the CHECK
  this installs, so the two cannot drift again (they had: the model lists
  were missing values this function admitted).
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
  """Provision the tenant schema only if it does not exist yet.

  ``provision_tenant_schema`` is idempotent but not free: its CHECK and
  trigger installs take AccessExclusive locks on every tenant table, so a
  caller that runs on every sync (the OLTP loader) must not re-run it
  against a schema that is already there — a concurrent report build or
  GraphQL read deadlocks against it and either the read or the whole load
  is aborted. Returns True when a schema was provisioned.
  """
  if tenant_schema_exists(graph_id):
    return False
  provision_tenant_schema(graph_id)
  return True


def provision_tenant_schema(graph_id: str) -> None:
  """Create all tenant tables in a new PostgreSQL schema for this graph_id.

  Called at graph creation (and by ``ensure_tenant_schema`` when a schema is
  missing). Creates the schema
  and all tenant-scoped tables (elements, transactions, entries, etc.)
  using ExtensionsBase metadata, then copies the canonical taxonomy
  library from ``public.*`` into the tenant schema per the graph's pin
  (or the default pin when none is set). Finally installs immutability
  triggers so library-seeded rows cannot be mutated from tenant scope.

  The public schema tables (fiscal_periods, generate_prefixed_id function,
  raise_library_immutable function) are managed by Alembic migrations,
  not this function.

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

  # Look up the graph's taxonomy_pin (or default) from the platform DB
  # before opening the extensions transaction, since they're separate
  # databases.
  from robosystems.database import platform_session
  from robosystems.models.core.graph.graph import Graph, GraphStatus
  from robosystems.taxonomy.pins import resolve_pin
  from robosystems.taxonomy.writer import copy_library_into_tenant

  with platform_session() as pdb:
    graph = pdb.get(Graph, graph_id)
    if graph is not None and (
      graph.status == GraphStatus.DEPROVISIONED.value or graph.deleted_at is not None
    ):
      # Teardown drops the schema before it deletes the graph's connections,
      # so a sync still in flight would otherwise re-create the schema and
      # write ledger rows into a tenant the platform no longer knows about.
      # `deleted_at` is stamped (and committed) at the start of teardown,
      # before any data is dropped, so the window between the drop and the
      # final status flip is covered too.
      raise TenantDeprovisionedError(graph_id)
    pin = resolve_pin(graph)

  with engine.connect() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))

    # Use schema_translate_map to create tables in the tenant schema.
    # Without this, create_all finds existing tables in public and skips them.
    tenant_conn = conn.execution_options(schema_translate_map={None: schema})
    ExtensionsBase.metadata.create_all(bind=tenant_conn, tables=tenant_tables)

    # Align CHECKs, copy library rows, then lock them. Order matters: the
    # copy runs before trigger install so library rows can land freely;
    # after the triggers are attached, subsequent UPDATE/DELETE on those
    # rows raises from tenant scope.
    _widen_library_checks(conn, schema)
    # Library copy is the one-shot step. Once the immutability triggers
    # are installed (below), re-running the copy raises from the
    # ``raise_insert_into_library_structure`` trigger because tenant
    # writes can't target library-seeded structure_ids — even though the
    # rows being copied ARE the library content. Detect "already copied"
    # via a sentinel query and skip; the trigger install + check widen
    # are themselves idempotent (DROP IF EXISTS internally) so they can
    # always run.
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
  """Every tenant schema present in the extensions database.

  Schemas whose name matches the tenant grammar (``kg`` + hex). Used by the
  orphan sweep — a schema with no live platform ``Graph`` row is a ghost a
  partial teardown left behind. Empty when no extension domain is enabled.
  """
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
  """Whether ``graph_id`` still has a tenant schema in the extensions DB.

  ``extensions_session`` refuses to bind to a missing schema (see
  ``_bind_statement``), so a session on a deprovisioned graph fails on its
  first statement with ``invalid_schema_name``. Cross-graph paths that must
  distinguish "the recipient was deprovisioned" from "something is broken"
  before opening a session at all — and report it as a per-target outcome
  rather than an error — ask up front with this; see ``_delete_shared_copy``
  and ``_share_to_target``.

  Returns ``False`` when no extension domain is enabled or when ``graph_id``
  is not a tenant-schema id (subgraphs share their parent's schema).
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
  """Drop a tenant's extensions OLTP schema and everything in it.

  The teardown counterpart to :func:`provision_tenant_schema`, called from the
  graph deprovisioning flow. Without it, a deprovisioned tenant's financial
  data (transactions, entries, facts, the per-tenant library copy, …) persists
  in the extensions database indefinitely. ``DROP SCHEMA … CASCADE`` removes
  the schema, every tenant table in it, and the per-schema immutability
  triggers that reference those tables.

  No-op (returns ``False``) when no extension domain is enabled (no extensions
  database to drop from) or when ``graph_id`` is not a tenant-schema id —
  subgraphs (``kg…_name``) share the parent graph's schema and have none of
  their own, so there is nothing to drop.

  Args:
      graph_id: The graph ID whose tenant schema should be dropped.

  Returns:
      ``True`` if a ``DROP SCHEMA`` executed, ``False`` if skipped.
  """
  if not env.EXTENSIONS_ENABLED:
    return False
  # Reuse the schema-name validator as a guard: a non-matching id (subgraph,
  # repository, etc.) has no tenant schema of its own → nothing to drop.
  if not _VALID_SCHEMA_PATTERN.match(graph_id):
    return False

  engine = _get_engine()
  with engine.connect() as conn:
    # graph_id is pattern-validated above (^kg[0-9a-f]{16,}$), safe to inline.
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{graph_id}" CASCADE'))
    conn.commit()
  return True
