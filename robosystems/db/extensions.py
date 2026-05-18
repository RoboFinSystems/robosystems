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
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from robosystems.config import env

# Graph IDs are validated upstream (GRAPH_ID_PATTERN: kg + 16+ hex chars)
# but we re-validate here for defense in depth against SQL injection.
_VALID_SCHEMA_PATTERN = re.compile(r"^kg[0-9a-f]{16,}$")


# Sentinel used in place of a real graph_id when the caller is routing to
# the taxonomy library. `extensions_session(LIBRARY_GRAPH_ID)` binds the
# session's search_path to `public`; the GraphQL context stamps it as the
# graph_type + schema_extension name; `check_graph_access` short-circuits
# on it (any authenticated user can read the library). Defined here so
# all sentinel call sites share one string and renames don't drift.
LIBRARY_GRAPH_ID = "library"


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
        "Extensions database access attempted but no extension domain is "
        "enabled. Set ROBOLEDGER_ENABLED=true or ROBOINVESTOR_ENABLED=true "
        "to enable the extensions OLTP database (the EXTENSIONS_ENABLED "
        "value is now derived from the per-domain flags)."
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
  session: Session = _get_session_factory()()
  try:
    if graph_id == LIBRARY_GRAPH_ID:
      session.execute(text("SET search_path TO public"))
    else:
      schema = _sanitize_schema(graph_id)
      session.execute(text(f"SET search_path TO {schema}, public"))
    yield session
    session.commit()
  except Exception:
    session.rollback()
    raise
  finally:
    session.close()


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
  # Reporting Style composition (Phase 1 of §3.2). Library-seeded rows
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
  """Align tenant-template CHECKs with the library's widened vocabulary.

  Matches the widening applied in the 0002 migration so a fresh provision
  lands in the same state as a backfilled tenant (admits ``equivalence``,
  ``general-special``, ``essence-alias`` association types and the
  ``fac``/``rs-gaap`` element sources, the rules taxonomy type, and the
  full Information Block structure-type vocabulary that the library uses).
  """
  widened_assoc = (
    "association_type IN ("
    "'presentation', 'calculation', 'mapping', "
    "'equivalence', 'general-special', 'essence-alias', "
    # 'definition' arcs land from rs-gaap-disclosure-mechanics,
    # rs-gaap-reporting-checklist, and rs-gaap-reporting-styles.
    "'definition', "
    # 'derivation' arcs map BS leaves to their CF default change tags
    # (rs-gaap-calculations).
    "'derivation'"
    ")"
  )
  widened_source = (
    "source IN ("
    "'fac', 'rs-gaap', 'us-gaap', 'ifrs', "
    "'quickbooks', 'xero', 'plaid', 'native', 'import', 'system', "
    # rs-gaap-base framework extension packages (Phase C) anchored to
    # sibling namespaces of rs-gaap.
    "'disclosures', 'checklist', 'styles'"
    ")"
  )
  widened_taxonomy_type = (
    "taxonomy_type IN ("
    # 'reporting' retained transitionally for any rows copied from an
    # un-backfilled public schema; tenant writes use
    # 'reporting_standard' / 'reporting_extension' / 'custom_ontology'.
    "'chart_of_accounts', 'reporting', 'mapping', 'schedule', "
    "'trait-vocabulary', 'trait-assignment', "
    "'classification-vocabulary', 'classification-assignment', 'rules', "
    "'reporting_standard', 'reporting_extension', 'custom_ontology'"
    ")"
  )
  # Must stay in sync with both the SQLAlchemy model
  # (``models/extensions/structure.py``) and the platform migration
  # (``migrations/extensions/versions/0002_taxonomy_library.py``).
  # ``copy_library_into_tenant`` mirrors rows from ``public.structures``
  # — a tenant-side CHECK narrower than public's silently fails graph
  # creation when the library introduces a new block_type value.
  widened_block_type = (
    "block_type IN ("
    # Renderable financial-statement presentations
    "'income_statement', 'balance_sheet', "
    "'cash_flow_statement', 'equity_statement', "
    "'comprehensive_income', "
    # Domain-specific working-paper / schedule patterns
    "'schedule', 'rollforward', 'reconciliation', 'policy', 'metric', "
    # CoA + CoA→GAAP mapping
    "'chart_of_accounts', 'coa_mapping', "
    # Reference-taxonomy structure kinds (XBRL network roles distinct
    # from presentation): formal calculation rules, named SEC/regulatory
    # disclosures, crosswalks between taxonomies.
    "'validation_rules', 'regulatory_disclosure', 'taxonomy_mapping', "
    # Reporting Style — the bundle a company picks (Phase 1 of §3.2);
    # composes Networks per statement_type via reporting_style_networks.
    "'reporting_style', "
    # Escape hatch
    "'custom'"
    ")"
  )
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


def provision_tenant_schema(graph_id: str) -> None:
  """Create all tenant tables in a new PostgreSQL schema for this graph_id.

  Called lazily on first extension access for a graph. Creates the schema
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
  from robosystems.models.core.graph.graph import Graph
  from robosystems.taxonomy.pins import resolve_pin
  from robosystems.taxonomy.writers.tenant_writer import copy_library_into_tenant

  with platform_session() as pdb:
    graph = pdb.get(Graph, graph_id)
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
