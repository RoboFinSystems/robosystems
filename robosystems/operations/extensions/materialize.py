"""Extensions materialization: PostgreSQL OLTP -> DuckDB staging -> LadybugDB.

Connector-agnostic — it materializes whatever is in the extensions tenant
schema, whichever connector (QuickBooks, Xero, Plaid, native) wrote it.

DuckDB's ``postgres_scanner`` reads the extensions database directly and shapes
each OLTP table into a graph-shaped staging table; the handoff to LadybugDB is
an Arrow record-batch stream (DuckDB result vectors -> Arrow -> COPY), with no
intermediate file. All of it runs on the instance that holds the graph::

    ExtensionsMaterializer.materialize(graph_id, entity_id)
      -> get_graph_client(graph_id, operation_type="write")   # routes to EC2
      -> client.query_table(sql)                              # -> staging table
      -> client.materialize_table(table_name)                 # -> LadybugDB

Order is a correctness constraint, not an optimization: ``NODE_TABLES`` runs
before ``RELATIONSHIP_TABLES``, and both lists are ordered so an edge's
endpoints are always staged first.
"""

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
  from robosystems.graph_api.client.client import GraphClient

from robosystems.logger import logger

# Association types the graph renderer needs. Anything omitted here exists in
# OLTP but is invisible to the graph — dropping `mapping`, for instance, empties
# every report while the CoA mappings still sit in PostgreSQL. Used as
# ``WHERE association_type IN <_MATERIALIZED_ASSOCIATION_TYPES>`` in the four
# SQL strings below.
#   presentation     — rendering hierarchies on Reporting Style structures
#   mapping          — CoA → rs-gaap projection
#   calculation      — XBRL rollup arcs
#   general-special  — IS-A inheritance
#   equivalence      — FAC ↔ rs-gaap bridge
#   definition       — XBRL definition arcs (dimensions, hypercubes)
#   derivation       — derived-from arcs
#   has-part         — cm:Debit/cm:Credit posting-role arcs (schedule legs)
_MATERIALIZED_ASSOCIATION_TYPES: tuple[str, ...] = (
  "presentation",
  "mapping",
  "calculation",
  "general-special",
  "equivalence",
  "definition",
  "derivation",
  "has-part",
)
_MATERIALIZED_ASSOCIATION_TYPES_SQL = (
  "(" + ", ".join(f"'{t}'" for t in _MATERIALIZED_ASSOCIATION_TYPES) + ")"
)


@dataclass
class MaterializeResult:
  """Result of a ledger materialization run."""

  graph_id: str
  status: str = "success"
  tables_staged: list[str] = field(default_factory=list)
  tables_materialized: list[str] = field(default_factory=list)
  total_rows: int = 0
  duration_ms: float = 0
  errors: list[str] = field(default_factory=list)


# Node tables in materialization order
NODE_TABLES = [
  "Entity",
  "Element",
  # REA primitives (base ontology).
  # Agent before Event so EVENT_INVOLVES_AGENT can reference it. Event
  # before Entry so EVENT_TRIGGERS_TRANSACTION (Event→Transaction) and
  # the entry-side audit chain land in the right order.
  "Agent",
  "Event",
  "Transaction",
  "Entry",
  "LineItem",
  "Dimension",
  "Structure",
  "Association",
  "Trait",
  # Reporting layer
  "Taxonomy",
  "Report",
  "Period",
  "Unit",
  "Fact",
  "FactSet",
  # Investor layer (empty tables if roboinvestor not enabled — that's fine)
  "Portfolio",
  "Security",
  "Position",
]

# Relationship tables in materialization order (nodes must exist first)
RELATIONSHIP_TABLES = [
  # REA edges (base ontology — Entity, Agent, Event, Element all exist by here)
  "ENTITY_HAS_AGENT",
  "ENTITY_HAS_EVENT",
  "EVENT_INVOLVES_AGENT",
  "EVENT_AFFECTS_RESOURCE",
  "EVENT_OBLIGATED_BY_EVENT",
  "EVENT_DISCHARGES_EVENT",
  "EVENT_REPLACES_EVENT",
  # roboledger transaction edges
  "ENTITY_HAS_TRANSACTION",
  "EVENT_TRIGGERS_TRANSACTION",
  "TRANSACTION_HAS_ENTRY",
  "ENTRY_HAS_LINE_ITEM",
  "LINE_ITEM_RELATES_TO_ELEMENT",
  "STRUCTURE_HAS_ASSOCIATION",
  "ASSOCIATION_HAS_FROM_ELEMENT",
  "ASSOCIATION_HAS_TO_ELEMENT",
  "ELEMENT_HAS_TRAIT",
  "TRANSACTION_HAS_DIMENSION",
  "ENTRY_HAS_DIMENSION",
  "LINE_ITEM_HAS_DIMENSION",
  "ENTRY_FROM_SCHEDULE",
  # Base ontology — entity ↔ taxonomy
  "ENTITY_HAS_TAXONOMY",
  "TAXONOMY_EXTENDS_TAXONOMY",
  # Reporting layer
  "STRUCTURE_HAS_TAXONOMY",
  "ENTITY_HAS_REPORT",
  "REPORT_USES_TAXONOMY",
  "REPORT_HAS_FACT",
  "FACT_HAS_ELEMENT",
  "FACT_HAS_PERIOD",
  "FACT_HAS_UNIT",
  "FACT_HAS_ENTITY",
  "STRUCTURE_HAS_FACT_SET",
  "REPORT_HAS_FACT_SET",
  "FACT_SET_CONTAINS_FACT",
  # Investor layer
  "ENTITY_HAS_PORTFOLIO",
  "PORTFOLIO_HAS_POSITION",
  "POSITION_IN_SECURITY",
  "ENTITY_ISSUES_SECURITY",
]

# Table → extension mapping. Tables marked "base" are always materialized
# (their corresponding graph tables come from schemas/base.py and exist on
# any extensions graph). Tables marked with an extension name are only
# materialized when that extension is enabled for the target graph —
# otherwise their graph tables don't exist and materialization would fail
# with "Table does not exist."
#
# For relationships, the rule is "whichever endpoint's extension is more
# specific." ENTITY_HAS_PORTFOLIO has Entity (base) → Portfolio (investor),
# so the edge belongs to roboinvestor because Portfolio's node table only
# exists when roboinvestor is installed.
TABLE_EXTENSIONS: dict[str, str] = {
  # ── Nodes ────────────────────────────────────────────────────────────
  "Entity": "base",
  "Element": "base",
  "Dimension": "base",
  "Structure": "base",
  "Association": "base",
  "Trait": "base",
  "Taxonomy": "base",
  "Period": "base",
  "Unit": "base",
  # REA primitives (base — universal across RoboX extensions)
  "Agent": "base",
  "Event": "base",
  # roboledger nodes
  "Transaction": "roboledger",
  "Entry": "roboledger",
  "LineItem": "roboledger",
  "Report": "roboledger",
  "Fact": "roboledger",
  "FactSet": "roboledger",
  # roboinvestor nodes
  "Portfolio": "roboinvestor",
  "Security": "roboinvestor",
  "Position": "roboinvestor",
  # ── Relationships ────────────────────────────────────────────────────
  # Base ontology edges (both endpoints are base nodes)
  "ENTITY_HAS_TAXONOMY": "base",
  "TAXONOMY_EXTENDS_TAXONOMY": "base",
  "STRUCTURE_HAS_TAXONOMY": "base",
  "STRUCTURE_HAS_ASSOCIATION": "base",
  "ASSOCIATION_HAS_FROM_ELEMENT": "base",
  "ASSOCIATION_HAS_TO_ELEMENT": "base",
  "ELEMENT_HAS_TRAIT": "base",
  # REA edges (base — Entity/Agent/Event/Element all in base)
  "ENTITY_HAS_AGENT": "base",
  "ENTITY_HAS_EVENT": "base",
  "EVENT_INVOLVES_AGENT": "base",
  "EVENT_AFFECTS_RESOURCE": "base",
  "EVENT_OBLIGATED_BY_EVENT": "base",
  "EVENT_DISCHARGES_EVENT": "base",
  "EVENT_REPLACES_EVENT": "base",
  # roboledger edges
  "ENTITY_HAS_TRANSACTION": "roboledger",
  "EVENT_TRIGGERS_TRANSACTION": "roboledger",
  "TRANSACTION_HAS_ENTRY": "roboledger",
  "ENTRY_HAS_LINE_ITEM": "roboledger",
  "LINE_ITEM_RELATES_TO_ELEMENT": "roboledger",
  "TRANSACTION_HAS_DIMENSION": "roboledger",
  "ENTRY_HAS_DIMENSION": "roboledger",
  "LINE_ITEM_HAS_DIMENSION": "roboledger",
  "ENTRY_FROM_SCHEDULE": "roboledger",
  "ENTITY_HAS_REPORT": "roboledger",
  "REPORT_USES_TAXONOMY": "roboledger",
  "REPORT_HAS_FACT": "roboledger",
  "FACT_HAS_ELEMENT": "roboledger",
  "FACT_HAS_PERIOD": "roboledger",
  "FACT_HAS_UNIT": "roboledger",
  "FACT_HAS_ENTITY": "roboledger",
  "STRUCTURE_HAS_FACT_SET": "roboledger",
  "REPORT_HAS_FACT_SET": "roboledger",
  "FACT_SET_CONTAINS_FACT": "roboledger",
  # roboinvestor edges
  "ENTITY_HAS_PORTFOLIO": "roboinvestor",
  "PORTFOLIO_HAS_POSITION": "roboinvestor",
  "POSITION_IN_SECURITY": "roboinvestor",
  "ENTITY_ISSUES_SECURITY": "roboinvestor",
}


def _filter_tables_for_extensions(
  tables: list[str], enabled_extensions: set[str]
) -> list[str]:
  """Return only the tables whose owning extension is enabled.

  Tables mapped to "base" are always included. Tables mapped to a specific
  extension are included only if that extension is in `enabled_extensions`.
  Unknown tables (not in TABLE_EXTENSIONS) are included by default — prefer
  "fail loud with unknown table" over "silently skip a typo."
  """
  effective = set(enabled_extensions) | {"base"}
  return [t for t in tables if TABLE_EXTENSIONS.get(t, "base") in effective]


# Schema tables intentionally NOT materialized by the extensions pipeline.
# Each entry falls into one of two categories:
#   (1) Populated via SEC XBRL ingestion (adapters/sec/...), not from OLTP.
#       These live in schemas/base.py or schemas/extensions/roboledger.py but
#       the extensions materializer correctly skips them — they're written
#       by the SEC processor when filings are ingested, not from
#       postgres_scanner.
#   (2) Declared in schemas/extensions/roboinvestor.py for schema
#       completeness but not yet backed by OLTP tables or materialization
#       SQL — not yet implemented.
_UNMATERIALIZED_TABLES: frozenset[str] = frozenset(
  {
    # Category 1: SEC XBRL-only (not written from extensions OLTP)
    "Label",
    "Reference",
    "Classification",
    "ELEMENT_HAS_LABEL",
    "ELEMENT_HAS_REFERENCE",
    "TAXONOMY_HAS_LABEL",
    "TAXONOMY_HAS_REFERENCE",
    "DIMENSION_HAS_AXIS_ELEMENT",
    "DIMENSION_HAS_MEMBER_ELEMENT",
    "ASSOCIATION_HAS_CLASSIFICATION",
    "FACT_HAS_DIMENSION",
    # Category 2: roboinvestor schema-declared, OLTP not yet wired
    "Trade",
    "Benchmark",
    "MarketData",
    "PORTFOLIO_HAS_TRADE",
    "TRADE_INVOLVES_SECURITY",
    "TRADE_CREATES_POSITION",
    "PORTFOLIO_BENCHMARKED_AGAINST",
    "SECURITY_HAS_MARKET_DATA",
  }
)


def _get_all_extension_schema_tables() -> set[str]:
  """Return the union of node+relationship names across every extension the
  extensions pipeline is expected to handle.

  Composes schemas for both roboledger and roboinvestor because the
  extensions materializer serves both products from a shared pipeline.
  """
  from robosystems.schemas.loader import get_contextual_schema_loader

  tables: set[str] = set()
  for ext_name in ("roboledger", "roboinvestor"):
    loader = get_contextual_schema_loader("application", ext_name)
    tables |= set(loader.nodes.keys())
    tables |= set(loader.relationships.keys())
  return tables


def validate_materializer_against_schema() -> None:
  """Fail loud if the extensions materializer has drifted from the schema.

  Three drift modes to catch:

  1. Schema declares a table the materializer doesn't know about (and it's
     not in the SEC-only / deferred allow-list). Usually means someone added
     a node or edge to schemas/ and forgot to wire it into NODE_TABLES,
     RELATIONSHIP_TABLES, TABLE_EXTENSIONS, and _staging_sql().
  2. Materializer references a table not declared in any extension schema.
     Usually means a schema entry was removed but the materializer still has
     stale references.
  3. Materializer-registered table has no TABLE_EXTENSIONS entry. Means the
     extension-filter pass would treat it as "base" by default, which might
     be wrong.

  Called at test time as a consistency check. When it fails, the message
  tells you exactly what to fix.
  """
  schema_tables = _get_all_extension_schema_tables()
  materializer_tables = set(NODE_TABLES) | set(RELATIONSHIP_TABLES)

  missing_from_materializer = (
    schema_tables - materializer_tables - _UNMATERIALIZED_TABLES
  )
  stale_in_materializer = materializer_tables - schema_tables
  missing_extension_mapping = materializer_tables - set(TABLE_EXTENSIONS.keys())

  errors: list[str] = []

  if missing_from_materializer:
    errors.append(
      f"Schema declares tables the extensions materializer does not handle: "
      f"{sorted(missing_from_materializer)}.\n"
      f"  Fix: add an entry to NODE_TABLES or RELATIONSHIP_TABLES, a mapping "
      f"in TABLE_EXTENSIONS, and a SQL generator in _staging_sql(). If the "
      f"table is populated only via SEC XBRL ingestion (not extensions "
      f"OLTP), add it to _UNMATERIALIZED_TABLES instead."
    )

  if stale_in_materializer:
    errors.append(
      f"Materializer references tables not declared in any extension schema: "
      f"{sorted(stale_in_materializer)}.\n"
      f"  Fix: these were probably removed from a schema file — delete them "
      f"from NODE_TABLES/RELATIONSHIP_TABLES, TABLE_EXTENSIONS, and "
      f"_staging_sql() too."
    )

  if missing_extension_mapping:
    errors.append(
      f"Materializer tables without TABLE_EXTENSIONS mapping: "
      f"{sorted(missing_extension_mapping)}.\n"
      f"  Fix: add each to TABLE_EXTENSIONS with the owning extension name "
      f"('base', 'roboledger', or 'roboinvestor')."
    )

  if errors:
    raise RuntimeError(
      "Extensions materializer is out of sync with the schema layer:\n\n"
      + "\n\n".join(errors)
    )


def build_postgres_connstr() -> str:
  """Connection string for ``postgres_scan()`` against the extensions database.

  The scanner runs inside the *Graph API* process, so the host has to be
  reachable from there, not from the caller: the RDS endpoint in production,
  the Docker service hostname locally. Both come from ``DATABASE_ENDPOINT`` in
  that process's environment.
  """
  from robosystems.config import env

  url = env.EXTENSIONS_DATABASE_URL
  parsed = urlparse(url)

  host = parsed.hostname or "localhost"
  port = parsed.port or 5432
  user = parsed.username or "postgres"
  password = parsed.password or "postgres"
  dbname = parsed.path.lstrip("/").split("?")[0] or "extensions"

  connstr = f"dbname={dbname} user={user} password={password} host={host} port={port}"

  return connstr


def _staging_sql(graph_id: str, entity_id: str, connstr: str) -> dict[str, str]:
  """``{table_name: CREATE TABLE SQL}`` for every node and relationship table.

  Each statement builds a DuckDB table from ``postgres_scan()`` whose columns
  match the graph schema exactly, ready to COPY into LadybugDB.
  """
  c = connstr  # shorthand for SQL interpolation
  s = graph_id  # tenant schema name in the extensions database

  tables: dict[str, str] = {}

  # ── Node Tables ──────────────────────────────────────────────────────

  # Entity: minimal row so ENTITY_HAS_TRANSACTION relationships can reference it
  tables["Entity"] = f"""
    CREATE OR REPLACE TABLE Entity AS
    SELECT
      id                              AS identifier,
      uri,
      NULL::VARCHAR                   AS scheme,
      cik,
      ticker,
      exchange,
      name,
      legal_name,
      industry,
      entity_type,
      sic,
      sic_description,
      category,
      state_of_incorporation,
      fiscal_year_end,
      tax_id,
      lei,
      phone,
      website,
      status,
      is_parent,
      parent_entity_id,
      CAST(created_at AS VARCHAR)     AS created_at,
      CAST(updated_at AS VARCHAR)     AS updated_at
    FROM postgres_scan('{c}', '{s}', 'entities')
  """

  tables["Element"] = f"""
    CREATE OR REPLACE TABLE Element AS
    SELECT
      e.id                            AS identifier,
      CASE
        -- Adapter elements: the loader now derives and stores the qname at
        -- sync time (operations/extensions/loader.py); prefer the stored
        -- value so OLTP stays the identity authority. The prefixed-code
        -- fallback covers rows loaded before qnames were written — one
        -- re-sync heals them and retires the fallback per tenant.
        WHEN e.external_source = 'quickbooks' THEN COALESCE(e.qname, 'qb:' || e.code)
        WHEN e.external_source = 'xero' THEN COALESCE(e.qname, 'xero:' || e.code)
        WHEN e.external_source = 'plaid' THEN COALESCE(e.qname, 'plaid:' || e.code)
        -- Library/taxonomy concepts (rs-gaap, fac, us-gaap, cm, disclosures,
        -- styles, …) already carry a canonical namespaced qname; emit it
        -- verbatim. Re-prefixing would yield rl:rs-gaap:X, which no canonical
        -- consumer (build-fact-grid included) recognises. Native/import/system
        -- CoA accounts do take the 'rl:' tenant prefix.
        WHEN e.source NOT IN ('native', 'import', 'system')
          THEN COALESCE(e.qname, e.code)
        ELSE 'rl:' || e.code
      END                             AS qname,
      e.name,
      e.description,
      e.item_type                     AS item_type,
      e.balance_type                  AS balance,
      CASE WHEN e.is_placeholder THEN true ELSE false END AS is_abstract,
      false                           AS is_dimension_item,
      false                           AS is_domain_member,
      false                           AS is_hypercube_item,
      COALESCE(e.item_type = 'integer', false)  AS is_integer,
      -- NULL item_type (untyped) keeps the legacy numeric default:
      -- NULL IN (...) is NULL, COALESCE'd to true.
      COALESCE(e.item_type IN ('monetary', 'shares', 'decimal', 'integer'), true)
                                      AS is_numeric,
      COALESCE(e.item_type = 'shares', false)   AS is_shares,
      false                           AS is_fraction,
      COALESCE(e.item_type = 'text_block', false) AS is_textblock,
      NULL::VARCHAR                   AS uri,
      e.substitution_group            AS substitution_group,
      e.period_type                   AS period_type,
      NULL::VARCHAR                   AS type,
      NULL::VARCHAR                   AS canonical_concept,
      NULL::DOUBLE                    AS canonical_confidence,
      NULL::FLOAT[384]                AS embedding
    FROM postgres_scan('{c}', '{s}', 'elements') e
    WHERE e.is_active = true
  """

  tables["Trait"] = f"""
    CREATE OR REPLACE TABLE "Trait" AS
    SELECT
      id          AS identifier,
      category    AS category,
      type        AS type,
      source      AS source,
      confidence  AS confidence
    FROM postgres_scan('{c}', '{s}', 'traits')
  """

  # ── REA primitives (base ontology) ──────────────────────────────────
  # Agent + Event are universal across RoboX extensions. Skip JSONB
  # columns (Agent.address, Agent.metadata_, Event.metadata_) — graph
  # layer is a curated knowledge surface. Event.amount converts cents to
  # currency-major (matches Transaction.amount convention).

  tables["Agent"] = f"""
    CREATE OR REPLACE TABLE "Agent" AS
    SELECT
      id                              AS identifier,
      NULL::VARCHAR                   AS uri,
      agent_type,
      name,
      legal_name,
      tax_id,
      registration_number,
      duns,
      lei,
      email,
      phone,
      source,
      external_id,
      is_active,
      is_1099_recipient,
      CAST(created_at AS VARCHAR)     AS created_at,
      CAST(updated_at AS VARCHAR)     AS updated_at
    FROM postgres_scan('{c}', '{s}', 'agents')
  """

  tables["Event"] = f"""
    CREATE OR REPLACE TABLE "Event" AS
    SELECT
      id                              AS identifier,
      NULL::VARCHAR                   AS uri,
      event_type,
      event_category,
      event_class,
      event_action,
      resource_type,
      CAST(occurred_at AS VARCHAR)    AS occurred_at,
      CAST(effective_at AS VARCHAR)   AS effective_at,
      status,
      (status NOT IN ('voided', 'superseded')) AS is_live,
      source,
      external_id,
      external_url,
      CAST(amount AS DOUBLE) / 100.0  AS amount,
      currency,
      description,
      CAST(created_at AS VARCHAR)     AS created_at,
      created_by
    FROM postgres_scan('{c}', '{s}', 'events')
  """

  tables["Transaction"] = f"""
    CREATE OR REPLACE TABLE "Transaction" AS
    SELECT
      id                              AS identifier,
      NULL::VARCHAR                   AS uri,
      number,
      CAST(amount AS DOUBLE) / 100.0  AS amount,
      description,
      date,
      reference_number,
      type,
      currency,
      merchant_name,
      category,
      CASE WHEN status = 'pending' THEN true ELSE false END AS pending,
      (status <> 'void')              AS is_live,
      CAST(updated_at AS VARCHAR)     AS updated_at
    FROM postgres_scan('{c}', '{s}', 'transactions')
  """

  tables["Entry"] = f"""
    CREATE OR REPLACE TABLE Entry AS
    SELECT
      id                              AS identifier,
      NULL::VARCHAR                   AS uri,
      number,
      memo,
      posting_date,
      type,
      status,
      (status = 'posted')             AS is_live,
      reversal_of,
      provenance,
      CAST(updated_at AS VARCHAR)     AS updated_at
    FROM postgres_scan('{c}', '{s}', 'entries')
  """

  # LineItem has no status of its own; its liveness is its parent Entry's.
  # Denormalize (e.status = 'posted') as is_live via the entry join so ad-hoc /
  # AI aggregations anchored at LineItem can filter with `WHERE li.is_live`
  # without traversing back to Entry. entry_id is NOT NULL, so the inner join
  # drops no rows.
  tables["LineItem"] = f"""
    CREATE OR REPLACE TABLE LineItem AS
    SELECT
      li.id                                      AS identifier,
      NULL::VARCHAR                              AS uri,
      li.description,
      CAST(li.debit_amount AS DOUBLE) / 100.0    AS debit_amount,
      CAST(li.credit_amount AS DOUBLE) / 100.0   AS credit_amount,
      false                                      AS has_dimensions,
      0::BIGINT                                  AS dimension_count,
      (e.status = 'posted')                      AS is_live,
      CAST(li.updated_at AS VARCHAR)             AS updated_at
    FROM postgres_scan('{c}', '{s}', 'line_items') li
    JOIN postgres_scan('{c}', '{s}', 'entries') e ON e.id = li.entry_id
  """

  tables["Dimension"] = f"""
    CREATE OR REPLACE TABLE Dimension AS
    SELECT
      id                              AS identifier,
      dimension_type                  AS axis,
      value                           AS member,
      dimension_type,
      NULL::VARCHAR                   AS axis_uri,
      NULL::VARCHAR                   AS member_uri,
      NULL::VARCHAR                   AS type,
      false                           AS is_explicit,
      false                           AS is_typed
    FROM postgres_scan('{c}', '{s}', 'dimensions')
    WHERE is_active = true
  """

  # Structure: synthetic CoA + seed reporting structures (IS, BS, CF)
  tables["Structure"] = f"""
    CREATE OR REPLACE TABLE Structure AS
    -- Synthetic Chart of Accounts structure
    SELECT
      '{graph_id}_coa'                AS identifier,
      NULL::VARCHAR                   AS uri,
      NULL::VARCHAR                   AS network_uri,
      'Chart of Accounts'             AS definition,
      '000001'                        AS number,
      'ChartOfAccounts'               AS type,
      'Chart of Accounts'             AS name,
      'ChartOfAccounts'               AS canonical_type,
      1.0::DOUBLE                     AS canonical_confidence,
      NULL::FLOAT[384]                AS embedding
    UNION ALL
    -- Seed reporting structures (income_statement, balance_sheet)
    SELECT
      id                              AS identifier,
      NULL::VARCHAR                   AS uri,
      NULL::VARCHAR                   AS network_uri,
      description                     AS definition,
      NULL::VARCHAR                   AS number,
      block_type                  AS type,
      name,
      block_type                  AS canonical_type,
      1.0::DOUBLE                     AS canonical_confidence,
      NULL::FLOAT[384]                AS embedding
    FROM postgres_scan('{c}', '{s}', 'structures')
    -- Exclude only the real chart_of_accounts structure — the synthetic
    -- '{graph_id}_coa' node above stands in for it. coa_mapping IS
    -- materialized so the curated 'mapping' associations (CoA -> rs-gaap)
    -- have a valid parent Structure; excluding it left every
    -- STRUCTURE_HAS_ASSOCIATION edge with a dangling FK, failing the whole
    -- (transactional) COPY and leaving the table empty.
    WHERE block_type NOT IN ('chart_of_accounts')
      AND is_active = true
  """

  # Association: CoA parent-child pairs + seed presentation associations
  tables["Association"] = f"""
    CREATE OR REPLACE TABLE Association AS
    -- CoA parent-child hierarchy
    SELECT
      child.id || '_assoc'            AS identifier,
      'parent-child'                  AS arcrole,
      CAST(ROW_NUMBER() OVER (
        PARTITION BY child.parent_id ORDER BY child.code
      ) AS DOUBLE)                    AS order_value,
      'presentation'                  AS association_type,
      NULL::DOUBLE                    AS weight,
      NULL::VARCHAR                   AS root,
      NULL::VARCHAR                   AS preferred_label
    FROM postgres_scan('{c}', '{s}', 'elements') child
    INNER JOIN postgres_scan('{c}', '{s}', 'elements') parent
      ON child.parent_id = parent.id
    WHERE child.parent_id IS NOT NULL
      AND child.is_active = true
    UNION ALL
    -- Seed reporting associations (structure hierarchies)
    SELECT
      id                              AS identifier,
      COALESCE(arcrole, 'parent-child') AS arcrole,
      order_value,
      association_type,
      weight,
      NULL::VARCHAR                   AS root,
      NULL::VARCHAR                   AS preferred_label
    FROM postgres_scan('{c}', '{s}', 'associations')
    -- See _MATERIALIZED_ASSOCIATION_TYPES for the curated list.
    WHERE association_type IN {_MATERIALIZED_ASSOCIATION_TYPES_SQL}
  """

  # ── Relationship Tables ──────────────────────────────────────────────

  tables["ENTITY_HAS_TRANSACTION"] = f"""
    CREATE OR REPLACE TABLE ENTITY_HAS_TRANSACTION AS
    SELECT
      '{entity_id}'                   AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'transactions')
  """

  tables["TRANSACTION_HAS_ENTRY"] = f"""
    CREATE OR REPLACE TABLE TRANSACTION_HAS_ENTRY AS
    SELECT
      transaction_id                  AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'entries')
    WHERE transaction_id IS NOT NULL
  """

  tables["ENTRY_HAS_LINE_ITEM"] = f"""
    CREATE OR REPLACE TABLE ENTRY_HAS_LINE_ITEM AS
    SELECT
      entry_id                        AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'line_items')
  """

  tables["LINE_ITEM_RELATES_TO_ELEMENT"] = f"""
    CREATE OR REPLACE TABLE LINE_ITEM_RELATES_TO_ELEMENT AS
    SELECT
      id                              AS src,
      element_id                      AS dst
    FROM postgres_scan('{c}', '{s}', 'line_items')
  """

  tables["STRUCTURE_HAS_ASSOCIATION"] = f"""
    CREATE OR REPLACE TABLE STRUCTURE_HAS_ASSOCIATION AS
    -- CoA structure → CoA associations
    SELECT
      '{graph_id}_coa'                AS src,
      child.id || '_assoc'            AS dst
    FROM postgres_scan('{c}', '{s}', 'elements') child
    WHERE child.parent_id IS NOT NULL
      AND child.is_active = true
    UNION ALL
    -- Seed structures → seed associations
    SELECT
      structure_id                    AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'associations')
    -- See _MATERIALIZED_ASSOCIATION_TYPES for the curated list.
    WHERE association_type IN {_MATERIALIZED_ASSOCIATION_TYPES_SQL}
      -- Drop edges whose structure was not materialized (e.g. inactive) so a
      -- single dangling FK can't fail the whole transactional COPY. Structure
      -- nodes are staged before this edge table, so the semi-join is valid.
      AND structure_id IN (SELECT identifier FROM Structure)
  """

  tables["ASSOCIATION_HAS_FROM_ELEMENT"] = f"""
    CREATE OR REPLACE TABLE ASSOCIATION_HAS_FROM_ELEMENT AS
    -- CoA associations
    SELECT
      child.id || '_assoc'            AS src,
      child.parent_id                 AS dst
    FROM postgres_scan('{c}', '{s}', 'elements') child
    WHERE child.parent_id IS NOT NULL
      AND child.is_active = true
    UNION ALL
    -- Seed associations
    SELECT
      id                              AS src,
      from_element_id                 AS dst
    FROM postgres_scan('{c}', '{s}', 'associations')
    -- See _MATERIALIZED_ASSOCIATION_TYPES for the curated list.
    WHERE association_type IN {_MATERIALIZED_ASSOCIATION_TYPES_SQL}
  """

  tables["ASSOCIATION_HAS_TO_ELEMENT"] = f"""
    CREATE OR REPLACE TABLE ASSOCIATION_HAS_TO_ELEMENT AS
    -- CoA associations
    SELECT
      child.id || '_assoc'            AS src,
      child.id                        AS dst
    FROM postgres_scan('{c}', '{s}', 'elements') child
    WHERE child.parent_id IS NOT NULL
      AND child.is_active = true
    UNION ALL
    -- Seed associations
    SELECT
      id                              AS src,
      to_element_id                   AS dst
    FROM postgres_scan('{c}', '{s}', 'associations')
    -- See _MATERIALIZED_ASSOCIATION_TYPES for the curated list.
    WHERE association_type IN {_MATERIALIZED_ASSOCIATION_TYPES_SQL}
  """

  tables["ELEMENT_HAS_TRAIT"] = f"""
    CREATE OR REPLACE TABLE ELEMENT_HAS_TRAIT AS
    SELECT
      element_id                      AS src,
      trait_id                        AS dst
    FROM postgres_scan('{c}', '{s}', 'element_traits')
    -- Drop rows referencing an element/trait that was not materialized so a
    -- single dangling FK can't fail the whole transactional COPY. Element and
    -- Trait nodes are staged before this edge table.
    WHERE element_id IN (SELECT identifier FROM Element)
      AND trait_id IN (SELECT identifier FROM "Trait")
  """

  # Dimension junction tables (may be empty if no dimensions loaded yet)
  tables["TRANSACTION_HAS_DIMENSION"] = f"""
    CREATE OR REPLACE TABLE TRANSACTION_HAS_DIMENSION AS
    SELECT
      transaction_id                  AS src,
      dimension_id                    AS dst
    FROM postgres_scan('{c}', '{s}', 'transaction_dimensions')
  """

  tables["ENTRY_HAS_DIMENSION"] = f"""
    CREATE OR REPLACE TABLE ENTRY_HAS_DIMENSION AS
    SELECT
      entry_id                        AS src,
      dimension_id                    AS dst
    FROM postgres_scan('{c}', '{s}', 'entry_dimensions')
  """

  tables["LINE_ITEM_HAS_DIMENSION"] = f"""
    CREATE OR REPLACE TABLE LINE_ITEM_HAS_DIMENSION AS
    SELECT
      line_item_id                    AS src,
      dimension_id                    AS dst
    FROM postgres_scan('{c}', '{s}', 'line_item_dimensions')
  """

  tables["ENTRY_FROM_SCHEDULE"] = f"""
    CREATE OR REPLACE TABLE ENTRY_FROM_SCHEDULE AS
    SELECT
      id                              AS src,
      source_structure_id             AS dst
    FROM postgres_scan('{c}', '{s}', 'entries')
    WHERE source_structure_id IS NOT NULL
  """

  # ── REA edges ────────────────────────────────────────────────────────
  # Entity is the per-graph singleton; fan it out to every Agent / Event.
  # Sibling edges populated only when the OLTP column is non-null.

  tables["ENTITY_HAS_AGENT"] = f"""
    CREATE OR REPLACE TABLE ENTITY_HAS_AGENT AS
    SELECT
      '{entity_id}'                   AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'agents')
  """

  tables["ENTITY_HAS_EVENT"] = f"""
    CREATE OR REPLACE TABLE ENTITY_HAS_EVENT AS
    SELECT
      '{entity_id}'                   AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'events')
  """

  tables["EVENT_INVOLVES_AGENT"] = f"""
    CREATE OR REPLACE TABLE EVENT_INVOLVES_AGENT AS
    SELECT
      id                              AS src,
      agent_id                        AS dst
    FROM postgres_scan('{c}', '{s}', 'events')
    WHERE agent_id IS NOT NULL
  """

  tables["EVENT_AFFECTS_RESOURCE"] = f"""
    CREATE OR REPLACE TABLE EVENT_AFFECTS_RESOURCE AS
    SELECT
      id                              AS src,
      resource_element_id             AS dst
    FROM postgres_scan('{c}', '{s}', 'events')
    WHERE resource_element_id IS NOT NULL
  """

  tables["EVENT_OBLIGATED_BY_EVENT"] = f"""
    CREATE OR REPLACE TABLE EVENT_OBLIGATED_BY_EVENT AS
    SELECT
      id                              AS src,
      obligated_by_event_id           AS dst
    FROM postgres_scan('{c}', '{s}', 'events')
    WHERE obligated_by_event_id IS NOT NULL
  """

  tables["EVENT_DISCHARGES_EVENT"] = f"""
    CREATE OR REPLACE TABLE EVENT_DISCHARGES_EVENT AS
    SELECT
      id                              AS src,
      discharges_event_id             AS dst
    FROM postgres_scan('{c}', '{s}', 'events')
    WHERE discharges_event_id IS NOT NULL
  """

  tables["EVENT_REPLACES_EVENT"] = f"""
    CREATE OR REPLACE TABLE EVENT_REPLACES_EVENT AS
    SELECT
      id                              AS src,
      replaces_event_id               AS dst
    FROM postgres_scan('{c}', '{s}', 'events')
    WHERE replaces_event_id IS NOT NULL
  """

  # McCarthy bridge — the GL Transaction this Event triggered. OLTP
  # source is transactions.triggered_by_event_id (audit column from
  # migration 0005). Edge exists only for transactions originating from
  # an Event; manual-only transactions have no event.
  tables["EVENT_TRIGGERS_TRANSACTION"] = f"""
    CREATE OR REPLACE TABLE EVENT_TRIGGERS_TRANSACTION AS
    SELECT
      triggered_by_event_id           AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'transactions')
    WHERE triggered_by_event_id IS NOT NULL
  """

  # ── Reporting Layer ────────────────────────────────────────────────────
  # These tables are populated from reports and facts
  # (generated by the report builder). They may be empty if no reports
  # have been generated yet — that's fine, they'll be populated on next
  # materialization after report creation.

  # Scenario guard: fact sets stamped with a scenario_id are forecast
  # months (NULL means actuals — see FactSet.scenario_id). The graph
  # schema carries no scenario discriminator, so materializing them would
  # blend plan into history for every graph reader (fact grids, Cypher,
  # analytical views). Until an OLAP scenario leg exists, scenario facts
  # and fact sets stay OLTP-only: every projection below that reads
  # `facts` goes through this derived table, and every `fact_sets`
  # projection filters `scenario_id IS NULL` directly.
  actual_facts = (
    f"(SELECT f.* FROM postgres_scan('{c}', '{s}', 'facts') f "
    f"LEFT JOIN postgres_scan('{c}', '{s}', 'fact_sets') sfs "
    f"ON sfs.id = f.fact_set_id "
    f"WHERE sfs.scenario_id IS NULL)"
  )

  tables["Taxonomy"] = f"""
    CREATE OR REPLACE TABLE Taxonomy AS
    SELECT
      id                              AS identifier,
      namespace_uri                   AS uri,
      name,
      version,
      namespace_uri                   AS namespace,
      description,
      taxonomy_type
    FROM postgres_scan('{c}', '{s}', 'taxonomies')
  """

  tables["Report"] = f"""
    CREATE OR REPLACE TABLE Report AS
    SELECT
      id                              AS identifier,
      NULL::VARCHAR                   AS uri,
      name,
      NULL::VARCHAR                   AS accession_number,
      NULL::VARCHAR                   AS form,
      CAST(last_generated AS VARCHAR) AS filing_date,
      CAST(period_end AS VARCHAR)     AS report_date,
      NULL::VARCHAR                   AS acceptance_date,
      false                           AS is_inline_xbrl,
      NULL::VARCHAR                   AS xbrl_processor_version,
      true                            AS processed,
      CASE WHEN generation_status = 'failed' THEN true ELSE false END AS failed,
      CAST(updated_at AS VARCHAR)     AS updated_at,
      NULL::INT                       AS fiscal_year_focus,
      NULL::VARCHAR                   AS fiscal_period_focus,
      NULL::INT                       AS fiscal_year_end_month
    FROM postgres_scan('{c}', '{s}', 'reports')
    WHERE generation_status = 'published'
  """

  tables["Period"] = f"""
    CREATE OR REPLACE TABLE Period AS
    SELECT DISTINCT
      md5(COALESCE(CAST(period_start AS VARCHAR), 'null')
          || '_' || CAST(period_end AS VARCHAR)
          || '_' || period_type)
                                      AS identifier,
      NULL::VARCHAR                   AS uri,
      CAST(period_start AS VARCHAR)   AS start_date,
      CAST(period_end AS VARCHAR)     AS end_date,
      EXTRACT(YEAR FROM period_end)::INT AS calendar_year,
      NULL::VARCHAR                   AS calendar_quarter,
      CASE
        WHEN period_start IS NOT NULL
        THEN (period_end - period_start + 1)::INT
        ELSE 0
      END                             AS days_in_period,
      period_type,
      CASE
        WHEN period_type = 'instant' THEN NULL
        WHEN period_start IS NULL THEN NULL
        WHEN (period_end - period_start) BETWEEN 80 AND 100 THEN 'quarterly'
        WHEN (period_end - period_start) BETWEEN 170 AND 190 THEN 'semi_annual'
        WHEN (period_end - period_start) BETWEEN 260 AND 280 THEN 'nine_months'
        WHEN (period_end - period_start) BETWEEN 350 AND 380 THEN 'annual'
        ELSE 'other'
      END                             AS duration_type,
      NULL::VARCHAR                   AS calendar_period_key
    FROM {actual_facts} f
  """

  # Units come from the facts actually present (numeric facts only — XBRL
  # nonNumeric facts carry no unit). The UNION'd static USD row keeps the
  # legacy ``unit_usd`` node present even for fact-less graphs.
  tables["Unit"] = f"""
    CREATE OR REPLACE TABLE Unit AS
    SELECT DISTINCT
      'unit_' || lower(unit)          AS identifier,
      'iso4217:' || unit              AS uri,
      'iso4217:' || unit              AS measure,
      unit                            AS value,
      NULL::VARCHAR                   AS numerator_uri,
      NULL::VARCHAR                   AS denominator_uri
    FROM {actual_facts} f
    WHERE fact_type = 'Numeric'
    UNION
    SELECT
      'unit_usd', 'iso4217:USD', 'iso4217:USD', 'USD',
      NULL::VARCHAR, NULL::VARCHAR
  """

  # decimals: numeric rows fall back to the legacy '-2' when unspecified so
  # existing graph output is unchanged; Nonnumeric rows pass NULL through
  # (XBRL nonNumeric facts carry no @decimals).
  tables["Fact"] = f"""
    CREATE OR REPLACE TABLE Fact AS
    SELECT
      f.id                            AS identifier,
      NULL::VARCHAR                   AS uri,
      COALESCE(f.string_value, CAST(f.value AS VARCHAR)) AS value,
      f.value                         AS numeric_value,
      f.fact_type                     AS fact_type,
      CASE WHEN f.fact_type = 'Numeric'
           THEN COALESCE(f.decimals, '-2')
           ELSE f.decimals END        AS decimals,
      f.value_type                    AS value_type,
      f.content_type                  AS content_type,
      -- Derived, not hardcoded. It read `false` / `0` for every fact while
      -- four graph read paths filter consolidated totals on exactly this
      -- column, so the contract they rely on was being satisfied by the
      -- scenario exclusion upstream rather than by the flag itself. Nothing
      -- is dimensioned in OLTP today except scenario facts, which the
      -- exclusion drops — so this changes no output. It changes what
      -- happens when something dimensioned does reach the graph: the
      -- filters start working instead of silently passing everything.
      COALESCE(fd.n, 0) > 0           AS has_dimensions,
      COALESCE(fd.n, 0)::BIGINT       AS dimension_count
    FROM {actual_facts} f
    LEFT JOIN (
      SELECT fact_id, COUNT(*) AS n
      FROM postgres_scan('{c}', '{s}', 'fact_dimensions')
      GROUP BY fact_id
    ) fd ON fd.fact_id = f.id
  """

  tables["FactSet"] = f"""
    CREATE OR REPLACE TABLE FactSet AS
    SELECT
      id                              AS identifier,
      COALESCE(factset_type, '')      AS factset_type,
      COALESCE(CAST(provenance AS VARCHAR), '')  AS provenance
    FROM postgres_scan('{c}', '{s}', 'fact_sets')
    WHERE scenario_id IS NULL
  """

  # ── Base Ontology Relationships (Entity ↔ Taxonomy) ────────────────────

  tables["ENTITY_HAS_TAXONOMY"] = f"""
    CREATE OR REPLACE TABLE ENTITY_HAS_TAXONOMY AS
    SELECT
      entity_id                       AS src,
      taxonomy_id                     AS dst,
      is_primary,
      basis,
      COALESCE(CAST(effective_from AS VARCHAR), '')  AS effective_from,
      COALESCE(CAST(effective_to AS VARCHAR), '')    AS effective_to,
      COALESCE(adoption_context, '')  AS adoption_context
    FROM postgres_scan('{c}', '{s}', 'entity_taxonomies')
  """

  tables["TAXONOMY_EXTENDS_TAXONOMY"] = f"""
    CREATE OR REPLACE TABLE TAXONOMY_EXTENDS_TAXONOMY AS
    SELECT
      id                              AS src,
      parent_taxonomy_id              AS dst,
      COALESCE(extension_type, '')    AS extension_type,
      COALESCE(CAST(effective_date AS VARCHAR), '')  AS effective_date
    FROM postgres_scan('{c}', '{s}', 'taxonomies')
    WHERE parent_taxonomy_id IS NOT NULL
  """

  # ── Reporting Relationships ────────────────────────────────────────────

  tables["STRUCTURE_HAS_TAXONOMY"] = f"""
    CREATE OR REPLACE TABLE STRUCTURE_HAS_TAXONOMY AS
    SELECT
      id                              AS src,
      taxonomy_id                     AS dst
    FROM postgres_scan('{c}', '{s}', 'structures')
    WHERE taxonomy_id IS NOT NULL
      AND block_type NOT IN ('chart_of_accounts', 'coa_mapping')
      AND is_active = true
  """

  tables["ENTITY_HAS_REPORT"] = f"""
    CREATE OR REPLACE TABLE ENTITY_HAS_REPORT AS
    -- Native reports (no source_graph_id) belong to the graph's own entity
    SELECT
      '{entity_id}'                   AS src,
      rd.id                           AS dst
    FROM postgres_scan('{c}', '{s}', 'reports') rd
    WHERE rd.generation_status = 'published'
      AND rd.source_graph_id IS NULL
    UNION ALL
    -- Shared reports belong to the linked entity matching source_graph_id
    SELECT
      e.id                            AS src,
      rd.id                           AS dst
    FROM postgres_scan('{c}', '{s}', 'reports') rd
    JOIN postgres_scan('{c}', '{s}', 'entities') e
      ON e.metadata->>'source_graph_id' = rd.source_graph_id
    WHERE rd.generation_status = 'published'
      AND rd.source_graph_id IS NOT NULL
  """

  # Inner-joined to `taxonomies` for the same reason as `FACT_HAS_ELEMENT`
  # below: `reports.taxonomy_id` carries no foreign key, so a report whose
  # taxonomy is absent from this schema — the cross-graph share case, where
  # the sender's reporting extension may not have travelled — reaches here
  # intact and costs the recipient their entire rebuild. Dropping the one
  # edge is the proportionate response; `_ensure_shared_elements` is
  # responsible for the taxonomy actually arriving.
  tables["REPORT_USES_TAXONOMY"] = f"""
    CREATE OR REPLACE TABLE REPORT_USES_TAXONOMY AS
    SELECT
      r.id                            AS src,
      r.taxonomy_id                   AS dst
    FROM postgres_scan('{c}', '{s}', 'reports') r
    JOIN postgres_scan('{c}', '{s}', 'taxonomies') t
      ON t.id = r.taxonomy_id
    WHERE r.generation_status = 'published'
  """

  tables["REPORT_HAS_FACT"] = f"""
    CREATE OR REPLACE TABLE REPORT_HAS_FACT AS
    SELECT
      fs.report_id                    AS src,
      f.id                            AS dst
    FROM postgres_scan('{c}', '{s}', 'facts') f
    JOIN postgres_scan('{c}', '{s}', 'fact_sets') fs
      ON fs.id = f.fact_set_id
    WHERE fs.report_id IS NOT NULL
      AND fs.scenario_id IS NULL
  """

  # Inner-joined to `elements` on purpose. `facts.element_id` has no
  # foreign key, so an unresolvable concept reaches this point without
  # complaint — and LadybugDB then rejects the edge, which blue/green
  # scores as a partial run and answers by abandoning the entire WIP
  # database. One dangling id would otherwise cost the graph everything,
  # including rows with no relationship to whatever wrote the bad
  # reference. Dropping the single edge is the proportionate response; the
  # write paths are responsible for not creating the dangle in the first
  # place (see `_ensure_shared_elements` for the cross-graph case).
  tables["FACT_HAS_ELEMENT"] = f"""
    CREATE OR REPLACE TABLE FACT_HAS_ELEMENT AS
    SELECT
      f.id                            AS src,
      f.element_id                    AS dst
    FROM {actual_facts} f
    JOIN postgres_scan('{c}', '{s}', 'elements') e
      ON e.id = f.element_id
  """

  tables["FACT_HAS_PERIOD"] = f"""
    CREATE OR REPLACE TABLE FACT_HAS_PERIOD AS
    SELECT
      id                              AS src,
      md5(COALESCE(CAST(period_start AS VARCHAR), 'null')
          || '_' || CAST(period_end AS VARCHAR)
          || '_' || period_type)
                                      AS dst
    FROM {actual_facts} f
  """

  # XBRL nonNumeric facts carry no unitRef — numeric facts only.
  tables["FACT_HAS_UNIT"] = f"""
    CREATE OR REPLACE TABLE FACT_HAS_UNIT AS
    SELECT
      id                              AS src,
      'unit_' || lower(unit)          AS dst
    FROM {actual_facts} f
    WHERE fact_type = 'Numeric'
  """

  tables["FACT_HAS_ENTITY"] = f"""
    CREATE OR REPLACE TABLE FACT_HAS_ENTITY AS
    -- Native facts: entity_id references a local entity directly
    SELECT
      rf.id                           AS src,
      rf.entity_id                    AS dst
    FROM postgres_scan('{c}', '{s}', 'facts') rf
    JOIN postgres_scan('{c}', '{s}', 'fact_sets') fs
      ON fs.id = rf.fact_set_id
    JOIN postgres_scan('{c}', '{s}', 'reports') rd
      ON fs.report_id = rd.id
    WHERE rd.source_graph_id IS NULL
      AND fs.scenario_id IS NULL
    UNION ALL
    -- Shared facts: remap entity_id to the linked entity on this graph
    SELECT
      rf.id                           AS src,
      e.id                            AS dst
    FROM postgres_scan('{c}', '{s}', 'facts') rf
    JOIN postgres_scan('{c}', '{s}', 'fact_sets') fs
      ON fs.id = rf.fact_set_id
    JOIN postgres_scan('{c}', '{s}', 'reports') rd
      ON fs.report_id = rd.id
    JOIN postgres_scan('{c}', '{s}', 'entities') e
      ON e.metadata->>'source_graph_id' = rd.source_graph_id
    WHERE rd.source_graph_id IS NOT NULL
      AND fs.scenario_id IS NULL
  """

  tables["STRUCTURE_HAS_FACT_SET"] = f"""
    CREATE OR REPLACE TABLE STRUCTURE_HAS_FACT_SET AS
    SELECT
      structure_id                    AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'fact_sets')
    WHERE structure_id IS NOT NULL
      AND scenario_id IS NULL
  """

  tables["REPORT_HAS_FACT_SET"] = f"""
    CREATE OR REPLACE TABLE REPORT_HAS_FACT_SET AS
    SELECT
      report_id                       AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'fact_sets')
    WHERE report_id IS NOT NULL
      AND scenario_id IS NULL
  """

  tables["FACT_SET_CONTAINS_FACT"] = f"""
    CREATE OR REPLACE TABLE FACT_SET_CONTAINS_FACT AS
    SELECT
      fact_set_id                     AS src,
      id                              AS dst
    FROM {actual_facts} f
    WHERE fact_set_id IS NOT NULL
  """

  # ── Investor Layer ─────────────────────────────────────────────────────
  # These tables are populated from roboinvestor OLTP tables (portfolios,
  # securities, positions). They produce empty staging tables if the
  # roboinvestor extension isn't enabled — the materializer handles that
  # gracefully (0-row tables are still valid).

  tables["Portfolio"] = f"""
    CREATE OR REPLACE TABLE Portfolio AS
    SELECT
      id                              AS identifier,
      name,
      description,
      strategy,
      inception_date,
      base_currency
    FROM postgres_scan('{c}', '{s}', 'portfolios')
  """

  tables["Security"] = f"""
    CREATE OR REPLACE TABLE Security AS
    SELECT
      sec.id                          AS identifier,
      sec.name,
      sec.security_type,
      sec.security_subtype,
      sec.is_active,
      NULL::VARCHAR                   AS ticker,
      NULL::VARCHAR                   AS figi,
      -- The security's own column first: it carries the investor's declared
      -- cross-graph intent from the moment the security is created, whereas the
      -- entity join only resolves once `_ensure_linked_entity` has run. Reading
      -- the join alone materialized NULL for every security still awaiting its
      -- handshake — exactly the pre-association state the design turns on.
      COALESCE(
        sec.source_graph_id,
        e.metadata->>'source_graph_id'
      )                               AS source_graph_id
    FROM postgres_scan('{c}', '{s}', 'securities') sec
    LEFT JOIN postgres_scan('{c}', '{s}', 'entities') e
      ON sec.entity_id = e.id
    WHERE sec.is_active = true
  """

  tables["Position"] = f"""
    CREATE OR REPLACE TABLE Position AS
    SELECT
      id                              AS identifier,
      quantity,
      quantity_type,
      CAST(cost_basis AS DOUBLE) / 100.0  AS cost_basis,
      CASE WHEN current_value IS NOT NULL
        THEN CAST(current_value AS DOUBLE) / 100.0
        ELSE NULL
      END                             AS current_value,
      currency,
      acquisition_date,
      status
    FROM postgres_scan('{c}', '{s}', 'positions')
    WHERE status = 'active'
  """

  tables["PORTFOLIO_HAS_POSITION"] = f"""
    CREATE OR REPLACE TABLE PORTFOLIO_HAS_POSITION AS
    SELECT
      portfolio_id                    AS src,
      id                              AS dst,
      NULL::DOUBLE                    AS allocation_percentage
    FROM postgres_scan('{c}', '{s}', 'positions')
    WHERE status = 'active'
  """

  tables["POSITION_IN_SECURITY"] = f"""
    CREATE OR REPLACE TABLE POSITION_IN_SECURITY AS
    SELECT
      id                              AS src,
      security_id                     AS dst
    FROM postgres_scan('{c}', '{s}', 'positions')
    WHERE status = 'active'
  """

  tables["ENTITY_ISSUES_SECURITY"] = f"""
    CREATE OR REPLACE TABLE ENTITY_ISSUES_SECURITY AS
    SELECT
      entity_id                       AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'securities')
    WHERE entity_id IS NOT NULL
      AND is_active = true
  """

  tables["ENTITY_HAS_PORTFOLIO"] = f"""
    CREATE OR REPLACE TABLE ENTITY_HAS_PORTFOLIO AS
    SELECT
      entity_id                       AS src,
      id                              AS dst,
      COALESCE(ownership_type, '')    AS ownership_type
    FROM postgres_scan('{c}', '{s}', 'portfolios')
    WHERE entity_id IS NOT NULL
  """

  return tables


class ExtensionsMaterializer:
  """Materializes extensions OLTP data to the LadybugDB graph.

  Connector-agnostic — reads whatever is in the extensions tenant schema
  and materializes it to graph nodes and relationships. The OLTPLoader
  is the reverse operation (load into OLTP); this is the forward path
  (OLTP → graph).
  """

  async def materialize(
    self,
    graph_id: str,
    entity_id: str | None = None,
    rebuild: bool = True,
  ) -> MaterializeResult:
    """Stage from PostgreSQL, then materialize into the graph.

    ``graph_id`` is both the graph database and the tenant schema name;
    ``entity_id`` defaults to ``entity_{graph_id}``.

    An existing database takes the blue-green path: a WIP copy is built
    alongside the live graph and swapped in on success, so the live graph keeps
    serving queries and downtime is the length of a file rename. First-time
    creation builds in place. Errors are collected on the result rather than
    raised — check ``status``.
    """
    from robosystems.graph_api.client.factory import get_graph_client

    start_time = time.time()
    entity_id = entity_id or f"entity_{graph_id}"

    result = MaterializeResult(graph_id=graph_id)

    try:
      client = await get_graph_client(graph_id=graph_id, operation_type="write")
    except Exception as e:
      logger.error(f"Failed to get graph client for {graph_id}: {e}")
      result.status = "error"
      result.errors.append(f"Graph client initialization failed: {e!s}")
      result.duration_ms = (time.time() - start_time) * 1000
      return result

    # Per-instance busy signal so ASG refresh workflows wait rather than
    # cycling the node mid-rebuild. The per-graph lock is separate and lives in
    # _materialize_blue_green.
    from robosystems.middleware.graph.instance_busy import (
      OP_KIND_EXTENSIONS_MATERIALIZE,
      begin_destructive_op,
      end_destructive_op,
    )

    busy_instance_id = client._instance_id or ""
    await begin_destructive_op(busy_instance_id, OP_KIND_EXTENSIONS_MATERIALIZE)

    try:
      async with client:
        db_exists = await client.database_exists(graph_id)

        if db_exists and rebuild:
          await self._materialize_blue_green(client, graph_id, entity_id, result)
        else:
          await self._materialize_direct(client, graph_id, entity_id, rebuild, result)

    except Exception as e:
      logger.error(f"Ledger materialization failed for {graph_id}: {e}", exc_info=True)
      result.status = "error"
      result.errors.append(str(e))
    finally:
      await end_destructive_op(busy_instance_id, OP_KIND_EXTENSIONS_MATERIALIZE)

    result.duration_ms = (time.time() - start_time) * 1000

    if result.status != "error":
      logger.info(
        f"Ledger materialization complete for {graph_id}: "
        f"{len(result.tables_materialized)} tables, "
        f"{result.total_rows} rows, "
        f"{result.duration_ms:.0f}ms"
      )

    return result

  async def _materialize_direct(
    self,
    client: "GraphClient",
    graph_id: str,
    entity_id: str,
    rebuild: bool,
    result: MaterializeResult,
  ) -> None:
    """Build in place — for first-time creation, or when ``rebuild`` is False.

    Unlike the blue-green path there is no fallback copy: a failure part-way
    leaves the graph in whatever state it reached.
    """
    await self._ensure_database(client, graph_id, rebuild)

    connstr = build_postgres_connstr()

    # Only stage tables whose extension is enabled — the others have no
    # node/rel table in LadybugDB to materialize into.
    enabled_extensions = set(await self._get_graph_extensions(graph_id))

    staging_sql = _staging_sql(graph_id, entity_id, connstr)
    await self._stage_tables(client, graph_id, staging_sql, result, enabled_extensions)

    await self._materialize_tables(client, graph_id, result)

  async def _materialize_blue_green(
    self,
    client: "GraphClient",
    graph_id: str,
    entity_id: str,
    result: MaterializeResult,
  ) -> None:
    """Build ``{graph_id}-wip`` alongside the live graph and swap on success.

    The live graph serves queries throughout; downtime is the length of the
    file rename. Only a fully clean build is swapped in — a partial one is
    discarded so the last good graph stays active.

    Holds a per-graph materialization lock. If Valkey is unavailable the
    rebuild proceeds unlocked, so concurrent rebuilds become possible.
    """
    from robosystems.graph_api.core.ladybug.materialization_lock import (
      MaterializationLock,
    )

    wip_id = f"{graph_id}-wip"
    lock: MaterializationLock | None = None

    logger.info(f"Blue-green materialization: building WIP {wip_id}")

    try:
      try:
        from robosystems.config.valkey_registry import (
          ValkeyDatabase,
          create_async_redis_client,
        )

        redis_client = create_async_redis_client(ValkeyDatabase.LOCKS)
        lock = MaterializationLock(redis_client, graph_id)
        acquired = await lock.acquire(timeout_seconds=5)
        if not acquired:
          raise RuntimeError(
            f"Could not acquire materialization lock for {graph_id} "
            "(another materialization may be in progress)"
          )
      except ImportError:
        logger.warning("Valkey not available — proceeding without materialization lock")
      except RuntimeError:
        raise
      except Exception as e:
        logger.warning(f"Could not acquire materialization lock: {e}")

      # A leftover WIP means a previous run died before cleanup.
      wip_exists = await client.database_exists(wip_id)
      if wip_exists:
        logger.info(f"Cleaning up leftover WIP database {wip_id}")
        await client.delete_database(
          wip_id, preserve_duckdb=True, lock_token=lock.token if lock else None
        )

      # is_subgraph exempts the WIP from the per-node max_databases cap: on a
      # dedicated single-database instance the live primary already fills the
      # quota, so the transient WIP would otherwise be rejected outright.
      await self._ensure_database(client, wip_id, rebuild=False, is_subgraph=True)

      # Only stage tables whose extension is enabled — the others have no
      # node/rel table in LadybugDB to materialize into.
      enabled_extensions = set(await self._get_graph_extensions(graph_id))

      # Staging writes into graph_id's DuckDB, so the WIP materializes with
      # source_graph_id pointing back at it.
      connstr = build_postgres_connstr()
      staging_sql = _staging_sql(graph_id, entity_id, connstr)
      await self._stage_tables(
        client, graph_id, staging_sql, result, enabled_extensions
      )

      await self._materialize_tables(client, wip_id, result, source_graph_id=graph_id)

      # Only a clean build ships. A 'partial' WIP — one where an edge table
      # failed to COPY — would swap in a ledger whose statements render empty,
      # so the last good graph stays active and staleness is left set to retry.
      if result.status == "success":
        logger.info(f"Swapping WIP {wip_id} → active {graph_id}")
        await client.swap_database(graph_id, lock_token=lock.token if lock else None)
        logger.info(f"Blue-green swap complete for {graph_id}")
      else:
        logger.warning(
          f"Blue-green materialization {result.status} for {graph_id} "
          f"({len(result.errors)} errors), abandoning WIP (active graph untouched)"
        )
        try:
          await client.delete_database(
            wip_id, preserve_duckdb=True, lock_token=lock.token if lock else None
          )
        except Exception as cleanup_err:
          logger.warning(f"Failed to clean up WIP {wip_id} after errors: {cleanup_err}")

    except Exception as e:
      logger.error(f"Blue-green materialization failed for {graph_id}: {e}")
      try:
        wip_exists = await client.database_exists(wip_id)
        if wip_exists:
          await client.delete_database(
            wip_id, preserve_duckdb=True, lock_token=lock.token if lock else None
          )
      except Exception as cleanup_err:
        logger.warning(
          f"Failed to clean up WIP {wip_id} after blue-green failure: {cleanup_err}"
        )
      raise
    finally:
      if lock is not None and lock.acquired:
        await lock.release()

  async def _ensure_database(
    self,
    client: "GraphClient",
    graph_id: str,
    rebuild: bool,
    is_subgraph: bool = False,
  ) -> None:
    """Ensure the LadybugDB database exists with the graph schema.

    ``is_subgraph=True`` makes the create bypass the per-node max_databases cap
    (graph_api exempts is_subgraph creations in manager.py). Used for the
    transient blue-green WIP so it can be built alongside the live primary on a
    dedicated single-database instance; the primary still counts against the cap.
    """
    from robosystems.schemas.loader import get_contextual_schema_loader

    db_exists = await client.database_exists(graph_id)

    if rebuild and db_exists:
      logger.info(f"Rebuilding LadybugDB database for {graph_id}")
      await client.delete_database(graph_id, preserve_duckdb=True)
      db_exists = False

    if not db_exists:
      logger.info(f"Creating LadybugDB database for {graph_id}")
      await client.create_database(
        graph_id, schema_type="entity", is_subgraph=is_subgraph
      )

      extensions = await self._get_graph_extensions(graph_id)
      ddl_parts = []

      for ext_name in extensions:
        try:
          loader = get_contextual_schema_loader("application", ext_name)
          for node in loader.nodes.values():
            ddl_parts.append(node.to_cypher() + ";")
          for rel in loader.relationships.values():
            ddl_parts.append(rel.to_cypher() + ";")
          logger.info(f"Loaded {ext_name} schema for {graph_id}")
        except Exception as e:
          logger.warning(f"Could not load schema for extension '{ext_name}': {e}")

      if ddl_parts:
        schema_ddl = "\n".join(ddl_parts)
        await client.install_schema(graph_id=graph_id, custom_ddl=schema_ddl)
        logger.info(f"Installed schema on {graph_id} (extensions: {extensions})")

  async def _get_graph_extensions(self, graph_id: str) -> list[str]:
    """The graph's ``schema_extensions``, defaulting to ``["roboledger"]``."""
    from robosystems.db.platform import SessionFactory
    from robosystems.models.core import Graph

    # The blue-green WIP database has no row in the platform DB, so resolve it
    # back to its source; otherwise the lookup misses, the fallback applies,
    # and the WIP is built without (say) the roboinvestor node tables.
    graph_id = graph_id.removesuffix("-wip")

    try:
      with SessionFactory() as session:
        graph = session.execute(
          __import__("sqlalchemy").select(Graph).where(Graph.graph_id == graph_id)
        ).scalar_one_or_none()
        if graph and graph.schema_extensions:
          return list(graph.schema_extensions)
    except Exception as e:
      logger.warning(f"Could not look up extensions for {graph_id}: {e}")

    return ["roboledger"]

  async def _stage_tables(
    self,
    client: "GraphClient",
    graph_id: str,
    staging_sql: dict[str, str],
    result: MaterializeResult,
    enabled_extensions: set[str],
  ) -> None:
    """Create the DuckDB staging tables from PostgreSQL via postgres_scanner.

    Only tables whose owning extension is enabled are staged: the rest have no
    node/rel table in LadybugDB and would fail with "Table does not exist".
    A dimension relationship table that fails to stage is treated as
    non-fatal — a tenant may simply have no dimension data.
    """
    all_tables = _filter_tables_for_extensions(
      NODE_TABLES + RELATIONSHIP_TABLES, enabled_extensions
    )

    for table_name in all_tables:
      sql = staging_sql.get(table_name)
      if not sql:
        continue

      try:
        logger.info(f"Staging {table_name} from PostgreSQL → DuckDB")
        # Must be the internal write path: the read-only /tables/query surface
        # rejects DDL and has postgres_scanner disabled.
        await client.execute_write(graph_id, sql.strip(), timeout=120.0)
        result.tables_staged.append(table_name)
      except Exception as e:
        error_msg = f"Failed to stage {table_name}: {e!s}"
        logger.warning(error_msg)
        if table_name in RELATIONSHIP_TABLES and "DIMENSION" in table_name:
          logger.info(f"Skipping {table_name} (no dimension data)")
        else:
          result.status = "error"
          result.errors.append(error_msg)

  async def _materialize_tables(
    self,
    client: "GraphClient",
    graph_id: str,
    result: MaterializeResult,
    source_graph_id: str | None = None,
  ) -> None:
    """COPY the staged DuckDB tables into LadybugDB, nodes before edges.

    ``source_graph_id`` points staging reads at a different graph's DuckDB;
    blue-green uses it so the WIP database reads the source's staging tables.

    A node failure aborts the whole run. Every edge table has an FK into some
    node table, so continuing past a missing node produces "Unable to find
    primary key value" on each dependent edge and a graph full of zero-row edge
    tables that still reports success. An edge failure is recorded and
    downgrades the run to ``partial`` but does not abort — one bad edge is not
    necessarily graph-invalidating.
    """
    node_tables = set(NODE_TABLES)
    for table_name in result.tables_staged:
      try:
        logger.info(f"Materializing {table_name} → LadybugDB ({graph_id})")
        response = await client.materialize_table(
          graph_id=graph_id,
          table_name=table_name,
          timeout=300.0,
          source_graph_id=source_graph_id,
        )
        rows = response.get("rows_ingested", 0)
        result.total_rows += rows
        result.tables_materialized.append(table_name)
        logger.info(f"Materialized {rows} rows for {table_name}")
      except Exception as e:
        error_msg = f"Failed to materialize {table_name}: {e!s}"
        logger.error(error_msg)
        result.errors.append(error_msg)
        if table_name in node_tables:
          result.status = "failed"
          raise RuntimeError(
            f"Node table '{table_name}' failed to materialize — aborting "
            f"so edge tables don't silently load with broken FK targets. "
            f"Original error: {e!s}"
          ) from e
        # Downgrade to 'partial': callers gating on status must not treat a
        # graph missing a relationship table as a clean build.
        if result.status == "success":
          result.status = "partial"
