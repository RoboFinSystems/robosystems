"""
Ledger Materialization — PostgreSQL OLTP → DuckDB staging → LadybugDB graph.

Connector-agnostic: materializes whatever is in the roboledger tenant schema,
regardless of which connector (QuickBooks, Xero, Plaid, native) put it there.

Uses DuckDB's postgres_scanner extension to read directly from the roboledger
database, transform OLTP rows into graph-shaped staging tables, then materialize
via the existing ATTACH + COPY FROM pipeline.

Architecture:
  Dagster/API → LedgerMaterializer.materialize(graph_id, entity_id)
    → get_graph_client(graph_id, operation_type="write")  # routes to correct EC2
    → client.query_table(sql)  # DuckDB postgres_scan → staging tables
    → client.materialize_table(table_name)  # DuckDB → LadybugDB
"""

import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
  from robosystems.graph_api.client.client import GraphClient

from robosystems.logger import logger


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
  "Element",
  "Transaction",
  "Entry",
  "LineItem",
  "Dimension",
  "Structure",
  "Association",
]

# Relationship tables in materialization order (nodes must exist first)
RELATIONSHIP_TABLES = [
  "ENTITY_HAS_TRANSACTION",
  "TRANSACTION_HAS_ENTRY",
  "ENTRY_HAS_LINE_ITEM",
  "LINE_ITEM_RELATES_TO_ELEMENT",
  "STRUCTURE_HAS_ASSOCIATION",
  "ASSOCIATION_HAS_FROM_ELEMENT",
  "ASSOCIATION_HAS_TO_ELEMENT",
  "TRANSACTION_HAS_DIMENSION",
  "ENTRY_HAS_DIMENSION",
  "LINE_ITEM_HAS_DIMENSION",
]


def build_postgres_connstr(graph_id: str) -> str:
  """Build a postgres_scanner connection string for the roboledger database.

  Uses ROBOLEDGER_DATABASE_URL if available, otherwise constructs from
  DATABASE_ENDPOINT + POSTGRES_PASSWORD. Sets search_path to the tenant
  schema so postgres_scanner reads from the correct graph's data.

  Args:
      graph_id: Tenant schema name (e.g., "kg0192...")

  Returns:
      Connection string for DuckDB's postgres_scan() function.
  """
  from robosystems.config import env

  url = env.ROBOLEDGER_DATABASE_URL
  parsed = urlparse(url)

  host = parsed.hostname or "localhost"
  port = parsed.port or 5432
  user = parsed.username or "postgres"
  password = parsed.password or "postgres"
  dbname = parsed.path.lstrip("/").split("?")[0] or "roboledger"

  # search_path scopes postgres_scanner to the tenant schema
  connstr = (
    f"dbname={dbname} user={user} password={password} "
    f"host={host} port={port} "
    f"options=-csearch_path={graph_id},public"
  )

  return connstr


def _staging_sql(graph_id: str, entity_id: str, connstr: str) -> dict[str, str]:
  """Build staging SQL for all node and relationship tables.

  Each SQL statement creates a DuckDB table from postgres_scan() that
  matches the graph schema column layout. The materialization endpoint
  will then COPY these into LadybugDB.

  Returns:
      Dict mapping table name → CREATE TABLE SQL.
  """
  c = connstr  # shorthand for SQL interpolation

  tables: dict[str, str] = {}

  # ── Node Tables ──────────────────────────────────────────────────────

  tables["Element"] = f"""
    CREATE OR REPLACE TABLE Element AS
    SELECT
      id                              AS identifier,
      'roboledger:' || code           AS qname,
      name,
      description,
      classification,
      sub_classification              AS item_type,
      balance_type                    AS balance,
      CASE WHEN is_placeholder THEN true ELSE false END AS is_abstract,
      false                           AS is_dimension_item,
      false                           AS is_domain_member,
      false                           AS is_hypercube_item,
      false                           AS is_integer,
      true                            AS is_numeric,
      false                           AS is_shares,
      false                           AS is_fraction,
      false                           AS is_textblock,
      NULL::VARCHAR                   AS uri,
      NULL::VARCHAR                   AS substitution_group,
      NULL::VARCHAR                   AS period_type,
      NULL::VARCHAR                   AS type,
      NULL::VARCHAR                   AS canonical_concept,
      NULL::DOUBLE                    AS canonical_confidence,
      NULL::FLOAT[384]                AS embedding
    FROM postgres_scan('{c}', '', 'accounts')
    WHERE is_active = true
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
      CAST(updated_at AS VARCHAR)     AS updated_at
    FROM postgres_scan('{c}', '', 'transactions')
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
      reversal_of,
      CAST(updated_at AS VARCHAR)     AS updated_at
    FROM postgres_scan('{c}', '', 'entries')
  """

  tables["LineItem"] = f"""
    CREATE OR REPLACE TABLE LineItem AS
    SELECT
      id                                         AS identifier,
      NULL::VARCHAR                              AS uri,
      description,
      CAST(debit_amount AS DOUBLE) / 100.0       AS debit_amount,
      CAST(credit_amount AS DOUBLE) / 100.0      AS credit_amount,
      false                                      AS has_dimensions,
      0::BIGINT                                  AS dimension_count,
      CAST(updated_at AS VARCHAR)                AS updated_at
    FROM postgres_scan('{c}', '', 'line_items')
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
    FROM postgres_scan('{c}', '', 'dimensions')
    WHERE is_active = true
  """

  # Structure: one row representing the Chart of Accounts hierarchy
  tables["Structure"] = f"""
    CREATE OR REPLACE TABLE Structure AS
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
  """

  # Association: one per parent-child account pair in the CoA tree
  tables["Association"] = f"""
    CREATE OR REPLACE TABLE Association AS
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
    FROM postgres_scan('{c}', '', 'accounts') child
    INNER JOIN postgres_scan('{c}', '', 'accounts') parent
      ON child.parent_id = parent.id
    WHERE child.parent_id IS NOT NULL
      AND child.is_active = true
  """

  # ── Relationship Tables ──────────────────────────────────────────────

  tables["ENTITY_HAS_TRANSACTION"] = f"""
    CREATE OR REPLACE TABLE ENTITY_HAS_TRANSACTION AS
    SELECT
      '{entity_id}'                   AS src,
      id                              AS dst,
      NULL::VARCHAR                   AS transaction_context
    FROM postgres_scan('{c}', '', 'transactions')
  """

  tables["TRANSACTION_HAS_ENTRY"] = f"""
    CREATE OR REPLACE TABLE TRANSACTION_HAS_ENTRY AS
    SELECT
      transaction_id                  AS src,
      id                              AS dst,
      NULL::VARCHAR                   AS entry_context
    FROM postgres_scan('{c}', '', 'entries')
    WHERE transaction_id IS NOT NULL
  """

  tables["ENTRY_HAS_LINE_ITEM"] = f"""
    CREATE OR REPLACE TABLE ENTRY_HAS_LINE_ITEM AS
    SELECT
      entry_id                        AS src,
      id                              AS dst,
      NULL::VARCHAR                   AS line_item_context
    FROM postgres_scan('{c}', '', 'line_items')
  """

  tables["LINE_ITEM_RELATES_TO_ELEMENT"] = f"""
    CREATE OR REPLACE TABLE LINE_ITEM_RELATES_TO_ELEMENT AS
    SELECT
      id                              AS src,
      account_id                      AS dst,
      NULL::VARCHAR                   AS mapping_context
    FROM postgres_scan('{c}', '', 'line_items')
  """

  tables["STRUCTURE_HAS_ASSOCIATION"] = f"""
    CREATE OR REPLACE TABLE STRUCTURE_HAS_ASSOCIATION AS
    SELECT
      '{graph_id}_coa'                AS src,
      child.id || '_assoc'            AS dst,
      NULL::VARCHAR                   AS association_context
    FROM postgres_scan('{c}', '', 'accounts') child
    WHERE child.parent_id IS NOT NULL
      AND child.is_active = true
  """

  tables["ASSOCIATION_HAS_FROM_ELEMENT"] = f"""
    CREATE OR REPLACE TABLE ASSOCIATION_HAS_FROM_ELEMENT AS
    SELECT
      child.id || '_assoc'            AS src,
      child.parent_id                 AS dst
    FROM postgres_scan('{c}', '', 'accounts') child
    WHERE child.parent_id IS NOT NULL
      AND child.is_active = true
  """

  tables["ASSOCIATION_HAS_TO_ELEMENT"] = f"""
    CREATE OR REPLACE TABLE ASSOCIATION_HAS_TO_ELEMENT AS
    SELECT
      child.id || '_assoc'            AS src,
      child.id                        AS dst
    FROM postgres_scan('{c}', '', 'accounts') child
    WHERE child.parent_id IS NOT NULL
      AND child.is_active = true
  """

  # Dimension junction tables (may be empty if no dimensions loaded yet)
  tables["TRANSACTION_HAS_DIMENSION"] = f"""
    CREATE OR REPLACE TABLE TRANSACTION_HAS_DIMENSION AS
    SELECT
      transaction_id                  AS src,
      dimension_id                    AS dst
    FROM postgres_scan('{c}', '', 'transaction_dimensions')
  """

  tables["ENTRY_HAS_DIMENSION"] = f"""
    CREATE OR REPLACE TABLE ENTRY_HAS_DIMENSION AS
    SELECT
      entry_id                        AS src,
      dimension_id                    AS dst
    FROM postgres_scan('{c}', '', 'entry_dimensions')
  """

  tables["LINE_ITEM_HAS_DIMENSION"] = f"""
    CREATE OR REPLACE TABLE LINE_ITEM_HAS_DIMENSION AS
    SELECT
      line_item_id                    AS src,
      dimension_id                    AS dst
    FROM postgres_scan('{c}', '', 'line_item_dimensions')
  """

  return tables


class LedgerMaterializer:
  """Materializes roboledger OLTP data to the LadybugDB graph.

  Connector-agnostic — reads whatever is in the roboledger tenant schema
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
    """Run full materialization: stage from PostgreSQL, then materialize to LadybugDB.

    Args:
        graph_id: Graph database identifier (also the tenant schema name).
        entity_id: Entity node identifier in the graph. Defaults to "entity_{graph_id}".
        rebuild: If True, delete and recreate the LadybugDB database before materializing.
                 For SMB-sized data this takes seconds and ensures a clean state.

    Returns:
        MaterializeResult with staging and materialization statistics.
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

    try:
      async with client:
        # Step 1: Ensure LadybugDB database exists with schema
        await self._ensure_database(client, graph_id, rebuild)

        # Step 2: Build connection string for postgres_scanner
        connstr = build_postgres_connstr(graph_id)

        # Step 3: Stage all tables from PostgreSQL → DuckDB
        staging_sql = _staging_sql(graph_id, entity_id, connstr)
        await self._stage_tables(client, graph_id, staging_sql, result)

        # Step 4: Materialize all tables from DuckDB → LadybugDB
        await self._materialize_tables(client, graph_id, result)

    except Exception as e:
      logger.error(f"Ledger materialization failed for {graph_id}: {e}", exc_info=True)
      result.status = "error"
      result.errors.append(str(e))

    result.duration_ms = (time.time() - start_time) * 1000

    if result.status != "error":
      logger.info(
        f"Ledger materialization complete for {graph_id}: "
        f"{len(result.tables_materialized)} tables, "
        f"{result.total_rows} rows, "
        f"{result.duration_ms:.0f}ms"
      )

    return result

  async def _ensure_database(
    self,
    client: "GraphClient",
    graph_id: str,
    rebuild: bool,
  ) -> None:
    """Ensure the LadybugDB database exists with the roboledger schema."""
    from robosystems.schemas.loader import get_contextual_schema_loader

    db_exists = await client.database_exists(graph_id)

    if rebuild and db_exists:
      logger.info(f"Rebuilding LadybugDB database for {graph_id}")
      await client.delete_database(graph_id, preserve_duckdb=True)
      db_exists = False

    if not db_exists:
      logger.info(f"Creating LadybugDB database for {graph_id}")
      await client.create_database(graph_id, schema_type="entity")

      # Install roboledger schema (full accounting: reporting + transaction nodes)
      loader = get_contextual_schema_loader("application", "roboledger")
      schema_ddl = loader.schema.to_cypher()
      await client.install_schema(graph_id=graph_id, custom_ddl=schema_ddl)
      logger.info(f"Installed roboledger schema on {graph_id}")

  async def _stage_tables(
    self,
    client: "GraphClient",
    graph_id: str,
    staging_sql: dict[str, str],
    result: MaterializeResult,
  ) -> None:
    """Execute staging SQL to create DuckDB tables from PostgreSQL via postgres_scanner."""
    all_tables = NODE_TABLES + RELATIONSHIP_TABLES

    for table_name in all_tables:
      sql = staging_sql.get(table_name)
      if not sql:
        continue

      try:
        logger.info(f"Staging {table_name} from PostgreSQL → DuckDB")
        await client.query_table(graph_id, sql.strip(), timeout=120.0)
        result.tables_staged.append(table_name)
      except Exception as e:
        error_msg = f"Failed to stage {table_name}: {e!s}"
        logger.warning(error_msg)
        # Junction tables may fail if tables don't exist (no dimensions loaded)
        # — this is non-fatal for dimension relationship tables
        if table_name in RELATIONSHIP_TABLES and "DIMENSION" in table_name:
          logger.info(f"Skipping {table_name} (no dimension data)")
        else:
          result.errors.append(error_msg)

  async def _materialize_tables(
    self,
    client: "GraphClient",
    graph_id: str,
    result: MaterializeResult,
  ) -> None:
    """Materialize staged DuckDB tables into LadybugDB graph."""
    # Materialize nodes first, then relationships
    for table_name in result.tables_staged:
      try:
        logger.info(f"Materializing {table_name} → LadybugDB")
        response = await client.materialize_table(
          graph_id=graph_id,
          table_name=table_name,
          ignore_errors=True,
          timeout=300.0,
        )
        rows = response.get("rows_ingested", 0)
        result.total_rows += rows
        result.tables_materialized.append(table_name)
        logger.info(f"Materialized {rows} rows for {table_name}")
      except Exception as e:
        error_msg = f"Failed to materialize {table_name}: {e!s}"
        logger.error(error_msg)
        result.errors.append(error_msg)
