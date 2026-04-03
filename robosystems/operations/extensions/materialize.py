"""
Ledger Materialization — PostgreSQL OLTP → DuckDB staging → LadybugDB graph.

Connector-agnostic: materializes whatever is in the extensions tenant schema,
regardless of which connector (QuickBooks, Xero, Plaid, native) put it there.

Uses DuckDB's postgres_scanner extension to read directly from the extensions
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
  "Entity",
  "Element",
  "Transaction",
  "Entry",
  "LineItem",
  "Dimension",
  "Structure",
  "Association",
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
  "ENTRY_FROM_SCHEDULE",
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
  "FACT_SET_CONTAINS_FACT",
  # Investor layer
  "PORTFOLIO_HAS_POSITION",
  "POSITION_IN_SECURITY",
  "ENTITY_ISSUES_SECURITY",
]


def build_postgres_connstr() -> str:
  """Build a postgres_scanner connection string for the extensions database.

  IMPORTANT: This connection string is used by DuckDB's postgres_scanner
  running INSIDE the Graph API process. The host must be reachable from
  that process's network context:
  - Production: RDS endpoint (via DATABASE_ENDPOINT env var on graph instances)
  - Local dev: Docker service hostname (via DATABASE_ENDPOINT in container .env)

  Returns:
      Connection string for DuckDB's postgres_scan() function.
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
  """Build staging SQL for all node and relationship tables.

  Each SQL statement creates a DuckDB table from postgres_scan() that
  matches the graph schema column layout. The materialization endpoint
  will then COPY these into LadybugDB.

  Returns:
      Dict mapping table name → CREATE TABLE SQL.
  """
  c = connstr  # shorthand for SQL interpolation
  s = graph_id  # schema name (tenant schema in extensions database)

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
      id                              AS identifier,
      CASE
        WHEN external_source = 'quickbooks' THEN 'qb:'
        WHEN external_source = 'xero' THEN 'xero:'
        WHEN external_source = 'plaid' THEN 'plaid:'
        ELSE 'rl:'
      END || code                     AS qname,
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
    FROM postgres_scan('{c}', '{s}', 'elements')
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
      reversal_of,
      CAST(updated_at AS VARCHAR)     AS updated_at
    FROM postgres_scan('{c}', '{s}', 'entries')
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
    FROM postgres_scan('{c}', '{s}', 'line_items')
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
    -- Seed reporting structures (income_statement, balance_sheet, cash_flow)
    SELECT
      id                              AS identifier,
      NULL::VARCHAR                   AS uri,
      NULL::VARCHAR                   AS network_uri,
      description                     AS definition,
      NULL::VARCHAR                   AS number,
      structure_type                  AS type,
      name,
      structure_type                  AS canonical_type,
      1.0::DOUBLE                     AS canonical_confidence,
      NULL::FLOAT[384]                AS embedding
    FROM postgres_scan('{c}', '{s}', 'structures')
    WHERE structure_type NOT IN ('chart_of_accounts', 'coa_mapping')
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
    FROM postgres_scan('{c}', '{s}', 'element_associations')
    WHERE association_type = 'presentation'
  """

  # ── Relationship Tables ──────────────────────────────────────────────

  tables["ENTITY_HAS_TRANSACTION"] = f"""
    CREATE OR REPLACE TABLE ENTITY_HAS_TRANSACTION AS
    SELECT
      '{entity_id}'                   AS src,
      id                              AS dst,
      NULL::VARCHAR                   AS transaction_context
    FROM postgres_scan('{c}', '{s}', 'transactions')
  """

  tables["TRANSACTION_HAS_ENTRY"] = f"""
    CREATE OR REPLACE TABLE TRANSACTION_HAS_ENTRY AS
    SELECT
      transaction_id                  AS src,
      id                              AS dst,
      NULL::VARCHAR                   AS entry_context
    FROM postgres_scan('{c}', '{s}', 'entries')
    WHERE transaction_id IS NOT NULL
  """

  tables["ENTRY_HAS_LINE_ITEM"] = f"""
    CREATE OR REPLACE TABLE ENTRY_HAS_LINE_ITEM AS
    SELECT
      entry_id                        AS src,
      id                              AS dst,
      NULL::VARCHAR                   AS line_item_context
    FROM postgres_scan('{c}', '{s}', 'line_items')
  """

  tables["LINE_ITEM_RELATES_TO_ELEMENT"] = f"""
    CREATE OR REPLACE TABLE LINE_ITEM_RELATES_TO_ELEMENT AS
    SELECT
      id                              AS src,
      element_id                      AS dst,
      NULL::VARCHAR                   AS mapping_context
    FROM postgres_scan('{c}', '{s}', 'line_items')
  """

  tables["STRUCTURE_HAS_ASSOCIATION"] = f"""
    CREATE OR REPLACE TABLE STRUCTURE_HAS_ASSOCIATION AS
    -- CoA structure → CoA associations
    SELECT
      '{graph_id}_coa'                AS src,
      child.id || '_assoc'            AS dst,
      NULL::VARCHAR                   AS association_context
    FROM postgres_scan('{c}', '{s}', 'elements') child
    WHERE child.parent_id IS NOT NULL
      AND child.is_active = true
    UNION ALL
    -- Seed structures → seed associations
    SELECT
      structure_id                    AS src,
      id                              AS dst,
      NULL::VARCHAR                   AS association_context
    FROM postgres_scan('{c}', '{s}', 'element_associations')
    WHERE association_type = 'presentation'
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
    FROM postgres_scan('{c}', '{s}', 'element_associations')
    WHERE association_type = 'presentation'
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
    FROM postgres_scan('{c}', '{s}', 'element_associations')
    WHERE association_type = 'presentation'
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

  # ── Reporting Layer ────────────────────────────────────────────────────
  # These tables are populated from report_definitions and facts
  # (generated by the report builder). They may be empty if no reports
  # have been generated yet — that's fine, they'll be populated on next
  # materialization after report creation.

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
    FROM postgres_scan('{c}', '{s}', 'report_definitions')
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
    FROM postgres_scan('{c}', '{s}', 'facts')
  """

  tables["Unit"] = """
    CREATE OR REPLACE TABLE Unit AS
    SELECT
      'unit_usd'                      AS identifier,
      'iso4217:USD'                   AS uri,
      'iso4217:USD'                   AS measure,
      'USD'                           AS value,
      NULL::VARCHAR                   AS numerator_uri,
      NULL::VARCHAR                   AS denominator_uri
  """

  tables["Fact"] = f"""
    CREATE OR REPLACE TABLE Fact AS
    SELECT
      id                              AS identifier,
      NULL::VARCHAR                   AS uri,
      CAST(value AS VARCHAR)          AS value,
      value                           AS numeric_value,
      'Numeric'                       AS fact_type,
      '-2'                            AS decimals,
      'inline'                        AS value_type,
      NULL::VARCHAR                   AS content_type,
      false                           AS has_dimensions,
      0::BIGINT                       AS dimension_count
    FROM postgres_scan('{c}', '{s}', 'facts')
  """

  tables["FactSet"] = f"""
    CREATE OR REPLACE TABLE FactSet AS
    SELECT DISTINCT
      fact_set_id                     AS identifier
    FROM postgres_scan('{c}', '{s}', 'facts')
    WHERE fact_set_id IS NOT NULL
  """

  # ── Reporting Relationships ────────────────────────────────────────────

  tables["STRUCTURE_HAS_TAXONOMY"] = f"""
    CREATE OR REPLACE TABLE STRUCTURE_HAS_TAXONOMY AS
    SELECT
      id                              AS src,
      taxonomy_id                     AS dst,
      NULL::VARCHAR                   AS taxonomy_context
    FROM postgres_scan('{c}', '{s}', 'structures')
    WHERE taxonomy_id IS NOT NULL
      AND structure_type NOT IN ('chart_of_accounts', 'coa_mapping')
      AND is_active = true
  """

  tables["ENTITY_HAS_REPORT"] = f"""
    CREATE OR REPLACE TABLE ENTITY_HAS_REPORT AS
    -- Native reports (no source_graph_id) belong to the graph's own entity
    SELECT
      '{entity_id}'                   AS src,
      rd.id                           AS dst,
      NULL::VARCHAR                   AS filing_context
    FROM postgres_scan('{c}', '{s}', 'report_definitions') rd
    WHERE rd.generation_status = 'published'
      AND rd.source_graph_id IS NULL
    UNION ALL
    -- Shared reports belong to the linked entity matching source_graph_id
    SELECT
      e.id                            AS src,
      rd.id                           AS dst,
      NULL::VARCHAR                   AS filing_context
    FROM postgres_scan('{c}', '{s}', 'report_definitions') rd
    JOIN postgres_scan('{c}', '{s}', 'entities') e
      ON e.metadata->>'source_graph_id' = rd.source_graph_id
    WHERE rd.generation_status = 'published'
      AND rd.source_graph_id IS NOT NULL
  """

  tables["REPORT_USES_TAXONOMY"] = f"""
    CREATE OR REPLACE TABLE REPORT_USES_TAXONOMY AS
    SELECT
      id                              AS src,
      taxonomy_id                     AS dst,
      NULL::VARCHAR                   AS taxonomy_context
    FROM postgres_scan('{c}', '{s}', 'report_definitions')
    WHERE generation_status = 'published'
  """

  tables["REPORT_HAS_FACT"] = f"""
    CREATE OR REPLACE TABLE REPORT_HAS_FACT AS
    SELECT
      report_id                       AS src,
      id                              AS dst,
      NULL::VARCHAR                   AS fact_context
    FROM postgres_scan('{c}', '{s}', 'facts')
  """

  tables["FACT_HAS_ELEMENT"] = f"""
    CREATE OR REPLACE TABLE FACT_HAS_ELEMENT AS
    SELECT
      id                              AS src,
      element_id                      AS dst
    FROM postgres_scan('{c}', '{s}', 'facts')
  """

  tables["FACT_HAS_PERIOD"] = f"""
    CREATE OR REPLACE TABLE FACT_HAS_PERIOD AS
    SELECT
      id                              AS src,
      md5(COALESCE(CAST(period_start AS VARCHAR), 'null')
          || '_' || CAST(period_end AS VARCHAR)
          || '_' || period_type)
                                      AS dst,
      NULL::VARCHAR                   AS period_context
    FROM postgres_scan('{c}', '{s}', 'facts')
  """

  tables["FACT_HAS_UNIT"] = f"""
    CREATE OR REPLACE TABLE FACT_HAS_UNIT AS
    SELECT
      id                              AS src,
      'unit_usd'                      AS dst,
      NULL::VARCHAR                   AS unit_context
    FROM postgres_scan('{c}', '{s}', 'facts')
  """

  tables["FACT_HAS_ENTITY"] = f"""
    CREATE OR REPLACE TABLE FACT_HAS_ENTITY AS
    -- Native facts: entity_id references a local entity directly
    SELECT
      rf.id                           AS src,
      rf.entity_id                    AS dst,
      NULL::VARCHAR                   AS entity_context
    FROM postgres_scan('{c}', '{s}', 'facts') rf
    JOIN postgres_scan('{c}', '{s}', 'report_definitions') rd
      ON rf.report_id = rd.id
    WHERE rd.source_graph_id IS NULL
    UNION ALL
    -- Shared facts: remap entity_id to the linked entity on this graph
    SELECT
      rf.id                           AS src,
      e.id                            AS dst,
      NULL::VARCHAR                   AS entity_context
    FROM postgres_scan('{c}', '{s}', 'facts') rf
    JOIN postgres_scan('{c}', '{s}', 'report_definitions') rd
      ON rf.report_id = rd.id
    JOIN postgres_scan('{c}', '{s}', 'entities') e
      ON e.metadata->>'source_graph_id' = rd.source_graph_id
    WHERE rd.source_graph_id IS NOT NULL
  """

  tables["STRUCTURE_HAS_FACT_SET"] = f"""
    CREATE OR REPLACE TABLE STRUCTURE_HAS_FACT_SET AS
    SELECT DISTINCT
      -- Derive structure_id from fact_set_id pattern: 'factset_{{structure_id}}_{{report_id}}'
      -- For now, join through report_facts to get structure mapping
      rf.fact_set_id                  AS dst,
      s.id                            AS src
    FROM postgres_scan('{c}', '{s}', 'facts') rf
    JOIN postgres_scan('{c}', '{s}', 'structures') s
      ON rf.fact_set_id LIKE '%' || s.id || '%'
    WHERE rf.fact_set_id IS NOT NULL
  """

  tables["FACT_SET_CONTAINS_FACT"] = f"""
    CREATE OR REPLACE TABLE FACT_SET_CONTAINS_FACT AS
    SELECT
      fact_set_id                     AS src,
      id                              AS dst
    FROM postgres_scan('{c}', '{s}', 'facts')
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
      e.metadata->>'source_graph_id'  AS source_graph_id
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

  return tables


class LedgerMaterializer:
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
        connstr = build_postgres_connstr()

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
    """Ensure the LadybugDB database exists with the graph schema."""
    from robosystems.schemas.loader import get_contextual_schema_loader

    db_exists = await client.database_exists(graph_id)

    if rebuild and db_exists:
      logger.info(f"Rebuilding LadybugDB database for {graph_id}")
      await client.delete_database(graph_id, preserve_duckdb=True)
      db_exists = False

    if not db_exists:
      logger.info(f"Creating LadybugDB database for {graph_id}")
      await client.create_database(graph_id, schema_type="entity")

      # Install schemas for all extensions on this graph
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
    """Look up schema_extensions for a graph from the platform database."""
    from robosystems.db.platform import SessionFactory
    from robosystems.models.core import Graph

    try:
      with SessionFactory() as session:
        graph = session.execute(
          __import__("sqlalchemy").select(Graph).where(Graph.graph_id == graph_id)
        ).scalar_one_or_none()
        if graph and graph.schema_extensions:
          return list(graph.schema_extensions)
    except Exception as e:
      logger.warning(f"Could not look up extensions for {graph_id}: {e}")

    # Fallback: at minimum assume roboledger
    return ["roboledger"]

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
          result.status = "error"
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
