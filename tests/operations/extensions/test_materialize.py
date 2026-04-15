"""Tests for extensions materialization pipeline."""

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.operations.extensions.materialize import (
  NODE_TABLES,
  RELATIONSHIP_TABLES,
  TABLE_EXTENSIONS,
  MaterializeResult,
  _filter_tables_for_extensions,
  _staging_sql,
  build_postgres_connstr,
  validate_materializer_against_schema,
)


def _env():
  from robosystems.config import env

  return env


GRAPH_ID = "kg01234567890abcdef"
ENTITY_ID = "entity_kg01234567890abcdef"
CONNSTR = "dbname=extensions user=postgres password=postgres host=pg port=5432"


class TestBuildPostgresConnstr:
  def test_parses_url(self):
    with patch.object(
      _env(),
      "EXTENSIONS_DATABASE_URL",
      "postgresql://myuser:mypass@myhost:5433/extensions",
    ):
      result = build_postgres_connstr()

    assert "dbname=extensions" in result
    assert "user=myuser" in result
    assert "password=mypass" in result
    assert "host=myhost" in result
    assert "port=5433" in result

  def test_defaults_for_minimal_url(self):
    with patch.object(
      _env(),
      "EXTENSIONS_DATABASE_URL",
      "postgresql://localhost/",
    ):
      result = build_postgres_connstr()

    assert "host=localhost" in result
    assert "port=5432" in result

  def test_extracts_dbname_from_path(self):
    with patch.object(
      _env(),
      "EXTENSIONS_DATABASE_URL",
      "postgresql://postgres:postgres@pg:5432/extensions",
    ):
      result = build_postgres_connstr()

    assert "dbname=extensions" in result


class TestStagingSql:
  def test_generates_all_tables(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    expected = set(NODE_TABLES + RELATIONSHIP_TABLES)
    assert set(tables.keys()) == expected

  def test_node_tables_use_postgres_scan(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    for name in ["Entity", "Element", "Transaction", "Entry", "LineItem", "Dimension"]:
      assert "postgres_scan" in tables[name]
      assert CONNSTR in tables[name]
      assert GRAPH_ID in tables[name]

  def test_entity_table_references_entities(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert "'entities'" in tables["Entity"]

  def test_element_uses_qname_prefix(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    sql = tables["Element"]
    # Should derive prefix from external_source, not hardcode
    assert "CASE" in sql
    assert "'qb:'" in sql
    assert "'rl:'" in sql

  def test_element_reads_from_elements(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert "'elements'" in tables["Element"]

  def test_amounts_converted_to_dollars(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    # Transaction amounts and LineItem amounts should divide by 100
    assert "/ 100.0" in tables["Transaction"]
    assert "/ 100.0" in tables["LineItem"]

  def test_relationship_tables_have_src_dst(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    for name in RELATIONSHIP_TABLES:
      sql = tables[name]
      assert "AS src" in sql
      assert "AS dst" in sql

  def test_entity_has_transaction_uses_entity_id(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert ENTITY_ID in tables["ENTITY_HAS_TRANSACTION"]

  def test_structure_table_is_static(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert f"'{GRAPH_ID}_coa'" in tables["Structure"]
    assert "ChartOfAccounts" in tables["Structure"]


class TestMaterializeResult:
  def test_defaults(self):
    result = MaterializeResult(graph_id=GRAPH_ID)
    assert result.status == "success"
    assert result.tables_staged == []
    assert result.tables_materialized == []
    assert result.total_rows == 0
    assert result.duration_ms == 0
    assert result.errors == []


class TestTableOrdering:
  def test_nodes_before_relationships(self):
    """Node tables should be listed before relationship tables."""
    all_tables = NODE_TABLES + RELATIONSHIP_TABLES
    node_end = max(all_tables.index(n) for n in NODE_TABLES)
    rel_start = min(all_tables.index(r) for r in RELATIONSHIP_TABLES)
    assert node_end < rel_start

  def test_entity_is_first_node(self):
    assert NODE_TABLES[0] == "Entity"

  def test_expected_node_count(self):
    assert len(NODE_TABLES) == 17  # 8 transaction + 6 reporting + 3 investor

  def test_expected_relationship_count(self):
    # 11 transaction + 2 base (ENTITY_HAS_TAXONOMY, TAXONOMY_EXTENDS_TAXONOMY)
    # + 10 reporting + 4 investor (+ ENTITY_HAS_PORTFOLIO)
    assert len(RELATIONSHIP_TABLES) == 27


class TestSchemaConsistency:
  """Guardrails that keep materialize.py in sync with the schema layer.

  These tests are the drift detector for option A of the schema-awareness
  question — they don't make the materializer schema-driven, but they catch
  the common failure modes (forgotten edge wiring, stale references,
  missing extension mappings) at test time instead of at runtime.
  """

  def test_materializer_in_sync_with_schema(self):
    """Fail loud if any schema table is missing from the materializer (or
    vice versa), unless explicitly in the SEC-only / deferred allow-list."""
    # Raises RuntimeError with a clear diff if drift is detected.
    validate_materializer_against_schema()

  def test_every_materializer_table_has_extension_mapping(self):
    """Every entry in NODE_TABLES and RELATIONSHIP_TABLES must be in
    TABLE_EXTENSIONS — otherwise the extension-filter pass would treat it
    as 'base' by default, which can silently wrong-classify investor
    tables onto ledger-only graphs."""
    mapped = set(TABLE_EXTENSIONS.keys())
    unmapped = (set(NODE_TABLES) | set(RELATIONSHIP_TABLES)) - mapped
    assert not unmapped, (
      f"Tables missing from TABLE_EXTENSIONS: {sorted(unmapped)}. "
      f"Add each with its owning extension name."
    )

  def test_extension_filter_excludes_investor_from_ledger_only_graph(self):
    """On a roboledger-only graph, investor node and edge tables must not
    be staged (they don't exist in the LadybugDB schema and would cause
    'Table does not exist' errors)."""
    tables = _filter_tables_for_extensions(
      NODE_TABLES + RELATIONSHIP_TABLES, {"roboledger"}
    )
    investor_leak = {
      "Portfolio",
      "Security",
      "Position",
      "ENTITY_HAS_PORTFOLIO",
      "PORTFOLIO_HAS_POSITION",
      "POSITION_IN_SECURITY",
      "ENTITY_ISSUES_SECURITY",
    } & set(tables)
    assert not investor_leak, (
      f"Investor tables leaked into ledger-only filter: {sorted(investor_leak)}"
    )

  def test_extension_filter_excludes_ledger_from_investor_only_graph(self):
    """Symmetrically — ledger-specific tables must not leak into an
    investor-only materialization."""
    tables = _filter_tables_for_extensions(
      NODE_TABLES + RELATIONSHIP_TABLES, {"roboinvestor"}
    )
    ledger_leak = {
      "Transaction",
      "Entry",
      "LineItem",
      "Report",
      "Fact",
      "FactSet",
      "ENTITY_HAS_TRANSACTION",
      "REPORT_HAS_FACT",
    } & set(tables)
    assert not ledger_leak, (
      f"Ledger tables leaked into investor-only filter: {sorted(ledger_leak)}"
    )

  def test_extension_filter_base_always_included(self):
    """Base tables (Entity, Taxonomy, ENTITY_HAS_TAXONOMY, etc.) must
    materialize regardless of which extension is enabled."""
    base_tables = {"Entity", "Element", "Taxonomy", "ENTITY_HAS_TAXONOMY"}
    for enabled in ({"roboledger"}, {"roboinvestor"}, {"roboledger", "roboinvestor"}):
      tables = set(
        _filter_tables_for_extensions(NODE_TABLES + RELATIONSHIP_TABLES, enabled)
      )
      missing = base_tables - tables
      assert not missing, (
        f"Base tables missing from filter with enabled={enabled}: {sorted(missing)}"
      )


class TestLedgerMaterializer:
  @pytest.mark.asyncio
  async def test_materialize_success(self):
    from robosystems.operations.extensions.materialize import LedgerMaterializer

    mock_client = AsyncMock()
    mock_client.database_exists.return_value = True
    mock_client.query_table.return_value = {"success": True}
    mock_client.materialize_table.return_value = {"rows_ingested": 10}

    materializer = LedgerMaterializer()

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        return_value=mock_client,
      ),
      patch.object(
        _env(),
        "EXTENSIONS_DATABASE_URL",
        "postgresql://postgres:postgres@pg:5432/extensions",
      ),
    ):
      mock_client.__aenter__ = AsyncMock(return_value=mock_client)
      mock_client.__aexit__ = AsyncMock(return_value=False)

      result = await materializer.materialize(GRAPH_ID)

    assert result.status == "success"
    assert result.total_rows > 0
    assert len(result.tables_staged) > 0
    assert len(result.tables_materialized) > 0

  @pytest.mark.asyncio
  async def test_materialize_client_failure(self):
    from robosystems.operations.extensions.materialize import LedgerMaterializer

    materializer = LedgerMaterializer()

    with patch(
      "robosystems.graph_api.client.factory.get_graph_client",
      side_effect=Exception("Connection refused"),
    ):
      result = await materializer.materialize(GRAPH_ID)

    assert result.status == "error"
    assert len(result.errors) > 0
    assert "Connection refused" in result.errors[0]

  @pytest.mark.asyncio
  async def test_default_entity_id(self):
    from robosystems.operations.extensions.materialize import LedgerMaterializer

    mock_client = AsyncMock()
    mock_client.database_exists.return_value = False
    mock_client.create_database.return_value = {"success": True}
    mock_client.install_schema.return_value = {"success": True}
    mock_client.query_table.return_value = {"success": True}
    mock_client.materialize_table.return_value = {"rows_ingested": 0}

    materializer = LedgerMaterializer()

    with (
      patch(
        "robosystems.graph_api.client.factory.get_graph_client",
        return_value=mock_client,
      ),
      patch.object(
        _env(),
        "EXTENSIONS_DATABASE_URL",
        "postgresql://postgres:postgres@pg:5432/extensions",
      ),
      patch(
        "robosystems.schemas.loader.get_contextual_schema_loader",
      ) as mock_loader,
    ):
      mock_loader.return_value.nodes = {}
      mock_loader.return_value.relationships = {}
      mock_client.__aenter__ = AsyncMock(return_value=mock_client)
      mock_client.__aexit__ = AsyncMock(return_value=False)

      result = await materializer.materialize(GRAPH_ID)

    assert result.graph_id == GRAPH_ID
