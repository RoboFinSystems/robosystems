"""Tests for extensions materialization pipeline."""

from unittest.mock import AsyncMock, patch

import pytest

from robosystems.operations.extensions.materialize import (
  NODE_TABLES,
  RELATIONSHIP_TABLES,
  MaterializeResult,
  _staging_sql,
  build_postgres_connstr,
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
      result = build_postgres_connstr(GRAPH_ID)

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
      result = build_postgres_connstr(GRAPH_ID)

    assert "host=localhost" in result
    assert "port=5432" in result

  def test_extracts_dbname_from_path(self):
    with patch.object(
      _env(),
      "EXTENSIONS_DATABASE_URL",
      "postgresql://postgres:postgres@pg:5432/extensions",
    ):
      result = build_postgres_connstr(GRAPH_ID)

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

  def test_element_reads_from_accounts(self):
    tables = _staging_sql(GRAPH_ID, ENTITY_ID, CONNSTR)
    assert "'accounts'" in tables["Element"]

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
    assert len(NODE_TABLES) == 8

  def test_expected_relationship_count(self):
    assert len(RELATIONSHIP_TABLES) == 10


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
