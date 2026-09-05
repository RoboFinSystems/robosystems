"""The XBRL tables are one asset with two consumers.

``xbrlkit.schema`` declares the property-graph tables a filing projects into;
``schemas/base.py`` and ``schemas/extensions/roboledger.py`` build their
XBRL ``Node`` / ``Relationship`` objects from it. The compiled SEC schema's
DDL must therefore equal xbrlkit's for every table xbrlkit declares — the
guarantee that Cypher written against a single-filing ``.lbug`` runs on the
shared ``sec`` graph, and that a parquet file written in xbrlkit's column
order loads positionally into either.
"""

import pytest
from xbrlkit.schema import ENRICHMENT_TABLES, NODE_TABLES, REL_TABLES, ddl

from robosystems.schemas.runtime.builder import LadybugSchemaBuilder
from robosystems.schemas.xbrl import (
  xbrl_node_properties,
  xbrl_relationship_properties,
)

SEC_SCHEMA_CONFIG = {
  "name": "SEC Database Schema",
  "description": "base + roboledger, as the SEC repository compiles it",
  "base_schema": "base",
  "extensions": ["roboledger"],
}


@pytest.fixture(scope="module")
def sec_schema():
  builder = LadybugSchemaBuilder(SEC_SCHEMA_CONFIG)
  builder.load_schemas()
  return builder.schema


@pytest.fixture(scope="module")
def sec_nodes(sec_schema):
  return {node.name: node for node in sec_schema.nodes}


@pytest.fixture(scope="module")
def sec_relationships(sec_schema):
  return {rel.name: rel for rel in sec_schema.relationships}


@pytest.mark.unit
class TestNodeTables:
  @pytest.mark.parametrize("table", NODE_TABLES, ids=lambda t: t.name)
  def test_ddl_is_xbrlkits(self, sec_nodes, table):
    assert table.name in sec_nodes, f"the SEC schema declares no {table.name}"
    assert sec_nodes[table.name].to_cypher() == table.ddl()

  @pytest.mark.parametrize("table", NODE_TABLES, ids=lambda t: t.name)
  def test_columns_in_xbrlkits_order(self, sec_nodes, table):
    node = sec_nodes[table.name]
    assert [p.name for p in node.properties] == list(table.columns)
    assert [p.type for p in node.properties] == [p.type for p in table.properties]
    assert [p.name for p in node.properties if p.is_primary_key] == [table.primary_key]


@pytest.mark.unit
class TestRelationshipTables:
  @pytest.mark.parametrize("table", REL_TABLES, ids=lambda t: t.name)
  def test_ddl_is_xbrlkits(self, sec_relationships, table):
    assert table.name in sec_relationships, f"the SEC schema declares no {table.name}"
    assert sec_relationships[table.name].to_cypher() == table.ddl()

  @pytest.mark.parametrize("table", REL_TABLES, ids=lambda t: t.name)
  def test_endpoints_are_xbrlkits(self, sec_relationships, table):
    rel = sec_relationships[table.name]
    assert (rel.from_node, rel.to_node) == (table.from_node, table.to_node)
    assert [(p.name, p.type) for p in rel.properties] == [
      (p.name, p.type) for p in table.properties
    ]


@pytest.mark.unit
class TestWholeDdl:
  def test_every_xbrlkit_statement_is_in_the_sec_schema(self, sec_schema):
    """The full DDL xbrlkit runs for a ``.lbug`` is a subset of the SEC schema's."""
    sec_statements = {n.to_cypher() for n in sec_schema.nodes} | {
      r.to_cypher() for r in sec_schema.relationships
    }
    missing = [statement for statement in ddl() if statement not in sec_statements]
    assert missing == []

  def test_enrichment_tables_are_declared_but_platform_filled(self):
    """FactSet / Classification and their edges exist in both schemas; only
    the platform's enrichment writes rows into them."""
    declared = {t.name for t in NODE_TABLES} | {t.name for t in REL_TABLES}
    assert declared >= ENRICHMENT_TABLES


@pytest.mark.unit
class TestHelpers:
  def test_node_properties_mark_the_primary_key(self):
    props = xbrl_node_properties("Fact")
    assert props[0].name == "identifier" and props[0].is_primary_key
    assert not any(p.is_primary_key for p in props[1:])

  def test_relationship_properties_follow_xbrlkit(self):
    assert [p.name for p in xbrl_relationship_properties("TAXONOMY_HAS_LABEL")] == [
      "element_uri"
    ]
    assert xbrl_relationship_properties("REPORT_HAS_FACT") == []

  def test_unknown_table_is_an_error(self):
    with pytest.raises(KeyError):
      xbrl_node_properties("Transaction")
