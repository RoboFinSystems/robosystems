"""Custom-schema names are bare identifiers before they become DDL."""

import pytest
from pydantic import ValidationError

from robosystems.models.api.graphs.schema import CustomSchemaDefinition
from robosystems.schemas.identifiers import (
  RESERVED_WORDS,
  require_identifier,
  require_type,
)
from robosystems.schemas.runtime.custom import CustomSchemaManager
from robosystems.security.cypher_analyzer import CypherSecurityAnalyzer

INJECTED_NAME = (
  "Leak(line STRING, PRIMARY KEY(line)); COPY Leak FROM '/tmp/x.csv' (header=false); "
  "CREATE NODE TABLE IF NOT EXISTS Item"
)


def _schema(node_name="Item", prop_name="id", prop_type="STRING", relationship=None):
  definition = {
    "name": "demo",
    "nodes": [
      {
        "name": node_name,
        "properties": [{"name": prop_name, "type": prop_type, "is_primary_key": True}],
      }
    ],
  }
  if relationship is not None:
    definition["relationships"] = [relationship]
  return definition


@pytest.mark.unit
class TestIdentifierRules:
  @pytest.mark.parametrize("name", ["Item", "_private", "Item_2", "a" * 128])
  def test_bare_identifiers_pass(self, name):
    assert require_identifier(name, "node") == name

  @pytest.mark.parametrize(
    "name",
    [
      INJECTED_NAME,
      "Item;",
      "Item Two",
      "`Item`",
      "1Item",
      "Item)",
      "",
      "a" * 129,
      None,
      7,
    ],
  )
  def test_anything_else_is_refused(self, name):
    with pytest.raises(ValueError):
      require_identifier(name, "node")

  @pytest.mark.parametrize(
    "name", ["copy", "Load", "IMPORT", "export", "use", "set", "call"]
  )
  def test_statement_keywords_are_refused_as_names(self, name):
    with pytest.raises(ValueError, match="reserved word"):
      require_identifier(name, "property")

  def test_reserved_words_cover_what_the_ddl_guard_refuses(self):
    guarded = (
      CypherSecurityAnalyzer.WRITE_KEYWORDS
      | CypherSecurityAnalyzer.BULK_KEYWORDS
      | CypherSecurityAnalyzer.ADMIN_KEYWORDS
      | {"CALL"}
    )
    assert guarded <= RESERVED_WORDS

  def test_wildcard_only_where_allowed(self):
    assert require_identifier("*", "from_node", allow_wildcard=True) == "*"
    with pytest.raises(ValueError):
      require_identifier("*", "node")

  @pytest.mark.parametrize("type_name", ["STRING", "float[384]", "INT64", "DOUBLE[3]"])
  def test_plain_and_sized_types_pass(self, type_name):
    assert require_type(type_name, "p") == type_name.upper()

  @pytest.mark.parametrize(
    "type_name", ["STRING); COPY x FROM 'y'", "STRING[", "STRING[a]", ""]
  )
  def test_type_text_cannot_carry_statements(self, type_name):
    with pytest.raises(ValueError):
      require_type(type_name, "p")


@pytest.mark.unit
class TestParserRefusesInjectedNames:
  def test_well_formed_schema_parses(self):
    schema = CustomSchemaManager().create_from_dict(_schema())
    assert [node.name for node in schema.nodes] == ["Item"]

  def test_node_name_with_statement_separator_is_refused(self):
    with pytest.raises(ValueError, match="node name"):
      CustomSchemaManager().create_from_dict(_schema(node_name=INJECTED_NAME))

  def test_property_name_and_type_are_checked(self):
    with pytest.raises(ValueError, match="property name"):
      CustomSchemaManager().create_from_dict(_schema(prop_name="id); COPY"))
    with pytest.raises(ValueError, match="reserved word"):
      CustomSchemaManager().create_from_dict(_schema(prop_name="copy"))
    with pytest.raises(ValueError, match="Invalid type"):
      CustomSchemaManager().create_from_dict(_schema(prop_type="STRING); COPY"))

  def test_relationship_names_and_ends_are_checked(self):
    bad = {"name": "HAS; COPY", "from_node": "Item", "to_node": "Item"}
    with pytest.raises(ValueError, match="relationship name"):
      CustomSchemaManager().create_from_dict(_schema(relationship=bad))
    bad = {"name": "HAS", "from_node": "Item); COPY", "to_node": "Item"}
    with pytest.raises(ValueError, match="from_node"):
      CustomSchemaManager().create_from_dict(_schema(relationship=bad))


@pytest.mark.unit
class TestApiModelRefusesInjectedNames:
  def test_well_formed_schema_validates(self):
    assert CustomSchemaDefinition.model_validate(_schema()).nodes[0]["name"] == "Item"

  @pytest.mark.parametrize(
    "definition",
    [
      _schema(node_name=INJECTED_NAME),
      _schema(prop_name="id); COPY"),
      _schema(prop_name="copy"),
      _schema(prop_type="STRING); COPY"),
      _schema(
        relationship={"name": "HAS; COPY", "from_node": "Item", "to_node": "Item"}
      ),
    ],
  )
  def test_bad_names_fail_at_the_api_edge(self, definition):
    with pytest.raises(ValidationError):
      CustomSchemaDefinition.model_validate(definition)
