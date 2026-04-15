"""Tests for custom schema parser — parameterized type support."""

import pytest

from robosystems.schemas.runtime.custom import CustomSchemaParser, SchemaFormat


@pytest.fixture
def parser():
  return CustomSchemaParser()


class TestParameterizedTypes:
  def test_float_array_type_accepted(self, parser):
    """FLOAT[384] should be accepted as a valid parameterized type."""
    prop = parser._build_property({"name": "embedding", "type": "FLOAT[384]"})
    assert prop.name == "embedding"
    assert prop.type == "FLOAT[384]"

  def test_float_array_lowercase(self, parser):
    """float[384] should be uppercased and accepted."""
    prop = parser._build_property({"name": "embedding", "type": "float[384]"})
    assert prop.type == "FLOAT[384]"

  def test_double_array_type(self, parser):
    """DOUBLE[128] should be accepted."""
    prop = parser._build_property({"name": "vec", "type": "DOUBLE[128]"})
    assert prop.type == "DOUBLE[128]"

  def test_invalid_base_type_still_rejected(self, parser):
    """INVALID[384] should be rejected since INVALID is not a valid base type."""
    with pytest.raises(ValueError, match="Invalid type"):
      parser._build_property({"name": "bad", "type": "INVALID[384]"})

  def test_plain_float_still_works(self, parser):
    """Plain FLOAT should still work."""
    prop = parser._build_property({"name": "value", "type": "FLOAT"})
    assert prop.type == "FLOAT"

  def test_full_schema_with_embedding_column(self, parser):
    """End-to-end: schema with a FLOAT[384] embedding column parses correctly."""
    schema = parser.parse(
      {
        "name": "test_schema",
        "nodes": [
          {
            "name": "Product",
            "properties": [
              {"name": "id", "type": "STRING", "is_primary_key": True},
              {"name": "name", "type": "STRING"},
              {"name": "embedding", "type": "FLOAT[384]"},
            ],
          }
        ],
        "relationships": [],
      },
      format=SchemaFormat.DICT,
    )
    product_node = schema.nodes[0]
    embedding_prop = next(p for p in product_node.properties if p.name == "embedding")
    assert embedding_prop.type == "FLOAT[384]"
