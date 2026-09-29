"""Tax identifiers are neither solicited nor returned whole on MCP."""

from unittest.mock import MagicMock

import pytest

from robosystems.middleware.mcp.tools.registrar import build_tools_for_extension
from robosystems.middleware.mcp.tools.sensitive import (
  drop_withheld_arguments,
  mask_tax_ids,
  withhold_input_fields,
)


@pytest.mark.unit
@pytest.mark.parametrize("extension", ["roboledger", "roboinvestor"])
def test_no_generated_tool_asks_for_a_tax_id(extension):
  client = MagicMock()
  client.graph_id = "kg1"
  for name, tool in build_tools_for_extension(extension, client).items():
    schema = tool.get_tool_definition()["inputSchema"]
    assert "tax_id" not in schema.get("properties", {}), name
    assert "tax_id" not in schema.get("required", []), name


@pytest.mark.unit
def test_withhold_input_fields_drops_property_and_requirement():
  schema = {
    "type": "object",
    "properties": {"name": {"type": "string"}, "tax_id": {"type": "string"}},
    "required": ["name", "tax_id"],
  }
  out = withhold_input_fields(schema)
  assert out["properties"] == {"name": {"type": "string"}}
  assert out["required"] == ["name"]
  assert "tax_id" in schema["properties"]


@pytest.mark.unit
def test_withheld_arguments_never_reach_validation():
  assert drop_withheld_arguments({"name": "Acme", "tax_id": "123-45-6789"}) == {
    "name": "Acme"
  }


@pytest.mark.unit
def test_tax_ids_are_masked_to_last_four_at_any_depth():
  result = {
    "agents": [
      {"name": "Acme", "tax_id": "123-45-6789"},
      {"name": "Blank", "tax_id": None},
    ],
    "data": {"agent": {"taxId": "12-3456789"}},
  }
  assert mask_tax_ids(result) == {
    "agents": [
      {"name": "Acme", "tax_id": "***6789"},
      {"name": "Blank", "tax_id": None},
    ],
    "data": {"agent": {"taxId": "***6789"}},
  }


@pytest.mark.unit
def test_masking_an_already_masked_value_is_a_no_op():
  assert mask_tax_ids({"tax_id": "***6789"}) == {"tax_id": "***6789"}


@pytest.mark.unit
def test_cypher_columns_are_matched_by_property_name():
  assert mask_tax_ids([{"e.tax_id": "123-45-6789", "e.name": "Solo"}]) == [
    {"e.tax_id": "***6789", "e.name": "Solo"}
  ]


@pytest.mark.unit
def test_shared_repositories_are_left_verbatim():
  from robosystems.middleware.mcp.tools.sensitive import mask_tax_ids_for_graph

  row = {"e.tax_id": "12-3456789"}
  assert mask_tax_ids_for_graph("sec", row) == row
  assert mask_tax_ids_for_graph("kg1", row) == {"e.tax_id": "***6789"}
