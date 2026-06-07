"""Smoke tests for the Taxonomy Block registry."""

from __future__ import annotations

import pytest

from robosystems.operations.taxonomy_block import registry as registry_module

EXPECTED_TYPES = {
  "chart_of_accounts",
  "schedule",
  "reporting_standard",
  "reporting_extension",
  "custom_ontology",
}


def test_registry_covers_all_writable_types() -> None:
  assert set(registry_module.REGISTRY.keys()) == EXPECTED_TYPES


def test_registry_entries_callable_handlers() -> None:
  for taxonomy_type in EXPECTED_TYPES:
    entry = registry_module.get(taxonomy_type)
    assert callable(entry.dispatch_create)
    assert callable(entry.dispatch_update)
    assert callable(entry.dispatch_delete)
    assert callable(entry.dispatch_build_envelope)


def test_reporting_extension_requires_parent() -> None:
  entry = registry_module.get("reporting_extension")
  assert entry.requires_parent_taxonomy is True
  assert entry.construction_mode == "extend"


def test_reporting_standard_is_read_only() -> None:
  entry = registry_module.get("reporting_standard")
  assert entry.construction_mode == "read_only"
  assert entry.surfaces_in_library is True


def test_custom_ontology_is_declarative() -> None:
  entry = registry_module.get("custom_ontology")
  assert entry.construction_mode == "declarative"
  assert entry.requires_parent_taxonomy is False


def test_unknown_type_raises_keyerror() -> None:
  with pytest.raises(KeyError):
    registry_module.get("not_a_real_type")


def test_strawberry_envelope_field_parity() -> None:
  """Catch Pydantic→Strawberry drift — every Pydantic field should map."""
  from robosystems.graphql.types.taxonomy_block import TaxonomyBlock
  from robosystems.models.api.taxonomy_block import TaxonomyBlockEnvelope

  pydantic_fields = set(TaxonomyBlockEnvelope.model_fields.keys())
  strawberry_fields = {
    f.python_name for f in TaxonomyBlock.__strawberry_definition__.fields
  }
  assert pydantic_fields == strawberry_fields, (
    f"Pydantic fields not in Strawberry: {pydantic_fields - strawberry_fields}; "
    f"Strawberry fields not in Pydantic: {strawberry_fields - pydantic_fields}"
  )
