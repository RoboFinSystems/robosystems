"""Smoke tests for ``TaxonomyBlockRegistryEntry``."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest
from pydantic import BaseModel

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  TaxonomyBlockEnvelope,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block.types import TaxonomyBlockRegistryEntry


def _dummy_create(session: Any, payload: BaseModel, created_by: str) -> str:
  return "tx_dummy"


def _dummy_update(session: Any, payload: BaseModel, updated_by: str) -> str:
  return "tx_dummy"


def _dummy_delete(session: Any, payload: BaseModel, deleted_by: str) -> str:
  return "tx_dummy"


def _dummy_build_envelope(
  session: Any, taxonomy_id: str
) -> TaxonomyBlockEnvelope | None:
  return None


def _make_entry(**overrides: Any) -> TaxonomyBlockRegistryEntry:
  defaults: dict[str, Any] = {
    "id": "custom_ontology",
    "display_name": "Custom Ontology",
    "display_plural": "Custom Ontologies",
    "category": "Custom",
    "icon": "book-open",
    "description": "Tenant-authored declarative ontology block.",
    "construction_mode": "declarative",
    "requires_parent_taxonomy": False,
    "create_request_model": CreateTaxonomyBlockRequest,
    "update_request_model": UpdateTaxonomyBlockRequest,
    "delete_request_model": DeleteTaxonomyBlockRequest,
    "dispatch_create": _dummy_create,
    "dispatch_update": _dummy_update,
    "dispatch_delete": _dummy_delete,
    "dispatch_build_envelope": _dummy_build_envelope,
    "surfaces_in_library": False,
  }
  defaults.update(overrides)
  return TaxonomyBlockRegistryEntry(**defaults)


def test_entry_happy_path_construction() -> None:
  entry = _make_entry()
  assert entry.id == "custom_ontology"
  assert entry.construction_mode == "declarative"
  assert entry.requires_parent_taxonomy is False
  assert entry.surfaces_in_library is False
  assert entry.create_request_model is CreateTaxonomyBlockRequest
  assert entry.dispatch_create is _dummy_create
  assert entry.dispatch_build_envelope is _dummy_build_envelope


def test_entry_is_frozen() -> None:
  entry = _make_entry()
  with pytest.raises(dataclasses.FrozenInstanceError):
    entry.id = "chart_of_accounts"  # type: ignore[misc]


def test_entry_library_surface_flag() -> None:
  entry = _make_entry(
    id="reporting_standard",
    display_name="Reporting Standard",
    display_plural="Reporting Standards",
    category="Library",
    construction_mode="read_only",
    surfaces_in_library=True,
  )
  assert entry.surfaces_in_library is True
  assert entry.construction_mode == "read_only"


def test_entry_requires_parent_for_extension_arm() -> None:
  entry = _make_entry(
    id="reporting_extension",
    display_name="Reporting Extension",
    display_plural="Reporting Extensions",
    category="Reporting",
    construction_mode="extend",
    requires_parent_taxonomy=True,
  )
  assert entry.requires_parent_taxonomy is True
  assert entry.construction_mode == "extend"
