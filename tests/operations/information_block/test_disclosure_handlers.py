"""Disclosure handler tests — registry entry + dispatch_build_envelope.

``regulatory_disclosure`` is the first render target beyond the
statement family. It shares the statement envelope builder
(parameterised on block_type) with two disclosure-specific behaviours:
arc-less structures (the library's disclosure-identity envelopes)
return no envelope, and the display name falls back to the structure's
own name (a note is named by its author/taxonomy, not its type).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from robosystems.operations.information_block import disclosure as disclosure_handlers
from robosystems.operations.information_block.registry import REGISTRY


def _exec_result(
  *,
  scalars_all: list[Any] | None = None,
  scalar: Any = None,
  all_rows: list[Any] | None = None,
) -> MagicMock:
  """Build a MagicMock shaped like a SQLAlchemy Result object."""
  result = MagicMock()
  if scalars_all is not None:
    result.scalars.return_value.all.return_value = scalars_all
  if all_rows is not None:
    result.all.return_value = all_rows
  if scalar is not None:
    result.scalar.return_value = scalar
  else:
    result.scalar.return_value = None
  return result


def _make_disclosure_structure(
  *,
  structure_id: str = "struct_inventory_note",
  name: str = "Inventory, by Category",
) -> MagicMock:
  """Shape a MagicMock like a tenant-authored disclosure Structure row."""
  structure = MagicMock()
  structure.id = structure_id
  structure.block_type = "regulatory_disclosure"
  structure.name = name
  structure.description = None
  structure.taxonomy_id = "tax_extension"
  structure.artifact_mechanics = None
  structure.concept_arrangement = "roll_up"
  structure.member_arrangement = None
  structure.renderer_note = None
  structure.metadata_ = {}
  return structure


class TestRegistryEntry:
  def test_regulatory_disclosure_is_registered(self) -> None:
    entry = REGISTRY["regulatory_disclosure"]
    assert entry.construction_mode == "compositional"
    assert entry.concept_arrangement_default == "roll_up"
    # Library-seeded disclosure rows are arc-less identity envelopes —
    # they must not surface on the library sentinel.
    assert entry.surfaces_in_library is False

  def test_create_points_at_taxonomy_block(self) -> None:
    entry = REGISTRY["regulatory_disclosure"]
    with pytest.raises(NotImplementedError, match="create-taxonomy-block"):
      entry.dispatch_create(MagicMock(), MagicMock(), "usr_test")

  def test_update_points_at_taxonomy_block(self) -> None:
    entry = REGISTRY["regulatory_disclosure"]
    with pytest.raises(NotImplementedError, match="update-taxonomy-block"):
      entry.dispatch_update(MagicMock(), MagicMock(), "usr_test")

  def test_delete_points_at_taxonomy_block(self) -> None:
    entry = REGISTRY["regulatory_disclosure"]
    with pytest.raises(NotImplementedError, match="delete-taxonomy-block"):
      entry.dispatch_delete(MagicMock(), MagicMock(), "usr_test")


class TestBuildEnvelope:
  def test_returns_none_when_structure_missing(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    assert disclosure_handlers.build_envelope(session, "struct_missing") is None

  def test_returns_none_for_wrong_block_type(self) -> None:
    """A statement structure must not surface through the disclosure handler."""
    session = MagicMock()
    structure = MagicMock()
    structure.block_type = "balance_sheet"
    session.get.return_value = structure
    assert disclosure_handlers.build_envelope(session, "struct_bs") is None

  def test_returns_none_for_arcless_identity_envelope(self) -> None:
    """The library's ``disclosures:*`` identity rows carry no arcs — they
    are disclosure registry entries, not renderable blocks, and must not
    leak into ``list-information-blocks`` as empty envelopes."""
    session = MagicMock()
    session.get.return_value = _make_disclosure_structure(
      structure_id="struct_ppe_disclosure",
      name="Property, Plant and Equipment Disclosure",
    )
    # Query order with no associations: fact_set → taxonomy name →
    # associations → rules → verification_results.
    session.execute.side_effect = [
      _exec_result(scalar=None),
      _exec_result(scalar="rs-gaap"),
      _exec_result(scalars_all=[]),  # associations — arc-less
      _exec_result(scalars_all=[]),
      _exec_result(scalars_all=[]),
    ]
    assert disclosure_handlers.build_envelope(session, "struct_ppe_disclosure") is None

  def test_renders_arc_bearing_note_with_structure_name_as_display(self) -> None:
    """An arc-bearing note builds an envelope; display name is the
    structure's own name, not a type-level constant."""
    session = MagicMock()
    structure = _make_disclosure_structure()
    session.get.return_value = structure

    association = MagicMock()
    association.id = "assoc_note_1"
    association.from_element_id = "elem_inventory_total"
    association.to_element_id = "elem_raw_materials"
    association.association_type = "presentation"
    association.arcrole = "http://…/parent-child"
    association.order_value = 1.0
    association.weight = None

    elem_total = MagicMock()
    elem_total.id = "elem_inventory_total"
    elem_total.qname = (
      "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings"
    )
    elem_total.name = "Inventory"
    elem_total.code = None
    elem_total.element_type = "concept"
    elem_total.is_abstract = False
    elem_total.is_monetary = True
    elem_total.balance_type = "debit"
    elem_total.period_type = "instant"
    elem_total.item_type = None

    elem_raw = MagicMock()
    elem_raw.id = "elem_raw_materials"
    elem_raw.qname = "ext:InventoryRawMaterials"
    elem_raw.name = "Raw Materials"
    elem_raw.code = None
    elem_raw.element_type = "concept"
    elem_raw.is_abstract = False
    elem_raw.is_monetary = True
    elem_raw.balance_type = "debit"
    elem_raw.period_type = "instant"
    elem_raw.item_type = None

    # Query order with associations: fact_set → taxonomy name →
    # associations → elements → rules → association classifications →
    # verification_results → documentation labels.
    session.execute.side_effect = [
      _exec_result(scalar=None),
      _exec_result(scalar="Driftline Extension"),
      _exec_result(scalars_all=[association]),
      _exec_result(scalars_all=[elem_total, elem_raw]),
      _exec_result(scalars_all=[]),
      _exec_result(all_rows=[]),
      _exec_result(scalars_all=[]),
      _exec_result(all_rows=[]),
    ]

    envelope = disclosure_handlers.build_envelope(session, "struct_inventory_note")

    assert envelope is not None
    assert envelope.block_type == "regulatory_disclosure"
    assert envelope.name == "Inventory, by Category"
    assert envelope.display_name == "Inventory, by Category"
    assert envelope.category == "Reporting"
    assert envelope.information_model.concept_arrangement == "roll_up"
    assert len(envelope.connections) == 1
    assert {e.id for e in envelope.elements} == {
      "elem_inventory_total",
      "elem_raw_materials",
    }
