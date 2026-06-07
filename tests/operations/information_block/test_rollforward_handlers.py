"""Rollforward handler tests — authoring surface.

Exercises create / update / delete / build_envelope for the
``rollforward`` block-type. Filter evaluation is tested separately in
``tests/operations/roboledger/reports/test_rollforward_filters.py`` —
this file only covers the authoring + envelope shape.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.rollforward import (
  AttributionFilter,
  CreateRollforwardRequest,
  DeleteRollforwardRequest,
  LineItemMetadataPredicate,
  UpdateRollforwardRequest,
)
from robosystems.models.api.information_block import RollforwardMechanics
from robosystems.operations.information_block import rollforward as rollforward_handlers

MODULE = "robosystems.operations.information_block.rollforward"


def _element(element_id: str, qname: str, taxonomy_id: str = "tax_mini") -> MagicMock:
  el = MagicMock()
  el.id = element_id
  el.qname = qname
  el.taxonomy_id = taxonomy_id
  return el


def _body() -> CreateRollforwardRequest:
  return CreateRollforwardRequest(
    name="Cash and Cash Equivalents Rollforward",
    bs_source_qname="mini:CashAndCashEquivalents",
    default_change_tag_qname=None,
    attribution_filters=[
      AttributionFilter(
        target_qname="mini:ProceedsFromInvestmentsByOwner",
        predicate=LineItemMetadataPredicate(
          field="transaction_description_code",
          values=["mini:ProceedsFromInvestmentsByOwner"],
        ),
      )
    ],
  )


class TestCreate:
  def test_resolves_qnames_and_persists_structure(self) -> None:
    """create resolves BS source + filter target qnames, persists
    Structure with typed mechanics."""
    bs = _element("elem_cash", "mini:CashAndCashEquivalents")
    target = _element("elem_proceeds", "mini:ProceedsFromInvestmentsByOwner")

    # Two _resolve_qname calls: bs_source then filter target #0.
    session = MagicMock()
    session.execute.return_value.scalar.side_effect = [bs, target]

    added_structures: list[Any] = []
    session.add.side_effect = lambda s: added_structures.append(s)

    # Structure.id is set by SQLAlchemy default after flush; simulate it.
    def _flush_side_effect() -> None:
      if added_structures:
        added_structures[0].id = "struct_rf_cash_01"

    session.flush.side_effect = _flush_side_effect

    structure_id = rollforward_handlers.create(session, _body(), "usr_test")

    assert structure_id == "struct_rf_cash_01"
    assert len(added_structures) == 1
    persisted = added_structures[0]
    assert persisted.block_type == "rollforward"
    assert persisted.taxonomy_id == "tax_mini"  # inherited from bs source
    assert persisted.concept_arrangement == "roll_forward"

    # Mechanics blob carries resolved element_ids
    mech = RollforwardMechanics.model_validate(persisted.artifact_mechanics)
    assert mech.bs_source_element_id == "elem_cash"
    assert mech.bs_source_qname == "mini:CashAndCashEquivalents"
    assert len(mech.attribution_filters) == 1
    assert mech.attribution_filters[0].target_element_id == "elem_proceeds"
    assert (
      mech.attribution_filters[0].target_qname == "mini:ProceedsFromInvestmentsByOwner"
    )

    session.commit.assert_called_once()

  def test_raises_when_bs_source_qname_does_not_resolve(self) -> None:
    session = MagicMock()
    session.execute.return_value.scalar.return_value = None

    with pytest.raises(ValueError) as exc:
      rollforward_handlers.create(session, _body(), "usr_test")
    assert "bs_source" in str(exc.value)
    assert "mini:CashAndCashEquivalents" in str(exc.value)
    session.commit.assert_not_called()

  def test_raises_when_filter_target_qname_does_not_resolve(self) -> None:
    bs = _element("elem_cash", "mini:CashAndCashEquivalents")
    session = MagicMock()
    # bs resolves; filter target does not.
    session.execute.return_value.scalar.side_effect = [bs, None]

    with pytest.raises(ValueError) as exc:
      rollforward_handlers.create(session, _body(), "usr_test")
    assert "filter target #0" in str(exc.value)
    session.commit.assert_not_called()

  def test_resolves_default_change_tag_when_supplied(self) -> None:
    bs = _element("elem_cash", "mini:CashAndCashEquivalents")
    default_tag = _element(
      "elem_default", "rs-gaap:IncreaseDecreaseInCashAndCashEquivalents"
    )
    target = _element("elem_proceeds", "mini:ProceedsFromInvestmentsByOwner")

    session = MagicMock()
    session.execute.return_value.scalar.side_effect = [bs, default_tag, target]

    added: list[Any] = []
    session.add.side_effect = lambda s: added.append(s)
    session.flush.side_effect = lambda: setattr(added[0], "id", "struct_rf_02")

    body = _body()
    body.default_change_tag_qname = "rs-gaap:IncreaseDecreaseInCashAndCashEquivalents"
    rollforward_handlers.create(session, body, "usr_test")

    mech = RollforwardMechanics.model_validate(added[0].artifact_mechanics)
    assert mech.default_change_tag_element_id == "elem_default"
    # Round-trip: qname is persisted alongside the resolved element_id
    # so envelope readers can display the operator-readable form without
    # a separate Element table lookup.
    assert (
      mech.default_change_tag_qname
      == "rs-gaap:IncreaseDecreaseInCashAndCashEquivalents"
    )


class TestUpdate:
  def test_updates_mutable_fields_preserves_bs_source(self) -> None:
    """Update touches name + filters; bs_source stays put."""
    existing_mechanics = {
      "kind": "rollforward",
      "bs_source_element_id": "elem_cash",
      "bs_source_qname": "mini:CashAndCashEquivalents",
      "default_change_tag_element_id": None,
      "attribution_filters": [],
      "validation_mode": "residual_as_default",
    }
    structure = MagicMock()
    structure.id = "struct_rf_01"
    structure.block_type = "rollforward"
    structure.artifact_mechanics = existing_mechanics

    session = MagicMock()
    session.get.return_value = structure
    # Filter target resolution
    session.execute.return_value.scalar.return_value = _element(
      "elem_proceeds", "mini:ProceedsFromInvestmentsByOwner"
    )

    body = UpdateRollforwardRequest(
      structure_id="struct_rf_01",
      name="Cash RF (renamed)",
      attribution_filters=[
        AttributionFilter(
          target_qname="mini:ProceedsFromInvestmentsByOwner",
          predicate=LineItemMetadataPredicate(
            field="transaction_description_code",
            values=["mini:ProceedsFromInvestmentsByOwner"],
          ),
        )
      ],
    )

    rollforward_handlers.update(session, body, "usr_test")

    assert structure.name == "Cash RF (renamed)"
    assert structure.updated_by == "usr_test"
    next_mech = RollforwardMechanics.model_validate(structure.artifact_mechanics)
    # bs source preserved from prior persist
    assert next_mech.bs_source_element_id == "elem_cash"
    assert next_mech.bs_source_qname == "mini:CashAndCashEquivalents"
    # New filter persisted with resolved target
    assert len(next_mech.attribution_filters) == 1
    assert next_mech.attribution_filters[0].target_element_id == "elem_proceeds"

  def test_raises_when_structure_not_a_rollforward(self) -> None:
    structure = MagicMock()
    structure.block_type = "schedule"
    session = MagicMock()
    session.get.return_value = structure

    with pytest.raises(ValueError) as exc:
      rollforward_handlers.update(
        session,
        UpdateRollforwardRequest(structure_id="struct_sched_01", name="x"),
        "usr",
      )
    assert "rollforward" in str(exc.value).lower()


class TestDelete:
  def test_deletes_and_returns_structure_id(self) -> None:
    structure = MagicMock()
    structure.id = "struct_rf_99"
    structure.block_type = "rollforward"
    session = MagicMock()
    session.get.return_value = structure

    result = rollforward_handlers.delete(
      session, DeleteRollforwardRequest(structure_id="struct_rf_99"), "usr"
    )

    assert result == "struct_rf_99"
    session.delete.assert_called_once_with(structure)
    session.commit.assert_called_once()


class TestBuildEnvelope:
  def test_returns_none_when_structure_missing(self) -> None:
    session = MagicMock()
    with patch(f"{MODULE}.load_base_envelope_atoms", return_value=None):
      result = rollforward_handlers.build_envelope(session, "struct_missing")
    assert result is None

  def test_envelope_has_empty_facts(self) -> None:
    """build_envelope does not persist rollforward facts; it returns the
    typed mechanics + empty facts list. Filter evaluation is performed
    directly by callers (the reconciliation harness)."""
    structure = MagicMock()
    structure.id = "struct_rf_01"
    structure.name = "Cash Rollforward"
    structure.taxonomy_id = "tax_mini"
    structure.description = None
    structure.renderer_note = None
    structure.concept_arrangement = "roll_forward"
    structure.member_arrangement = None
    structure.artifact_mechanics = {
      "kind": "rollforward",
      "bs_source_element_id": "elem_cash",
      "bs_source_qname": "mini:CashAndCashEquivalents",
      "default_change_tag_element_id": None,
      "attribution_filters": [],
      "validation_mode": "residual_as_default",
    }

    atoms = MagicMock()
    atoms.structure = structure
    atoms.taxonomy_name = "mini"
    atoms.elements = []
    atoms.associations = []
    atoms.classifications_by_assoc = {}
    atoms.rules = []
    atoms.fact_set = None
    atoms.verification_results = []

    session = MagicMock()
    with patch(f"{MODULE}.load_base_envelope_atoms", return_value=atoms):
      envelope = rollforward_handlers.build_envelope(session, "struct_rf_01")

    assert envelope is not None
    assert envelope.block_type == "rollforward"
    assert envelope.display_name == "Rollforward"
    assert envelope.category == "Reporting"
    assert envelope.facts == []
    # Mechanics round-trips through the envelope
    assert isinstance(envelope.artifact.mechanics, RollforwardMechanics)
    assert envelope.artifact.mechanics.bs_source_qname == "mini:CashAndCashEquivalents"
