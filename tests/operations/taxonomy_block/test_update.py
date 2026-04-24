"""Tests for Taxonomy Block update handlers (Phase 2.4)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.taxonomy_block import (
  ElementUpdatePatch,
  StructureUpdatePatch,
  TaxonomyBlockElementRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block import custom_ontology
from robosystems.operations.taxonomy_block.update_validator import (
  reject_unsupported_deltas,
)


def _fake_taxonomy(taxonomy_type: str, *, is_locked: bool = False) -> MagicMock:
  taxonomy = MagicMock()
  taxonomy.id = "tax_42"
  taxonomy.taxonomy_type = taxonomy_type
  taxonomy.is_locked = is_locked
  taxonomy.standard = None
  taxonomy.name = "Original"
  taxonomy.description = "orig"
  taxonomy.version = "v1"
  taxonomy.parent_taxonomy_id = None
  taxonomy.namespace_uri = None
  return taxonomy


class TestRejectUnsupportedDeltas:
  def test_elements_to_update_rejected(self) -> None:
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_1",
      elements_to_update=[ElementUpdatePatch(qname="x:A", name="A")],
    )
    with pytest.raises(ValueError) as exc:
      reject_unsupported_deltas(payload)
    assert "elements_to_update" in str(exc.value)
    assert "Phase 2.4.1" in str(exc.value)

  def test_elements_to_remove_rejected(self) -> None:
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_1", elements_to_remove=["x:A"]
    )
    with pytest.raises(ValueError) as exc:
      reject_unsupported_deltas(payload)
    assert "elements_to_remove" in str(exc.value)

  def test_structures_to_update_rejected(self) -> None:
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_1",
      structures_to_update=[StructureUpdatePatch(structure_id="s1", name="new")],
    )
    with pytest.raises(ValueError) as exc:
      reject_unsupported_deltas(payload)
    assert "structures_to_update" in str(exc.value)

  def test_associations_to_remove_rejected(self) -> None:
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_1", associations_to_remove=["assoc_1"]
    )
    with pytest.raises(ValueError) as exc:
      reject_unsupported_deltas(payload)
    assert "associations_to_remove" in str(exc.value)

  def test_empty_payload_accepted(self) -> None:
    payload = UpdateTaxonomyBlockRequest(taxonomy_id="tax_1")
    reject_unsupported_deltas(payload)  # no raise

  def test_additive_deltas_accepted(self) -> None:
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_1",
      elements_to_add=[TaxonomyBlockElementRequest(qname="x:A", name="A")],
    )
    reject_unsupported_deltas(payload)  # no raise


class TestCustomOntologyUpdate:
  def test_wrong_type_rejected(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("chart_of_accounts")
    payload = UpdateTaxonomyBlockRequest(taxonomy_id="tax_42")
    with pytest.raises(ValueError) as exc:
      custom_ontology.update(session, payload, "usr_1")
    assert "custom_ontology" in str(exc.value)

  def test_missing_taxonomy_rejected(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    payload = UpdateTaxonomyBlockRequest(taxonomy_id="tax_missing")
    with pytest.raises(ValueError) as exc:
      custom_ontology.update(session, payload, "usr_1")
    assert "not a custom_ontology" in str(exc.value)

  def test_unsupported_delta_rejected_before_validation(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("custom_ontology")
    payload = UpdateTaxonomyBlockRequest(
      taxonomy_id="tax_42",
      elements_to_remove=["x:A"],
    )
    with pytest.raises(ValueError) as exc:
      custom_ontology.update(session, payload, "usr_1")
    assert "Phase 2.4.1" in str(exc.value)

  def test_top_level_field_update_only(self) -> None:
    """Empty-delta update still allows name/description/version rename."""
    session = MagicMock()
    tax = _fake_taxonomy("custom_ontology")
    session.get.return_value = tax
    with (
      patch(
        "robosystems.operations.taxonomy_block.custom_ontology.validate_update_envelope",
        return_value=[],
      ),
    ):
      payload = UpdateTaxonomyBlockRequest(
        taxonomy_id="tax_42",
        name="Renamed",
        description="new desc",
        version="v2",
      )
      result = custom_ontology.update(session, payload, "usr_1")
    assert result == "tax_42"
    assert tax.name == "Renamed"
    assert tax.description == "new desc"
    assert tax.version == "v2"
