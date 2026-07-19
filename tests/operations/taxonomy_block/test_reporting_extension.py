"""Smoke tests for ``reporting_extension`` handler.

Reporting extensions must carry ``parent_taxonomy_id`` at the request-
model layer (Pydantic validator) AND at the handler layer (defense in
depth). The handler also rejects when the parent row doesn't exist or
isn't of type ``reporting_standard``.
"""

from __future__ import annotations

from typing import get_args
from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  TaxonomyBlockStructureRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.models.extensions.structure import CONCEPT_ARRANGEMENT_VALUES
from robosystems.operations.taxonomy_block import reporting_extension


def test_structure_request_accepts_disclosure_note_shape() -> None:
  """A disclosure note is authorable through the envelope: the
  ``regulatory_disclosure`` block_type plus a CAP from the closed
  ``concept_arrangement`` vocabulary."""
  req = TaxonomyBlockStructureRequest(
    name="Inventory Components",
    block_type="regulatory_disclosure",
    concept_arrangement="roll_up",
    role_uri="https://example.com/roles/disclosures/InventoryComponents",
  )
  assert req.block_type == "regulatory_disclosure"
  assert req.concept_arrangement == "roll_up"


def test_structure_request_rejects_unknown_concept_arrangement() -> None:
  with pytest.raises(ValidationError):
    TaxonomyBlockStructureRequest(
      name="Bad CAP",
      block_type="regulatory_disclosure",
      concept_arrangement="not_a_cap",  # type: ignore[arg-type]
    )


def test_concept_arrangement_literal_matches_canonical_vocabulary() -> None:
  """Drift guard: the API request ``concept_arrangement`` Literal must stay in
  lockstep with ``CONCEPT_ARRANGEMENT_VALUES`` — the single source that also
  builds the ``structures.concept_arrangement`` DB CHECK constraint."""
  annotation = TaxonomyBlockStructureRequest.model_fields[
    "concept_arrangement"
  ].annotation
  literal_values: set[str] = set()
  for arg in get_args(annotation):
    literal_values.update(get_args(arg))
  assert literal_values == set(CONCEPT_ARRANGEMENT_VALUES)


def test_create_request_rejects_missing_parent() -> None:
  """Pydantic-level validator enforces parent_taxonomy_id for extensions."""
  with pytest.raises(ValidationError) as exc:
    CreateTaxonomyBlockRequest(
      name="Carbon ext",
      taxonomy_type="reporting_extension",
    )
  assert "parent_taxonomy_id" in str(exc.value)


def test_create_rejects_wrong_taxonomy_type() -> None:
  payload = CreateTaxonomyBlockRequest(
    name="Test",
    taxonomy_type="chart_of_accounts",
    parent_taxonomy_id="tx_lib",
  )
  with pytest.raises(ValueError) as exc:
    reporting_extension.create(MagicMock(), payload, "usr_1")
  assert "chart_of_accounts" in str(exc.value)


def test_create_rejects_missing_parent_taxonomy_row() -> None:
  """Handler-layer guard: parent_taxonomy_id must resolve to a row."""
  session = MagicMock()
  session.get.return_value = None

  payload = CreateTaxonomyBlockRequest(
    name="Carbon ext",
    taxonomy_type="reporting_extension",
    parent_taxonomy_id="tx_does_not_exist",
  )
  with pytest.raises(ValueError) as exc:
    reporting_extension.create(session, payload, "usr_1")
  assert "does not exist" in str(exc.value)


def test_create_rejects_non_reporting_standard_parent() -> None:
  """Handler rejects when parent_taxonomy_id points at a non-library row."""
  session = MagicMock()
  wrong_parent = MagicMock()
  wrong_parent.taxonomy_type = "chart_of_accounts"
  session.get.return_value = wrong_parent

  payload = CreateTaxonomyBlockRequest(
    name="Carbon ext",
    taxonomy_type="reporting_extension",
    parent_taxonomy_id="tx_coa",
  )
  with pytest.raises(ValueError) as exc:
    reporting_extension.create(session, payload, "usr_1")
  assert "reporting_standard" in str(exc.value)


def test_update_rejects_missing_taxonomy() -> None:
  """Update is live; unknown taxonomy_id raises ValueError."""
  session = MagicMock()
  session.get.return_value = None
  payload = UpdateTaxonomyBlockRequest(taxonomy_id="tx_1")
  with pytest.raises(ValueError) as exc:
    reporting_extension.update(session, payload, "usr_1")
  assert "not a reporting_extension" in str(exc.value)


def test_delete_rejects_missing_taxonomy() -> None:
  """Delete is live; unknown taxonomy_id raises ValueError."""
  session = MagicMock()
  session.get.return_value = None
  payload = DeleteTaxonomyBlockRequest(taxonomy_id="tx_1", reason="cleanup")
  with pytest.raises(ValueError) as exc:
    reporting_extension.delete(session, payload, "usr_1")
  assert "not a reporting_extension" in str(exc.value)


def test_build_envelope_returns_none_when_missing() -> None:
  session = MagicMock()
  session.get.return_value = None
  assert reporting_extension.build_envelope(session, "tx_missing") is None


def test_build_envelope_returns_none_on_type_mismatch() -> None:
  session = MagicMock()
  wrong_taxonomy = MagicMock()
  wrong_taxonomy.taxonomy_type = "custom_ontology"
  session.get.return_value = wrong_taxonomy
  assert reporting_extension.build_envelope(session, "tx_wrong") is None


def test_module_constants() -> None:
  assert reporting_extension.REPORTING_EXTENSION_BLOCK_TYPE == "reporting_extension"
  assert reporting_extension.CATEGORY == "Reporting"
