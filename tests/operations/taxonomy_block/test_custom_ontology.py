"""Smoke tests for ``custom_ontology`` handler."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  TaxonomyBlockElementRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block import custom_ontology


def test_create_rejects_wrong_taxonomy_type() -> None:
  payload = CreateTaxonomyBlockRequest(name="Test", taxonomy_type="chart_of_accounts")
  with pytest.raises(ValueError) as exc:
    custom_ontology.create(MagicMock(), payload, "usr_1")
  assert "chart_of_accounts" in str(exc.value)


def test_update_rejects_missing_taxonomy() -> None:
  """Update is live; unknown taxonomy_id raises ValueError."""
  session = MagicMock()
  session.get.return_value = None
  payload = UpdateTaxonomyBlockRequest(taxonomy_id="tx_1")
  with pytest.raises(ValueError) as exc:
    custom_ontology.update(session, payload, "usr_1")
  assert "not a custom_ontology" in str(exc.value)


def test_delete_rejects_missing_taxonomy() -> None:
  """Delete is live; unknown taxonomy_id raises ValueError."""
  session = MagicMock()
  session.get.return_value = None
  payload = DeleteTaxonomyBlockRequest(taxonomy_id="tx_1", reason="cleanup")
  with pytest.raises(ValueError) as exc:
    custom_ontology.delete(session, payload, "usr_1")
  assert "not a custom_ontology" in str(exc.value)


def test_build_envelope_returns_none_when_missing() -> None:
  session = MagicMock()
  session.get.return_value = None
  assert custom_ontology.build_envelope(session, "tx_missing") is None


def test_build_envelope_returns_none_on_type_mismatch() -> None:
  session = MagicMock()
  wrong_taxonomy = MagicMock()
  wrong_taxonomy.taxonomy_type = "reporting_standard"
  session.get.return_value = wrong_taxonomy
  assert custom_ontology.build_envelope(session, "tx_wrong") is None


def test_module_constants() -> None:
  assert custom_ontology.CUSTOM_ONTOLOGY_BLOCK_TYPE == "custom_ontology"
  assert custom_ontology.CATEGORY == "Custom"


def test_element_request_accepts_loose_fields() -> None:
  """Custom ontologies permit elements without balance_type / classification."""
  req = TaxonomyBlockElementRequest(
    qname="custom:KPI",
    name="KPI",
    trait=None,
    balance_type=None,
    period_type=None,
  )
  assert req.qname == "custom:KPI"
  assert req.trait is None
  assert req.balance_type is None
