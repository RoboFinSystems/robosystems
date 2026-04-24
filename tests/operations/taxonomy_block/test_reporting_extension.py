"""Smoke tests for ``reporting_extension`` handler.

Reporting extensions must carry ``parent_taxonomy_id`` at the request-
model layer (Pydantic validator) AND at the handler layer (defense in
depth). The handler also rejects when the parent row doesn't exist or
isn't of type ``reporting_standard``.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from pydantic import ValidationError

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block import reporting_extension


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
  """Phase 2.4: update is live; unknown taxonomy_id raises ValueError."""
  session = MagicMock()
  session.get.return_value = None
  payload = UpdateTaxonomyBlockRequest(taxonomy_id="tx_1")
  with pytest.raises(ValueError) as exc:
    reporting_extension.update(session, payload, "usr_1")
  assert "not a reporting_extension" in str(exc.value)


def test_delete_rejects_missing_taxonomy() -> None:
  """Phase 2.4: delete is live; unknown taxonomy_id raises ValueError."""
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
