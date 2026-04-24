"""Smoke tests for ``reporting_standard`` handler.

Library reporting taxonomies are read-only on the public envelope
surface — ``create`` / ``update`` / ``delete`` raise
:class:`NotImplementedError` with an admin-path hint.
``build_envelope`` is the only live code path; it returns None when the
taxonomy doesn't exist or isn't of the right type.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.taxonomy_block import (
  CreateTaxonomyBlockRequest,
  DeleteTaxonomyBlockRequest,
  UpdateTaxonomyBlockRequest,
)
from robosystems.operations.taxonomy_block import reporting_standard


def _make_create_request() -> CreateTaxonomyBlockRequest:
  return CreateTaxonomyBlockRequest(
    name="us-gaap 2024",
    taxonomy_type="reporting_standard",
  )


def test_create_raises_admin_only() -> None:
  session = MagicMock()
  with pytest.raises(NotImplementedError) as exc:
    reporting_standard.create(session, _make_create_request(), "usr_admin")
  assert "library_creator" in str(exc.value)


def test_update_raises_admin_only() -> None:
  session = MagicMock()
  payload = UpdateTaxonomyBlockRequest(taxonomy_id="tx_1")
  with pytest.raises(NotImplementedError) as exc:
    reporting_standard.update(session, payload, "usr_admin")
  assert "library_creator" in str(exc.value)


def test_delete_raises_admin_only() -> None:
  session = MagicMock()
  payload = DeleteTaxonomyBlockRequest(taxonomy_id="tx_1", reason="cleanup")
  with pytest.raises(NotImplementedError) as exc:
    reporting_standard.delete(session, payload, "usr_admin")
  assert "library_creator" in str(exc.value)


def test_build_envelope_returns_none_when_missing() -> None:
  session = MagicMock()
  session.get.return_value = None
  assert reporting_standard.build_envelope(session, "tx_missing") is None


def test_build_envelope_returns_none_on_type_mismatch() -> None:
  session = MagicMock()
  wrong_taxonomy = MagicMock()
  wrong_taxonomy.taxonomy_type = "chart_of_accounts"
  session.get.return_value = wrong_taxonomy
  assert reporting_standard.build_envelope(session, "tx_wrong") is None


def test_module_constants() -> None:
  assert reporting_standard.REPORTING_STANDARD_BLOCK_TYPE == "reporting_standard"
  assert reporting_standard.CATEGORY == "Library"
