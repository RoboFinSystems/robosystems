"""Tests for Taxonomy Block delete handlers + cascade helpers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.taxonomy_block import DeleteTaxonomyBlockRequest
from robosystems.operations.taxonomy_block import (
  chart_of_accounts,
  custom_ontology,
  reporting_extension,
)
from robosystems.operations.taxonomy_block.cascade import DeletePreflight


def _fake_taxonomy(taxonomy_type: str, *, is_locked: bool = False) -> MagicMock:
  taxonomy = MagicMock()
  taxonomy.id = "tax_42"
  taxonomy.taxonomy_type = taxonomy_type
  taxonomy.is_locked = is_locked
  return taxonomy


def _req(cascade_facts: bool = False) -> DeleteTaxonomyBlockRequest:
  return DeleteTaxonomyBlockRequest(
    taxonomy_id="tax_42",
    cascade_facts=cascade_facts,
    reason="cleanup",
  )


class TestCustomOntologyDelete:
  def test_happy_path_no_dependencies(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("custom_ontology")
    with (
      patch(
        "robosystems.operations.taxonomy_block.custom_ontology.preflight_delete",
        return_value=DeletePreflight(),
      ),
      patch(
        "robosystems.operations.taxonomy_block.custom_ontology.cascade_delete_taxonomy",
        return_value=0,
      ) as mock_cascade,
    ):
      result = custom_ontology.delete(session, _req(), "usr_1")
    assert result == 0
    mock_cascade.assert_called_once()

  def test_wrong_type_rejected(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("chart_of_accounts")
    with pytest.raises(ValueError) as exc:
      custom_ontology.delete(session, _req(), "usr_1")
    assert "not a custom_ontology" in str(exc.value)

  def test_library_taxonomy_rejected(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("custom_ontology", is_locked=True)
    with pytest.raises(ValueError) as exc:
      custom_ontology.delete(session, _req(), "usr_1")
    assert "library" in str(exc.value)

  def test_facts_without_cascade_rejected(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("custom_ontology")
    with patch(
      "robosystems.operations.taxonomy_block.custom_ontology.preflight_delete",
      return_value=DeletePreflight(fact_count=5),
    ):
      with pytest.raises(ValueError) as exc:
        custom_ontology.delete(session, _req(cascade_facts=False), "usr_1")
    assert "5 live facts" in str(exc.value)

  def test_facts_with_cascade_allowed(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("custom_ontology")
    with (
      patch(
        "robosystems.operations.taxonomy_block.custom_ontology.preflight_delete",
        return_value=DeletePreflight(fact_count=5),
      ),
      patch(
        "robosystems.operations.taxonomy_block.custom_ontology.cascade_delete_taxonomy",
        return_value=5,
      ),
    ):
      result = custom_ontology.delete(session, _req(cascade_facts=True), "usr_1")
    assert result == 5

  def test_line_items_always_reject(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("custom_ontology")
    with patch(
      "robosystems.operations.taxonomy_block.custom_ontology.preflight_delete",
      return_value=DeletePreflight(line_item_count=3),
    ):
      with pytest.raises(ValueError) as exc:
        custom_ontology.delete(session, _req(cascade_facts=True), "usr_1")
    assert "3 journal" in str(exc.value)

  def test_cross_taxonomy_mappings_reject(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("custom_ontology")
    with patch(
      "robosystems.operations.taxonomy_block.custom_ontology.preflight_delete",
      return_value=DeletePreflight(cross_taxonomy_mapping_count=2),
    ):
      with pytest.raises(ValueError) as exc:
        custom_ontology.delete(session, _req(), "usr_1")
    assert "2 mapping" in str(exc.value)


class TestChartOfAccountsDelete:
  def test_happy_path(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("chart_of_accounts")
    with (
      patch(
        "robosystems.operations.taxonomy_block.chart_of_accounts.preflight_delete",
        return_value=DeletePreflight(),
      ),
      patch(
        "robosystems.operations.taxonomy_block.chart_of_accounts.cascade_delete_taxonomy",
        return_value=0,
      ),
    ):
      result = chart_of_accounts.delete(session, _req(), "usr_1")
    assert result == 0

  def test_wrong_type_rejected(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("custom_ontology")
    with pytest.raises(ValueError) as exc:
      chart_of_accounts.delete(session, _req(), "usr_1")
    assert "not a chart_of_accounts" in str(exc.value)


class TestReportingExtensionDelete:
  def test_happy_path(self) -> None:
    session = MagicMock()
    session.get.return_value = _fake_taxonomy("reporting_extension")
    with (
      patch(
        "robosystems.operations.taxonomy_block.reporting_extension.preflight_delete",
        return_value=DeletePreflight(),
      ),
      patch(
        "robosystems.operations.taxonomy_block.reporting_extension.cascade_delete_taxonomy",
        return_value=0,
      ),
    ):
      result = reporting_extension.delete(session, _req(), "usr_1")
    assert result == 0
