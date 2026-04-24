"""Tests for ``get_taxonomy_block`` + ``list_taxonomy_blocks``.

MagicMock-driven — matches the project's test style for the Taxonomy
Block ops layer. Verifies registry dispatch, sentinel filtering, and
that legacy / unregistered ``taxonomy_type`` values surface as ``None``
without raising.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from robosystems.operations.taxonomy_block import reads


def _make_row(taxonomy_type: str, taxonomy_id: str = "tx_1") -> MagicMock:
  row = MagicMock()
  row.id = taxonomy_id
  row.taxonomy_type = taxonomy_type
  return row


def test_get_returns_none_when_taxonomy_missing() -> None:
  session = MagicMock()
  session.get.return_value = None
  assert reads.get_taxonomy_block(session, "tx_missing") is None


def test_get_returns_none_for_unregistered_type() -> None:
  """Legacy 'mapping' taxonomies don't have an envelope representation."""
  session = MagicMock()
  session.get.return_value = _make_row("mapping")
  assert reads.get_taxonomy_block(session, "tx_mapping") is None


def test_get_dispatches_to_registered_handler() -> None:
  session = MagicMock()
  session.get.return_value = _make_row("chart_of_accounts")

  fake_envelope = MagicMock()
  fake_entry = MagicMock()
  fake_entry.dispatch_build_envelope.return_value = fake_envelope

  with patch.object(reads.registry_module, "get", return_value=fake_entry):
    result = reads.get_taxonomy_block(session, "tx_1")

  assert result is fake_envelope
  fake_entry.dispatch_build_envelope.assert_called_once_with(session, "tx_1")


def test_list_rejects_unknown_taxonomy_type() -> None:
  session = MagicMock()
  with pytest.raises(ValueError) as exc:
    reads.list_taxonomy_blocks(session, taxonomy_type="not_a_real_type")
  assert "not_a_real_type" in str(exc.value)


def test_list_empty_when_type_hidden_from_library_sentinel() -> None:
  """A tenant-only type requested against the sentinel returns []."""
  session = MagicMock()
  rows = reads.list_taxonomy_blocks(
    session, taxonomy_type="chart_of_accounts", library_sentinel=True
  )
  assert rows == []


def test_list_library_sentinel_permits_surfaces_in_library_types() -> None:
  """reporting_standard has surfaces_in_library=True — should pass through."""
  session = MagicMock()
  session.execute.return_value.scalars.return_value.all.return_value = []
  rows = reads.list_taxonomy_blocks(
    session, taxonomy_type="reporting_standard", library_sentinel=True
  )
  assert rows == []
  assert session.execute.called


def test_list_empty_when_category_filter_mismatches() -> None:
  """CoA entry has category 'Chart' — filtering by 'Library' returns []."""
  session = MagicMock()
  rows = reads.list_taxonomy_blocks(
    session, taxonomy_type="chart_of_accounts", category="Library"
  )
  assert rows == []


def test_list_skips_rows_whose_type_isnt_registered() -> None:
  """A row with taxonomy_type outside the registry is silently skipped."""
  session = MagicMock()
  unknown_row = _make_row("mapping", "tx_unk")
  session.execute.return_value.scalars.return_value.all.return_value = [unknown_row]

  result = reads.list_taxonomy_blocks(session)
  assert result == []


def test_list_projects_each_row_through_dispatcher() -> None:
  session = MagicMock()
  coa_row = _make_row("chart_of_accounts", "tx_coa")
  session.execute.return_value.scalars.return_value.all.return_value = [coa_row]

  fake_envelope = MagicMock()
  fake_entry = MagicMock()
  fake_entry.dispatch_build_envelope.return_value = fake_envelope

  fake_registry_entries = [
    MagicMock(
      id="chart_of_accounts",
      category="Chart",
      surfaces_in_library=False,
    )
  ]

  with (
    patch.object(
      reads.registry_module, "list_registered", return_value=fake_registry_entries
    ),
    patch.object(reads.registry_module, "get", return_value=fake_entry),
  ):
    result = reads.list_taxonomy_blocks(session)

  assert result == [fake_envelope]
  fake_entry.dispatch_build_envelope.assert_called_once_with(session, "tx_coa")
