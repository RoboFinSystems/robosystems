"""Tests for ``get_information_block`` + ``list_information_blocks`` reads."""

from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  ScheduleMechanics,
)
from robosystems.operations.information_block.reads import (
  get_information_block,
  list_information_blocks,
)
from robosystems.operations.information_block.registry import SCHEDULE_BLOCK

REGISTRY_PATH = "robosystems.operations.information_block.registry.REGISTRY"


def _envelope(structure_id: str = "struct_1") -> InformationBlockEnvelope:
  return InformationBlockEnvelope(
    id=structure_id,
    block_type="schedule",
    name=f"Schedule-{structure_id}",
    display_name="Schedule",
    category="Close",
    information_model=InformationModelResponse(concept_arrangement="roll_forward"),
    artifact=ArtifactResponse(mechanics=ScheduleMechanics()),
  )


def _schedule_entry_with_build(mock_build: MagicMock):
  return dataclasses.replace(SCHEDULE_BLOCK, dispatch_build_envelope=mock_build)


# ── get_information_block ──────────────────────────────────────────────────


class TestGetInformationBlock:
  def test_returns_none_when_structure_missing(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    assert get_information_block(session, "struct_missing") is None

  def test_unknown_block_type_returns_none(self) -> None:
    """Structures with a structure_type not yet modeled (e.g., 'balance_sheet'
    pre-Phase-β) return None rather than raising — can't build the envelope."""
    session = MagicMock()
    structure = MagicMock()
    structure.structure_type = "balance_sheet"
    session.get.return_value = structure
    assert get_information_block(session, "struct_bs") is None

  def test_dispatches_to_schedule_handler(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.structure_type = "schedule"
    session.get.return_value = structure
    expected = _envelope("struct_1")
    mock_build = MagicMock(return_value=expected)
    patched = _schedule_entry_with_build(mock_build)
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      result = get_information_block(session, "struct_1")
    assert result is expected
    mock_build.assert_called_once_with(session, "struct_1")


# ── list_information_blocks ────────────────────────────────────────────────


class TestListInformationBlocks:
  def test_unknown_block_type_filter_raises_value_error(self) -> None:
    session = MagicMock()
    with pytest.raises(ValueError, match="Unknown block_type"):
      list_information_blocks(session, block_type="totally_made_up")

  def test_library_sentinel_returns_empty_for_non_surfacing_block_type(
    self,
  ) -> None:
    """Schedule has surfaces_in_library=False, so the library sentinel
    returns [] — schedule data lives in tenant schemas, not in public."""
    session = MagicMock()
    result = list_information_blocks(
      session, block_type="schedule", library_sentinel=True
    )
    assert result == []
    # No query attempted — we short-circuited on the library gate.
    session.execute.assert_not_called()

  def test_library_sentinel_without_block_type_returns_empty_when_none_opt_in(
    self,
  ) -> None:
    """Phase a: no block type has surfaces_in_library=True, so the sentinel
    returns [] even without a block_type filter."""
    session = MagicMock()
    result = list_information_blocks(session, library_sentinel=True)
    assert result == []
    session.execute.assert_not_called()

  def test_tenant_query_includes_registered_block_types(self) -> None:
    """On a tenant graph, the query runs against Structure rows for all
    registered block_type ids."""
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_1"
    structure.structure_type = "schedule"
    session.execute.return_value.scalars.return_value.all.return_value = [structure]
    expected = _envelope("struct_1")
    mock_build = MagicMock(return_value=expected)
    patched = _schedule_entry_with_build(mock_build)
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      result = list_information_blocks(session, library_sentinel=False)

    session.execute.assert_called_once()
    assert len(result) == 1
    assert result[0] is expected

  def test_category_filter_narrows_to_matching_block_types(self) -> None:
    session = MagicMock()
    # Category 'Close' matches SCHEDULE_BLOCK; 'Reporting' matches nothing
    result = list_information_blocks(
      session, category="Reporting", library_sentinel=False
    )
    assert result == []
    session.execute.assert_not_called()

  def test_block_type_filter_with_non_matching_category_returns_empty(self) -> None:
    """`block_type='schedule'` (Close category) + `category='Reporting'`
    is a contradictory filter — returns [] without touching the DB."""
    session = MagicMock()
    result = list_information_blocks(
      session, block_type="schedule", category="Reporting", library_sentinel=False
    )
    assert result == []
    session.execute.assert_not_called()

  def test_skips_structures_whose_block_type_is_unregistered(self) -> None:
    """If the DB returns a Structure row whose structure_type is not
    registered (legacy row, or mid-migration), it's skipped cleanly."""
    session = MagicMock()
    schedule_row = MagicMock()
    schedule_row.id = "struct_s"
    schedule_row.structure_type = "schedule"
    bs_row = MagicMock()
    bs_row.id = "struct_bs"
    bs_row.structure_type = "balance_sheet"  # not in registry yet
    session.execute.return_value.scalars.return_value.all.return_value = [
      schedule_row,
      bs_row,
    ]
    expected = _envelope("struct_s")
    mock_build = MagicMock(return_value=expected)
    patched = _schedule_entry_with_build(mock_build)
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      result = list_information_blocks(session, library_sentinel=False)
    assert len(result) == 1
    assert result[0].id == "struct_s"
