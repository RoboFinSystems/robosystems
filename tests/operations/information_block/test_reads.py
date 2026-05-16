"""Tests for ``get_information_block`` + ``list_information_blocks`` reads."""

from __future__ import annotations

import dataclasses
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.schedules import EntryTemplateRequest
from robosystems.models.api.information_block import (
  ArtifactResponse,
  InformationBlockEnvelope,
  InformationModelResponse,
  ScheduleMechanics,
  StatementMechanics,
)
from robosystems.operations.information_block.reads import (
  get_information_block,
  get_information_block_for_fact_set,
  list_information_blocks,
)
from robosystems.operations.information_block.registry import SCHEDULE_BLOCK

REGISTRY_PATH = "robosystems.operations.information_block.registry.REGISTRY"

_TEST_ENTRY_TEMPLATE = EntryTemplateRequest(
  debit_element_id="elem_dr", credit_element_id="elem_cr"
)


def _envelope(structure_id: str = "struct_1") -> InformationBlockEnvelope:
  return InformationBlockEnvelope(
    id=structure_id,
    block_type="schedule",
    name=f"Schedule-{structure_id}",
    display_name="Schedule",
    category="Close",
    information_model=InformationModelResponse(concept_arrangement="roll_forward"),
    artifact=ArtifactResponse(
      mechanics=ScheduleMechanics(entry_template=_TEST_ENTRY_TEMPLATE)
    ),
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
    """Structures with a block_type not yet modeled in the registry
    (e.g., legacy ``custom`` or ``chart_of_accounts`` rows) return None
    rather than raising — can't build the envelope."""
    session = MagicMock()
    structure = MagicMock()
    structure.block_type = "chart_of_accounts"
    session.get.return_value = structure
    assert get_information_block(session, "struct_coa") is None

  def test_dispatches_to_schedule_handler(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.block_type = "schedule"
    session.get.return_value = structure
    expected = _envelope("struct_1")
    mock_build = MagicMock(return_value=expected)
    patched = _schedule_entry_with_build(mock_build)
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      result = get_information_block(session, "struct_1")
    assert result is expected
    mock_build.assert_called_once_with(session, "struct_1", None)


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

  def test_library_sentinel_surfaces_only_opt_in_block_types(self) -> None:
    """Phase b: statement block types have surfaces_in_library=True;
    schedule does not. On the sentinel we run one query scoped to the
    opt-in types and skip the rest."""
    session = MagicMock()
    # No library structures seeded in this unit test → empty result.
    session.execute.return_value.scalars.return_value.all.return_value = []
    result = list_information_blocks(session, library_sentinel=True)
    assert result == []
    # One query attempted — against the 4 statement block types only.
    session.execute.assert_called_once()

  def test_tenant_query_includes_registered_block_types(self) -> None:
    """On a tenant graph, the query runs against Structure rows for all
    registered block_type ids."""
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_1"
    structure.block_type = "schedule"
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
    """Category 'Nonexistent' matches no registered block type — empty
    result without a DB query. Phase b categories in use are 'Close'
    (schedule) and 'Reporting' (4 statement types)."""
    session = MagicMock()
    result = list_information_blocks(
      session, category="Nonexistent", library_sentinel=False
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
    """If the DB returns a Structure row whose block_type is not
    registered (legacy row, or mid-migration), it's skipped cleanly."""
    session = MagicMock()
    schedule_row = MagicMock()
    schedule_row.id = "struct_s"
    schedule_row.block_type = "schedule"
    coa_row = MagicMock()
    coa_row.id = "struct_coa"
    coa_row.block_type = "chart_of_accounts"  # not in registry yet
    session.execute.return_value.scalars.return_value.all.return_value = [
      schedule_row,
      coa_row,
    ]
    expected = _envelope("struct_s")
    mock_build = MagicMock(return_value=expected)
    patched = _schedule_entry_with_build(mock_build)
    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      result = list_information_blocks(session, library_sentinel=False)
    assert len(result) == 1
    assert result[0].id == "struct_s"

  def test_library_sentinel_balance_sheet_filter_queries_statement_types(
    self,
  ) -> None:
    """When ``block_type='balance_sheet'`` is supplied on the sentinel,
    ``surfaces_in_library=True`` → query runs, scoped to the one type."""
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    result = list_information_blocks(
      session, block_type="balance_sheet", library_sentinel=True
    )
    assert result == []
    session.execute.assert_called_once()

  def test_list_surfaces_statement_blocks_on_tenant_graph(self) -> None:
    """A tenant Structure with ``block_type='balance_sheet'`` flows
    through the statement handler and surfaces its envelope."""
    from robosystems.operations.information_block.registry import (
      BALANCE_SHEET_BLOCK,
    )

    session = MagicMock()
    bs_row = MagicMock()
    bs_row.id = "struct_balance_sheet"
    bs_row.block_type = "balance_sheet"
    session.execute.return_value.scalars.return_value.all.return_value = [bs_row]

    expected = InformationBlockEnvelope(
      id="struct_balance_sheet",
      block_type="balance_sheet",
      name="Balance Sheet",
      display_name="Balance Sheet",
      category="Reporting",
      information_model=InformationModelResponse(
        concept_arrangement="roll_up", member_arrangement="whole_part"
      ),
      artifact=ArtifactResponse(mechanics=StatementMechanics()),
    )
    mock_build = MagicMock(return_value=expected)
    patched = dataclasses.replace(
      BALANCE_SHEET_BLOCK, dispatch_build_envelope=mock_build
    )
    with patch.dict(REGISTRY_PATH, {"balance_sheet": patched}):
      result = list_information_blocks(
        session, block_type="balance_sheet", library_sentinel=False
      )
    assert len(result) == 1
    assert result[0].block_type == "balance_sheet"


# ── get_information_block_for_fact_set ─────────────────────────────────────


class TestGetInformationBlockForFactSet:
  """The Report-Block read path: pin envelope rehydration to a FactSet id."""

  def _patched_session(
    self,
    *,
    fact_set: object | None,
    structure: object | None = None,
  ) -> MagicMock:
    """Session whose ``get`` returns the FactSet first, Structure second."""
    session = MagicMock()
    session.get.side_effect = [fact_set, structure]
    return session

  def test_returns_none_when_fact_set_missing(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    assert get_information_block_for_fact_set(session, "fs_missing") is None

  def test_returns_none_when_fact_set_has_no_structure(self) -> None:
    """Legacy FactSets with a null ``structure_id`` can't drive a handler."""
    fs = MagicMock()
    fs.structure_id = None
    session = MagicMock()
    session.get.return_value = fs
    assert get_information_block_for_fact_set(session, "fs_legacy") is None

  def test_returns_none_when_block_type_unregistered(self) -> None:
    fs = MagicMock()
    fs.id = "fs_01"
    fs.structure_id = "struct_coa"
    structure = MagicMock()
    structure.block_type = "chart_of_accounts"
    session = self._patched_session(fact_set=fs, structure=structure)

    assert get_information_block_for_fact_set(session, "fs_01") is None

  def test_dispatches_with_fact_set_pin(self) -> None:
    """The pin (fact_set_id) is forwarded to the handler unchanged."""
    fs = MagicMock()
    fs.id = "fs_01"
    fs.structure_id = "struct_1"
    structure = MagicMock()
    structure.block_type = "schedule"
    session = self._patched_session(fact_set=fs, structure=structure)

    expected = _envelope("struct_1")
    mock_build = MagicMock(return_value=expected)
    patched = _schedule_entry_with_build(mock_build)

    with patch.dict(REGISTRY_PATH, {"schedule": patched}):
      result = get_information_block_for_fact_set(session, "fs_01")

    assert result is expected
    mock_build.assert_called_once_with(session, "struct_1", "fs_01")
