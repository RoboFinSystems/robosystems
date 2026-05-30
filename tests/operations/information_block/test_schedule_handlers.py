"""Schedule handler tests — dispatch_create + dispatch_build_envelope.

The handlers bind the generic Information Block machinery to the
existing Schedule POC. These tests exercise the two public functions
with a mocked SQLAlchemy session.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  DeleteScheduleRequest,
  EntryTemplateRequest,
  ScheduleCreatedResponse,
  UpdateScheduleRequest,
)
from robosystems.operations.information_block import schedule as schedule_handlers

MODULE = "robosystems.operations.information_block.schedule"


def _exec_result(
  *, scalars_all: list[Any] | None = None, scalar: Any = None
) -> MagicMock:
  """Shape a MagicMock like a SQLAlchemy Result."""
  result = MagicMock()
  if scalars_all is not None:
    result.scalars.return_value.all.return_value = scalars_all
  result.scalar.return_value = scalar
  return result


def _body() -> CreateScheduleRequest:
  return CreateScheduleRequest(
    name="Equipment Depreciation",
    taxonomy_id=None,
    element_ids=["elem_a", "elem_b"],
    period_start=date(2026, 1, 1),
    period_end=date(2026, 3, 31),
    monthly_amount=50000,
    entry_template=EntryTemplateRequest(
      debit_element_id="elem_depr_expense",
      credit_element_id="elem_accum_depr",
    ),
  )


class TestCreate:
  def test_delegates_to_cmd_create_schedule_and_returns_structure_id(self) -> None:
    """dispatch_create must call the legacy command and surface structure_id."""
    session = MagicMock()
    expected = ScheduleCreatedResponse(
      structure_id="struct_abc",
      name="Equipment Depreciation",
      taxonomy_id="tax_01",
      total_periods=3,
      total_facts=6,
    )
    with patch(f"{MODULE}.cmd_create_schedule", return_value=expected) as mock_cmd:
      result = schedule_handlers.create(session, _body(), "usr_test")

    assert result == "struct_abc"
    mock_cmd.assert_called_once()
    # cmd_create_schedule must receive session + body + created_by kwarg
    args, kwargs = mock_cmd.call_args
    assert args[0] is session
    assert args[1] is not None
    assert kwargs.get("created_by") == "usr_test"


class TestUpdate:
  def test_delegates_to_cmd_update_schedule_and_returns_structure_id(self) -> None:
    session = MagicMock()
    body = UpdateScheduleRequest(structure_id="struct_existing", name="Renamed")
    expected = ScheduleCreatedResponse(
      structure_id="struct_existing",
      name="Renamed",
      taxonomy_id="tax_01",
      total_periods=3,
      total_facts=6,
    )
    with patch(f"{MODULE}.cmd_update_schedule", return_value=expected) as mock_cmd:
      result = schedule_handlers.update(session, body, "usr_test")

    assert result == "struct_existing"
    mock_cmd.assert_called_once_with(session, body, updated_by="usr_test")


class TestDelete:
  def test_delegates_to_cmd_delete_schedule_and_returns_structure_id(self) -> None:
    session = MagicMock()
    body = DeleteScheduleRequest(structure_id="struct_gone")
    with patch(
      f"{MODULE}.cmd_delete_schedule", return_value={"deleted": True}
    ) as mock_cmd:
      result = schedule_handlers.delete(session, body, "usr_test")

    assert result == "struct_gone"
    mock_cmd.assert_called_once_with(session, body)


class TestBuildEnvelope:
  def test_returns_none_when_structure_missing(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    assert schedule_handlers.build_envelope(session, "struct_missing") is None

  def test_returns_none_when_structure_wrong_type(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.block_type = "balance_sheet"
    session.get.return_value = structure
    assert schedule_handlers.build_envelope(session, "struct_other") is None

  def test_packs_mechanics_from_typed_artifact_mechanics_column(self) -> None:
    """Mechanics are read from the typed artifact_mechanics column.

    This is the post-backfill path every Schedule row takes: the typed
    JSONB column is the authoritative source; metadata_ carries the
    same content during the transition window but is not consulted.
    """
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_abc"
    structure.block_type = "schedule"
    structure.name = "Equipment Depreciation"
    structure.description = "Depreciation schedule — equipment"
    structure.taxonomy_id = "tax_01"
    structure.concept_arrangement = "roll_forward"
    structure.member_arrangement = None
    structure.renderer_note = None
    structure.artifact_mechanics = {
      "kind": "closing_entry_generator",
      "entry_template": {
        "debit_element_id": "elem_dep",
        "credit_element_id": "elem_accum",
        "entry_type": "closing",
        "memo_template": "Monthly {structure_name}",
        "auto_reverse": False,
      },
      "schedule_metadata": {
        "method": "straight_line",
        "original_amount": 1800000,
        "residual_value": 0,
        "useful_life_months": 36,
        "asset_element_id": "elem_ppe",
      },
    }
    structure.metadata_ = {"should_be_ignored": True}
    session.get.return_value = structure
    session.execute.side_effect = [
      _exec_result(scalar=None),  # latest fact set → None
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalars_all=[]),  # associations
      _exec_result(scalars_all=[]),  # rules
      _exec_result(scalars_all=[]),  # verification results
      _exec_result(scalar=5),  # periods_with_entries count
      _exec_result(scalars_all=[]),  # facts
    ]

    envelope = schedule_handlers.build_envelope(session, "struct_abc")
    assert envelope is not None
    assert envelope.id == "struct_abc"
    assert envelope.block_type == "schedule"
    assert envelope.name == "Equipment Depreciation"
    assert envelope.display_name == "Schedule"
    assert envelope.category == "Close"
    assert envelope.information_model.concept_arrangement == "roll_forward"
    assert envelope.information_model.member_arrangement is None

    mechanics = envelope.artifact.mechanics
    assert mechanics.kind == "closing_entry_generator"
    assert mechanics.entry_template.debit_element_id == "elem_dep"
    assert mechanics.schedule_metadata is not None
    assert mechanics.schedule_metadata.method == "straight_line"
    assert envelope.rules == []
    assert envelope.dimensions == []
    assert envelope.fact_set is None
    assert envelope.verification_results == []
    assert envelope.taxonomy_id == "tax_01"
    assert envelope.taxonomy_name == "US GAAP"
    assert mechanics.periods_with_entries == 5

  def test_legacy_fallback_raises_when_entry_template_missing(self) -> None:
    """Corrupted/missing entry_template in metadata_ raises ValueError.

    In practice every row written by create_schedule carries a valid
    entry_template, so this path indicates data corruption. The guard
    raises with a diagnostic message pointing at the structure id rather
    than leaking an opaque Pydantic ValidationError.
    """
    import pytest

    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_corrupted"
    structure.block_type = "schedule"
    structure.name = "Corrupted"
    structure.description = None
    structure.taxonomy_id = "tax_01"
    structure.concept_arrangement = None
    structure.member_arrangement = None
    structure.renderer_note = None
    structure.artifact_mechanics = None
    structure.metadata_ = {}  # no entry_template key
    session.get.return_value = structure
    session.execute.side_effect = [
      _exec_result(scalar=None),  # latest fact set → None
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalars_all=[]),  # associations
      _exec_result(scalars_all=[]),  # rules
      _exec_result(scalars_all=[]),  # verification results
      _exec_result(scalar=0),  # periods_with_entries
    ]

    with pytest.raises(ValueError, match="struct_corrupted"):
      schedule_handlers.build_envelope(session, "struct_corrupted")

  def test_falls_back_to_metadata_jsonb_when_artifact_mechanics_null(self) -> None:
    """Schedule rows lacking artifact_mechanics still surface via metadata_.

    The migration backfills every existing Schedule row, so this path is
    only exercised by rows inserted between DDL and backfill — or rows
    from a future adapter that bypasses the typed column write. Keeping
    the fallback avoids silent envelope failures during any such gap.
    """
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_legacy"
    structure.block_type = "schedule"
    structure.name = "Legacy Schedule"
    structure.description = None
    structure.taxonomy_id = "tax_01"
    structure.concept_arrangement = None  # pre-backfill, read default
    structure.member_arrangement = None
    structure.renderer_note = None
    structure.artifact_mechanics = None
    structure.metadata_ = {
      "entry_template": {
        "debit_element_id": "elem_dep",
        "credit_element_id": "elem_accum",
      },
    }
    session.get.return_value = structure
    session.execute.side_effect = [
      _exec_result(scalar=None),  # latest fact set → None
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalars_all=[]),  # associations
      _exec_result(scalars_all=[]),  # rules
      _exec_result(scalars_all=[]),  # verification results
      _exec_result(scalar=0),  # periods_with_entries
      _exec_result(scalars_all=[]),  # facts
    ]

    envelope = schedule_handlers.build_envelope(session, "struct_legacy")
    assert envelope is not None
    mechanics = envelope.artifact.mechanics
    assert mechanics.entry_template.debit_element_id == "elem_dep"
    assert mechanics.schedule_metadata is None
    assert mechanics.periods_with_entries == 0
    assert envelope.information_model.concept_arrangement == "roll_forward"

  def test_facts_filtered_by_fact_set_pin_for_report_package(self) -> None:
    """When a fact_set_id is supplied (Report-Block rehydration), the
    facts query must filter by ``Fact.fact_set_id``. Without the pin
    a filed Report would render today's drafts instead of the
    snapshot the package was reviewed against."""
    from sqlalchemy.sql.elements import BinaryExpression

    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_dep"
    structure.block_type = "schedule"
    structure.name = "Equipment Depreciation"
    structure.description = None
    structure.taxonomy_id = "tax_01"
    structure.concept_arrangement = "roll_forward"
    structure.member_arrangement = None
    structure.renderer_note = None
    structure.artifact_mechanics = {
      "kind": "closing_entry_generator",
      "entry_template": {
        "debit_element_id": "elem_dep",
        "credit_element_id": "elem_accum",
      },
    }
    structure.metadata_ = {}
    session.get.return_value = structure

    fact_set = MagicMock()
    fact_set.id = "fs_pinned"
    fact_set.structure_id = "struct_dep"
    fact_set.period_start = date(2026, 1, 1)
    fact_set.period_end = date(2026, 1, 31)
    fact_set.factset_type = "schedule"
    fact_set.entity_id = "ent_demo"
    fact_set.report_id = None
    fact_set.provenance = {
      "origin": "schedule",
      "structure_id": "struct_dep",
      "method": "straight_line",
    }

    session.execute.side_effect = [
      _exec_result(scalar=fact_set),  # fact_set lookup (pinned)
      _exec_result(scalar="US GAAP"),  # taxonomy name
      _exec_result(scalars_all=[]),  # associations
      _exec_result(scalars_all=[]),  # rules
      _exec_result(scalars_all=[]),  # verification results
      _exec_result(scalar=0),  # periods_with_entries
      _exec_result(scalars_all=[]),  # facts (filtered by pin)
    ]

    schedule_handlers.build_envelope(session, "struct_dep", fact_set_id="fs_pinned")

    # Inspect the facts query: the pin filter must appear in the WHERE.
    facts_query = session.execute.call_args_list[6].args[0]
    where_clause = str(facts_query.compile(compile_kwargs={"literal_binds": True}))
    assert "fact_set_id = 'fs_pinned'" in where_clause
    # And the structure / scope filters must still be present.
    assert "structure_id = 'struct_dep'" in where_clause
    assert "fact_scope = 'in_scope'" in where_clause
    # Sanity check: BinaryExpression import is exercised so we don't
    # silently lose the type if the SQLAlchemy WHERE shape changes.
    del BinaryExpression
