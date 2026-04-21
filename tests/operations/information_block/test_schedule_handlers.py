"""Schedule handler tests — dispatch_create + dispatch_build_envelope.

The handlers bind the generic Information Block machinery to the
existing Schedule POC. These tests exercise the two public functions
with a mocked SQLAlchemy session.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  EntryTemplateRequest,
  ScheduleCreatedResponse,
)
from robosystems.operations.information_block import schedule as schedule_handlers

MODULE = "robosystems.operations.information_block.schedule"


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


class TestBuildEnvelope:
  def test_returns_none_when_structure_missing(self) -> None:
    session = MagicMock()
    session.get.return_value = None
    assert schedule_handlers.build_envelope(session, "struct_missing") is None

  def test_returns_none_when_structure_wrong_type(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.structure_type = "balance_sheet"
    session.get.return_value = structure
    assert schedule_handlers.build_envelope(session, "struct_other") is None

  def test_packs_mechanics_from_metadata_jsonb(self) -> None:
    """Phase a reads mechanics from structures.metadata_ JSONB."""
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_abc"
    structure.structure_type = "schedule"
    structure.name = "Equipment Depreciation"
    structure.description = "Depreciation schedule — equipment"
    structure.metadata_ = {
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
    session.get.return_value = structure
    # No associations / elements / facts for this shape test —
    # execute().scalars().all() returns empty lists.
    session.execute.return_value.scalars.return_value.all.return_value = []

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
    assert mechanics.entry_template["debit_element_id"] == "elem_dep"
    assert mechanics.schedule_metadata["method"] == "straight_line"
    # Phase a: reserved fields are empty
    assert envelope.rules == []
    assert envelope.dimensions == []
    assert envelope.fact_set is None
    assert envelope.verification_results == []

  def test_empty_metadata_defaults_to_empty_mechanics(self) -> None:
    session = MagicMock()
    structure = MagicMock()
    structure.id = "struct_empty"
    structure.structure_type = "schedule"
    structure.name = "Empty"
    structure.description = None
    structure.metadata_ = None
    session.get.return_value = structure
    session.execute.return_value.scalars.return_value.all.return_value = []

    envelope = schedule_handlers.build_envelope(session, "struct_empty")
    assert envelope is not None
    assert envelope.artifact.mechanics.entry_template == {}
    assert envelope.artifact.mechanics.schedule_metadata == {}
