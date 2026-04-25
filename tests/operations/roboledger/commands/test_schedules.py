"""Tests for schedule command lifecycle behavior."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

from robosystems.models.api.extensions.schedules import (
  DeleteScheduleRequest,
  UpdateScheduleRequest,
)
from robosystems.models.extensions import (
  Association,
  AssociationClassification,
  FactSet,
  Rule,
  VerificationResult,
)
from robosystems.models.extensions.roboledger import Fact
from robosystems.operations.roboledger.commands.schedules import (
  delete_schedule,
  update_schedule,
)


def _exec_result(
  *, row=None, scalars_all: list[str] | None = None, fetchone_row=None
) -> MagicMock:
  result = MagicMock()
  result.scalar_one_or_none.return_value = row
  result.scalars.return_value.all.return_value = scalars_all or []
  result.fetchone.return_value = fetchone_row
  return result


@dataclass
class _Query:
  model: type
  deleted_models: list[type]

  def filter(self, *_args) -> _Query:
    return self

  def delete(self, *, synchronize_session: bool = False) -> int:
    assert synchronize_session is False
    self.deleted_models.append(self.model)
    return 1


def test_delete_schedule_removes_information_block_dependents_before_structure() -> (
  None
):
  from unittest.mock import patch

  structure = MagicMock()
  structure.id = "struct_sched"

  deleted_models: list[type] = []
  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(scalars_all=["assoc_1", "assoc_2"]),
    _exec_result(scalars_all=["rule_1"]),
  ]
  session.query.side_effect = lambda model: _Query(model, deleted_models)

  with patch(
    "robosystems.operations.roboledger.commands.schedules."
    "ScheduleService.void_pending_obligations",
    return_value=0,
  ):
    result = delete_schedule(
      session,
      DeleteScheduleRequest(structure_id="struct_sched"),
    )

  assert result == {"deleted": True}
  assert deleted_models == [
    VerificationResult,
    Fact,
    FactSet,
    Rule,
    AssociationClassification,
    Association,
  ]
  session.delete.assert_called_once_with(structure)
  session.commit.assert_called_once()


def test_delete_schedule_voids_pending_obligations_before_deletion() -> None:
  """Pending obligations must be voided so they don't outlive their originator."""
  from unittest.mock import patch

  structure = MagicMock()
  structure.id = "struct_sched"

  deleted_models: list[type] = []
  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(scalars_all=[]),
    _exec_result(scalars_all=[]),
  ]
  session.query.side_effect = lambda model: _Query(model, deleted_models)

  with patch(
    "robosystems.operations.roboledger.commands.schedules."
    "ScheduleService.void_pending_obligations",
    return_value=12,
  ) as void:
    delete_schedule(
      session,
      DeleteScheduleRequest(structure_id="struct_sched"),
    )

  void.assert_called_once()
  kwargs = void.call_args.kwargs
  assert kwargs["structure"] is structure
  assert kwargs["void_reason"] == "schedule_deleted"
  # Schedule deletion does NOT pass voided_by_event_id — there is no
  # successor event, the schedule simply ceases to exist.
  assert "voided_by_event_id" not in kwargs or kwargs.get("voided_by_event_id") is None


def test_update_schedule_keeps_omitted_metadata_null_in_typed_mechanics() -> None:
  structure = MagicMock()
  structure.id = "struct_sched"
  structure.name = "Old Name"
  structure.taxonomy_id = "tax_1"
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr",
      "credit_element_id": "elem_cr",
    }
  }

  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(fetchone_row=MagicMock(cnt=2)),
    _exec_result(fetchone_row=MagicMock(cnt=1)),
  ]

  response = update_schedule(
    session,
    UpdateScheduleRequest(structure_id="struct_sched", name="New Name"),
  )

  assert response.name == "New Name"
  assert structure.artifact_mechanics["schedule_metadata"] is None


def test_update_schedule_template_change_triggers_supersession() -> None:
  """Stream 2.E: changing entry_template voids + re-materializes pending events."""
  from unittest.mock import patch

  from robosystems.models.api.extensions.schedules import EntryTemplateRequest

  structure = MagicMock()
  structure.id = "struct_sched"
  structure.name = "Existing"
  structure.taxonomy_id = "tax_1"
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr_old",
      "credit_element_id": "elem_cr_old",
      "entry_type": "closing",
      "memo_template": "Old memo",
      "auto_reverse": False,
    },
    "schedule_created_event_id": "evt_origin",
  }

  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(fetchone_row=MagicMock(cnt=12)),
    _exec_result(fetchone_row=MagicMock(cnt=12)),
  ]

  with patch(
    "robosystems.operations.roboledger.commands.schedules."
    "ScheduleService.supersede_pending_obligations",
    return_value=12,
  ) as supersede:
    update_schedule(
      session,
      UpdateScheduleRequest(
        structure_id="struct_sched",
        entry_template=EntryTemplateRequest(
          debit_element_id="elem_dr_new",
          credit_element_id="elem_cr_new",
          entry_type="closing",
          memo_template="New memo",
          auto_reverse=False,
        ),
      ),
      updated_by="usr_admin",
    )

  supersede.assert_called_once()
  kwargs = supersede.call_args.kwargs
  assert kwargs["structure"] is structure
  assert kwargs["created_by"] == "usr_admin"


def test_update_schedule_no_template_change_skips_supersession() -> None:
  """Renaming the schedule must not touch the obligation chain."""
  from unittest.mock import patch

  structure = MagicMock()
  structure.id = "struct_sched"
  structure.name = "Old Name"
  structure.taxonomy_id = "tax_1"
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr",
      "credit_element_id": "elem_cr",
    },
    "schedule_created_event_id": "evt_origin",
  }

  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(fetchone_row=MagicMock(cnt=2)),
    _exec_result(fetchone_row=MagicMock(cnt=1)),
  ]

  with patch(
    "robosystems.operations.roboledger.commands.schedules."
    "ScheduleService.supersede_pending_obligations",
  ) as supersede:
    update_schedule(
      session,
      UpdateScheduleRequest(structure_id="struct_sched", name="New Name"),
    )

  supersede.assert_not_called()


def test_update_schedule_identical_template_skips_supersession() -> None:
  """Submitting the exact same template values is a no-op for the chain."""
  from unittest.mock import patch

  from robosystems.models.api.extensions.schedules import EntryTemplateRequest

  structure = MagicMock()
  structure.id = "struct_sched"
  structure.name = "Existing"
  structure.taxonomy_id = "tax_1"
  structure.metadata_ = {
    "entry_template": {
      "debit_element_id": "elem_dr",
      "credit_element_id": "elem_cr",
      "entry_type": "closing",
      "memo_template": "memo",
      "auto_reverse": False,
    },
    "schedule_created_event_id": "evt_origin",
  }

  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(fetchone_row=MagicMock(cnt=2)),
    _exec_result(fetchone_row=MagicMock(cnt=1)),
  ]

  with patch(
    "robosystems.operations.roboledger.commands.schedules."
    "ScheduleService.supersede_pending_obligations",
  ) as supersede:
    update_schedule(
      session,
      UpdateScheduleRequest(
        structure_id="struct_sched",
        entry_template=EntryTemplateRequest(
          debit_element_id="elem_dr",
          credit_element_id="elem_cr",
          entry_type="closing",
          memo_template="memo",
          auto_reverse=False,
        ),
      ),
    )

  supersede.assert_not_called()
