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
  session.flush.assert_called_once()


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
