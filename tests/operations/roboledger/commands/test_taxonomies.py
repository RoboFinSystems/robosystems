"""Tests for taxonomy command lifecycle behavior."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.taxonomies import (
  CreateMappingAssociationOperation,
  DeleteAssociationRequest,
  DeleteMappingAssociationOperation,
)
from robosystems.models.extensions import (
  Association,
  AssociationClassification,
  Rule,
  VerificationResult,
)
from robosystems.operations.roboledger.commands.taxonomies import (
  AssociationNotFoundError,
  create_mapping_association,
  delete_association,
  delete_mapping_association,
)
from robosystems.operations.taxonomy_block.immutability import ProtectedFactsError

_GATE = (
  "robosystems.operations.roboledger.commands.taxonomies.assert_history_undisturbed"
)


def _closed_history() -> ProtectedFactsError:
  return ProtectedFactsError(
    filed_report_count=0,
    closed_period_count=0,
    disturbed_count=1,
    closed_period_names=("2026-06",),
  )


def _exec_result(*, row=None, scalars_all: list[str] | None = None) -> MagicMock:
  result = MagicMock()
  result.scalar_one_or_none.return_value = row
  result.scalars.return_value.all.return_value = scalars_all or []
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


def test_delete_association_removes_information_block_dependents_first() -> None:
  assoc = MagicMock()
  assoc.id = "assoc_1"
  assoc.created_by = "usr_test"

  deleted_models: list[type] = []
  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=assoc),
    _exec_result(scalars_all=["rule_1"]),
  ]
  session.query.side_effect = lambda model: _Query(model, deleted_models)

  result = delete_association(
    session,
    DeleteAssociationRequest(association_id="assoc_1"),
  )

  assert result == {"deleted": True}
  assert deleted_models == [
    VerificationResult,
    Rule,
    AssociationClassification,
    Association,
  ]


def test_delete_mapping_association_removes_edge_and_dependents() -> None:
  assoc = MagicMock()
  assoc.id = "assoc_1"
  assoc.created_by = "usr_test"
  assoc.from_element_id = "el_cash"

  deleted_models: list[type] = []
  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=assoc),
    _exec_result(scalars_all=["rule_1"]),
  ]
  session.query.side_effect = lambda model: _Query(model, deleted_models)

  with patch(_GATE) as gate:
    result = delete_mapping_association(
      session,
      DeleteMappingAssociationOperation(mapping_id="map_1", association_id="assoc_1"),
    )

  gate.assert_called_once_with(session, account_ids=["el_cash"])
  assert result.deleted is True
  assert deleted_models == [
    VerificationResult,
    Rule,
    AssociationClassification,
    Association,
  ]


def test_delete_mapping_association_refuses_an_account_with_closed_history() -> None:
  """The closed month's stamp was computed through this arc; nothing is
  deleted and the refusal reaches the caller."""
  assoc = MagicMock()
  assoc.id = "assoc_1"
  assoc.created_by = "usr_test"
  assoc.from_element_id = "el_cash"

  deleted_models: list[type] = []
  session = MagicMock()
  session.execute.side_effect = [_exec_result(row=assoc)]
  session.query.side_effect = lambda model: _Query(model, deleted_models)

  with (
    patch(_GATE, side_effect=_closed_history()) as gate,
    pytest.raises(ProtectedFactsError),
  ):
    delete_mapping_association(
      session,
      DeleteMappingAssociationOperation(mapping_id="map_1", association_id="assoc_1"),
    )

  gate.assert_called_once_with(session, account_ids=["el_cash"])
  assert deleted_models == []


def test_create_mapping_association_refuses_an_account_with_closed_history() -> None:
  """Map before you close: a new arc for an account with landed history in a
  closed month would restate it. Checked before the target lookup and the
  insert."""
  structure = MagicMock()
  structure.created_by = "usr_test"
  from_elem = MagicMock()
  from_elem.created_by = "usr_test"
  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=structure),
    _exec_result(row=from_elem),
  ]

  with (
    patch(_GATE, side_effect=_closed_history()) as gate,
    pytest.raises(ProtectedFactsError),
  ):
    create_mapping_association(
      session,
      CreateMappingAssociationOperation(
        mapping_id="map_1",
        from_element_id="el_cash",
        to_element_id="rs_cash",
        association_type="mapping",
      ),
      created_by="usr_test",
    )

  gate.assert_called_once_with(session, account_ids=["el_cash"])
  session.add.assert_not_called()


def test_delete_mapping_association_missing_edge_raises() -> None:
  session = MagicMock()
  session.execute.side_effect = [_exec_result(row=None)]

  with pytest.raises(AssociationNotFoundError):
    delete_mapping_association(
      session,
      DeleteMappingAssociationOperation(mapping_id="map_1", association_id="nope"),
    )
