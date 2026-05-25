"""Tests for taxonomy command lifecycle behavior."""

from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import MagicMock

import pytest

from robosystems.models.api.extensions.taxonomies import (
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
  delete_association,
  delete_mapping_association,
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

  deleted_models: list[type] = []
  session = MagicMock()
  session.execute.side_effect = [
    _exec_result(row=assoc),
    _exec_result(scalars_all=["rule_1"]),
  ]
  session.query.side_effect = lambda model: _Query(model, deleted_models)

  result = delete_mapping_association(
    session,
    DeleteMappingAssociationOperation(mapping_id="map_1", association_id="assoc_1"),
  )

  assert result.deleted is True
  assert deleted_models == [
    VerificationResult,
    Rule,
    AssociationClassification,
    Association,
  ]


def test_delete_mapping_association_missing_edge_raises() -> None:
  session = MagicMock()
  session.execute.side_effect = [_exec_result(row=None)]

  with pytest.raises(AssociationNotFoundError):
    delete_mapping_association(
      session,
      DeleteMappingAssociationOperation(mapping_id="map_1", association_id="nope"),
    )
