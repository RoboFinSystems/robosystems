"""Tests for Phase epsilon association-classification packing.

Exercises :func:`load_classifications_for_associations` and the
``classifications`` pass-through in :func:`association_to_connection`
from :mod:`robosystems.operations.information_block.envelope` with a
mocked SQLAlchemy session — no database required.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from robosystems.models.api.information_block import (
  ClassificationLite,
)
from robosystems.operations.information_block.envelope import (
  association_to_connection,
  load_classifications_for_associations,
)


def _make_association(assoc_id: str = "assoc_1") -> MagicMock:
  assoc = MagicMock()
  assoc.id = assoc_id
  assoc.from_element_id = "elem_from"
  assoc.to_element_id = "elem_to"
  assoc.association_type = "presentation"
  assoc.arcrole = None
  assoc.order_value = None
  assoc.weight = None
  return assoc


def _make_classification(
  *,
  cls_id: str = "cls_rollup",
  category: str = "concept_arrangement",
  identifier: str = "RollUp",
) -> MagicMock:
  cls = MagicMock()
  cls.id = cls_id
  cls.category = category
  cls.identifier = identifier
  return cls


def _make_ac(
  *,
  association_id: str,
  is_primary: bool = True,
  confidence: float | None = None,
  source: str | None = "arcrole_analysis",
) -> MagicMock:
  ac = MagicMock()
  ac.association_id = association_id
  ac.is_primary = is_primary
  ac.confidence = confidence
  ac.source = source
  return ac


def _exec_all(*, rows: list[Any]) -> MagicMock:
  result = MagicMock()
  result.all.return_value = rows
  return result


class TestAssociationToConnection:
  def test_empty_classifications_default(self) -> None:
    connection = association_to_connection(_make_association())
    assert connection.classifications == []

  def test_classifications_passed_through(self) -> None:
    cls = ClassificationLite(
      id="cls_rollup",
      category="concept_arrangement",
      identifier="RollUp",
    )
    connection = association_to_connection(_make_association(), [cls])
    assert len(connection.classifications) == 1
    assert connection.classifications[0].identifier == "RollUp"


class TestLoadClassificationsForAssociations:
  def test_returns_empty_dict_when_no_association_ids(self) -> None:
    """Skip the query entirely when callers hand in an empty list —
    the envelope builder should never issue a round-trip for a
    Structure with no associations."""
    session = MagicMock()
    result = load_classifications_for_associations(session, [])
    assert result == {}
    session.execute.assert_not_called()

  def test_returns_empty_dict_when_no_rows(self) -> None:
    session = MagicMock()
    session.execute.return_value = _exec_all(rows=[])
    result = load_classifications_for_associations(session, ["assoc_1"])
    assert result == {}
    session.execute.assert_called_once()

  def test_groups_classifications_by_association(self) -> None:
    session = MagicMock()
    cls_rollup = _make_classification(identifier="RollUp")
    cls_aggr = _make_classification(
      cls_id="cls_aggr", category="member_arrangement", identifier="whole_part"
    )
    cls_hyper = _make_classification(
      cls_id="cls_hyper", category="concept_arrangement", identifier="Hypercube"
    )
    session.execute.return_value = _exec_all(
      rows=[
        (_make_ac(association_id="assoc_1"), cls_rollup),
        (_make_ac(association_id="assoc_1", is_primary=False), cls_aggr),
        (_make_ac(association_id="assoc_2"), cls_hyper),
      ]
    )
    result = load_classifications_for_associations(session, ["assoc_1", "assoc_2"])
    assert set(result.keys()) == {"assoc_1", "assoc_2"}
    assert len(result["assoc_1"]) == 2
    assert len(result["assoc_2"]) == 1
    assert result["assoc_1"][0].identifier == "RollUp"
    assert result["assoc_1"][0].is_primary is True
    assert result["assoc_1"][1].identifier == "whole_part"
    assert result["assoc_1"][1].is_primary is False
    assert result["assoc_2"][0].identifier == "Hypercube"

  def test_carries_confidence_and_source_through(self) -> None:
    session = MagicMock()
    cls = _make_classification()
    session.execute.return_value = _exec_all(
      rows=[
        (
          _make_ac(
            association_id="assoc_1",
            confidence=0.95,
            source="disclosure_mechanics",
          ),
          cls,
        ),
      ]
    )
    result = load_classifications_for_associations(session, ["assoc_1"])
    projected = result["assoc_1"][0]
    assert projected.confidence == 0.95
    assert projected.source == "disclosure_mechanics"
