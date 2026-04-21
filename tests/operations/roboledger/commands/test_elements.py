"""Unit tests for element CRUD commands.

Tests cover:
- _would_create_cycle — pure cycle detection on the materialized path hierarchy
- _self_path_prefix — path prefix computation for descendant queries
- _hierarchy_for_parent — parent depth/path derivation with session mock
- create_element — taxonomy gate, parent gate, depth/path assignment
- update_element — not-found gate, scalar field updates, reparent cascade, cycle rejection
- delete_element — not-found gate, soft-delete semantics
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from robosystems.models.api.extensions.taxonomies import (
  CreateElementRequest,
  DeleteElementRequest,
  UpdateElementRequest,
)
from robosystems.operations.roboledger.commands.elements import (
  ElementCycleError,
  ElementNotFoundError,
  _hierarchy_for_parent,
  _self_path_prefix,
  _would_create_cycle,
  create_element,
  delete_element,
  update_element,
)
from robosystems.operations.roboledger.commands.taxonomies import TaxonomyNotFoundError

# ── Helpers ──────────────────────────────────────────────────────────────


def _elem(id: str, path: str = "", depth: int = 0, parent_id: str | None = None):
  """Minimal mock element with all attributes needed by _element_to_response."""
  e = MagicMock()
  e.id = id
  e.path = path
  e.depth = depth
  e.parent_id = parent_id
  # Fields consumed by _element_to_response / ElementResponse
  e.code = None
  e.name = "Test Element"
  e.description = None
  e.qname = None
  e.namespace = None
  e.classification = "asset"
  e.sub_classification = None
  e.balance_type = "debit"
  e.period_type = "duration"
  e.is_abstract = False
  e.element_type = "concept"
  e.source = "native"
  e.taxonomy_id = "tax_01"
  e.is_active = True
  e.external_id = None
  e.external_source = None
  return e


def _scalar_exec(value):
  r = MagicMock()
  r.scalar_one_or_none.return_value = value
  return r


def _scalars_exec(values):
  r = MagicMock()
  r.scalars.return_value.all.return_value = values
  return r


def _all_exec(rows):
  """Mock for a ``session.execute(...).all()`` call returning tuple rows.

  Used for the EFS-classification lookup inside update_element/delete_element
  which iterates (element_id, identifier) tuples directly rather than going
  through .scalars()."""
  r = MagicMock()
  r.all.return_value = rows
  return r


# ── _would_create_cycle ──────────────────────────────────────────────────


class TestWouldCreateCycle:
  def test_self_reference_is_cycle(self):
    e = _elem("A")
    assert _would_create_cycle(e, e) is True

  def test_direct_child_is_cycle(self):
    """Moving A under its own direct child B creates a cycle."""
    parent = _elem("A", path="", depth=0)
    child = _elem("B", path="A", depth=1)
    assert _would_create_cycle(parent, child) is True

  def test_grandchild_is_cycle(self):
    """Moving A under its grandchild C creates a cycle."""
    root = _elem("A", path="", depth=0)
    grandchild = _elem("C", path="A/B", depth=2)
    assert _would_create_cycle(root, grandchild) is True

  def test_deep_descendant_is_cycle(self):
    e = _elem("A", path="", depth=0)
    deep = _elem("D", path="A/B/C", depth=3)
    assert _would_create_cycle(e, deep) is True

  def test_sibling_is_safe(self):
    root = _elem("A", path="", depth=0)
    sibling = _elem("X", path="", depth=0)
    assert _would_create_cycle(root, sibling) is False

  def test_unrelated_element_is_safe(self):
    e = _elem("B", path="A", depth=1)
    unrelated = _elem("Z", path="Y", depth=1)
    assert _would_create_cycle(e, unrelated) is False

  def test_parent_of_element_is_safe(self):
    """Moving child B back under its existing parent A is not a cycle."""
    parent = _elem("A", path="", depth=0)
    child = _elem("B", path="A", depth=1)
    assert _would_create_cycle(child, parent) is False

  def test_path_prefix_collision_not_triggered_by_different_id(self):
    """Element AA should not falsely trigger cycle check for element A."""
    e = _elem("A", path="", depth=0)
    # AA's path starts with "A" but is not a descendant of A
    other = _elem("AA", path="ROOT", depth=1)
    # other.path is "ROOT", not "A" or "A/..." so no cycle
    assert _would_create_cycle(e, other) is False


# ── _self_path_prefix ────────────────────────────────────────────────────


class TestSelfPathPrefix:
  def test_root_element_returns_id(self):
    e = _elem("A", path="", depth=0)
    assert _self_path_prefix(e) == "A"

  def test_child_element_appends_id(self):
    e = _elem("B", path="A", depth=1)
    assert _self_path_prefix(e) == "A/B"

  def test_grandchild_element(self):
    e = _elem("C", path="A/B", depth=2)
    assert _self_path_prefix(e) == "A/B/C"

  def test_deep_element(self):
    e = _elem("D", path="A/B/C", depth=3)
    assert _self_path_prefix(e) == "A/B/C/D"


# ── _hierarchy_for_parent ────────────────────────────────────────────────


class TestHierarchyForParent:
  def test_root_returns_zero_empty_no_db_query(self):
    session = MagicMock()
    depth, path = _hierarchy_for_parent(session, None)
    assert depth == 0
    assert path == ""
    session.execute.assert_not_called()

  def test_child_of_root_element(self):
    session = MagicMock()
    parent = _elem("elem_A", path="", depth=0)
    session.execute.return_value.scalar_one_or_none.return_value = parent
    depth, path = _hierarchy_for_parent(session, "elem_A")
    assert depth == 1
    assert path == "elem_A"

  def test_grandchild_element(self):
    session = MagicMock()
    parent = _elem("elem_B", path="elem_A", depth=1)
    session.execute.return_value.scalar_one_or_none.return_value = parent
    depth, path = _hierarchy_for_parent(session, "elem_B")
    assert depth == 2
    assert path == "elem_A/elem_B"

  def test_missing_parent_raises(self):
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    with pytest.raises(ElementNotFoundError) as exc_info:
      _hierarchy_for_parent(session, "elem_missing")
    assert exc_info.value.element_id == "elem_missing"


# ── create_element ────────────────────────────────────────────────────────


class TestCreateElement:
  def _body(self, taxonomy_id: str = "tax_01", parent_id: str | None = None):
    return CreateElementRequest(
      taxonomy_id=taxonomy_id,
      code="TEST",
      name="Test Element",
      classification="asset",
      parent_id=parent_id,
    )

  def test_missing_taxonomy_raises(self):
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    with pytest.raises(TaxonomyNotFoundError):
      create_element(session, self._body(), "usr_1")

  def test_root_element_gets_zero_depth(self):
    session = MagicMock()
    taxonomy = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = taxonomy
    result = create_element(session, self._body(), "usr_1")
    assert result.depth == 0

  def test_child_element_gets_parent_depth_plus_one(self):
    session = MagicMock()
    taxonomy = MagicMock()
    parent = _elem("elem_parent", path="", depth=0)
    session.execute.side_effect = [
      _scalar_exec(taxonomy),
      _scalar_exec(parent),
      _scalar_exec(None),  # EFS Classification lookup — None = no seed row
    ]
    result = create_element(session, self._body(parent_id="elem_parent"), "usr_1")
    assert result.depth == 1

  def test_missing_parent_raises(self):
    session = MagicMock()
    taxonomy = MagicMock()
    session.execute.side_effect = [
      _scalar_exec(taxonomy),
      _scalar_exec(None),  # parent not found
    ]
    with pytest.raises(ElementNotFoundError):
      create_element(session, self._body(parent_id="elem_missing"), "usr_1")

  def test_element_added_and_flushed(self):
    session = MagicMock()
    taxonomy = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = taxonomy
    create_element(session, self._body(), "usr_1")
    # Two adds: the Element itself and the ElementClassification junction
    # row that anchors it to the FASB elementsOfFinancialStatements trait.
    assert session.add.call_count == 2
    session.flush.assert_called()


# ── update_element ────────────────────────────────────────────────────────


class TestUpdateElement:
  def test_missing_element_raises(self):
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    with pytest.raises(ElementNotFoundError):
      update_element(session, UpdateElementRequest(element_id="elem_missing", name="X"))

  def test_scalar_field_update_mutates_element(self):
    element = _elem("elem_A")
    element.name = "Old Name"
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = element
    update_element(session, UpdateElementRequest(element_id="elem_A", name="New Name"))
    assert element.name == "New Name"

  def test_reparent_cascades_path_to_descendant(self):
    """Moving B (under A) to X rewrites B's path and C's path."""
    # Before: A(root) → B(depth=1, path="elem_A") → C(depth=2, path="elem_A/elem_B")
    # After:  X(root) → B(depth=1, path="elem_X") and C(depth=2, path="elem_X/elem_B")
    elem_B = _elem("elem_B", path="elem_A", depth=1, parent_id="elem_A")
    elem_C = MagicMock()
    elem_C.id = "elem_C"
    elem_C.path = "elem_A/elem_B"
    elem_C.depth = 2
    elem_X = _elem("elem_X", path="", depth=0)

    session = MagicMock()
    session.execute.side_effect = [
      _scalar_exec(elem_B),  # load element being updated
      _scalar_exec(elem_X),  # load new parent for cycle check
      _scalar_exec(elem_X),  # load new parent inside _hierarchy_for_parent
      _scalars_exec([elem_C]),  # load descendants
      _all_exec([]),  # post-flush EFS classification lookup
    ]

    update_element(
      session, UpdateElementRequest(element_id="elem_B", parent_id="elem_X")
    )

    assert elem_B.parent_id == "elem_X"
    assert elem_B.depth == 1
    assert elem_B.path == "elem_X"
    assert elem_C.path == "elem_X/elem_B"
    assert (
      elem_C.depth == 2
    )  # depth delta = 0 (both moved from depth-1 parent to depth-0 parent)

  def test_detach_from_parent_makes_root(self):
    """Passing parent_id=None detaches element from its parent."""
    # Before: A(root) → B(depth=1) → C(depth=2)
    # Detach B → B becomes root (depth=0), C becomes depth=1
    elem_B = _elem("elem_B", path="elem_A", depth=1, parent_id="elem_A")
    elem_C = MagicMock()
    elem_C.id = "elem_C"
    elem_C.path = "elem_A/elem_B"
    elem_C.depth = 2

    session = MagicMock()
    session.execute.side_effect = [
      _scalar_exec(elem_B),  # load element
      # No cycle-check execute for None parent
      _scalars_exec([elem_C]),  # load descendants
      _all_exec([]),  # post-flush EFS classification lookup
    ]

    update_element(session, UpdateElementRequest(element_id="elem_B", parent_id=None))

    assert elem_B.parent_id is None
    assert elem_B.depth == 0
    assert elem_B.path == ""
    # C's old path "elem_A/elem_B" suffix after old_prefix "elem_A/elem_B" is ""
    # new_self_prefix for B is "elem_B" (root)
    assert elem_C.path == "elem_B"
    assert elem_C.depth == 1  # depth_delta = -1

  def test_reparent_to_deeper_position_increases_descendant_depths(self):
    """Moving a root element under a depth-1 parent increases descendants' depths."""
    # Before: X(root, depth=0) → Y(root, depth=0) → Z(child of Y, depth=1)
    # Reparent X under Z → X becomes depth=2, its child (none here) would increase too
    elem_X = _elem("elem_X", path="", depth=0)
    elem_Z = _elem("elem_Z", path="elem_Y", depth=1)

    session = MagicMock()
    session.execute.side_effect = [
      _scalar_exec(elem_X),  # load element X
      _scalar_exec(elem_Z),  # load new parent Z for cycle check
      _scalar_exec(elem_Z),  # load Z in _hierarchy_for_parent
      _scalars_exec([]),  # no descendants of X
      _all_exec([]),  # post-flush EFS classification lookup
    ]

    update_element(
      session, UpdateElementRequest(element_id="elem_X", parent_id="elem_Z")
    )

    assert elem_X.parent_id == "elem_Z"
    assert elem_X.depth == 2
    assert elem_X.path == "elem_Y/elem_Z"

  def test_cycle_detection_raises_when_reparenting_under_descendant(self):
    """Reparenting A under its own child B is rejected."""
    elem_A = _elem("elem_A", path="", depth=0)
    elem_B = _elem("elem_B", path="elem_A", depth=1)  # B is a child of A

    session = MagicMock()
    session.execute.side_effect = [
      _scalar_exec(elem_A),  # load element A
      _scalar_exec(elem_B),  # load proposed new parent B
    ]

    with pytest.raises(ElementCycleError):
      update_element(
        session, UpdateElementRequest(element_id="elem_A", parent_id="elem_B")
      )

  def test_cycle_detection_raises_for_self_reparent(self):
    elem_A = _elem("elem_A", path="", depth=0)

    session = MagicMock()
    session.execute.side_effect = [
      _scalar_exec(elem_A),  # load element
      _scalar_exec(elem_A),  # proposed new parent is itself
    ]

    with pytest.raises(ElementCycleError):
      update_element(
        session, UpdateElementRequest(element_id="elem_A", parent_id="elem_A")
      )

  def test_flush_called_after_update(self):
    element = _elem("elem_A")
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = element
    update_element(session, UpdateElementRequest(element_id="elem_A", name="Updated"))
    session.flush.assert_called()


# ── delete_element ────────────────────────────────────────────────────────


class TestDeleteElement:
  def test_missing_element_raises(self):
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    with pytest.raises(ElementNotFoundError) as exc_info:
      delete_element(session, DeleteElementRequest(element_id="elem_missing"))
    assert exc_info.value.element_id == "elem_missing"

  def test_soft_delete_sets_is_active_false(self):
    element = _elem("elem_A")
    element.is_active = True
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = element
    result = delete_element(session, DeleteElementRequest(element_id="elem_A"))
    assert element.is_active is False
    assert result.is_active is False

  def test_element_is_not_hard_deleted(self):
    element = _elem("elem_A")
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = element
    delete_element(session, DeleteElementRequest(element_id="elem_A"))
    session.delete.assert_not_called()

  def test_flush_called_after_soft_delete(self):
    element = _elem("elem_A")
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = element
    delete_element(session, DeleteElementRequest(element_id="elem_A"))
    session.flush.assert_called()
