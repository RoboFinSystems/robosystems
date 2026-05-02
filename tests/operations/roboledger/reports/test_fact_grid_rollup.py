"""Tests for the ancestor rollup added in Option C step #3.

When a fact's element isn't in the rendered structure, the renderer
walks ``general-special`` arcs upward to find the nearest in-structure
ancestor and aggregates the fact value there. Facts with no
in-structure ancestor are dropped from the rendered view.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

from robosystems.operations.roboledger.reports.fact_grid import (
  ReportFact,
  _collect_hierarchy_element_ids,
  _resolve_renderable_ancestor,
  _roll_up_facts_to_structure,
)


def _row(**kwargs):
  m = MagicMock()
  for k, v in kwargs.items():
    setattr(m, k, v)
  return m


def _node(element_id: str, children=None):
  """Build a hierarchy node with the minimum fields used by the
  collector."""
  n = MagicMock()
  n.element_id = element_id
  n.children = children or []
  return n


# ── _collect_hierarchy_element_ids ─────────────────────────────────────────


def test_collect_hierarchy_walks_full_tree():
  tree = [
    _node(
      "root_a",
      children=[
        _node("a1"),
        _node("a2", children=[_node("a2a"), _node("a2b")]),
      ],
    ),
    _node("root_b", children=[_node("b1")]),
  ]
  ids = _collect_hierarchy_element_ids(tree)
  assert ids == {"root_a", "a1", "a2", "a2a", "a2b", "root_b", "b1"}


def test_collect_hierarchy_handles_empty():
  assert _collect_hierarchy_element_ids([]) == set()


# ── _resolve_renderable_ancestor ───────────────────────────────────────────


def _ancestor_session(parents_by_child: dict[str, list[str]]):
  """Build a session that responds to general-special parent queries.

  ``parents_by_child`` maps a child element_id to its general-special
  parents. The session walks the BFS frontier across multiple calls,
  so ``side_effect`` returns different rows depending on which
  children are queried.
  """
  session = MagicMock()

  def execute_side_effect(_stmt, params):
    children = params["children"]
    parent_ids: set[str] = set()
    for c in children:
      for p in parents_by_child.get(c, []):
        parent_ids.add(p)
    result = MagicMock()
    result.fetchall.return_value = [_row(parent_id=pid) for pid in parent_ids]
    return result

  session.execute.side_effect = execute_side_effect
  return session


def test_resolve_returns_self_if_already_in_structure():
  session = MagicMock()
  cache: dict[str, str | None] = {}
  result = _resolve_renderable_ancestor(session, "in_struct", {"in_struct"}, cache)
  assert result == "in_struct"
  # Cache populated; no DB call needed.
  session.execute.assert_not_called()


def test_resolve_finds_direct_parent():
  """Element X has parent P which is in the structure — direct hit."""
  session = _ancestor_session({"x": ["p"]})
  result = _resolve_renderable_ancestor(session, "x", {"p"}, {})
  assert result == "p"


def test_resolve_walks_multiple_levels():
  """X → Y → Z (in structure). BFS finds Z at depth 2."""
  session = _ancestor_session({"x": ["y"], "y": ["z"]})
  result = _resolve_renderable_ancestor(session, "x", {"z"}, {})
  assert result == "z"


def test_resolve_picks_nearest_when_multiple_paths():
  """X → A (in structure) AND X → B → C (in structure).
  Nearest wins — picks A even though both are reachable."""
  session = _ancestor_session({"x": ["a", "b"], "b": ["c"]})
  result = _resolve_renderable_ancestor(session, "x", {"a", "c"}, {})
  assert result == "a"


def test_resolve_returns_none_when_no_ancestor_in_structure():
  """Walk reaches a dead-end without hitting any in-structure node."""
  session = _ancestor_session({"x": ["y"], "y": ["z"]})  # none in {"q"}
  result = _resolve_renderable_ancestor(session, "x", {"q"}, {})
  assert result is None


def test_resolve_handles_cycles_safely():
  """A cycle in general-special arcs (data quality bug) shouldn't infinite-loop.
  Visited-set guards termination."""
  session = _ancestor_session({"x": ["y"], "y": ["x"]})  # cycle
  result = _resolve_renderable_ancestor(session, "x", {"q"}, {})
  assert result is None  # no exception


def test_resolve_caches_result():
  """Second call with same element_id reads from cache, no second DB walk."""
  session = _ancestor_session({"x": ["p"]})
  cache: dict[str, str | None] = {}
  _resolve_renderable_ancestor(session, "x", {"p"}, cache)
  assert cache["x"] == "p"
  call_count_after_first = session.execute.call_count
  _resolve_renderable_ancestor(session, "x", {"p"}, cache)
  # No additional DB calls on the cached path.
  assert session.execute.call_count == call_count_after_first


# ── _roll_up_facts_to_structure ────────────────────────────────────────────


def _fact(element_id: str, value: float) -> ReportFact:
  return ReportFact(
    element_id=element_id,
    element_qname=f"rs-gaap:{element_id}",
    element_name=element_id,
    classification="expense",
    balance_type="debit",
    value=value,
    period_start=date(2026, 1, 1),
    period_end=date(2026, 3, 31),
    period_type="duration",
  )


def test_rollup_passes_through_in_structure_facts():
  """A fact whose element is already in the structure is returned unchanged."""
  session = MagicMock()
  facts = [_fact("in_struct", 100.0)]
  result = _roll_up_facts_to_structure(session, facts, {"in_struct"})
  assert len(result) == 1
  assert result[0].element_id == "in_struct"
  assert result[0].value == 100.0


def test_rollup_rewrites_out_of_structure_to_ancestor():
  """A fact on an out-of-structure element is rewritten to its
  in-structure ancestor; value preserved."""
  session = _ancestor_session({"orphan": ["ancestor"]})
  facts = [_fact("orphan", 50.0)]
  result = _roll_up_facts_to_structure(session, facts, {"ancestor"})
  assert len(result) == 1
  assert result[0].element_id == "ancestor"
  assert result[0].value == 50.0


def test_rollup_drops_facts_with_no_renderable_ancestor():
  """Facts that can't reach an in-structure ancestor are silently
  dropped from the rendered view (still persisted, just invisible)."""
  session = _ancestor_session({"orphan": ["dead_end"]})
  facts = [_fact("orphan", 50.0), _fact("in_struct", 100.0)]
  result = _roll_up_facts_to_structure(session, facts, {"in_struct"})
  assert len(result) == 1
  assert result[0].element_id == "in_struct"


def test_rollup_aggregates_multiple_facts_to_same_ancestor():
  """Two separate facts on different out-of-structure elements that
  both resolve to the same ancestor should produce two rewritten
  ReportFacts (summing happens in _facts_to_balance_dict, not here)."""
  session = _ancestor_session({"orph_a": ["anc"], "orph_b": ["anc"]})
  facts = [_fact("orph_a", 10.0), _fact("orph_b", 25.0)]
  result = _roll_up_facts_to_structure(session, facts, {"anc"})
  assert len(result) == 2
  assert all(f.element_id == "anc" for f in result)
  total = sum(f.value for f in result)
  assert total == 35.0


def test_rollup_handles_empty_input():
  session = MagicMock()
  result = _roll_up_facts_to_structure(session, [], {"a", "b"})
  assert result == []
  session.execute.assert_not_called()
