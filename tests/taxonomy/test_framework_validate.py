"""Unit tests for the pure (DB-free) logic in ``scripts/framework_validate.py``.

The validator's three DB-backed modes (A structural, B coverage, C package
integrity) need a provisioned extensions tenant and run on-demand via
``just framework-validate`` — CI has no extensions DB. These tests cover the
pure-function core so the helpers + the ``GapReport`` model have a regression
net in CI, and so that a refactor of the private symbols the script imports
from the operations/reporting layer fails *here* rather than silently at
runtime.
"""

from __future__ import annotations

from robosystems.operations.roboledger.reports.fact_grid import _HierarchyNode
from robosystems.scripts.framework_validate import (
  CYCLE,
  ERROR,
  WARNING,
  Finding,
  GapReport,
  StatementSummary,
  _calc_reachable_set,
  _check_calc_cycles,
  _concrete_leaves,
  _count_subtotals,
  _invert_calc_children,
)


def _node(
  element_id: str,
  *,
  abstract: bool = False,
  children: list[_HierarchyNode] | None = None,
) -> _HierarchyNode:
  return _HierarchyNode(
    element_id=element_id,
    qname=f"rs-gaap:{element_id}",
    name=element_id,
    classification="asset",
    balance_type="debit",
    is_abstract=abstract,
    depth=0,
    children=children or [],
  )


class TestConcreteLeaves:
  def test_returns_only_nonabstract_leaves(self) -> None:
    tree = [_node("Assets", children=[_node("Cash"), _node("AR")])]
    assert {n.element_id for n in _concrete_leaves(tree)} == {"Cash", "AR"}

  def test_excludes_abstract_leaves(self) -> None:
    tree = [_node("Header", abstract=True), _node("Cash")]
    assert {n.element_id for n in _concrete_leaves(tree)} == {"Cash"}

  def test_recurses_through_nesting(self) -> None:
    tree = [_node("A", children=[_node("B", children=[_node("C")])])]
    assert {n.element_id for n in _concrete_leaves(tree)} == {"C"}

  def test_empty_hierarchy(self) -> None:
    assert _concrete_leaves([]) == []


class TestCountSubtotals:
  def test_counts_nonabstract_parents(self) -> None:
    assert _count_subtotals([_node("Assets", children=[_node("Cash")])]) == 1

  def test_abstract_parent_not_counted(self) -> None:
    tree = [_node("Header", abstract=True, children=[_node("Cash")])]
    assert _count_subtotals(tree) == 0

  def test_counts_nested_subtotals(self) -> None:
    tree = [_node("A", children=[_node("B", children=[_node("C")])])]
    assert _count_subtotals(tree) == 2  # A and B, not the leaf C


class TestInvertCalcChildren:
  def test_inverts_child_to_parent(self) -> None:
    assert _invert_calc_children({"c1": {"P"}, "c2": {"P"}}) == {"P": {"c1", "c2"}}

  def test_multiple_parents_per_child(self) -> None:
    assert _invert_calc_children({"c": {"P1", "P2"}}) == {"P1": {"c"}, "P2": {"c"}}

  def test_empty(self) -> None:
    assert _invert_calc_children({}) == {}


class TestCalcReachableSet:
  def test_bfs_down_from_roots(self) -> None:
    children = {"Root": {"A", "B"}, "A": {"leaf"}}
    assert _calc_reachable_set(children, {"Root"}) == {"Root", "A", "B", "leaf"}

  def test_unreachable_branch_excluded(self) -> None:
    children = {"Root": {"A"}, "Orphan": {"X"}}
    assert _calc_reachable_set(children, {"Root"}) == {"Root", "A"}

  def test_cycle_does_not_loop_forever(self) -> None:
    assert _calc_reachable_set({"A": {"B"}, "B": {"A"}}, {"A"}) == {"A", "B"}


class TestCheckCalcCycles:
  def test_acyclic_produces_no_finding(self) -> None:
    report = GapReport()
    _check_calc_cycles({"Root": {"A"}, "A": {"leaf"}}, report)
    assert report.ok()
    assert not [f for f in report.findings if f.category == CYCLE]

  def test_cycle_is_flagged_as_error(self) -> None:
    report = GapReport()
    _check_calc_cycles({"A": {"B"}, "B": {"A"}}, report)
    assert not report.ok()
    assert any(f.category == CYCLE and f.severity == ERROR for f in report.findings)


class TestGapReport:
  def test_ok_only_when_no_errors(self) -> None:
    report = GapReport()
    assert report.ok()
    report.add(Finding(severity=WARNING, category="x", message="m"))
    assert report.ok()  # warnings don't fail the gate
    report.add(Finding(severity=ERROR, category="x", message="m"))
    assert not report.ok()

  def test_errors_and_warnings_partition(self) -> None:
    report = GapReport()
    report.add(Finding(severity=ERROR, category="x", message="e"))
    report.add(Finding(severity=WARNING, category="y", message="w"))
    assert len(report.errors) == 1
    assert len(report.warnings) == 1

  def test_uncomposed_statement_types(self) -> None:
    report = GapReport()
    report.summary = [
      StatementSummary(form="CORP", statement="balance_sheet", structure_name="BS"),
      StatementSummary(form="CORP", statement="comprehensive_income"),  # not composed
    ]
    uncomposed = report._uncomposed_statement_types()
    assert "comprehensive_income" in uncomposed
    assert "balance_sheet" not in uncomposed

  def test_format_text_runs_clean(self) -> None:
    assert "PASS" in GapReport().format_text()


class TestFinding:
  def test_str_includes_location_and_message(self) -> None:
    rendered = str(
      Finding(
        severity=ERROR,
        category="reachability",
        message="dead branch",
        form="CORP",
        statement="balance_sheet",
        element_qname="rs-gaap:Cash",
      )
    )
    assert "CORP" in rendered
    assert "balance_sheet" in rendered
    assert "dead branch" in rendered
    assert "rs-gaap:Cash" in rendered
