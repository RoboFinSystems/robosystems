"""Unit tests for ``build_verification_summary`` — the panel aggregate.

The helper aggregates a block's verification results into overall counts
plus a per-``rule_category`` breakdown, resolving each result's category by
joining to the block's rules on ``rule_id``. Pure function — no DB.
"""

from __future__ import annotations

from robosystems.models.api.information_block import RuleLite, VerificationResultLite
from robosystems.operations.information_block.envelope import build_verification_summary


def _rule(rid: str, category: str) -> RuleLite:
  return RuleLite(id=rid, rule_category=category, rule_expression="$A = $B")


def _result(rid: str, status: str) -> VerificationResultLite:
  return VerificationResultLite(id=f"vr_{rid}_{status}", rule_id=rid, status=status)


def test_empty_results_returns_none() -> None:
  """No results → None, so the viewer hides the panel rather than rendering
  an all-zero summary."""
  assert build_verification_summary([], []) is None
  assert build_verification_summary([], [_rule("r1", "Consistency")]) is None


def test_overall_and_by_category_counts() -> None:
  rules = [
    _rule("r1", "FundamentalAccountingConceptRelation"),
    _rule("r2", "FundamentalAccountingConceptRelation"),
    _rule("r3", "Consistency"),
  ]
  results = [
    _result("r1", "pass"),
    _result("r2", "fail"),
    _result("r3", "pass"),
    _result("r3", "skipped"),
    _result("r1", "error"),
  ]

  summary = build_verification_summary(results, rules)

  assert summary is not None
  assert (
    summary.total,
    summary.passed,
    summary.failed,
    summary.errored,
    summary.skipped,
  ) == (
    5,
    2,
    1,
    1,
    1,
  )

  by_cat = {c.category: c for c in summary.by_category}
  assert set(by_cat) == {"FundamentalAccountingConceptRelation", "Consistency"}
  fac = by_cat["FundamentalAccountingConceptRelation"]
  assert (fac.total, fac.passed, fac.failed, fac.errored, fac.skipped) == (
    3,
    1,
    1,
    1,
    0,
  )
  cons = by_cat["Consistency"]
  assert (cons.total, cons.passed, cons.failed, cons.errored, cons.skipped) == (
    2,
    1,
    0,
    0,
    1,
  )

  # Deterministic order: by_category sorted by category name.
  assert [c.category for c in summary.by_category] == [
    "Consistency",
    "FundamentalAccountingConceptRelation",
  ]


def test_result_without_matching_rule_is_uncategorized() -> None:
  """A result whose rule_id isn't among the block's rules (category can't be
  resolved) falls into an 'Uncategorized' bucket rather than being dropped."""
  summary = build_verification_summary([_result("orphan", "fail")], [])
  assert summary is not None
  assert summary.total == 1 and summary.failed == 1
  assert [c.category for c in summary.by_category] == ["Uncategorized"]
  assert summary.by_category[0].failed == 1


def test_unknown_status_is_silently_skipped() -> None:
  """A status outside the CHECK closure (pass|fail|error|skipped) is skipped
  defensively — not counted — so a future schema drift can't inflate totals."""
  rules = [_rule("r1", "Consistency")]
  results = [_result("r1", "pass"), _result("r1", "unknown_future")]
  summary = build_verification_summary(results, rules)
  assert summary is not None
  assert summary.total == 1  # only the "pass" is counted
  assert summary.passed == 1
  assert summary.by_category[0].total == 1
