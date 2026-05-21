"""Tests for the rollforward filter evaluation engine (Phase 2 MVP).

Covers :func:`evaluate_attribution_filters` aggregation, residual
handling under each validation_mode, and audit ``event_ids[]``
provenance. Mocks at the ``session.execute`` boundary — the engine is
SQL-driven, and re-running the SQL through a live test DB would
require fixtures that the rest of the Phase 2 MVP doesn't need.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import pytest

from robosystems.models.api.extensions.rollforward import (
  AttributionFilter,
  LineItemMetadataPredicate,
)
from robosystems.models.api.information_block import RollforwardMechanics
from robosystems.operations.roboledger.reports.rollforward_filters import (
  AttributedFact,
  RollforwardResidualError,
  evaluate_attribution_filters,
)

PERIOD_START = date(2024, 1, 1)
PERIOD_END = date(2024, 1, 31)


def _make_mechanics(
  *,
  bs_element_id: str = "elem_cash",
  bs_qname: str = "mini:CashAndCashEquivalents",
  default_tag_element_id: str | None = None,
  filters: list[AttributionFilter] | None = None,
  validation_mode: str = "residual_as_default",
) -> RollforwardMechanics:
  return RollforwardMechanics(
    bs_source_element_id=bs_element_id,
    bs_source_qname=bs_qname,
    default_change_tag_element_id=default_tag_element_id,
    attribution_filters=filters or [],
    validation_mode=validation_mode,  # type: ignore[arg-type]
  )


def _filter(
  target_qname: str, target_element_id: str, *values: str
) -> AttributionFilter:
  return AttributionFilter(
    target_qname=target_qname,
    target_element_id=target_element_id,
    predicate=LineItemMetadataPredicate(
      field="transaction_description_code",
      values=list(values),
    ),
  )


def _session(
  *, bs_delta_cents: int, filter_results: list[tuple[int, int, list[str]]]
) -> MagicMock:
  """Build a session mock for the engine's query sequence.

  Per period the engine issues: one BS-delta query (``fetchone``), then
  for each filter a flow-qname → element_id resolution query
  (``fetchall``) followed by the per-filter aggregation query
  (``fetchone``). The resolution stub returns one element id per filter so
  every filter proceeds to aggregation (the no-resolve short-circuit isn't
  exercised by these aggregation/residual tests).

  ``filter_results`` items are ``(value_cents, matched_count, event_ids)``
  tuples returned by the aggregation query. Rows are returned as plain
  tuples — SQLAlchemy Row objects are both indexable and iterable, so
  tuple stand-ins satisfy both access patterns the engine uses.
  """
  session = MagicMock()

  results_in_order: list[MagicMock] = []

  bs = MagicMock()
  bs.fetchone.return_value = (bs_delta_cents,)
  results_in_order.append(bs)

  for i, (value_cents, matched_count, event_ids) in enumerate(filter_results):
    resolve = MagicMock()
    resolve.fetchall.return_value = [(f"elem_flow_{i}",)]
    results_in_order.append(resolve)
    agg = MagicMock()
    agg.fetchone.return_value = (value_cents, matched_count, event_ids)
    results_in_order.append(agg)

  call_count = {"n": 0}

  def _execute(*_args, **_kwargs):
    idx = call_count["n"]
    call_count["n"] += 1
    if idx < len(results_in_order):
      return results_in_order[idx]
    fallback = MagicMock()
    fallback.fetchone.return_value = None
    fallback.fetchall.return_value = []
    return fallback

  session.execute.side_effect = _execute
  return session


class TestSingleFilterMatch:
  def test_emits_fact_with_value_and_event_ids(self) -> None:
    """One filter, one period, matched LineItems → one AttributedFact
    carrying value_cents + event_ids."""
    mechanics = _make_mechanics(
      filters=[
        _filter(
          "mini:ProceedsFromInvestmentsByOwner",
          "elem_proceeds",
          "mini:ProceedsFromInvestmentsByOwner",
        ),
      ]
    )
    # BS delta = +10000 cents (debit-positive cash inflow); filter aggregates
    # the same 10000 cents → residual = 0, no residual fact.
    session = _session(
      bs_delta_cents=10000,
      filter_results=[(10000, 1, ["evt_je_201"])],
    )

    facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert len(facts) == 1
    f = facts[0]
    assert isinstance(f, AttributedFact)
    assert f.target_element_id == "elem_proceeds"
    assert f.target_qname == "mini:ProceedsFromInvestmentsByOwner"
    assert f.value_cents == 10000
    assert f.matched_line_count == 1
    assert f.event_ids == ["evt_je_201"]
    assert f.is_residual is False

  def test_zero_match_count_returns_no_fact(self) -> None:
    """Filter that matches zero LineItems → no fact emitted. The "no
    activity in this period" case is the only suppression path; see
    the docstring for the rationale on the alternate "lines matched
    but signed sum is zero" case (which DOES emit a fact)."""
    mechanics = _make_mechanics(
      filters=[
        _filter("mini:ProceedsFromInvestmentsByOwner", "elem_p", "mini:Foo"),
      ]
    )
    session = _session(
      bs_delta_cents=0,
      filter_results=[(0, 0, [])],
    )

    facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert facts == []

  def test_matched_lines_netting_to_zero_still_emits_fact(self) -> None:
    """Lines matched but signed sum nets to zero (e.g. 3 DR + 2 CR
    cancelling) → fact IS emitted with ``value_cents=0`` and the
    matched event_ids. The activity happened; downstream renderers
    decide whether to surface it. This is the audit-trail guarantee
    documented in ``evaluate_attribution_filters``'s docstring."""
    mechanics = _make_mechanics(
      filters=[
        _filter("mini:Flow", "elem_flow", "mini:F"),
      ]
    )
    # 5 matched lines, signed sum = 0. BS delta also 0 → no residual.
    session = _session(
      bs_delta_cents=0,
      filter_results=[(0, 5, ["evt_a", "evt_b"])],
    )

    facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert len(facts) == 1
    f = facts[0]
    assert f.value_cents == 0
    assert f.matched_line_count == 5
    assert f.event_ids == ["evt_a", "evt_b"]
    assert f.is_residual is False


class TestMultipleFilters:
  def test_each_filter_emits_one_fact(self) -> None:
    """N filters → up to N facts (skip filters with zero matches)."""
    mechanics = _make_mechanics(
      filters=[
        _filter("mini:ProceedsFromInvestmentsByOwner", "elem_a", "mini:A"),
        _filter("mini:ProceedsFromAdditionalLongtermBorrowings", "elem_b", "mini:B"),
        _filter(
          "mini:PaymentForCapitalAdditionsOfPropertyPlantEquipment", "elem_c", "mini:C"
        ),
      ]
    )
    # ΔCash = +11000 = 10000 (filter A) + 2000 (filter B) - 1000 (filter C). No residual.
    session = _session(
      bs_delta_cents=11000,
      filter_results=[
        (10000, 1, ["evt_201"]),
        (2000, 1, ["evt_202"]),
        (-1000, 1, ["evt_203"]),
      ],
    )

    facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert len(facts) == 3
    assert [f.target_qname for f in facts] == [
      "mini:ProceedsFromInvestmentsByOwner",
      "mini:ProceedsFromAdditionalLongtermBorrowings",
      "mini:PaymentForCapitalAdditionsOfPropertyPlantEquipment",
    ]
    assert sum(f.value_cents for f in facts) == 11000


class TestResidualResidualAsDefault:
  def test_emits_default_tag_fact_when_default_tag_set(self) -> None:
    """Residual with default_change_tag_element_id set → residual fact
    tagged to the default tag."""
    mechanics = _make_mechanics(
      default_tag_element_id="elem_default_cf",
      filters=[
        _filter("mini:ProceedsFromInvestmentsByOwner", "elem_a", "mini:A"),
      ],
    )
    # ΔCash = +12000; filter aggregates 10000 → residual = +2000.
    session = _session(
      bs_delta_cents=12000,
      filter_results=[(10000, 1, ["evt_201"])],
    )

    facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert len(facts) == 2
    matched, residual = facts
    assert matched.value_cents == 10000
    assert matched.is_residual is False
    assert residual.value_cents == 2000
    assert residual.is_residual is True
    assert residual.target_element_id == "elem_default_cf"

  def test_emits_unattributed_residual_when_no_default_tag(self) -> None:
    """Residual with no default_tag → still emits a residual fact;
    target_element_id is None and target_qname uses the '#residual'
    sentinel."""
    mechanics = _make_mechanics(
      default_tag_element_id=None,
      filters=[
        _filter("mini:ProceedsFromInvestmentsByOwner", "elem_a", "mini:A"),
      ],
    )
    session = _session(
      bs_delta_cents=12000,
      filter_results=[(10000, 1, ["evt_201"])],
    )

    facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert len(facts) == 2
    residual = facts[1]
    assert residual.is_residual is True
    assert residual.target_element_id is None
    assert residual.value_cents == 2000
    assert "#residual" in residual.target_qname

  def test_no_residual_fact_when_residual_is_zero(self) -> None:
    """Σ filters == Δ BS → no residual fact emitted (just the matched
    filter fact)."""
    mechanics = _make_mechanics(
      default_tag_element_id="elem_default",
      filters=[
        _filter("mini:Flow", "elem_a", "mini:F"),
      ],
    )
    session = _session(
      bs_delta_cents=10000,
      filter_results=[(10000, 1, ["evt_x"])],
    )

    facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert len(facts) == 1
    assert facts[0].is_residual is False


class TestResidualStrict:
  def test_raises_rollforward_residual_error(self) -> None:
    """validation_mode='strict' + non-zero residual → raises with
    carried-over context."""
    mechanics = _make_mechanics(
      validation_mode="strict",
      filters=[
        _filter("mini:Flow", "elem_a", "mini:F"),
      ],
    )
    session = _session(
      bs_delta_cents=12000,
      filter_results=[(10000, 1, ["evt_x"])],
    )

    with pytest.raises(RollforwardResidualError) as exc:
      evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    err = exc.value
    assert err.bs_source_qname == "mini:CashAndCashEquivalents"
    assert err.bs_delta_cents == 12000
    assert err.sum_matched_cents == 10000
    assert err.residual_cents == 2000


class TestResidualWarnOnly:
  def test_logs_and_omits_residual_fact(self) -> None:
    """validation_mode='warn_only' + non-zero residual → logs WARNING,
    emits no residual fact.

    RoboSystems uses a structured logger that doesn't route through
    Python's standard ``logging`` module, so ``caplog`` doesn't see
    the emission. We assert behavior by patching the module-level
    logger directly.
    """
    from unittest.mock import patch

    mechanics = _make_mechanics(
      validation_mode="warn_only",
      filters=[
        _filter("mini:Flow", "elem_a", "mini:F"),
      ],
    )
    session = _session(
      bs_delta_cents=12000,
      filter_results=[(10000, 1, ["evt_x"])],
    )

    module = "robosystems.operations.roboledger.reports.rollforward_filters"
    with patch(f"{module}.logger") as mock_logger:
      facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert len(facts) == 1
    assert all(not f.is_residual for f in facts)
    # Confirm WARNING-level log was emitted with the expected template.
    mock_logger.warning.assert_called_once()
    msg = mock_logger.warning.call_args[0][0]
    assert "rollforward residual" in msg


class TestNoFilters:
  def test_no_filters_with_zero_delta_returns_empty(self) -> None:
    """No filters declared AND no period activity → empty fact list."""
    mechanics = _make_mechanics(filters=[])
    session = _session(bs_delta_cents=0, filter_results=[])

    facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert facts == []

  def test_no_filters_with_nonzero_delta_emits_residual(self) -> None:
    """No filters but non-zero ΔBS → residual fact captures the full
    delta (residual_as_default mode)."""
    mechanics = _make_mechanics(
      default_tag_element_id="elem_default",
      filters=[],
    )
    session = _session(bs_delta_cents=5000, filter_results=[])

    facts = evaluate_attribution_filters(session, mechanics, PERIOD_START, PERIOD_END)

    assert len(facts) == 1
    assert facts[0].is_residual is True
    assert facts[0].value_cents == 5000
    assert facts[0].target_element_id == "elem_default"
