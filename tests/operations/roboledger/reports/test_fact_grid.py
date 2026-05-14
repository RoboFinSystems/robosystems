"""Tests for fact generation, structure rendering, and hierarchy rollup."""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

from robosystems.operations.roboledger.reports.fact_grid import (
  ReportFact,
  _Balance,
  _build_rows,
  _close_prior_periods_to_retained_earnings,
  _close_to_retained_earnings,
  _compute_prior_period,
  _facts_to_balance_dict,
  _find_close_target,
  _HierarchyNode,
  _infer_classification,
  _infer_period_type,
  _natural_sign,
)


class TestNaturalSign:
  def test_debit_normal_positive(self):
    assert _natural_sign(100.0, "debit") == 100.0

  def test_debit_normal_negative(self):
    assert _natural_sign(-50.0, "debit") == -50.0

  def test_credit_normal_positive_credits(self):
    # Credits exceed debits → net_balance = -100 → display as +100
    assert _natural_sign(-100.0, "credit") == 100.0

  def test_credit_normal_negative_credits(self):
    # Debits exceed credits → net_balance = 50 → display as -50
    assert _natural_sign(50.0, "credit") == -50.0

  def test_zero(self):
    assert _natural_sign(0.0, "debit") == 0.0
    assert _natural_sign(0.0, "credit") == 0.0


class TestComputePriorPeriod:
  def test_quarterly(self):
    start, end = _compute_prior_period(date(2026, 1, 1), date(2026, 3, 31))
    assert end == date(2025, 12, 31)
    # Q1 = 90 days, so prior period is 90 days ending Dec 31
    duration = (date(2026, 3, 31) - date(2026, 1, 1)).days + 1
    assert (end - start).days + 1 == duration

  def test_monthly(self):
    start, end = _compute_prior_period(date(2026, 3, 1), date(2026, 3, 31))
    assert end == date(2026, 2, 28)
    assert start.month == 1 or start.month == 2  # 31-day month prior

  def test_single_day(self):
    start, end = _compute_prior_period(date(2026, 6, 15), date(2026, 6, 15))
    assert start == date(2026, 6, 14)
    assert end == date(2026, 6, 14)


class TestBuildRows:
  """Test the hierarchy rollup logic with mock data."""

  def _make_balances(
    self, data: dict[str, float], balance_type: str = "credit"
  ) -> dict[str, _Balance]:
    """Helper to create balances dict from {element_id: net_balance}."""
    return {
      eid: _Balance(
        element_id=eid,
        qname=f"us-gaap:{eid}",
        name=eid,
        classification="revenue",
        balance_type=balance_type,
        total_debits=0.0,
        total_credits=abs(val),
        net_balance=-abs(val) if balance_type == "credit" else abs(val),
      )
      for eid, val in data.items()
    }

  def test_simple_hierarchy(self):
    """Revenue parent with two children. Post-order emit: leaves first,
    then the parent subtotal — matches financial-statement reading
    convention (line items above their total)."""
    hierarchy = [
      _HierarchyNode(
        element_id="root",
        qname="fac:Revenues",
        name="Revenues",
        classification="revenue",
        balance_type="credit",
        is_abstract=True,
        depth=0,
        children=[
          _HierarchyNode(
            element_id="rev_product",
            qname="us-gaap:ProductRevenue",
            name="Product Revenue",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
          _HierarchyNode(
            element_id="rev_service",
            qname="us-gaap:ServiceRevenue",
            name="Service Revenue",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
        ],
      )
    ]

    balances = self._make_balances({"rev_product": 300.0, "rev_service": 200.0})
    rows = _build_rows(hierarchy, [balances], {})

    assert len(rows) == 3
    # Post-order: children first, then their parent subtotal.
    assert rows[0].element_name == "Product Revenue"
    assert rows[0].values == [300.0]
    assert rows[0].is_subtotal is False
    assert rows[1].element_name == "Service Revenue"
    assert rows[1].values == [200.0]
    assert rows[1].is_subtotal is False
    # Parent rolls up to 500.0; flagged as subtotal because it has children.
    assert rows[2].element_name == "Revenues"
    assert rows[2].values == [500.0]
    assert rows[2].is_subtotal is True

  def test_nested_hierarchy(self):
    """Two-level nesting: root → abstract parent → leaves."""
    hierarchy = [
      _HierarchyNode(
        element_id="expenses_root",
        qname="fac:Expenses",
        name="Expenses",
        classification="expense",
        balance_type="debit",
        is_abstract=True,
        depth=0,
        children=[
          _HierarchyNode(
            element_id="opex",
            qname="us-gaap:OperatingExpenses",
            name="Operating Expenses",
            classification="expense",
            balance_type="debit",
            is_abstract=True,
            depth=1,
            children=[
              _HierarchyNode(
                element_id="rnd",
                qname="us-gaap:RnD",
                name="R&D",
                classification="expense",
                balance_type="debit",
                is_abstract=False,
                depth=2,
              ),
              _HierarchyNode(
                element_id="sga",
                qname="us-gaap:SGA",
                name="SG&A",
                classification="expense",
                balance_type="debit",
                is_abstract=False,
                depth=2,
              ),
            ],
          ),
        ],
      )
    ]

    balances = {
      "rnd": _Balance(
        "rnd", "us-gaap:RnD", "R&D", "expense", "debit", 85000.0, 0.0, 85000.0
      ),
      "sga": _Balance(
        "sga", "us-gaap:SGA", "SG&A", "expense", "debit", 120000.0, 0.0, 120000.0
      ),
    }
    rows = _build_rows(hierarchy, [balances], {})

    assert len(rows) == 4
    # Post-order: deepest leaves first, then ascending parent subtotals.
    assert rows[0].element_name == "R&D"
    assert rows[0].values == [85000.0]
    assert rows[0].depth == 2
    assert rows[1].element_name == "SG&A"
    assert rows[1].values == [120000.0]
    assert rows[1].depth == 2
    # Operating expenses rolls up its leaves
    assert rows[2].element_name == "Operating Expenses"
    assert rows[2].values == [205000.0]
    assert rows[2].depth == 1
    # Root rolls up everything below (same value here — only one branch)
    assert rows[3].element_name == "Expenses"
    assert rows[3].values == [205000.0]
    assert rows[3].depth == 0

  def test_empty_balances(self):
    """No mapped balances → every row is zero, and the renderer
    suppresses zero-value rows so the grid comes back empty. Readers
    should never see a wall of $0.00 lines for a structure no facts
    populate."""
    hierarchy = [
      _HierarchyNode(
        element_id="root",
        qname="fac:Revenues",
        name="Revenues",
        classification="revenue",
        balance_type="credit",
        is_abstract=True,
        depth=0,
        children=[
          _HierarchyNode(
            element_id="rev",
            qname="us-gaap:Revenues",
            name="Revenue",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
        ],
      )
    ]

    rows = _build_rows(hierarchy, [{}], {})
    assert rows == []

  def test_multi_period(self):
    """Multiple period columns should produce values list per row."""
    hierarchy = [
      _HierarchyNode(
        element_id="rev",
        qname="us-gaap:Revenues",
        name="Revenue",
        classification="revenue",
        balance_type="credit",
        is_abstract=False,
        depth=0,
      ),
    ]

    current = self._make_balances({"rev": 500.0})
    prior = self._make_balances({"rev": 400.0})
    rows = _build_rows(hierarchy, [current, prior], {})

    assert len(rows) == 1
    assert rows[0].values == [500.0, 400.0]

  def test_single_period(self):
    """Single period produces one-element values list."""
    hierarchy = [
      _HierarchyNode(
        element_id="rev",
        qname="us-gaap:Revenues",
        name="Revenue",
        classification="revenue",
        balance_type="credit",
        is_abstract=False,
        depth=0,
      ),
    ]

    current = self._make_balances({"rev": 500.0})
    rows = _build_rows(hierarchy, [current], {})

    assert len(rows) == 1
    assert rows[0].values == [500.0]

  def test_calculation_elements(self):
    """Computed elements resolve via calculation associations."""
    hierarchy = [
      _HierarchyNode(
        element_id="root",
        qname="fac:Revenues",
        name="Revenues",
        classification="revenue",
        balance_type="credit",
        is_abstract=True,
        depth=0,
        children=[
          _HierarchyNode(
            element_id="revenue",
            qname="us-gaap:Revenue",
            name="Revenue",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
          _HierarchyNode(
            element_id="cogs",
            qname="us-gaap:COGS",
            name="Cost of Revenue",
            classification="expense",
            balance_type="debit",
            is_abstract=False,
            depth=1,
          ),
          _HierarchyNode(
            element_id="gross_profit",
            qname="us-gaap:GrossProfit",
            name="Gross Profit",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
        ],
      )
    ]

    current = self._make_balances({"revenue": 1000.0, "cogs": 400.0}, "credit")
    # COGS is debit-normal, set it separately
    current["cogs"] = _Balance(
      "cogs", "us-gaap:COGS", "COGS", "expense", "debit", 400.0, 0.0, 400.0
    )

    calculations = {
      "gross_profit": [("revenue", 1.0), ("cogs", -1.0)],
    }

    rows = _build_rows(hierarchy, [current], calculations)

    assert len(rows) == 4
    # Post-order: leaves first, then parent.
    assert rows[0].element_name == "Revenue"
    assert rows[0].values == [1000.0]
    assert rows[1].element_name == "Cost of Revenue"
    assert rows[1].values == [400.0]
    # Gross Profit is a calc-DAG target — flagged as subtotal even though
    # its summands (Revenue, COGS) are siblings, not children.
    assert rows[2].element_name == "Gross Profit"
    assert rows[2].values == [600.0]
    assert rows[2].is_subtotal is True
    # Root parent rolls up its concrete-fact children (Revenue + COGS),
    # NOT Gross Profit (which would double-count).
    assert rows[3].element_name == "Revenues"
    assert rows[3].is_subtotal is True

  def test_chained_calculations(self):
    """Calculations can reference other calculated elements."""
    hierarchy = [
      _HierarchyNode(
        element_id="root",
        qname="report:Root",
        name="Root",
        classification="revenue",
        balance_type="credit",
        is_abstract=True,
        depth=0,
        children=[
          _HierarchyNode(
            element_id="a",
            qname="a",
            name="A",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
          _HierarchyNode(
            element_id="b",
            qname="b",
            name="B",
            classification="expense",
            balance_type="debit",
            is_abstract=False,
            depth=1,
          ),
          _HierarchyNode(
            element_id="subtotal",
            qname="subtotal",
            name="Subtotal",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
          _HierarchyNode(
            element_id="c",
            qname="c",
            name="C",
            classification="expense",
            balance_type="debit",
            is_abstract=False,
            depth=1,
          ),
          _HierarchyNode(
            element_id="total",
            qname="total",
            name="Total",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
        ],
      )
    ]

    current = self._make_balances({"a": 100.0}, "credit")
    current["b"] = _Balance("b", "b", "B", "expense", "debit", 30.0, 0.0, 30.0)
    current["c"] = _Balance("c", "c", "C", "expense", "debit", 10.0, 0.0, 10.0)

    # subtotal = a - b, total = subtotal - c (chained)
    calculations = {
      "subtotal": [("a", 1.0), ("b", -1.0)],
      "total": [("subtotal", 1.0), ("c", -1.0)],
    }

    rows = _build_rows(hierarchy, [current], calculations)

    subtotal_row = next(r for r in rows if r.element_id == "subtotal")
    total_row = next(r for r in rows if r.element_id == "total")
    assert subtotal_row.values == [70.0]  # 100 - 30
    assert total_row.values == [60.0]  # 70 - 10

  def test_calculation_multi_period(self):
    """Calculations resolve correctly across multiple periods."""
    hierarchy = [
      _HierarchyNode(
        element_id="root",
        qname="report:Root",
        name="Root",
        classification="revenue",
        balance_type="credit",
        is_abstract=True,
        depth=0,
        children=[
          _HierarchyNode(
            element_id="rev",
            qname="rev",
            name="Revenue",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
          _HierarchyNode(
            element_id="cogs",
            qname="cogs",
            name="COGS",
            classification="expense",
            balance_type="debit",
            is_abstract=False,
            depth=1,
          ),
          _HierarchyNode(
            element_id="gp",
            qname="gp",
            name="Gross Profit",
            classification="revenue",
            balance_type="credit",
            is_abstract=False,
            depth=1,
          ),
        ],
      )
    ]

    p1 = self._make_balances({"rev": 1000.0}, "credit")
    p1["cogs"] = _Balance("cogs", "cogs", "COGS", "expense", "debit", 400.0, 0.0, 400.0)

    p2 = self._make_balances({"rev": 800.0}, "credit")
    p2["cogs"] = _Balance("cogs", "cogs", "COGS", "expense", "debit", 350.0, 0.0, 350.0)

    calculations = {"gp": [("rev", 1.0), ("cogs", -1.0)]}

    rows = _build_rows(hierarchy, [p1, p2], calculations)

    gp_row = next(r for r in rows if r.element_id == "gp")
    assert gp_row.values == [600.0, 450.0]  # 1000-400, 800-350


class TestInferPeriodType:
  def test_balance_sheet_items_are_instant(self):
    assert _infer_period_type("asset") == "instant"
    assert _infer_period_type("liability") == "instant"
    assert _infer_period_type("equity") == "instant"

  def test_income_items_are_duration(self):
    assert _infer_period_type("revenue") == "duration"
    assert _infer_period_type("expense") == "duration"


class TestFactsToBalanceDict:
  def test_filters_by_period(self):
    facts = [
      ReportFact(
        element_id="rev1",
        element_qname="us-gaap:Revenue",
        element_name="Revenue",
        classification="revenue",
        balance_type="credit",
        value=500.0,
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        period_type="duration",
      ),
      ReportFact(
        element_id="rev1",
        element_qname="us-gaap:Revenue",
        element_name="Revenue",
        classification="revenue",
        balance_type="credit",
        value=400.0,
        period_start=date(2025, 10, 3),
        period_end=date(2025, 12, 31),
        period_type="duration",
      ),
    ]

    current = _facts_to_balance_dict(facts, date(2026, 1, 1), date(2026, 3, 31))
    assert len(current) == 1
    assert current["rev1"].net_balance == 500.0

    prior = _facts_to_balance_dict(facts, date(2025, 10, 3), date(2025, 12, 31))
    assert len(prior) == 1
    assert prior["rev1"].net_balance == 400.0

  def test_empty_for_no_matching_period(self):
    facts = [
      ReportFact(
        element_id="rev1",
        element_qname="us-gaap:Revenue",
        element_name="Revenue",
        classification="revenue",
        balance_type="credit",
        value=500.0,
        period_start=date(2026, 1, 1),
        period_end=date(2026, 3, 31),
        period_type="duration",
      ),
    ]

    result = _facts_to_balance_dict(facts, date(2025, 1, 1), date(2025, 3, 31))
    assert len(result) == 0


# TestDeriveClosedThroughFromLedger removed — the helper it tested
# (`_derive_closed_through_from_ledger`) was deleted along with
# `_ledger_has_re_postings`. The synthetic retained-earnings close now
# always runs from inception, because real closing entries zero out the
# rev/exp accounts they close so the cumulative sum naturally returns
# only the still-unclosed portion. See the module docstring of
# `_close_prior_periods_to_retained_earnings` for the full argument.


class TestClosePriorPeriodsToRetainedEarnings:
  """Pins the retained-earnings math for the three data shapes the
  private OLTP path can produce.

  The SQL query itself is trivial (SUM aggregation); the risk surface
  is the Python arithmetic that subtracts current-period NI from the
  cumulative total and adds the remainder to retained earnings. These
  tests mock `session.execute` with canned rows representing each
  scenario and assert the resulting RE fact value.
  """

  MAPPING_ID = "struct_coa_mapping"
  PERIOD_START = date(2026, 1, 1)
  PERIOD_END = date(2026, 1, 31)

  def _mock_session(self, revenue_cents: int, expense_cents: int) -> MagicMock:
    """Mock an extensions_session that returns cumulative rev/exp rows.

    Amounts are in cents (minor currency units) because that's what the
    line_items table stores. `cents_to_dollars` converts during read.
    """
    rows = []
    if revenue_cents:
      rows.append(
        SimpleNamespace(
          classification="revenue",
          balance_type="credit",
          total_debits=0,
          total_credits=revenue_cents,
        )
      )
    if expense_cents:
      rows.append(
        SimpleNamespace(
          classification="expense",
          balance_type="debit",
          total_debits=expense_cents,
          total_credits=0,
        )
      )
    session = MagicMock()
    session.execute.return_value = iter(rows)
    return session

  def _current_period_facts(self, revenue: float, expense: float) -> list[ReportFact]:
    """Build a minimal facts list for the current period after
    `_close_to_retained_earnings` has already run.

    Matches what `generate_report_facts` passes into
    `_close_prior_periods_to_retained_earnings` — revenue and expense
    facts for the current period plus the RE fact created by the
    current-period close.
    """
    facts = [
      ReportFact(
        element_id="elem_gaap_revenues",
        element_qname="us-gaap:Revenues",
        element_name="Revenue",
        classification="revenue",
        balance_type="credit",
        value=revenue,
        period_start=self.PERIOD_START,
        period_end=self.PERIOD_END,
        period_type="duration",
      ),
      ReportFact(
        element_id="elem_gaap_operating_expenses",
        element_qname="us-gaap:OperatingExpenses",
        element_name="Operating Expenses",
        classification="expense",
        balance_type="debit",
        value=expense,
        period_start=self.PERIOD_START,
        period_end=self.PERIOD_END,
        period_type="duration",
      ),
    ]
    _close_to_retained_earnings(facts, self.PERIOD_START, self.PERIOD_END)
    return facts

  def _get_re_fact(self, facts: list[ReportFact]) -> ReportFact:
    return next(
      f for f in facts if "retainedearnings" in (f.element_qname or "").lower()
    )

  def test_qb_history_no_closes(self):
    """QuickBooks shape: 12 months of history with no closing entries.

    QB soft-closes — it never posts year-end journal entries. A 3-year
    history means the cumulative rev/exp query returns everything from
    inception, and RE must roll up all of that prior-period NI.

    Scenario: 12 prior months at $10k rev / $6k exp = $4k NI each,
    plus a current month at $10k rev / $6k exp = $4k NI.
    Cumulative: 13 * $10k rev, 13 * $6k exp → cumulative NI = $52k.
    Current period NI is $4k (already added to RE by
    `_close_to_retained_earnings`), so prior-period NI = $48k rolled
    into the existing RE fact. Final RE = $4k + $48k = $52k.
    """
    facts = self._current_period_facts(revenue=10_000.0, expense=6_000.0)
    # Sanity check: current-period close set RE = $4k
    assert self._get_re_fact(facts).value == 4_000.0

    session = self._mock_session(
      revenue_cents=13_000_000,  # 13 * $10k
      expense_cents=7_800_000,  # 13 * $6k
    )
    _close_prior_periods_to_retained_earnings(
      session, self.MAPPING_ID, facts, self.PERIOD_START, self.PERIOD_END
    )

    re = self._get_re_fact(facts)
    assert re.value == 52_000.0, (
      "RE should contain cumulative NI from inception; this is the "
      "exact QB-history shape Risk 1 flagged as untested."
    )

  def test_post_real_close_is_noop(self):
    """After a well-formed closing entry, cumulative rev/exp are zero.

    Real closing entries (roboledger `close_period`, manual year-end
    JEs) zero out the rev/exp accounts they close:
        DR Revenue 100k / CR Expense 60k / CR RE 40k
    After this, `SUM(debits) - SUM(credits)` on rev/exp accounts for
    closed periods is zero, so the cumulative query returns only
    still-unclosed activity. If *everything* is closed, the query
    returns 0 rows or only the current period's activity.

    Scenario: all prior periods real-closed (zeroed). Cumulative query
    returns only the current month's $10k rev / $6k exp. Current NI =
    prior_NI, so prior_NI = 0 and the function early-returns.
    """
    facts = self._current_period_facts(revenue=10_000.0, expense=6_000.0)
    re_before = self._get_re_fact(facts).value

    session = self._mock_session(
      revenue_cents=1_000_000,  # current period only
      expense_cents=600_000,
    )
    _close_prior_periods_to_retained_earnings(
      session, self.MAPPING_ID, facts, self.PERIOD_START, self.PERIOD_END
    )

    assert self._get_re_fact(facts).value == re_before, (
      "Real closing entries zero out rev/exp; cumulative NI equals "
      "current NI so prior-period NI is zero — function must be a no-op."
    )

  def test_partial_close_rolls_only_unclosed(self):
    """Only some periods are real-closed; unclosed ones roll into RE.

    Scenario: 6 months were real-closed (zeroed out), 6 months plus
    the current month are unclosed. Cumulative query returns the
    7 months of unclosed activity: $70k rev, $42k exp → $28k NI.
    Current month is $4k (already in RE). Prior periods to roll:
    $28k - $4k = $24k.

    This is the "mixed" scenario that the old
    `_derive_closed_through_from_ledger` code path tried to handle
    with a lower-bound filter on posting_date. The simpler "from
    inception" approach gets the same answer naturally because the
    closed periods contribute zero to the sum.
    """
    facts = self._current_period_facts(revenue=10_000.0, expense=6_000.0)

    session = self._mock_session(
      revenue_cents=7_000_000,  # 7 unclosed months at $10k
      expense_cents=4_200_000,  # 7 unclosed months at $6k
    )
    _close_prior_periods_to_retained_earnings(
      session, self.MAPPING_ID, facts, self.PERIOD_START, self.PERIOD_END
    )

    re = self._get_re_fact(facts)
    assert re.value == 28_000.0, (
      "Partial close: RE = current-period NI ($4k already posted) + "
      "unclosed prior-period NI ($24k) = $28k total."
    )

  def test_creates_re_fact_when_none_exists(self):
    """If the current period has no rev/exp activity, the current-period
    close is a no-op and no RE fact is created. The prior-period close
    must still create an RE fact from the cumulative query.
    """
    # No revenue/expense facts — current period is empty
    facts: list[ReportFact] = []
    _close_to_retained_earnings(facts, self.PERIOD_START, self.PERIOD_END)
    assert facts == [], "No RE fact yet — current period had no activity"

    session = self._mock_session(
      revenue_cents=13_000_000,
      expense_cents=7_800_000,
    )
    _close_prior_periods_to_retained_earnings(
      session, self.MAPPING_ID, facts, self.PERIOD_START, self.PERIOD_END
    )

    re = self._get_re_fact(facts)
    assert re.value == 52_000.0
    assert re.period_start == self.PERIOD_START
    assert re.period_end == self.PERIOD_END
    assert re.period_type == "instant"
    assert re.classification == "equity"

  def test_loss_position_reduces_re(self):
    """Cumulative expenses exceed revenue — prior-period NI is negative
    and reduces RE. Verifies sign handling.
    """
    facts = self._current_period_facts(revenue=10_000.0, expense=6_000.0)

    # 13 months total: $130k rev, $156k exp → cumulative NI = -$26k
    # Current period NI = $4k, so prior_NI = -$30k
    # RE = $4k + (-$30k) = -$26k
    session = self._mock_session(
      revenue_cents=13_000_000,
      expense_cents=15_600_000,
    )
    _close_prior_periods_to_retained_earnings(
      session, self.MAPPING_ID, facts, self.PERIOD_START, self.PERIOD_END
    )

    assert self._get_re_fact(facts).value == -26_000.0


class TestInferClassification:
  """Qname/balance_type fallback for elements without FASB traits.

  Reference taxonomies (FAC, rs-gaap, type-subtype) often ship without
  the elementsOfFinancialStatements trait wired. The close-to-RE
  pipeline depends on classification to compute Net Income; this
  fallback restores it from the qname + balance_type.
  """

  def test_revenue_inferred_from_qname_and_credit_balance(self):
    assert _infer_classification("fac:Revenues", "credit") == "revenue"
    assert _infer_classification("us-gaap:SalesRevenueNet", "credit") == "revenue"

  def test_expense_inferred_from_debit_balance_and_token(self):
    assert _infer_classification("fac:CostsAndExpenses", "debit") == "expense"
    assert _infer_classification("us-gaap:DepreciationExpense", "debit") == "expense"
    assert _infer_classification("us-gaap:LossOnDisposal", "debit") == "expense"

  def test_equity_takes_precedence_over_liability_for_credit_capital(self):
    assert _infer_classification("fac:Equity", "credit") == "equity"
    assert (
      _infer_classification("us-gaap:RetainedEarningsAccumulatedDeficit", "credit")
      == "equity"
    )

  def test_asset_inferred_from_debit_balance_and_token(self):
    assert _infer_classification("fac:CurrentAssets", "debit") == "asset"
    assert _infer_classification("fac:FixedAssets", "debit") == "asset"

  def test_liability_inferred_from_credit_balance_and_token(self):
    assert _infer_classification("fac:CurrentLiabilities", "credit") == "liability"

  def test_returns_none_for_unrecognized_qname(self):
    assert _infer_classification("fac:Foo", "debit") is None
    assert _infer_classification(None, "debit") is None
    assert _infer_classification("", "credit") is None


class TestFindCloseTarget:
  """Resolution order: seed RE → us-gaap-shaped RE → any equity fact."""

  P_START = date(2026, 1, 1)
  P_END = date(2026, 12, 31)

  def _equity_fact(self, *, qname: str, value: float, element_id: str) -> ReportFact:
    return ReportFact(
      element_id=element_id,
      element_qname=qname,
      element_name=qname,
      classification="equity",
      balance_type="credit",
      value=value,
      period_start=self.P_START,
      period_end=self.P_END,
      period_type="instant",
    )

  def test_matches_retainedearnings_qname(self):
    """Canonical case: rs-gaap:RetainedEarningsAccumulatedDeficit wins."""
    re = self._equity_fact(
      qname="rs-gaap:RetainedEarningsAccumulatedDeficit",
      value=100.0,
      element_id="elem_xyz",
    )
    apic = self._equity_fact(
      qname="rs-gaap:AdditionalPaidInCapital",
      value=200.0,
      element_id="elem_apic",
    )
    target = _find_close_target([apic, re], self.P_START, self.P_END)
    assert target is re

  def test_matches_retaineddeficit_qname(self):
    """Loose qname matcher: ``*RetainedDeficit*`` shape also wins."""
    re = self._equity_fact(
      qname="us-gaap:RetainedDeficit",
      value=100.0,
      element_id="elem_xyz",
    )
    target = _find_close_target([re], self.P_START, self.P_END)
    assert target is re

  def test_does_not_fall_back_to_generic_equity(self):
    """Without a RetainedEarnings-shaped fact, no close target is picked.

    Generic equity facts (APIC, fac:Equity, etc.) are NOT acceptable
    fallbacks under the rs-gaap-anchored architecture — dumping NI on
    APIC corrupts the equity composition. Caller appends a fresh
    anonymous RE fact instead.
    """
    apic = self._equity_fact(
      qname="rs-gaap:AdditionalPaidInCapital",
      value=49_800.0,
      element_id="elem_apic",
    )
    assert _find_close_target([apic], self.P_START, self.P_END) is None

  def test_returns_none_when_no_equity_fact(self):
    asset = ReportFact(
      element_id="a",
      element_qname="fac:CurrentAssets",
      element_name="Current Assets",
      classification="asset",
      balance_type="debit",
      value=100.0,
      period_start=self.P_START,
      period_end=self.P_END,
      period_type="instant",
    )
    assert _find_close_target([asset], self.P_START, self.P_END) is None


class TestCloseToRetainedEarningsRsGaap:
  """End-to-end: close NI into rs-gaap:RetainedEarningsAccumulatedDeficit."""

  P_START = date(2025, 1, 1)
  P_END = date(2025, 12, 31)

  def _fact(
    self,
    *,
    qname: str,
    classification: str,
    balance_type: str,
    value: float,
    element_id: str,
    period_type: str = "duration",
  ) -> ReportFact:
    return ReportFact(
      element_id=element_id,
      element_qname=qname,
      element_name=qname,
      classification=classification,
      balance_type=balance_type,
      value=value,
      period_start=self.P_START,
      period_end=self.P_END,
      period_type=period_type,
    )

  def test_net_income_added_to_rs_gaap_re(self):
    """When an rs-gaap RE fact exists, close routes NI to it."""
    facts = [
      self._fact(
        qname="rs-gaap:SalesRevenueNet",
        classification="revenue",
        balance_type="credit",
        value=192_500.0,
        element_id="r",
      ),
      self._fact(
        qname="rs-gaap:SellingGeneralAndAdministrativeExpense",
        classification="expense",
        balance_type="debit",
        value=157_950.0,
        element_id="c",
      ),
      self._fact(
        qname="rs-gaap:RetainedEarningsAccumulatedDeficit",
        classification="equity",
        balance_type="credit",
        value=0.0,
        element_id="re",
        period_type="instant",
      ),
    ]

    _close_to_retained_earnings(facts, self.P_START, self.P_END)

    re = next(
      f
      for f in facts
      if f.element_qname == "rs-gaap:RetainedEarningsAccumulatedDeficit"
    )
    assert re.value == 192_500.0 - 157_950.0
    # No phantom legacy RE fact created.
    assert not any(f.element_id == "elem_gaap_retained_earnings" for f in facts)
