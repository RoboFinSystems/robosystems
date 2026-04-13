"""Tests for fact generation, structure rendering, and hierarchy rollup."""

from __future__ import annotations

from datetime import date

from robosystems.operations.roboledger.reports.fact_grid import (
  ReportFact,
  _Balance,
  _build_rows,
  _compute_prior_period,
  _facts_to_balance_dict,
  _HierarchyNode,
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
    """Revenue parent with two children — header has no value."""
    hierarchy = [
      _HierarchyNode(
        element_id="root",
        qname="sfac6:Revenues",
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
    # First row: section header now rolls up its children (500.0),
    # not zero. Abstract/parent rows show the sum of their descendants.
    assert rows[0].is_subtotal is True
    assert rows[0].element_name == "Revenues"
    assert rows[0].values == [500.0]
    # Children
    assert rows[1].element_name == "Product Revenue"
    assert rows[1].values == [300.0]
    assert rows[2].element_name == "Service Revenue"
    assert rows[2].values == [200.0]

  def test_nested_hierarchy(self):
    """Two-level nesting: root → abstract parent → leaves."""
    hierarchy = [
      _HierarchyNode(
        element_id="expenses_root",
        qname="sfac6:Expenses",
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
    # Root expenses header now rolls up (85k + 120k = 205k)
    assert rows[0].element_name == "Expenses"
    assert rows[0].values == [205000.0]
    assert rows[0].depth == 0
    # Operating expenses header rolls up its descendants too
    assert rows[1].element_name == "Operating Expenses"
    assert rows[1].values == [205000.0]
    assert rows[1].depth == 1
    # Leaves
    assert rows[2].element_name == "R&D"
    assert rows[2].values == [85000.0]
    assert rows[3].element_name == "SG&A"
    assert rows[3].values == [120000.0]

  def test_empty_balances(self):
    """No mapped balances → all zeros."""
    hierarchy = [
      _HierarchyNode(
        element_id="root",
        qname="sfac6:Revenues",
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
    assert len(rows) == 2
    assert rows[0].values == [0.0]
    assert rows[1].values == [0.0]

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
        qname="sfac6:Revenues",
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
    assert rows[0].is_subtotal is True  # header
    assert rows[1].values == [1000.0]  # Revenue
    assert rows[2].values == [400.0]  # COGS
    assert rows[3].element_name == "Gross Profit"
    assert rows[3].values == [600.0]  # 1000 - 400

  def test_chained_calculations(self):
    """Calculations can reference other calculated elements."""
    hierarchy = [
      _HierarchyNode(
        element_id="root",
        qname="sfac6:Root",
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
        qname="sfac6:Root",
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
