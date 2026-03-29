"""Tests for FactGridBuilder — hierarchy rollup and fact grid construction."""

from __future__ import annotations

from datetime import date

from robosystems.operations.reports.fact_grid import (
  _Balance,
  _build_rows,
  _compute_prior_period,
  _HierarchyNode,
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
    """Revenue parent with two children should sum correctly."""
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
    rows = _build_rows(hierarchy, balances, {})

    assert len(rows) == 3
    # First row: subtotal header
    assert rows[0].is_subtotal is True
    assert rows[0].element_name == "Revenues"
    assert rows[0].current_value == 500.0
    # Children
    assert rows[1].element_name == "Product Revenue"
    assert rows[1].current_value == 300.0
    assert rows[2].element_name == "Service Revenue"
    assert rows[2].current_value == 200.0

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
    rows = _build_rows(hierarchy, balances, {})

    assert len(rows) == 4
    # Root expenses subtotal
    assert rows[0].element_name == "Expenses"
    assert rows[0].current_value == 205000.0
    assert rows[0].depth == 0
    # Operating expenses subtotal
    assert rows[1].element_name == "Operating Expenses"
    assert rows[1].current_value == 205000.0
    assert rows[1].depth == 1
    # Leaves
    assert rows[2].element_name == "R&D"
    assert rows[2].current_value == 85000.0
    assert rows[3].element_name == "SG&A"
    assert rows[3].current_value == 120000.0

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

    rows = _build_rows(hierarchy, {}, {})
    assert len(rows) == 2
    assert rows[0].current_value == 0.0
    assert rows[1].current_value == 0.0

  def test_comparative_periods(self):
    """Prior values should be populated when prior_balances provided."""
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
    rows = _build_rows(hierarchy, current, prior)

    assert len(rows) == 1
    assert rows[0].current_value == 500.0
    assert rows[0].prior_value == 400.0

  def test_no_comparative(self):
    """When no prior_balances, prior_value should be None."""
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
    rows = _build_rows(hierarchy, current, {})

    assert len(rows) == 1
    assert rows[0].prior_value is None
