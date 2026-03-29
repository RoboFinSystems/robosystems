"""Tests for guard rails — structural and semantic validation."""

from __future__ import annotations

from robosystems.operations.reports.fact_grid import FactRow
from robosystems.operations.reports.guard_rails import validate_report


def _row(
  name: str,
  value: float,
  classification: str = "revenue",
  is_subtotal: bool = False,
  depth: int = 0,
  qname: str = "",
  prior: float | None = None,
) -> FactRow:
  return FactRow(
    element_id=f"elem_{name.lower().replace(' ', '_')}",
    element_qname=qname or f"us-gaap:{name.replace(' ', '')}",
    element_name=name,
    classification=classification,
    balance_type="credit"
    if classification in ("revenue", "liability", "equity")
    else "debit",
    current_value=value,
    prior_value=prior,
    is_subtotal=is_subtotal,
    depth=depth,
  )


class TestIncomeStatementValidation:
  def test_valid_income_statement(self):
    rows = [
      _row("Revenues", 1000.0, is_subtotal=True, depth=0),
      _row("Revenue", 1000.0, depth=1, qname="us-gaap:Revenues"),
      _row("Expenses", 700.0, classification="expense", is_subtotal=True, depth=0),
      _row(
        "Cost of Revenue",
        300.0,
        classification="expense",
        depth=1,
        qname="us-gaap:CostOfRevenue",
      ),
      _row(
        "Operating Expenses", 400.0, classification="expense", is_subtotal=True, depth=1
      ),
      _row("R&D", 200.0, classification="expense", depth=2),
      _row("SG&A", 200.0, classification="expense", depth=2),
      _row("Net Income", 300.0, depth=0, qname="us-gaap:NetIncomeLoss"),
    ]
    result = validate_report("income_statement", rows)
    assert result.passed is True
    assert len(result.failures) == 0

  def test_missing_revenue_warning(self):
    rows = [
      _row("Expenses", 500.0, classification="expense", is_subtotal=True, depth=0),
      _row("SG&A", 500.0, classification="expense", depth=1),
    ]
    result = validate_report("income_statement", rows)
    assert any("revenue" in w.lower() for w in result.warnings)

  def test_missing_net_income_warning(self):
    rows = [
      _row("Revenues", 1000.0, is_subtotal=True, depth=0),
      _row("Revenue", 1000.0, depth=1, qname="us-gaap:Revenues"),
    ]
    result = validate_report("income_statement", rows)
    assert any("Net Income" in w for w in result.warnings)


class TestBalanceSheetValidation:
  def test_balanced_sheet(self):
    rows = [
      _row("Assets", 1000.0, classification="asset", is_subtotal=True, depth=0),
      _row("Cash", 1000.0, classification="asset", depth=1),
      _row("Liabilities", 600.0, classification="liability", is_subtotal=True, depth=0),
      _row("AP", 600.0, classification="liability", depth=1),
      _row("Equity", 400.0, classification="equity", is_subtotal=True, depth=0),
      _row("Retained Earnings", 400.0, classification="equity", depth=1),
    ]
    result = validate_report("balance_sheet", rows)
    assert result.passed is True
    assert len(result.failures) == 0
    assert "accounting_equation" in result.checks

  def test_unbalanced_sheet(self):
    rows = [
      _row("Assets", 1000.0, classification="asset", is_subtotal=True, depth=0),
      _row("Cash", 1000.0, classification="asset", depth=1),
      _row("Liabilities", 500.0, classification="liability", is_subtotal=True, depth=0),
      _row("AP", 500.0, classification="liability", depth=1),
      _row("Equity", 300.0, classification="equity", is_subtotal=True, depth=0),
      _row("RE", 300.0, classification="equity", depth=1),
    ]
    result = validate_report("balance_sheet", rows)
    assert result.passed is False
    assert len(result.failures) == 1
    assert "does not balance" in result.failures[0]

  def test_empty_balance_sheet_passes(self):
    """All zeros should not trigger a balance failure."""
    rows = [
      _row("Assets", 0.0, classification="asset", is_subtotal=True, depth=0),
      _row("Liabilities", 0.0, classification="liability", is_subtotal=True, depth=0),
      _row("Equity", 0.0, classification="equity", is_subtotal=True, depth=0),
    ]
    result = validate_report("balance_sheet", rows)
    assert result.passed is True


class TestTotalsFoot:
  def test_matching_subtotals(self):
    rows = [
      _row("Total", 300.0, is_subtotal=True, depth=0),
      _row("A", 100.0, depth=1),
      _row("B", 200.0, depth=1),
    ]
    result = validate_report("income_statement", rows)
    # totals_foot check should pass
    foot_warnings = [
      w for w in result.warnings if "Subtotal" in w and "does not match" in w
    ]
    assert len(foot_warnings) == 0

  def test_mismatched_subtotals(self):
    rows = [
      _row("Total", 999.0, is_subtotal=True, depth=0),
      _row("A", 100.0, depth=1),
      _row("B", 200.0, depth=1),
    ]
    result = validate_report("income_statement", rows)
    foot_warnings = [w for w in result.warnings if "does not match" in w]
    assert len(foot_warnings) == 1


class TestSemanticChecks:
  def test_zero_subtotal_warning(self):
    rows = [
      _row("Empty Section", 0.0, is_subtotal=True, depth=0),
    ]
    result = validate_report("income_statement", rows)
    assert any("zero balance" in w.lower() for w in result.warnings)

  def test_comparative_all_zeros_warning(self):
    rows = [
      _row("Revenue", 500.0, depth=0, prior=0.0, qname="us-gaap:Revenues"),
    ]
    result = validate_report("income_statement", rows)
    assert any("Prior period" in w for w in result.warnings)

  def test_unknown_report_type(self):
    result = validate_report("unknown_type", [])
    assert result.passed is True
    assert "no_validation_rules" in result.checks
