"""Tests for guard rails — structural and semantic validation."""

from __future__ import annotations

from robosystems.operations.roboledger.reports.fact_grid import FactRow
from robosystems.operations.roboledger.reports.guard_rails import validate_report


def _row(
  name: str,
  value: float,
  classification: str = "revenue",
  is_subtotal: bool = False,
  depth: int = 0,
  qname: str = "",
  prior: float | None = None,
) -> FactRow:
  vals = [value]
  if prior is not None:
    vals.append(prior)
  return FactRow(
    element_id=f"elem_{name.lower().replace(' ', '_')}",
    element_qname=qname or f"us-gaap:{name.replace(' ', '')}",
    element_name=name,
    classification=classification,
    balance_type="credit"
    if classification in ("revenue", "liability", "equity")
    else "debit",
    values=vals,
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

  def test_missing_revenue_is_inconclusive_failure(self):
    """Was a warning under the old qname-substring semantic check; the
    new structural validator fails inconclusively because Net Income
    can't be computed without a revenue rollup."""
    rows = [
      _row("Expenses", 500.0, classification="expense", is_subtotal=True, depth=0),
      _row("SG&A", 500.0, classification="expense", depth=1),
    ]
    result = validate_report("income_statement", rows)
    assert result.passed is False
    assert any("inconclusive" in f and "revenue" in f for f in result.failures)

  def test_missing_net_income_emits_implied_warning(self):
    """No NetIncome row but revenue + expense both present — the
    validator surfaces the implied NI as an informational warning rather
    than failing (multi-step structures often end at 'Income from
    Continuing Operations' without an explicit NetIncome line)."""
    rows = [
      _row("Revenues", 1000.0, is_subtotal=True, depth=0),
      _row("Revenue", 1000.0, depth=1, qname="us-gaap:Revenues"),
      _row("Expenses", 600.0, classification="expense", is_subtotal=True, depth=0),
      _row("SG&A", 600.0, classification="expense", depth=1),
    ]
    result = validate_report("income_statement", rows)
    assert result.passed is True
    assert any("implied NI" in w for w in result.warnings)


class TestIncomeStatementStructuralValidation:
  """Cover the Net Income = Revenue - Expenses check, which the prior
  IS validator was missing entirely."""

  def test_net_income_matches_revenue_minus_expenses_passes(self):
    rows = [
      _row("Revenues", 200.0, classification="revenue", is_subtotal=True, depth=1),
      _row("Expenses", 120.0, classification="expense", is_subtotal=True, depth=1),
      _row(
        "Net Income",
        80.0,
        classification="",
        qname="us-gaap:NetIncomeLoss",
        is_subtotal=True,
        depth=1,
      ),
    ]
    result = validate_report("income_statement", rows)
    assert result.passed is True, f"Failures: {result.failures}"
    assert "net_income_equation" in result.checks

  def test_net_income_mismatch_fails(self):
    rows = [
      _row("Revenues", 200.0, classification="revenue", is_subtotal=True, depth=1),
      _row("Expenses", 120.0, classification="expense", is_subtotal=True, depth=1),
      # Reports 100 but math says 80 — mismatch.
      _row(
        "Net Income",
        100.0,
        classification="",
        qname="us-gaap:NetIncomeLoss",
        is_subtotal=True,
        depth=1,
      ),
    ]
    result = validate_report("income_statement", rows)
    assert result.passed is False
    assert any("Net Income mismatch" in f for f in result.failures)

  def test_missing_revenue_classification_is_inconclusive_not_silent_pass(self):
    """Regression: prior IS validator had NO structural check. With FAC
    elements having no classifications, it would just emit warnings and
    return passed=True. The new validator fails loudly so a missing-
    classification rendering can't read as green."""
    rows = [
      # Subtotal with no classification and no inferable qname.
      _row(
        "Container",
        100.0,
        classification="",
        qname="x:Container",
        is_subtotal=True,
        depth=0,
      ),
    ]
    result = validate_report("income_statement", rows)
    assert result.passed is False
    assert any("inconclusive" in f for f in result.failures)

  def test_no_net_income_row_emits_warning_not_failure(self):
    """Multi-step structures may end at 'Income from Continuing
    Operations' without an explicit NetIncome line. Validator should
    surface the implied NI as info (warning), not fail."""
    rows = [
      _row("Revenues", 200.0, classification="revenue", is_subtotal=True, depth=1),
      _row("Expenses", 120.0, classification="expense", is_subtotal=True, depth=1),
    ]
    result = validate_report("income_statement", rows)
    assert result.passed is True
    assert any("implied NI" in w for w in result.warnings)

  def test_classification_inferred_from_fac_qnames(self):
    rows = [
      # No FASB classifications — qname inference does the work.
      _row(
        "Revenues Roll Up",
        192_500.0,
        classification="",
        qname="fac:RevenuesRollUp",
        is_subtotal=True,
        depth=3,
      ),
      _row(
        "Costs and Expenses Roll Up",
        157_950.0,
        classification="",
        qname="fac:CostsAndExpensesRollUp",
        is_subtotal=True,
        depth=3,
      ),
    ]
    result = validate_report("income_statement", rows)
    # No NetIncome row — implied NI = 192500 - 157950 = 34550, warning only.
    assert result.passed is True, f"Failures: {result.failures}"
    assert any("34550" in w for w in result.warnings)


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
    """All zeros, with explicit rollup rows for all three classifications,
    should pass the equation (0 = 0 + 0)."""
    rows = [
      _row("Assets", 0.0, classification="asset", is_subtotal=True, depth=0),
      _row("Liabilities", 0.0, classification="liability", is_subtotal=True, depth=0),
      _row("Equity", 0.0, classification="equity", is_subtotal=True, depth=0),
    ]
    result = validate_report("balance_sheet", rows)
    assert result.passed is True

  def test_no_rollup_rows_is_inconclusive_not_silent_pass(self):
    """Regression test: prior behaviour silently returned passed=True when
    no rows matched the classification filter (zero rollups found).
    The validator now treats missing rollups as a hard failure so a green
    check can't mask a misclassified or empty rendering."""
    # Rows with no asset/liability/equity classification at all — could
    # happen when FASB element_traits aren't wired and qname inference
    # also fails (e.g. abstract container rows only).
    rows = [
      _row(
        "Container",
        100.0,
        classification="",
        qname="x:Container",
        is_subtotal=True,
        depth=0,
      ),
    ]
    result = validate_report("balance_sheet", rows)
    assert result.passed is False
    assert any("inconclusive" in f for f in result.failures)
    assert any("asset" in f for f in result.failures)

  def test_classification_inferred_from_fac_qnames(self):
    """FAC elements have no FASB traits — classification falls back to
    qname inference. Top-most subtotal per classification wins."""
    rows = [
      # Combined L+E rollup at depth 3 — should be skipped, not double-count.
      _row(
        "Liabilities and Equity Roll Up",
        85_150.0,
        classification="",
        qname="fac:LiabilitiesAndEquityRollUp",
        is_subtotal=True,
        depth=3,
      ),
      # Asset rollup at depth 3 — top-most asset subtotal.
      _row(
        "Assets Roll Up",
        85_150.0,
        classification="",
        qname="fac:AssetsRollUp",
        is_subtotal=True,
        depth=3,
      ),
      # Liabilities rollup at depth 4.
      _row(
        "Liabilities Roll Up",
        800.0,
        classification="",
        qname="fac:LiabilitiesRollUp",
        is_subtotal=True,
        depth=4,
      ),
      # Equity rollup at depth 4.
      _row(
        "Equity Roll Up",
        84_350.0,
        classification="",
        qname="fac:EquityRollUp",
        is_subtotal=True,
        depth=4,
      ),
    ]
    result = validate_report("balance_sheet", rows)
    assert result.passed is True, f"Failures: {result.failures}"

  def test_among_ties_at_same_depth_largest_value_wins(self):
    """FAC structures place several equity-classified subtotals at the
    same depth (Temporary Equity, Redeemable Noncontrolling Interest,
    Equity [Roll Up] all at depth=4). The one with actual data — largest
    absolute value — must win, otherwise a zero-valued placeholder hides
    the real total and the equation falsely reports a $0 equity."""
    rows = [
      _row("Assets", 100.0, classification="asset", is_subtotal=True, depth=0),
      _row("Liabilities", 20.0, classification="liability", is_subtotal=True, depth=0),
      # Three equity subtotals at the same depth — the placeholder zeros
      # appear FIRST so the tie-breaker (largest value) is what saves us.
      _row(
        "Temporary Equity",
        0.0,
        classification="equity",
        qname="fac:TemporaryEquity",
        is_subtotal=True,
        depth=4,
      ),
      _row(
        "Redeemable Noncontrolling Interest",
        0.0,
        classification="equity",
        qname="fac:RedeemableNoncontrollingInterest",
        is_subtotal=True,
        depth=4,
      ),
      _row(
        "Equity Roll Up",
        80.0,
        classification="equity",
        qname="fac:EquityRollUp",
        is_subtotal=True,
        depth=4,
      ),
    ]
    result = validate_report("balance_sheet", rows)
    # 100 = 20 + 80 ✓ — only true if Equity Roll Up wins the tie.
    assert result.passed is True, f"Failures: {result.failures}"

  def test_top_most_subtotal_wins_when_multiple_per_classification(self):
    """Avoid double-counting: nested rollups at multiple depths must not
    sum together. The smallest-depth subtotal per classification is used."""
    rows = [
      # depth=0 top-level Assets rollup is the canonical total.
      _row("Assets", 100.0, classification="asset", is_subtotal=True, depth=0),
      # Nested asset subtotals — should NOT contribute to the equation.
      _row("Current Assets", 60.0, classification="asset", is_subtotal=True, depth=1),
      _row(
        "Noncurrent Assets", 40.0, classification="asset", is_subtotal=True, depth=1
      ),
      _row("Liabilities", 60.0, classification="liability", is_subtotal=True, depth=0),
      _row("Equity", 40.0, classification="equity", is_subtotal=True, depth=0),
    ]
    result = validate_report("balance_sheet", rows)
    # Equation: 100 = 60 + 40 (NOT 100 + 60 + 40 = 60 + 40)
    assert result.passed is True, f"Failures: {result.failures}"


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
    assert any("no data" in w for w in result.warnings)

  def test_unknown_report_type(self):
    result = validate_report("unknown_type", [])
    assert result.passed is True
    assert "no_validation_rules" in result.checks
