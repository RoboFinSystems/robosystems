"""Tests for structure heuristic classification."""

from robosystems.adapters.sec.enrichment import classify_structure_heuristic


class TestClassifyStructureHeuristic:
  def test_income_statement(self):
    stype, conf = classify_structure_heuristic(
      "CONSOLIDATED STATEMENTS OF INCOME",
      "0001001 - Statement - CONSOLIDATED STATEMENTS OF INCOME",
      block_type="Statement",
    )
    assert stype == "income_statement"
    assert conf == 0.85

  def test_balance_sheet(self):
    stype, conf = classify_structure_heuristic(
      "CONSOLIDATED BALANCE SHEETS",
      "0001002 - Statement - CONSOLIDATED BALANCE SHEETS",
      block_type="Statement",
    )
    assert stype == "balance_sheet"
    assert conf == 0.85

  def test_cash_flow(self):
    stype, conf = classify_structure_heuristic(
      "CONSOLIDATED STATEMENTS OF CASH FLOWS",
      "0001003 - Statement - CONSOLIDATED STATEMENTS OF CASH FLOWS",
      block_type="Statement",
    )
    assert stype == "cash_flow_statement"
    assert conf == 0.85

  def test_equity_statement(self):
    stype, conf = classify_structure_heuristic(
      "CONSOLIDATED STATEMENTS OF STOCKHOLDERS' EQUITY",
      "0001004 - Statement - CONSOLIDATED STATEMENTS OF STOCKHOLDERS' EQUITY",
      block_type="Statement",
    )
    assert stype == "equity_statement"
    assert conf == 0.85

  def test_comprehensive_income(self):
    stype, conf = classify_structure_heuristic(
      "CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME",
      "0001005 - Statement - CONSOLIDATED STATEMENTS OF COMPREHENSIVE INCOME",
      block_type="Statement",
    )
    assert stype == "comprehensive_income"
    assert conf == 0.85

  def test_parenthetical_lower_confidence(self):
    stype, conf = classify_structure_heuristic(
      "CONSOLIDATED BALANCE SHEETS [Parenthetical]",
      "0001002 - Statement - CONSOLIDATED BALANCE SHEETS [Parenthetical]",
      block_type="Statement",
    )
    assert stype == "balance_sheet"
    assert conf == 0.75

  def test_disclosure_skipped_by_block_type(self):
    """Disclosure structures should be skipped when block_type is provided."""
    stype, conf = classify_structure_heuristic(
      "Balance Sheet Components - Inventories (Details)",
      "995410 - Disclosure - Balance Sheet Components - Inventories (Details)",
      block_type="Disclosure",
    )
    assert stype is None
    assert conf == 0.0

  def test_disclosure_with_cash_flow_keyword_skipped(self):
    """Disclosure about cash flows should not match cash_flow_statement."""
    stype, conf = classify_structure_heuristic(
      "Cash Flows (Details)",
      "995415 - Disclosure - Cash Flows (Details)",
      block_type="Disclosure",
    )
    assert stype is None
    assert conf == 0.0

  def test_disclosure_not_matched_by_keyword(self):
    """Disclosure definitions without 'statement' keyword should not match."""
    stype, conf = classify_structure_heuristic(
      "Organization",
      "0002001 - Disclosure - Organization",
    )
    assert stype is None
    assert conf == 0.0

  def test_empty_input(self):
    stype, conf = classify_structure_heuristic(None, None)
    assert stype is None
    assert conf == 0.0

  def test_operations_as_income_statement(self):
    """'Operations' is a common synonym for income statement."""
    stype, conf = classify_structure_heuristic(
      "CONSOLIDATED STATEMENTS OF OPERATIONS",
      "0001001 - Statement - CONSOLIDATED STATEMENTS OF OPERATIONS",
      block_type="Statement",
    )
    assert stype == "income_statement"
    assert conf == 0.85

  def test_comprehensive_income_before_income(self):
    """Comprehensive income should match before plain income (more specific)."""
    stype, conf = classify_structure_heuristic(
      "STATEMENTS OF COMPREHENSIVE INCOME",
      "Statement - STATEMENTS OF COMPREHENSIVE INCOME",
    )
    assert stype == "comprehensive_income"
    assert conf == 0.85

  def test_unknown_block_type_still_classifies(self):
    """When block_type is None, classification proceeds normally."""
    stype, conf = classify_structure_heuristic(
      "CONSOLIDATED BALANCE SHEETS",
      "0001002 - Statement - CONSOLIDATED BALANCE SHEETS",
      block_type=None,
    )
    assert stype == "balance_sheet"
    assert conf == 0.85

  def test_empty_block_type_still_classifies(self):
    """When block_type is empty string, classification proceeds normally."""
    stype, conf = classify_structure_heuristic(
      "CONSOLIDATED BALANCE SHEETS",
      "0001002 - Statement - CONSOLIDATED BALANCE SHEETS",
      block_type="",
    )
    assert stype == "balance_sheet"
    assert conf == 0.85
