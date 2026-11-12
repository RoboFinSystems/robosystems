"""The reporting acceptance checks: the ledger sums and the comparisons (no stack)."""

from datetime import date

import pytest

from benchmarks.reporting.__main__ import Entity
from benchmarks.reporting.checks import Checker, expected_rows
from benchmarks.reporting.expected import combine, fold, less

pytestmark = pytest.mark.unit

CONCEPTS = {
  "1000": "rs-gaap:CashAndCashEquivalentsAtCarryingValue",
  "1300": "rs-gaap:PropertyPlantAndEquipmentGross",
  "1350": "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
  "2000": "rs-gaap:AccountsPayableCurrent",
  "3000": "rs-gaap:AdditionalPaidInCapital",
  "4000": "rs-gaap:Revenues",
  "6000": "rs-gaap:GeneralAndAdministrativeExpense",
}


def _tb(*rows):
  return [
    {"accountCode": code, "trait": trait, "totalDebits": dr, "totalCredits": cr}
    for code, trait, dr, cr in rows
  ]


OPENING = _tb(("1000", "asset", 1000, 0), ("3000", "equity", 0, 1000))
CLOSING = _tb(
  ("1000", "asset", 1700, 300),
  ("1300", "asset", 500, 0),
  ("1350", "asset", 0, 50),
  ("2000", "liability", 0, 250),
  ("3000", "equity", 0, 1000),
  ("4000", "revenue", 0, 900),
  ("6000", "expense", 300, 0),
)


def _fact(qname, value, depth, trait, *, subtotal=False, role=None):
  return {
    "qname": qname,
    "values": [value],
    "depth": depth,
    "trait": trait,
    "is_subtotal": subtotal,
    "period_role": role,
  }


def _statement(*facts, status="passed"):
  return {
    "facts": list(facts),
    "validation": {"passed": status == "passed", "status": status, "failures": []},
  }


def _balance_sheet(ppe=450.0):
  return _statement(
    _fact("rs-gaap:CashAndCashEquivalentsAtCarryingValue", 1400.0, 2, "asset"),
    _fact("rs-gaap:AssetsCurrent", 1400.0, 1, "asset", subtotal=True),
    _fact("rs-gaap:PropertyPlantAndEquipmentNet", ppe, 2, "asset"),
    _fact("rs-gaap:AssetsNoncurrent", ppe, 1, "asset", subtotal=True),
    _fact("rs-gaap:Assets", 1400.0 + ppe, 0, "asset", subtotal=True),
    _fact("rs-gaap:AccountsPayableCurrent", 250.0, 2, "liability"),
    _fact("rs-gaap:Liabilities", 250.0, 1, "liability", subtotal=True),
    _fact("rs-gaap:AdditionalPaidInCapital", 1000.0, 2, "equity"),
    _fact("rs-gaap:RetainedEarningsAccumulatedDeficit", 600.0, 2, "equity"),
    _fact("rs-gaap:StockholdersEquity", 1600.0, 1, "equity", subtotal=True),
    _fact("rs-gaap:LiabilitiesAndStockholdersEquity", 1850.0, 0, None, subtotal=True),
  )


def _failed(checker):
  return [c.name for c in checker.checks if not c.passed]


class TestLedger:
  def test_fold_sums_by_concept_and_trait(self):
    ledger = fold(CLOSING, CONCEPTS)
    assert ledger.natural("asset") == 1850
    assert ledger.natural("liability") == 250
    assert ledger.earnings == 600
    assert ledger.equity == 1600
    assert ledger.cash == 1400

  def test_property_is_stated_net_of_its_depreciation(self):
    ledger = fold(CLOSING, CONCEPTS)
    placed = ledger.placed()
    assert placed["rs-gaap:PropertyPlantAndEquipmentNet"] == (450, "asset")
    assert "rs-gaap:PropertyPlantAndEquipmentGross" not in placed

  def test_an_account_with_a_balance_and_no_mapping_is_reported(self):
    ledger = fold(_tb(("9999", "expense", 12, 0)), CONCEPTS)
    assert ledger.unmapped == {"9999": 12}

  def test_only_the_taxonomy_cash_rows_count_as_cash(self):
    concepts = {"1000": "rs-gaap:CashSurrenderValueOfLifeInsurance"}
    assert fold(_tb(("1000", "asset", 500, 0)), concepts).cash == 0

  def test_a_contra_account_belongs_to_the_trait_it_offsets(self):
    rows = _tb(("1300", "asset", 500, 0), ("1350", "contraAsset", 0, 50))
    assert fold(rows, CONCEPTS).natural("asset") == 450

  def test_an_account_mapped_twice_counts_once_on_the_row_shown(self):
    concepts = {"4000": ["rs-gaap:Revenues", "acme:SubscriptionRevenue"]}
    ledger = fold(_tb(("4000", "revenue", 0, 900)), concepts)
    assert ledger.placed({"acme:SubscriptionRevenue", "rs-gaap:Revenues"}) == {
      "acme:SubscriptionRevenue": (-900, "revenue")
    }
    assert ledger.placed({"rs-gaap:Revenues"}) == {
      "rs-gaap:Revenues": (-900, "revenue")
    }
    assert ledger.placed() == {"rs-gaap:Revenues": (-900, "revenue")}

  def test_less_is_the_activity_between_two_dates(self):
    period = less(fold(CLOSING, CONCEPTS), fold(OPENING, CONCEPTS))
    assert period.cash == 400
    assert period.earnings == 600
    assert period.natural("equity") == 0

  def test_combine_adds_entities_at_the_concept(self):
    both = combine([fold(CLOSING, CONCEPTS, "a"), fold(OPENING, CONCEPTS, "b")])
    assert both.natural("asset") == 2850
    assert both.placed()["rs-gaap:Revenues"][0] == -900


class TestRows:
  def test_a_parent_row_is_its_own_accounts_plus_the_rows_beneath_it(self):
    rows = {
      fact["qname"]: net
      for fact, net in expected_rows(_balance_sheet(), fold(CLOSING, CONCEPTS))
    }
    assert rows["rs-gaap:AssetsNoncurrent"] == 450
    assert rows["rs-gaap:Assets"] == 1850
    assert rows["rs-gaap:Liabilities"] == -250

  def test_a_statement_listed_parent_first_fails_rather_than_passes(self):
    statement = _balance_sheet()
    statement["facts"].reverse()
    checker = Checker("Co", "2026-01")
    checker.balance_sheet(statement, fold(CLOSING, CONCEPTS))
    assert "every row equals its accounts" in _failed(checker)


class TestScope:
  def test_a_statement_that_will_not_render_fails_and_the_rest_still_run(self):
    before, at_end = fold(OPENING, CONCEPTS), fold(CLOSING, CONCEPTS)
    checker = Checker("Co", "2026-01")
    checker.statements(
      {"balance_sheet": _balance_sheet(), "income_statement": "failed (500): boom"},
      before,
      at_end,
      less(at_end, before),
    )
    assert _failed(checker) == ["the statement renders"]
    assert {c.statement for c in checker.checks} == {
      "ledger",
      "balance_sheet",
      "income_statement",
    }


class TestBalanceSheet:
  def test_a_statement_that_matches_the_ledger_passes(self):
    checker = Checker("Co", "2026-01")
    checker.balance_sheet(_balance_sheet(), fold(CLOSING, CONCEPTS))
    assert checker.checks and not _failed(checker)

  def test_a_misstated_row_fails_the_row_and_the_totals(self):
    checker = Checker("Co", "2026-01")
    checker.balance_sheet(_balance_sheet(ppe=500.0), fold(CLOSING, CONCEPTS))
    failed = _failed(checker)
    assert "every row equals its accounts" in failed
    assert "total assets" in failed
    assert "assets = liabilities + equity" in failed

  def test_a_balance_with_no_row_fails(self):
    statement = _balance_sheet()
    statement["facts"] = [
      f for f in statement["facts"] if f["qname"] != "rs-gaap:AdditionalPaidInCapital"
    ]
    checker = Checker("Co", "2026-01")
    checker.balance_sheet(statement, fold(CLOSING, CONCEPTS))
    missing = next(
      c for c in checker.checks if c.name == "every row equals its accounts"
    )
    assert not missing.passed
    assert "rs-gaap:AdditionalPaidInCapital 1,000.00 absent" in missing.detail

  def test_a_window_the_engine_declined_is_not_checked(self):
    checker = Checker("Co", "2026-01")
    checker.balance_sheet(_statement(status="inconclusive"), fold(CLOSING, CONCEPTS))
    assert checker.checks == []


class TestFlows:
  def _ledgers(self):
    return fold(OPENING, CONCEPTS), fold(CLOSING, CONCEPTS)

  def test_income_statement_rows_and_net_income(self):
    before, at_end = self._ledgers()
    statement = _statement(
      _fact("rs-gaap:Revenues", 900.0, 1, "revenue"),
      _fact("rs-gaap:GeneralAndAdministrativeExpense", 300.0, 2, "expense"),
      _fact("rs-gaap:OperatingExpenses", 300.0, 1, "expense", subtotal=True),
      _fact("rs-gaap:NetIncomeLoss", 600.0, 1, "metric", subtotal=True),
    )
    checker = Checker("Co", "2026-01")
    checker.income_statement(statement, less(at_end, before))
    assert not _failed(checker)

  def _cash_flow(self, change=400.0, plug=0.0):
    cash = "rs-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"
    return _statement(
      _fact(cash, 1000.0, 0, None, role="start"),
      _fact("rs-gaap:NetIncomeLoss", 600.0, 3, "metric"),
      _fact("rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet", plug, 3, None),
      _fact(
        "rs-gaap:NetCashProvidedByUsedInOperatingActivities",
        900.0,
        2,
        None,
        subtotal=True,
      ),
      _fact(
        "rs-gaap:NetCashProvidedByUsedInInvestingActivities",
        change - 900.0,
        2,
        None,
        subtotal=True,
      ),
      _fact(
        "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease",
        change,
        1,
        None,
        subtotal=True,
      ),
      _fact(cash, 1000.0 + change, 0, None, subtotal=True, role="end"),
    )

  def test_cash_flow_ties_to_the_cash_accounts(self):
    before, at_end = self._ledgers()
    checker = Checker("Co", "2026-01")
    checker.cash_flow(self._cash_flow(), before, at_end, 600.0)
    assert not _failed(checker)

  def test_cash_flow_that_misses_the_cash_accounts_fails(self):
    before, at_end = self._ledgers()
    checker = Checker("Co", "2026-01")
    checker.cash_flow(self._cash_flow(change=350.0), before, at_end, 600.0)
    assert {"closing cash", "change in cash"} <= set(_failed(checker))

  def test_a_reconciling_item_fails(self):
    before, at_end = self._ledgers()
    checker = Checker("Co", "2026-01")
    checker.cash_flow(self._cash_flow(plug=75.0), before, at_end, 600.0)
    assert _failed(checker) == ["operating activities need no reconciling item"]

  def test_equity_statement_ties_to_the_ledger_at_both_ends(self):
    before, at_end = self._ledgers()
    equity = "rs-gaap:StockholdersEquity"
    statement = _statement(
      _fact(equity, 1000.0, 0, "equity", role="start"),
      _fact("rs-gaap:NetIncomeLoss", 600.0, 1, "metric"),
      _fact(equity, 1600.0, 0, "equity", subtotal=True, role="end"),
    )
    checker = Checker("Co", "2026-01")
    checker.equity_statement(statement, before, at_end, 600.0)
    assert not _failed(checker)

    statement["facts"][2]["values"] = [1550.0]
    wrong = Checker("Co", "2026-01")
    wrong.equity_statement(statement, before, at_end, 600.0)
    assert set(_failed(wrong)) == {
      "closing equity",
      "the movements carry opening to closing",
    }


class TestMembership:
  def _member(self, **overrides):
    values = {
      "id": "e",
      "name": "Member",
      "is_parent": False,
      "method": "full",
      "role": "operating",
      "joined_on": date(2025, 11, 1),
      "left_on": date(2026, 6, 30),
      "closed_through": "2026-06",
      "concepts_by_code": {},
    }
    return Entity(**{**values, **overrides})

  def test_a_member_is_summed_from_the_day_it_joins_through_the_day_it_leaves(self):
    member = self._member()
    assert not member.summed_at(date(2025, 10, 31))
    assert member.summed_at(date(2025, 11, 30))
    assert member.summed_at(date(2026, 6, 30))
    assert not member.summed_at(date(2026, 7, 31))
    assert member.summed_during(date(2026, 6, 1), date(2026, 6, 30))
    assert not member.summed_during(date(2026, 7, 1), date(2026, 7, 31))

  def test_an_equity_method_member_is_never_summed(self):
    assert not self._member(method="equity").summed_at(date(2026, 1, 31))

  def test_the_parent_and_the_consolidation_entity_always_are(self):
    assert self._member(is_parent=True, method="none").summed_at(date(2026, 1, 31))
    assert self._member(role="consolidation", method="none").summed_at(
      date(2026, 1, 31)
    )
