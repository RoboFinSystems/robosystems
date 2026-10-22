"""Shape tests for the rs-gaap cash flow presentation's roll-forward.

Each indirect cash flow network roots at the cash total the statement opens
and closes on (ASU 2016-18), with net change in cash beneath it, so the
renderer can open the statement on the beginning-of-period balance and close
it on the balance sheet's cash. The total is summed by calculation from the
two cash rows every balance-sheet network presents, and is never a mapping
target.

These tests guard against:

- a network losing its cash total root, or net change moving out from under it
- the calc arcs that make the total the balance sheet's own sum drifting
- the total gaining a statement-classification trait (becoming mappable)
- the export's header concept disappearing from the rs-gaap package
"""

from __future__ import annotations

import pytest

from robosystems.taxonomy import load_taxonomy_package
from robosystems.taxonomy.discovery import framework_root
from robosystems.taxonomy.model import TaxonomyPackage

_PACKAGES = framework_root("rs-gaap") / "packages"
_PRES = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-presentation/"
_CALC = "https://robosystems.ai/seattle/cm-roles/roles/rs-gaap-calculations/"

CASH_TOTAL = "rs-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"
NET_CHANGE = "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"
CASH_HEADER = "rs-gaap:StatementOfCashFlowsAbstract"
CASH_FLOW_ROLES = (
  f"{_PRES}CashFlow-indirect",
  f"{_PRES}CashFlow-indirect-LLC",
  f"{_PRES}CashFlow-indirect-PART",
)
BALANCE_SHEET_CASH_ROWS = {
  "rs-gaap:CashAndCashEquivalentsAtCarryingValue",
  "rs-gaap:RestrictedCashAndInvestmentsCurrent",
}


@pytest.fixture(scope="module")
def presentation() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-presentation" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def calculations() -> TaxonomyPackage:
  return load_taxonomy_package(
    _PACKAGES / "rs-gaap-calculations" / "v1" / "taxonomy.jsonld"
  )


@pytest.fixture(scope="module")
def rs_gaap() -> TaxonomyPackage:
  return load_taxonomy_package(_PACKAGES / "rs-gaap" / "v1" / "taxonomy.jsonld")


class TestCashFlowRollForward:
  @pytest.mark.parametrize("role", CASH_FLOW_ROLES)
  def test_every_cash_flow_network_roots_at_the_cash_total(
    self, presentation: TaxonomyPackage, role: str
  ) -> None:
    arcs = [a for a in presentation.associations if a.role == role]
    targets = {a.to_qname for a in arcs}
    roots = {a.from_qname for a in arcs} - targets
    assert CASH_TOTAL in roots, role
    assert NET_CHANGE in targets, role
    above_net_change = {a.from_qname for a in arcs if a.to_qname == NET_CHANGE}
    assert above_net_change == {CASH_TOTAL}, role

  def test_the_total_is_the_balance_sheets_own_sum(
    self, calculations: TaxonomyPackage
  ) -> None:
    arcs = [
      a
      for a in calculations.associations
      if a.role == f"{_CALC}CashFlowStatement" and a.from_qname == CASH_TOTAL
    ]
    assert {a.to_qname for a in arcs} == BALANCE_SHEET_CASH_ROWS
    assert {a.weight for a in arcs} == {1.0}

  def test_the_total_and_its_header_are_rs_gaap_concepts(
    self, rs_gaap: TaxonomyPackage
  ) -> None:
    by_qname = {e.qname: e for e in rs_gaap.elements}
    total = by_qname[CASH_TOTAL]
    assert (total.period_type, total.balance_type, total.is_abstract) == (
      "instant",
      "debit",
      False,
    )
    header = by_qname[CASH_HEADER]
    assert header.is_abstract

  def test_the_total_is_never_a_mapping_target(self, rs_gaap: TaxonomyPackage) -> None:
    """Derived from the balance sheet's cash rows: a chart account mapped to
    it would be counted beside the rows it sums."""
    traits = [t for t in rs_gaap.trait_assignments if t.element_qname == CASH_TOTAL]
    assert not any(
      t.trait_category == "elementsOfFinancialStatements" for t in traits
    ), traits
