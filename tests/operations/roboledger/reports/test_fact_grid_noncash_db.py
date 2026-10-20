"""The cash flow's non-cash and settlement corrections on a real tenant.

Against the extensions database with a throwaway tenant provisioned from the
library, so the cash flow network, its calc arcs and derivations are the real
ones. One shape per test (spec ``cash-flow-noncash-and-settlement``): an
entry with no cash line against an investment, a debt or equity; capex on the
card; a bill settled by a linked payment, in one period and across two; a
partial and an unlinked payment; a tagged payment; a cash sale at a gain. In
each the reconciling adjustment is zero.
"""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.association import Association
from robosystems.models.extensions.element_trait import ElementTrait
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.taxonomy import Taxonomy
from robosystems.models.extensions.trait import Trait
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec,
  generate_report_facts,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd48"
PARENT = "ent_noncash_parent"
ADJUSTMENTS = "ent_noncash_adjustments"
JAN = PeriodSpec(start=date(2026, 1, 1), end=date(2026, 1, 31), label="Jan")
FEB = PeriodSpec(start=date(2026, 2, 1), end=date(2026, 2, 28), label="Feb")

RECONCILING = "rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet"
OPERATING = "rs-gaap:NetCashProvidedByUsedInOperatingActivities"
OTHER_NONCASH = "rs-gaap:OtherNoncashIncomeExpense"
CARD_ROW = "rs-gaap:IncreaseDecreaseInOtherCurrentLiabilities"
PAYABLES_ROW = "rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities"
REPAYMENTS = "rs-gaap:RepaymentsOfLongTermDebt"
CAPEX = "rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment"
PPE_PROCEEDS = "rs-gaap:ProceedsFromSaleOfPropertyPlantAndEquipment"
DISPOSAL_GAIN = "rs-gaap:GainLossOnDispositionOfAssets"
OTHER_INVESTMENTS = "rs-gaap:PaymentsToAcquireOtherInvestments"
NET_CHANGE = "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"

ACCOUNTS = {
  "cash": ("rs-gaap:CashAndCashEquivalentsAtCarryingValue", "debit", "instant"),
  "payable": ("rs-gaap:AccountsPayableCurrent", "credit", "instant"),
  "card": ("rs-gaap:OtherLiabilitiesCurrent", "credit", "instant"),
  "equipment": ("rs-gaap:PropertyPlantAndEquipmentGross", "debit", "instant"),
  "credit_line": ("rs-gaap:OtherLiabilitiesNoncurrent", "credit", "instant"),
  "metals": ("rs-gaap:OtherAssetsNoncurrent", "debit", "instant"),
  "clearing": ("rs-gaap:OtherAssetsCurrent", "debit", "instant"),
  "capital": ("rs-gaap:AdditionalPaidInCapital", "credit", "instant"),
  "revenue": (
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
    "credit",
    "duration",
  ),
  "interest": ("rs-gaap:InterestExpense", "debit", "duration"),
  "income_tax": ("rs-gaap:IncomeTaxExpenseBenefit", "debit", "duration"),
  "other_income": ("rs-gaap:OtherNonoperatingIncomeExpense", "credit", "duration"),
  "disposal_gain": (DISPOSAL_GAIN, "credit", "duration"),
  "accumulated_depreciation": (
    "rs-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment",
    "credit",
    "instant",
  ),
  "depreciation": ("rs-gaap:DepreciationDepletionAndAmortization", "debit", "duration"),
  "inventory": (
    "rs-gaap:InventoryNetOfAllowancesCustomerAdvancesAndProgressBillings",
    "debit",
    "instant",
  ),
  "cogs": ("rs-gaap:CostOfGoodsAndServicesSold", "debit", "duration"),
  "receivable": ("rs-gaap:ReceivablesNetCurrent", "debit", "instant"),
  "extinguishment_gain": (
    "rs-gaap:GainsLossesOnExtinguishmentOfDebt",
    "credit",
    "duration",
  ),
}

# A tenant-authored disclosure member the inventory account is also mapped to,
# as an inventory-components note maps it: an asset, as the statement concept
# is, with no working-capital row, and a qname that sorts before rs-gaap.
INVENTORY_MEMBER = "acme:InventoryRawMaterials"


@pytest.fixture(scope="module")
def tenant():
  url = env.EXTENSIONS_DATABASE_URL
  if not url:
    pytest.skip("EXTENSIONS_DATABASE_URL not configured")
  engine = create_engine(url)
  try:
    with engine.connect() as probe:
      probe.execute(text("SELECT 1"))
  except OperationalError as exc:
    engine.dispose()
    pytest.skip(f"extensions database unreachable: {exc.orig}")
  with engine.begin() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
  try:
    provision_tenant_schema(GRAPH)
    yield _seed()
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _seed() -> dict[str, str]:
  """A parent on the corporate Style and a chart mapped onto rs-gaap."""
  ids: dict[str, str] = {}
  with extensions_session(GRAPH) as session:
    session.add(
      Entity(
        id=PARENT,
        name="Noncash Corp",
        entity_type="corporation",
        is_parent=True,
        created_by="usr_1",
      )
    )
    session.add(
      Entity(
        id=ADJUSTMENTS,
        name="Consolidation Adjustments",
        entity_type="corporation",
        is_parent=False,
        parent_entity_id=PARENT,
        entity_role="consolidation",
        consolidation_method="none",
        created_by="usr_1",
      )
    )
    taxonomy = Taxonomy(name="chart", taxonomy_type="mapping")
    session.add(taxonomy)
    session.flush()
    mapping = Structure(
      name="CoA mapping", block_type="coa_mapping", taxonomy_id=taxonomy.id
    )
    session.add(mapping)
    session.flush()
    ids["mapping"] = mapping.id
    for key, (qname, balance, period) in ACCOUNTS.items():
      target = session.execute(
        select(Element.id).where(Element.qname == qname)
      ).scalar_one()
      account = Element(
        qname=f"coa:{key}",
        name=key,
        balance_type=balance,
        period_type=period,
        element_type="concept",
        is_abstract=False,
      )
      session.add(account)
      session.flush()
      session.add(
        Association(
          structure_id=mapping.id,
          from_element_id=account.id,
          to_element_id=target,
          association_type="mapping",
        )
      )
      ids[key] = account.id
    member = Element(
      qname=INVENTORY_MEMBER,
      name="Raw materials",
      balance_type="debit",
      period_type="instant",
      element_type="concept",
      is_abstract=False,
    )
    session.add(member)
    session.flush()
    asset = session.execute(
      select(Trait.id).where(
        Trait.category == "elementsOfFinancialStatements",
        Trait.identifier == "asset",
      )
    ).scalar_one()
    session.add(ElementTrait(element_id=member.id, trait_id=asset, is_primary=True))
    session.add(
      Association(
        structure_id=mapping.id,
        from_element_id=ids["inventory"],
        to_element_id=member.id,
        association_type="mapping",
      )
    )
    for qname in (REPAYMENTS, CAPEX, OTHER_INVESTMENTS):
      ids[qname] = str(
        session.execute(select(Element.id).where(Element.qname == qname)).scalar_one()
      )
  return ids


@pytest.fixture(autouse=True)
def empty_ledger(tenant):
  with extensions_session(GRAPH) as session:
    # A purge of the tenant's books, which the permanence triggers allow only
    # under the purge setting.
    session.execute(text("SET LOCAL robosystems.ledger_purge = 'on'"))
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
    session.execute(text("DELETE FROM events"))
  yield


def _event(event_type: str, discharges: str | None = None) -> str:
  with extensions_session(GRAPH) as session:
    event = Event(
      entity_id=PARENT,
      event_type=event_type,
      event_category="purchase",
      occurred_at=datetime(2026, 1, 1),
      source="test",
      created_by="test",
      discharges_event_id=discharges,
    )
    session.add(event)
    session.flush()
    return str(event.id)


def _post(
  ids, lines, posted=date(2026, 1, 15), event_id=None, entity_id=PARENT
) -> None:
  """``lines`` are ``(account, debit dollars, credit dollars)``, or with a
  fourth element, the flow qname the line is tagged with."""
  with extensions_session(GRAPH) as session:
    entry = Entry(
      entity_id=entity_id,
      type="standard",
      status="posted",
      posting_date=posted,
      memo="test",
      created_by="test",
      triggered_by_event_id=event_id,
    )
    session.add(entry)
    session.flush()
    for order, (key, debit, credit, *flow) in enumerate(lines, start=1):
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=ids[key],
          debit_amount=round(debit * 100),
          credit_amount=round(credit * 100),
          line_order=order,
          flow_element_id=ids[flow[0]] if flow else None,
        )
      )


def _cash_flow(ids, period=JAN, periods=None, entity_id=PARENT) -> dict[str, float]:
  with extensions_session(GRAPH) as session:
    facts = generate_report_facts(
      session, "", ids["mapping"], periods or [period], entity_id=entity_id
    ).facts
  out: dict[str, float] = {}
  for f in facts:
    if f.period_start == period.start and f.period_end == period.end:
      out[f.element_qname] = round(out.get(f.element_qname, 0.0) + f.value, 2)
  return out


def _assert_no_reconciling_adjustment(cf: dict[str, float]) -> None:
  assert cf.get(RECONCILING, 0.0) == 0.0, cf


def test_a_fair_value_gain_with_no_cash_comes_out_of_operating(tenant):
  _post(tenant, [("metals", 1000, 0), ("other_income", 0, 1000)])
  cf = _cash_flow(tenant)
  assert cf[OTHER_NONCASH] == 1000.0
  assert cf.get(OPERATING, 0.0) == 0.0
  _assert_no_reconciling_adjustment(cf)


def test_interest_added_to_a_debt_is_a_noncash_expense(tenant):
  _post(tenant, [("interest", 150, 0), ("credit_line", 0, 150)])
  cf = _cash_flow(tenant)
  assert cf[OTHER_NONCASH] == -150.0
  assert cf.get(OPERATING, 0.0) == 0.0
  _assert_no_reconciling_adjustment(cf)


def test_capex_on_the_card_stays_out_of_investing(tenant):
  """Decision 1: bought on the card is non-cash; paying the card is
  operating, on the card's own row (G0), not the reconciling adjustment."""
  _post(tenant, [("equipment", 500, 0), ("card", 0, 500)])
  _post(tenant, [("card", 500, 0), ("cash", 0, 500)], posted=date(2026, 1, 20))
  cf = _cash_flow(tenant)
  assert cf[CARD_ROW] == -500.0
  assert CAPEX not in cf
  assert cf[NET_CHANGE] == -500.0
  _assert_no_reconciling_adjustment(cf)


def _bill_and_payment(ids, paid: float, paid_on=date(2026, 1, 20), link=True):
  bill = _event("bill_received")
  _post(
    ids,
    [("credit_line", 900, 0), ("interest", 100, 0), ("payable", 0, 1000)],
    event_id=bill,
  )
  payment = _event("bill_paid", discharges=bill if link else None)
  _post(
    ids, [("payable", paid, 0), ("cash", 0, paid)], posted=paid_on, event_id=payment
  )


def test_a_bill_paid_in_the_period_splits_principal_from_interest(tenant):
  _bill_and_payment(tenant, 1000)
  cf = _cash_flow(tenant)
  assert cf[REPAYMENTS] == -900.0
  assert cf[OPERATING] == -100.0
  assert cf.get(PAYABLES_ROW, 0.0) == 0.0
  _assert_no_reconciling_adjustment(cf)


def test_a_bill_paid_the_next_period_moves_cash_when_it_is_paid(tenant):
  _bill_and_payment(tenant, 1000, paid_on=date(2026, 2, 10))
  jan = _cash_flow(tenant, JAN, [JAN, FEB])
  feb = _cash_flow(tenant, FEB, [JAN, FEB])
  assert REPAYMENTS not in jan
  assert jan.get(OPERATING, 0.0) == 0.0
  assert feb[REPAYMENTS] == -900.0
  assert feb[OPERATING] == -100.0
  _assert_no_reconciling_adjustment(jan)
  _assert_no_reconciling_adjustment(feb)


def test_a_partial_payment_allocates_pro_rata(tenant):
  _bill_and_payment(tenant, 500)
  cf = _cash_flow(tenant)
  assert cf[REPAYMENTS] == -450.0
  assert cf[NET_CHANGE] == -500.0
  _assert_no_reconciling_adjustment(cf)


def test_an_unlinked_payment_stays_in_operating(tenant):
  _bill_and_payment(tenant, 1000, link=False)
  cf = _cash_flow(tenant)
  assert REPAYMENTS not in cf
  assert cf[OPERATING] == -1000.0
  _assert_no_reconciling_adjustment(cf)


def test_a_tagged_payment_keeps_its_tag(tenant):
  bill = _event("bill_received")
  _post(
    tenant,
    [("equipment", 1000, 0), ("payable", 0, 1000)],
    event_id=bill,
  )
  payment = _event("bill_paid", discharges=bill)
  _post(
    tenant,
    [("payable", 1000, 0, REPAYMENTS), ("cash", 0, 1000, REPAYMENTS)],
    posted=date(2026, 1, 20),
    event_id=payment,
  )
  cf = _cash_flow(tenant)
  assert cf[REPAYMENTS] == -1000.0
  assert CAPEX not in cf
  _assert_no_reconciling_adjustment(cf)


def test_a_cash_sale_at_a_gain_puts_all_the_cash_in_investing(tenant):
  _post(tenant, [("equipment", 1000, 0), ("cash", 0, 1000)], posted=date(2026, 1, 5))
  _post(
    tenant,
    [("cash", 1200, 0), ("equipment", 0, 1000), ("disposal_gain", 0, 200)],
  )
  cf = _cash_flow(tenant)
  assert cf[PPE_PROCEEDS] == 1200.0
  assert cf[DISPOSAL_GAIN] == 200.0
  assert cf.get(OPERATING, 0.0) == 0.0
  _assert_no_reconciling_adjustment(cf)


@pytest.mark.parametrize(
  "proceeds, result",
  [(500, ("disposal_gain", 0, 300)), (200, None), (100, ("disposal_gain", 100, 0))],
  ids=["at_a_gain", "at_book_value", "at_a_loss"],
)
def test_a_depreciated_asset_sold_for_cash_shows_the_cash_received(
  tenant, proceeds, result
):
  """Cost 1,000 with 800 depreciated: the sale writes the depreciation off
  with the cost, so the proceeds are the cash, not the cost."""
  _post(tenant, [("equipment", 1000, 0), ("cash", 0, 1000)], posted=date(2026, 1, 5))
  _post(
    tenant,
    [("depreciation", 800, 0), ("accumulated_depreciation", 0, 800)],
    posted=date(2026, 1, 6),
  )
  _post(
    tenant,
    [
      ("cash", proceeds, 0),
      ("accumulated_depreciation", 800, 0),
      ("equipment", 0, 1000),
      *([result] if result else []),
    ],
  )
  cf = _cash_flow(tenant)
  assert cf[PPE_PROCEEDS] == float(proceeds)
  assert cf[NET_CHANGE] == float(proceeds - 1000)
  _assert_no_reconciling_adjustment(cf)


@pytest.mark.parametrize(
  "proceeds, result",
  [(900, ("disposal_gain", 0, 300)), (500, ("disposal_gain", 100, 0))],
  ids=["at_a_gain", "at_a_loss"],
)
@pytest.mark.parametrize("collected", [date(2026, 2, 10), date(2026, 1, 25)])
def test_an_asset_sold_on_account_shows_the_cash_when_collected(
  tenant, proceeds, result, collected
):
  """Cost 1,000 with 400 depreciated, sold on account and collected later
  (or in the same month): the receivable is the proceeds, never working
  capital, and the collection is investing cash of exactly what came in."""
  _post(tenant, [("equipment", 1000, 0), ("cash", 0, 1000)], posted=date(2026, 1, 5))
  _post(
    tenant,
    [("depreciation", 400, 0), ("accumulated_depreciation", 0, 400)],
    posted=date(2026, 1, 6),
  )
  invoice = _event("invoice_issued")
  _post(
    tenant,
    [
      ("receivable", proceeds, 0),
      ("accumulated_depreciation", 400, 0),
      ("equipment", 0, 1000),
      result,
    ],
    event_id=invoice,
  )
  receipt = _event("payment_received", discharges=invoice)
  _post(
    tenant,
    [("cash", proceeds, 0), ("receivable", 0, proceeds)],
    posted=collected,
    event_id=receipt,
  )
  jan = _cash_flow(tenant, JAN, [JAN, FEB])
  feb = _cash_flow(tenant, FEB, [JAN, FEB])
  paid_in = feb if collected.month == 2 else jan

  assert paid_in[PPE_PROCEEDS] == float(proceeds)
  assert jan.get(OPERATING, 0.0) == 0.0
  assert feb.get(OPERATING, 0.0) == 0.0
  if collected.month == 2:
    assert PPE_PROCEEDS not in jan
  _assert_no_reconciling_adjustment(jan)
  _assert_no_reconciling_adjustment(feb)


def test_depreciation_charged_on_the_sale_invoice_is_no_write_off(tenant):
  """The invoice also books the month's last depreciation, to an expense the
  cash flow does not present on its own row: that credit to accumulated
  depreciation is a charge, not part of the disposal, so the collection is
  still exactly the cash received."""
  _post(tenant, [("equipment", 1000, 0), ("cash", 0, 1000)], posted=date(2026, 1, 5))
  _post(
    tenant,
    [("depreciation", 400, 0), ("accumulated_depreciation", 0, 400)],
    posted=date(2026, 1, 6),
  )
  invoice = _event("invoice_issued")
  _post(
    tenant,
    [
      ("receivable", 500, 0),
      ("cogs", 50, 0),
      ("accumulated_depreciation", 0, 50),
      ("accumulated_depreciation", 450, 0),
      ("equipment", 0, 1000),
      ("disposal_gain", 50, 0),
    ],
    event_id=invoice,
  )
  receipt = _event("payment_received", discharges=invoice)
  _post(
    tenant,
    [("cash", 500, 0), ("receivable", 0, 500)],
    posted=date(2026, 2, 10),
    event_id=receipt,
  )
  feb = _cash_flow(tenant, FEB, [JAN, FEB])
  assert feb[PPE_PROCEEDS] == 500.0
  assert feb.get(OPERATING, 0.0) == 0.0


def test_a_linked_payment_through_clearing_still_routes_the_bill(tenant):
  """The cash leg lands in clearing, on none of the bill's accounts: the
  payment is still measured against the whole bill, so the equipment it
  pays for is investing cash."""
  bill = _event("bill_received")
  _post(tenant, [("equipment", 1000, 0), ("payable", 0, 1000)], event_id=bill)
  payment = _event("bill_paid", discharges=bill)
  _post(
    tenant,
    [("clearing", 1000, 0), ("cash", 0, 1000)],
    posted=date(2026, 1, 20),
    event_id=payment,
  )
  cf = _cash_flow(tenant)
  assert cf[CAPEX] == -1000.0
  _assert_no_reconciling_adjustment(cf)


def test_a_debt_retired_through_a_payable_at_a_gain_shows_the_cash_paid(tenant):
  _post(tenant, [("cash", 1000, 0), ("credit_line", 0, 1000)], posted=date(2026, 1, 2))
  bill = _event("bill_received")
  _post(
    tenant,
    [("credit_line", 1000, 0), ("payable", 0, 800), ("extinguishment_gain", 0, 200)],
    event_id=bill,
  )
  payment = _event("bill_paid", discharges=bill)
  _post(
    tenant,
    [("payable", 800, 0), ("cash", 0, 800)],
    posted=date(2026, 2, 10),
    event_id=payment,
  )
  jan = _cash_flow(tenant, JAN, [JAN, FEB])
  feb = _cash_flow(tenant, FEB, [JAN, FEB])

  assert feb[REPAYMENTS] == -800.0
  assert feb.get(OPERATING, 0.0) == 0.0
  _assert_no_reconciling_adjustment(jan)
  _assert_no_reconciling_adjustment(feb)


def test_the_consolidation_entitys_cash_flow_nets_to_nothing(tenant):
  """It holds no cash, so a tagged line with nothing offsetting it (a hand
  entry tagging one side only) cannot move the group's net change in cash:
  the difference is its reconciling adjustment."""
  _post(
    tenant,
    [("metals", 100, 0), ("credit_line", 0, 100, REPAYMENTS)],
    entity_id=ADJUSTMENTS,
  )
  cf = _cash_flow(tenant, entity_id=ADJUSTMENTS)
  assert cf[REPAYMENTS] == 100.0, cf
  assert cf.get(NET_CHANGE, 0.0) == 0.0, cf
  assert cf[RECONCILING] == -100.0, cf


def test_an_owner_paying_the_card_is_not_operating_cash(tenant):
  _post(tenant, [("card", 300, 0), ("capital", 0, 300)])
  _post(tenant, [("revenue", 0, 300), ("cash", 300, 0)])
  cf = _cash_flow(tenant)
  assert cf.get(CARD_ROW, 0.0) == 0.0
  assert cf[OPERATING] == 300.0
  _assert_no_reconciling_adjustment(cf)


def test_a_subtracted_row_shows_its_cash_effect(tenant):
  """The gain's fact keeps its income statement sign; its cash flow row
  shows the cash effect, so the operating section foots as a plain sum."""
  from robosystems.operations.roboledger.reports.fact_grid import (
    render_structure_view,
  )
  from robosystems.operations.roboledger.reports.guard_rails import validate_report
  from robosystems.operations.roboledger.reports.network_picker import (
    load_entity_reporting_style,
  )

  _post(tenant, [("equipment", 1000, 0), ("cash", 0, 1000)], posted=date(2026, 1, 5))
  _post(
    tenant,
    [("cash", 1200, 0), ("equipment", 0, 1000), ("disposal_gain", 0, 200)],
  )
  _post(tenant, [("cash", 500, 0), ("revenue", 0, 500)])
  with extensions_session(GRAPH) as session:
    facts = generate_report_facts(
      session, "", tenant["mapping"], [JAN], entity_id=PARENT
    ).facts
    grid = render_structure_view(
      session,
      facts,
      "cash_flow_statement",
      [JAN],
      load_entity_reporting_style(session, PARENT),
    )
  rows = {r.element_qname: r for r in grid.rows}
  assert rows[DISPOSAL_GAIN].values == [-200.0]
  assert rows["rs-gaap:NetIncomeLoss"].values == [700.0]
  assert rows[OPERATING].values == [500.0]
  result = validate_report("cash_flow_statement", grid.rows)
  assert not [w for w in result.warnings if "does not match" in w], result.warnings


CAPEX_UNPAID = "rs-gaap:CapitalExpendituresIncurredButNotYetPaid"
ACQUIRED = "rs-gaap:NoncashOrPartNoncashAcquisitionValueOfAssetsAcquired1"


def _warnings(ids, periods) -> list[str]:
  with extensions_session(GRAPH) as session:
    return generate_report_facts(
      session, "", ids["mapping"], periods, entity_id=PARENT
    ).warnings


def test_capex_on_the_card_is_a_noncash_acquisition(tenant):
  _post(tenant, [("equipment", 500, 0), ("card", 0, 500)])
  cf = _cash_flow(tenant)
  assert cf[ACQUIRED] == 500.0
  assert CAPEX_UNPAID not in cf


def test_an_asset_taken_in_exchange_is_a_noncash_acquisition(tenant):
  _post(tenant, [("equipment", 600, 0), ("metals", 0, 400), ("disposal_gain", 0, 200)])
  assert _cash_flow(tenant)[ACQUIRED] == 600.0


def test_a_remeasurement_acquires_nothing(tenant):
  _post(tenant, [("metals", 1000, 0), ("other_income", 0, 1000)])
  assert ACQUIRED not in _cash_flow(tenant)


def _equipment_bill(ids, paid_on: date | None) -> None:
  bill = _event("bill_received")
  _post(ids, [("equipment", 1000, 0), ("payable", 0, 1000)], event_id=bill)
  if paid_on is not None:
    payment = _event("bill_paid", discharges=bill)
    _post(
      ids, [("payable", 1000, 0), ("cash", 0, 1000)], posted=paid_on, event_id=payment
    )


def test_capex_on_a_bill_unpaid_at_period_end_is_disclosed(tenant):
  _equipment_bill(tenant, paid_on=date(2026, 2, 10))
  jan = _cash_flow(tenant, JAN, [JAN, FEB])
  feb = _cash_flow(tenant, FEB, [JAN, FEB])
  assert jan[CAPEX_UNPAID] == 1000.0
  assert CAPEX not in jan
  assert feb[CAPEX] == -1000.0
  assert CAPEX_UNPAID not in feb
  _assert_no_reconciling_adjustment(jan)
  _assert_no_reconciling_adjustment(feb)


def test_capex_on_a_bill_paid_in_the_period_is_cash_capex(tenant):
  _equipment_bill(tenant, paid_on=date(2026, 1, 20))
  cf = _cash_flow(tenant)
  assert cf[CAPEX] == -1000.0
  assert CAPEX_UNPAID not in cf
  assert ACQUIRED not in cf


def test_an_unlinked_payment_against_an_open_capex_bill_is_named(tenant):
  _bill_and_payment(tenant, 1000, link=False)
  (warning,) = [w for w in _warnings(tenant, [JAN]) if "no link" in w]
  assert "1 payment(s) to payables (1,000.00)" in warning
  assert "(1,000.00) are unpaid" in warning


def test_linked_payments_raise_no_warning(tenant):
  _bill_and_payment(tenant, 1000)
  assert not [w for w in _warnings(tenant, [JAN]) if "no link" in w]


def test_the_supplemental_section_renders_outside_the_net_change(tenant):
  from robosystems.operations.roboledger.reports.fact_grid import (
    render_structure_view,
  )
  from robosystems.operations.roboledger.reports.guard_rails import validate_report
  from robosystems.operations.roboledger.reports.network_picker import (
    load_entity_reporting_style,
  )

  _post(tenant, [("equipment", 500, 0), ("card", 0, 500)])
  _post(tenant, [("cash", 800, 0), ("revenue", 0, 800)])
  with extensions_session(GRAPH) as session:
    facts = generate_report_facts(
      session, "", tenant["mapping"], [JAN], entity_id=PARENT
    ).facts
    grid = render_structure_view(
      session,
      facts,
      "cash_flow_statement",
      [JAN],
      load_entity_reporting_style(session, PARENT),
    )
  rows = {r.element_qname: r for r in grid.rows}
  assert rows[ACQUIRED].values == [500.0]
  assert rows[NET_CHANGE].values == [800.0]
  result = validate_report("cash_flow_statement", grid.rows)
  assert result.status == "passed", (result.failures, result.warnings)
  assert not [w for w in result.warnings if "does not match" in w]


def test_a_bill_posted_as_two_entries_is_one_bill(tenant):
  bill = _event("bill_received")
  _post(tenant, [("equipment", 600, 0), ("payable", 0, 600)], event_id=bill)
  _post(tenant, [("equipment", 400, 0), ("payable", 0, 400)], event_id=bill)
  assert _cash_flow(tenant)[CAPEX_UNPAID] == 1000.0


def test_an_unlinked_payment_against_a_bill_from_before_the_window_is_named(tenant):
  bill = _event("bill_received")
  _post(
    tenant,
    [("credit_line", 900, 0), ("payable", 0, 900)],
    posted=date(2025, 12, 15),
    event_id=bill,
  )
  _post(tenant, [("payable", 900, 0), ("cash", 0, 900)])
  (warning,) = [w for w in _warnings(tenant, [JAN]) if "no link" in w]
  assert "(900.00) are unpaid" in warning


def test_a_mixed_entry_keeps_the_cash_flow_footed(tenant):
  """An entry pairing several unrelated sides is split by sign and size,
  which can blur which row an amount lands on; operating cash is still the
  cash that moved, with nothing left to reconcile."""
  _post(
    tenant,
    [
      ("metals", 1000, 0),
      ("other_income", 0, 1000),
      ("interest", 300, 0),
      ("card", 0, 300),
    ],
  )
  cf = _cash_flow(tenant)
  assert cf.get(OPERATING, 0.0) == 0.0
  _assert_no_reconciling_adjustment(cf)


def test_a_tagged_payment_of_a_capex_bill_is_counted_once(tenant):
  """The Harbinger gold purchase: bought on a bill, the payment's lines
  tagged by hand. The bill's payable is backed out as a non-cash acquisition;
  the tagged payment's payable must come out too, or the cash is in
  operating and investing both."""
  bill = _event("bill_received")
  _post(tenant, [("metals", 11000, 0), ("payable", 0, 11000)], event_id=bill)
  payment = _event("bill_paid", discharges=bill)
  _post(
    tenant,
    [
      ("payable", 11000, 0, OTHER_INVESTMENTS),
      ("cash", 0, 11000, OTHER_INVESTMENTS),
    ],
    posted=date(2026, 1, 29),
    event_id=payment,
  )
  cf = _cash_flow(tenant)
  assert cf[OTHER_INVESTMENTS] == -11000.0
  assert cf.get(OPERATING, 0.0) == 0.0
  assert cf.get(PAYABLES_ROW, 0.0) == 0.0
  _assert_no_reconciling_adjustment(cf)


def test_a_tagged_payment_of_an_expense_bill_moves_the_cash_to_its_flow(tenant):
  """A person tags a payable's payment as investing: the tag wins, and the
  cash is in investing once."""
  _post(tenant, [("interest", 500, 0), ("payable", 0, 500)])
  _post(
    tenant,
    [("payable", 500, 0, CAPEX), ("cash", 0, 500, CAPEX)],
    posted=date(2026, 1, 20),
  )
  cf = _cash_flow(tenant)
  assert cf[CAPEX] == -500.0
  assert cf[NET_CHANGE] == -500.0
  _assert_no_reconciling_adjustment(cf)


def test_a_bill_settled_through_clearing_is_a_noncash_acquisition(tenant):
  """The Harbinger platinum: silver sold into a clearing account, platinum
  bought on a bill, the bill settled from clearing. No cash moves; the
  platinum is acquired in kind, not capex awaiting payment."""
  _post(tenant, [("clearing", 6000, 0), ("metals", 0, 6000)], posted=date(2026, 1, 10))
  bill = _event("bill_received")
  _post(tenant, [("metals", 6000, 0), ("payable", 0, 6000)], event_id=bill)
  payment = _event("bill_paid", discharges=bill)
  _post(
    tenant,
    [("payable", 6000, 0), ("clearing", 0, 6000)],
    posted=date(2026, 1, 29),
    event_id=payment,
  )
  cf = _cash_flow(tenant)
  assert CAPEX_UNPAID not in cf
  assert cf[ACQUIRED] == 6000.0
  assert cf.get(OPERATING, 0.0) == 0.0
  _assert_no_reconciling_adjustment(cf)


INTEREST_PAID = "rs-gaap:InterestPaidNet"
TAXES_PAID = "rs-gaap:IncomeTaxesPaidNet"


def test_interest_paid_in_cash_is_disclosed(tenant):
  _post(tenant, [("interest", 120, 0), ("cash", 0, 120)])
  assert _cash_flow(tenant)[INTEREST_PAID] == 120.0


def test_a_settled_bill_pays_its_interest_share(tenant):
  """The bill splits 900 principal from 100 interest; the payment settles
  half, so half the interest is paid."""
  _bill_and_payment(tenant, 500)
  assert _cash_flow(tenant)[INTEREST_PAID] == 50.0


def test_interest_added_to_a_debt_is_not_paid(tenant):
  _post(tenant, [("interest", 150, 0), ("credit_line", 0, 150)])
  assert INTEREST_PAID not in _cash_flow(tenant)


def test_income_tax_paid_in_cash_is_disclosed(tenant):
  _post(tenant, [("income_tax", 300, 0), ("cash", 0, 300)])
  assert _cash_flow(tenant)[TAXES_PAID] == 300.0


def test_interest_settled_partly_in_cash_is_paid_at_the_cash_share(tenant):
  """150 of interest: 100 paid, 50 added to the debt."""
  _post(tenant, [("interest", 150, 0), ("cash", 0, 100), ("credit_line", 0, 50)])
  assert _cash_flow(tenant)[INTEREST_PAID] == 100.0


def test_a_multi_mapped_account_counts_once(tenant):
  """An inventory account also mapped to a disclosure member: cost of goods
  sold against it moves no cash and is no non-cash item. Each line is read
  through the account's primary target, as the close reads it, not once per
  target."""
  _post(tenant, [("cogs", 364, 0), ("inventory", 0, 364)])
  cf = _cash_flow(tenant)
  assert cf.get(OTHER_NONCASH, 0.0) == 0.0, cf
  assert cf["rs-gaap:IncreaseDecreaseInInventories"] == 364.0
  _assert_no_reconciling_adjustment(cf)


def test_the_exported_cash_flow_negates_what_its_summation_subtracts(tenant):
  """On the library's own networks: the indirect cash flow's operating
  summation is not legal XBRL and does not ship, yet the rows it subtracts
  (the gain the in-app grid shows at its cash effect) are still named, so
  the export can negate them as the grid does."""
  from robosystems.models.extensions.association import Association
  from robosystems.operations.graph.reporting_style_defaults import (
    default_style_for,
  )
  from robosystems.operations.roboledger.reports.network_picker import (
    get_render_network,
  )
  from robosystems.operations.serialization.bundle import _source_calculation_arcs

  with extensions_session(GRAPH) as session:
    network = get_render_network(
      session, default_style_for("corporation"), "cash_flow_statement"
    )
    arcs = list(
      session.execute(
        select(Association).where(
          Association.structure_id == network.structure_id,
          Association.association_type == "presentation",
        )
      ).scalars()
    )
    ids = {str(a.from_element_id) for a in arcs} | {str(a.to_element_id) for a in arcs}
    elements = {
      str(e.id): e
      for e in session.execute(select(Element).where(Element.id.in_(ids))).scalars()
    }
    structures = {
      network.structure_id: SimpleNamespace(block_type="cash_flow_statement")
    }
    subtracted: set[tuple[str, str]] = set()
    sourced = _source_calculation_arcs(session, arcs, structures, elements, subtracted)
    operating = next(i for i, e in elements.items() if e.qname == OPERATING)

  assert (OPERATING, DISPOSAL_GAIN) in subtracted
  assert not any(str(a.from_element_id) == operating for a in sourced)
