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

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.association import Association
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.taxonomy import Taxonomy
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec,
  generate_report_facts,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd48"
PARENT = "ent_noncash_parent"
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
NET_CHANGE = "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"

ACCOUNTS = {
  "cash": ("rs-gaap:CashAndCashEquivalentsAtCarryingValue", "debit", "instant"),
  "payable": ("rs-gaap:AccountsPayableCurrent", "credit", "instant"),
  "card": ("rs-gaap:OtherLiabilitiesCurrent", "credit", "instant"),
  "equipment": ("rs-gaap:PropertyPlantAndEquipmentGross", "debit", "instant"),
  "credit_line": ("rs-gaap:OtherLiabilitiesNoncurrent", "credit", "instant"),
  "metals": ("rs-gaap:OtherAssetsNoncurrent", "debit", "instant"),
  "capital": ("rs-gaap:AdditionalPaidInCapital", "credit", "instant"),
  "revenue": (
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
    "credit",
    "duration",
  ),
  "interest": ("rs-gaap:InterestExpense", "debit", "duration"),
  "other_income": ("rs-gaap:OtherNonoperatingIncomeExpense", "credit", "duration"),
  "disposal_gain": (DISPOSAL_GAIN, "credit", "duration"),
}


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
    for qname in (REPAYMENTS, CAPEX):
      ids[qname] = str(
        session.execute(select(Element.id).where(Element.qname == qname)).scalar_one()
      )
  return ids


@pytest.fixture(autouse=True)
def empty_ledger(tenant):
  with extensions_session(GRAPH) as session:
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


def _post(ids, lines, posted=date(2026, 1, 15), event_id=None) -> None:
  """``lines`` are ``(account, debit dollars, credit dollars)``, or with a
  fourth element, the flow qname the line is tagged with."""
  with extensions_session(GRAPH) as session:
    entry = Entry(
      entity_id=PARENT,
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


def _cash_flow(ids, period=JAN, periods=None) -> dict[str, float]:
  with extensions_session(GRAPH) as session:
    facts = generate_report_facts(
      session, "", ids["mapping"], periods or [period], entity_id=PARENT
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
