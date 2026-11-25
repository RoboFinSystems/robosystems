"""The cash flow event list: each kind of cash event, run through the engine.

A cash flow row is picked from the account a line hits and the direction it
moves, or from the flow a line is tagged with. Each case posts one ordinary
event on a provisioned tenant and checks the section the cash lands in and
the row that carries it, against where ASC 230 puts it, and that nothing
reaches the statement through the reconciling plug. Section subtotals are
cash effects, so these assertions hold whatever sign a working-capital row
uses.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session
from robosystems.models.extensions import Element, Entity
from robosystems.models.extensions.association import Association
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.taxonomy import Taxonomy
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec,
  generate_report_facts,
)
from tests.tenant_schema import claim_provisioned_schema, release_tenant_schema

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd91"
PARENT = "ent_event_list"
JAN = PeriodSpec(start=date(2026, 1, 1), end=date(2026, 1, 31), label="Jan")

OPERATING = "rs-gaap:NetCashProvidedByUsedInOperatingActivities"
PLUG = "rs-gaap:IncreaseDecreaseInOtherOperatingCapitalNet"
INVESTING = "rs-gaap:NetCashProvidedByUsedInInvestingActivities"
FINANCING = "rs-gaap:NetCashProvidedByUsedInFinancingActivities"

# The chart: each account mapped onto the rs-gaap concept a small company's
# chart maps it to. (qname, balance, period type)
ACCOUNTS = {
  "cash": ("rs-gaap:CashAndCashEquivalentsAtCarryingValue", "debit", "instant"),
  "receivable": ("rs-gaap:ReceivablesNetCurrent", "debit", "instant"),
  "payable": ("rs-gaap:AccountsPayableCurrent", "credit", "instant"),
  "equipment": ("rs-gaap:PropertyPlantAndEquipmentGross", "debit", "instant"),
  "software": ("rs-gaap:IntangibleAssetsNetExcludingGoodwill", "debit", "instant"),
  "treasuries": ("rs-gaap:ShortTermInvestments", "debit", "instant"),
  "note_receivable": (
    "rs-gaap:LongTermInvestmentsAndReceivablesNet",
    "debit",
    "instant",
  ),
  "deposit_paid": ("rs-gaap:OtherAssetsNoncurrent", "debit", "instant"),
  "goodwill": ("rs-gaap:Goodwill", "debit", "instant"),
  "line_of_credit": ("rs-gaap:DebtCurrent", "credit", "instant"),
  "term_loan": ("rs-gaap:LongTermDebtAndCapitalLeaseObligations", "credit", "instant"),
  "customer_deposit": ("rs-gaap:OtherLiabilitiesNoncurrent", "credit", "instant"),
  "related_party_note": ("rs-gaap:OtherLiabilitiesNoncurrent", "credit", "instant"),
  "gold": ("rs-gaap:OtherAssetsNoncurrent", "debit", "instant"),
  "shareholder_loan": ("rs-gaap:DueToAffiliateCurrent", "credit", "instant"),
  "finance_lease": ("rs-gaap:FinanceLeaseLiabilityCurrent", "credit", "instant"),
  "stock": ("rs-gaap:AdditionalPaidInCapital", "credit", "instant"),
  "retained": ("rs-gaap:RetainedEarningsAccumulatedDeficit", "credit", "instant"),
  "revenue": (
    "rs-gaap:RevenueFromContractWithCustomerExcludingAssessedTax",
    "credit",
    "duration",
  ),
  "insurance_gain": ("rs-gaap:GainLossOnDispositionOfAssets", "credit", "duration"),
}


@dataclass(frozen=True)
class Case:
  """One event: its lines (account, debit, credit) and where its cash goes."""

  lines: tuple[tuple[str, float, float], ...]
  section: str
  cash: float
  row: str
  basis: str
  flow: str | None = None  # the flow every line is tagged with


CASES: dict[str, Case] = {
  # -- operating: the controls -----------------------------------------------
  "a customer pays an invoice": Case(
    (("cash", 500, 0), ("receivable", 0, 500)),
    OPERATING,
    500,
    "rs-gaap:IncreaseDecreaseInAccountsReceivable",
    "ASC 230-10-45-16(a): cash from customers",
  ),
  "a supplier is paid": Case(
    (("payable", 300, 0), ("cash", 0, 300)),
    OPERATING,
    -300,
    "rs-gaap:IncreaseDecreaseInAccountsPayableAndAccruedLiabilities",
    "ASC 230-10-45-17(b): cash to suppliers",
  ),
  # -- investing ---------------------------------------------------------------
  "equipment bought for cash": Case(
    (("equipment", 1000, 0), ("cash", 0, 1000)),
    INVESTING,
    -1000,
    "rs-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
    "ASC 230-10-45-13(c)",
  ),
  "software bought for cash": Case(
    (("software", 400, 0), ("cash", 0, 400)),
    INVESTING,
    -400,
    "rs-gaap:PaymentsToAcquireIntangibleAssets",
    "ASC 230-10-45-13(c): productive assets",
  ),
  "treasury bills bought": Case(
    (("treasuries", 2000, 0), ("cash", 0, 2000)),
    INVESTING,
    -2000,
    "rs-gaap:PaymentsToAcquireShortTermInvestments",
    "ASC 230-10-45-13(b)",
  ),
  "treasury bills mature": Case(
    (("cash", 2000, 0), ("treasuries", 0, 2000)),
    INVESTING,
    2000,
    "rs-gaap:ProceedsFromSaleOfShortTermInvestments",
    "ASC 230-10-45-12(b)",
  ),
  "a loan made to another company, untagged": Case(
    (("note_receivable", 800, 0), ("cash", 0, 800)),
    INVESTING,
    -800,
    "rs-gaap:PaymentsToAcquireInvestments",
    "ASC 230-10-45-13(a): the account holds investments and loans alike",
  ),
  "a loan made to another company": Case(
    (("note_receivable", 800, 0), ("cash", 0, 800)),
    INVESTING,
    -800,
    "rs-gaap:PaymentsToAcquireNotesReceivable",
    "ASC 230-10-45-13(a): loans made",
    flow="rs-gaap:PaymentsToAcquireNotesReceivable",
  ),
  "a loan made is collected": Case(
    (("cash", 800, 0), ("note_receivable", 0, 800)),
    INVESTING,
    800,
    "rs-gaap:ProceedsFromCollectionOfNotesReceivable",
    "ASC 230-10-45-12(a): collections of loans",
    flow="rs-gaap:ProceedsFromCollectionOfNotesReceivable",
  ),
  "a business bought for cash": Case(
    (("goodwill", 5000, 0), ("cash", 0, 5000)),
    INVESTING,
    -5000,
    "rs-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired",
    "ASC 230-10-45-13(c)",
  ),
  "a security deposit paid on a lease": Case(
    (("deposit_paid", 600, 0), ("cash", 0, 600)),
    OPERATING,
    -600,
    "rs-gaap:IncreaseDecreaseInOtherNoncurrentAssets",
    "a deposit securing an operating lease is part of operating; not an investment",
  ),
  "a security deposit refunded": Case(
    (("cash", 600, 0), ("deposit_paid", 0, 600)),
    OPERATING,
    600,
    "rs-gaap:IncreaseDecreaseInOtherNoncurrentAssets",
    "the refund of the same deposit",
  ),
  "cash for equipment already written off, booked to the gain": Case(
    (("cash", 700, 0), ("insurance_gain", 0, 700)),
    INVESTING,
    700,
    "rs-gaap:ProceedsFromSaleOfPropertyPlantAndEquipment",
    "ASC 230-10-45-12(c): the proceeds of the disposal the gain records",
  ),
  "an insurance payout for damaged equipment": Case(
    (("cash", 700, 0), ("insurance_gain", 0, 700)),
    INVESTING,
    700,
    "rs-gaap:ProceedsFromInsuranceSettlementInvestingActivities",
    "ASC 230-10-45-21B: classified by the nature of the loss",
    flow="rs-gaap:ProceedsFromInsuranceSettlementInvestingActivities",
  ),
  "an investment booked to other noncurrent assets": Case(
    (("gold", 900, 0), ("cash", 0, 900)),
    INVESTING,
    -900,
    "rs-gaap:PaymentsToAcquireOtherInvestments",
    "ASC 230-10-45-13(b): tagged, it leaves operating",
    flow="rs-gaap:PaymentsToAcquireOtherInvestments",
  ),
  # -- financing ---------------------------------------------------------------
  "a draw on the line of credit": Case(
    (("cash", 1500, 0), ("line_of_credit", 0, 1500)),
    FINANCING,
    1500,
    "rs-gaap:ProceedsFromShortTermDebt",
    "ASC 230-10-45-14(b)",
  ),
  "a term loan taken": Case(
    (("cash", 10000, 0), ("term_loan", 0, 10000)),
    FINANCING,
    10000,
    "rs-gaap:ProceedsFromIssuanceOfLongTermDebt",
    "ASC 230-10-45-14(b)",
  ),
  "a term loan repaid": Case(
    (("term_loan", 1000, 0), ("cash", 0, 1000)),
    FINANCING,
    -1000,
    "rs-gaap:RepaymentsOfLongTermDebt",
    "ASC 230-10-45-15(b)",
  ),
  "a finance lease payment of principal": Case(
    (("finance_lease", 250, 0), ("cash", 0, 250)),
    FINANCING,
    -250,
    "rs-gaap:FinanceLeasePrincipalPayments",
    "ASC 842-20-45-5(a)",
  ),
  "shares issued for cash": Case(
    (("cash", 20000, 0), ("stock", 0, 20000)),
    FINANCING,
    20000,
    "rs-gaap:ProceedsFromIssuanceOfCommonStock",
    "ASC 230-10-45-14(a)",
  ),
  "a dividend paid": Case(
    (("retained", 900, 0), ("cash", 0, 900)),
    FINANCING,
    -900,
    "rs-gaap:PaymentsOfDividends",
    "ASC 230-10-45-15(a)",
  ),
  "a loan from a shareholder": Case(
    (("cash", 3000, 0), ("shareholder_loan", 0, 3000)),
    FINANCING,
    3000,
    "rs-gaap:ProceedsFromRelatedPartyDebt",
    "ASC 230-10-45-14(b): borrowing, whoever lends",
    flow="rs-gaap:ProceedsFromRelatedPartyDebt",
  ),
  "a customer deposit received ahead of the work": Case(
    (("cash", 1200, 0), ("customer_deposit", 0, 1200)),
    OPERATING,
    1200,
    "rs-gaap:IncreaseDecreaseInOtherNoncurrentLiabilities",
    "ASC 230-10-45-16(a): cash from customers; not debt",
  ),
  "a borrowing booked to other noncurrent liabilities": Case(
    (("cash", 5000, 0), ("related_party_note", 0, 5000)),
    FINANCING,
    5000,
    "rs-gaap:ProceedsFromIssuanceOfLongTermDebt",
    "ASC 230-10-45-14(b): tagged, it leaves operating",
    flow="rs-gaap:ProceedsFromIssuanceOfLongTermDebt",
  ),
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
  try:
    claim_provisioned_schema(GRAPH)
    yield _seed()
  finally:
    with engine.begin() as conn:
      release_tenant_schema(conn, GRAPH)
    engine.dispose()


def _seed() -> dict[str, str]:
  ids: dict[str, str] = {}
  with extensions_session(GRAPH) as session:
    session.add(
      Entity(
        id=PARENT,
        name="Event List Corp",
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
  return ids


@pytest.fixture(autouse=True)
def empty_ledger(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("SET LOCAL robosystems.ledger_purge = 'on'"))
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
    session.execute(text("DELETE FROM events"))
  yield


def _post(ids, lines, flow: str | None = None) -> None:
  with extensions_session(GRAPH) as session:
    flow_id = (
      session.execute(select(Element.id).where(Element.qname == flow)).scalar_one()
      if flow
      else None
    )
    entry = Entry(
      entity_id=PARENT,
      type="standard",
      status="posted",
      posting_date=date(2026, 1, 15),
      memo="event list",
      created_by="test",
    )
    session.add(entry)
    session.flush()
    for order, (key, debit, credit) in enumerate(lines, start=1):
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=ids[key],
          debit_amount=round(debit * 100),
          credit_amount=round(credit * 100),
          line_order=order,
          flow_element_id=flow_id,
        )
      )


def _cash_flow(ids) -> dict[str, float]:
  with extensions_session(GRAPH) as session:
    facts = generate_report_facts(
      session, "", ids["mapping"], [JAN], entity_id=PARENT
    ).facts
  out: dict[str, float] = {}
  for f in facts:
    if f.period_start == JAN.start and f.period_end == JAN.end:
      out[f.element_qname] = round(out.get(f.element_qname, 0.0) + f.value, 2)
  return out


@pytest.mark.parametrize("name", list(CASES))
def test_the_cash_lands_where_asc_230_puts_it(tenant, name: str) -> None:
  case = CASES[name]
  _post(tenant, case.lines, case.flow)
  cf = _cash_flow(tenant)
  assert cf.get(case.section, 0.0) == case.cash, (case.basis, cf)
  assert case.row in cf, (case.basis, sorted(q for q in cf if cf[q]))
  assert not cf.get(PLUG), (case.basis, cf)
