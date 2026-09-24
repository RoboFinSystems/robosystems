"""Report facts against a real tenant schema: every income-statement class
reaches net income and the retained-earnings close, and each fact carries its
reporting element's own period type."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.extensions.association import Association
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.element_trait import ElementTrait
from robosystems.models.extensions.roboledger import Entry, LineItem
from robosystems.models.extensions.structure import Structure
from robosystems.models.extensions.taxonomy import Taxonomy
from robosystems.models.extensions.trait import Trait
from robosystems.operations.roboledger.reports.fact_grid import (
  PeriodSpec,
  generate_report_facts,
)

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd09"
JAN = PeriodSpec(start=date(2026, 1, 1), end=date(2026, 1, 31), label="Jan")

# qname, classification, balance_type, period_type
TARGETS = {
  "cash": ("rs-gaap:Cash", "asset", "debit", "instant"),
  "gain": ("rs-gaap:GainLossOnDispositionOfAssets", "gain", "credit", "duration"),
  "loss": ("rs-gaap:LossOnDispositionOfAssets", "loss", "debit", "duration"),
  "reversal": ("rs-gaap:ReversalOfExpense", "expenseReversal", "credit", "duration"),
  "revenue": ("rs-gaap:Revenues", "revenue", "credit", "duration"),
  "expense": ("rs-gaap:OperatingExpenses", "expense", "debit", "duration"),
  "accum": (
    "rs-gaap:AccumulatedDepreciation",
    "contraAsset",
    "credit",
    "instant",
  ),
  "re": (
    "rs-gaap:RetainedEarningsAccumulatedDeficit",
    "equity",
    "credit",
    "instant",
  ),
  "ni": ("rs-gaap:NetIncomeLoss", None, "credit", "duration"),
  "ar": ("rs-gaap:AccountsReceivableNetCurrent", "asset", "debit", "instant"),
}

AR_CHANGE = "rs-gaap:IncreaseDecreaseInAccountsReceivable"


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
  tables = [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]
  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=tables,
      )
    yield _seed()
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _seed() -> dict[str, str]:
  """A chart of one source account per target, mapped one to one."""
  ids: dict[str, str] = {}
  with extensions_session(GRAPH) as session:
    taxonomy = Taxonomy(name="test", taxonomy_type="mapping")
    session.add(taxonomy)
    session.flush()
    mapping = Structure(
      name="CoA mapping", block_type="coa_mapping", taxonomy_id=taxonomy.id
    )
    session.add(mapping)
    session.flush()
    ids["mapping"] = mapping.id
    traits: dict[str, str] = {}
    for key, (qname, cls, balance, period) in TARGETS.items():
      target = Element(
        qname=qname,
        name=qname.split(":")[1],
        balance_type=balance,
        period_type=period,
        element_type="concept",
        is_abstract=False,
      )
      source = Element(
        qname=f"coa:{key}",
        name=f"CoA {key}",
        balance_type=balance,
        period_type=period,
        element_type="concept",
        is_abstract=False,
      )
      session.add_all([target, source])
      session.flush()
      if cls is not None:
        if cls not in traits:
          trait = Trait(category="elementsOfFinancialStatements", identifier=cls)
          session.add(trait)
          session.flush()
          traits[cls] = trait.id
        session.add(
          ElementTrait(element_id=target.id, trait_id=traits[cls], is_primary=True)
        )
      session.add(
        Association(
          structure_id=mapping.id,
          from_element_id=source.id,
          to_element_id=target.id,
          association_type="mapping",
        )
      )
      ids[key] = source.id
      if key == "ar":
        ar_target_id = target.id
    # The indirect method: the AR change leaf is AR's movement, weight -1.
    cf_leaf = Element(
      qname=AR_CHANGE,
      name="IncreaseDecreaseInAccountsReceivable",
      balance_type="debit",
      period_type="duration",
      element_type="concept",
      is_abstract=False,
    )
    session.add(cf_leaf)
    session.flush()
    session.add(
      Association(
        structure_id=mapping.id,
        from_element_id=cf_leaf.id,
        to_element_id=ar_target_id,
        association_type="derivation",
        weight=-1,
      )
    )
  return ids


def _post(ids, lines, posting_date=date(2026, 1, 15)):
  with extensions_session(GRAPH) as session:
    entry = Entry(
      type="standard",
      status="posted",
      posting_date=posting_date,
      memo="test",
      created_by="test",
    )
    session.add(entry)
    session.flush()
    for order, (key, debit, credit) in enumerate(lines, start=1):
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=ids[key],
          debit_amount=debit,
          credit_amount=credit,
          line_order=order,
        )
      )


@pytest.fixture(autouse=True)
def empty_ledger(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
  yield


def _facts(ids, periods=(JAN,)):
  with extensions_session(GRAPH) as session:
    return generate_report_facts(session, "", ids["mapping"], list(periods)).facts


def _column(facts, qname, period):
  return sum(
    f.value
    for f in facts
    if f.element_qname == qname
    and f.period_start == period.start
    and f.period_end == period.end
  )


def _value(facts, qname):
  return sum(f.value for f in facts if f.element_qname == qname)


@pytest.mark.parametrize(
  ("lines", "net_income"),
  [
    ([("cash", 100_000, 0), ("gain", 0, 100_000)], 1000.0),
    ([("loss", 40_000, 0), ("cash", 0, 40_000)], -400.0),
    ([("cash", 25_000, 0), ("reversal", 0, 25_000)], 250.0),
    (
      [
        ("cash", 500_000, 0),
        ("revenue", 0, 500_000),
        ("expense", 200_000, 0),
        ("cash", 0, 200_000),
        ("cash", 100_000, 0),
        ("gain", 0, 100_000),
        ("loss", 30_000, 0),
        ("cash", 0, 30_000),
      ],
      3700.0,
    ),
  ],
)
def test_every_income_class_reaches_net_income_and_the_close(tenant, lines, net_income):
  _post(tenant, lines)
  facts = _facts(tenant)

  assert _value(facts, "rs-gaap:NetIncomeLoss") == pytest.approx(net_income)
  assets = _value(facts, "rs-gaap:Cash")
  equity = _value(facts, "rs-gaap:RetainedEarningsAccumulatedDeficit")
  assert assets == pytest.approx(equity)


def test_a_contra_asset_balance_is_an_instant(tenant):
  _post(tenant, [("expense", 12_000, 0), ("accum", 0, 12_000)])
  facts = _facts(tenant)

  accumulated = [
    f for f in facts if f.element_qname == "rs-gaap:AccumulatedDepreciation"
  ]
  assert accumulated and all(f.period_type == "instant" for f in accumulated)


def _sell_on_account(ids, posting_date, cents):
  _post(ids, [("ar", cents, 0), ("revenue", 0, cents)], posting_date=posting_date)


Q3 = PeriodSpec(start=date(2026, 7, 1), end=date(2026, 9, 30), label="Q3")
YTD = PeriodSpec(start=date(2026, 1, 1), end=date(2026, 9, 30), label="9M")


def test_a_10q_layout_does_not_double_the_balance_sheet(tenant):
  _sell_on_account(tenant, date(2026, 2, 10), 10_000)
  _sell_on_account(tenant, date(2026, 8, 10), 5_000)

  facts = _facts(tenant, (Q3, YTD))

  assert _column(facts, AR_CHANGE, Q3) == pytest.approx(-50.0)
  assert _column(facts, AR_CHANGE, YTD) == pytest.approx(-150.0)


def test_a_single_column_cash_flow_is_derived(tenant):
  _sell_on_account(tenant, date(2026, 2, 10), 10_000)
  _sell_on_account(tenant, date(2026, 8, 10), 5_000)

  facts = _facts(tenant, (Q3,))

  assert _column(facts, AR_CHANGE, Q3) == pytest.approx(-50.0)
