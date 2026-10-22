"""The cash flow's roll-forward on real Postgres: the cash total is stated at
each column's end as the balance sheet's own sum, its balance the day before
the earliest column opens is stored, and the rendered statement opens on the
beginning-of-period cash and closes on the end-of-period cash that ties."""

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
  CASH_ROLLFORWARD_ROOTS,
  FactRow,
  PeriodSpec,
  _HierarchyNode,
  _with_opening_balance,
  generate_report_facts,
)
from robosystems.operations.roboledger.reports.guard_rails import validate_report
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity_on

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd14"
FY26 = PeriodSpec(start=date(2026, 1, 1), end=date(2026, 12, 31), label="2026")
FY25 = PeriodSpec(start=date(2025, 1, 1), end=date(2025, 12, 31), label="2025")

CASH = "rs-gaap:CashAndCashEquivalentsAtCarryingValue"
RESTRICTED = "rs-gaap:RestrictedCashAndInvestmentsCurrent"
TOTAL = "rs-gaap:CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"
NET_CHANGE = "rs-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease"


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
      seed_parent_entity_on(conn.execution_options(schema_translate_map={None: GRAPH}))
    yield _seed()
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _element(qname: str, balance: str, period: str) -> Element:
  return Element(
    qname=qname,
    name=qname.split(":")[1],
    balance_type=balance,
    period_type=period,
    element_type="concept",
    is_abstract=False,
  )


def _seed() -> dict[str, str]:
  """A small chart mapped to rs-gaap: an operating account and a restricted
  account on the two balance-sheet cash rows, paid-in capital, retained
  earnings, revenue and an expense. The cash total is a library concept no
  account maps to, summed from the two cash rows by calculation."""
  ids: dict[str, str] = {}
  with extensions_session(GRAPH) as session:
    mapping_taxonomy = Taxonomy(name="test", taxonomy_type="mapping")
    calc_taxonomy = Taxonomy(
      name="calc", taxonomy_type="reporting", standard="rs-gaap-calculations"
    )
    session.add_all([mapping_taxonomy, calc_taxonomy])
    session.flush()
    mapping = Structure(
      name="CoA mapping", block_type="coa_mapping", taxonomy_id=mapping_taxonomy.id
    )
    calc = Structure(
      name="CF calc", block_type="cash_flow_statement", taxonomy_id=calc_taxonomy.id
    )
    session.add_all([mapping, calc])
    session.flush()
    ids["mapping"] = mapping.id
    traits = {
      name: Trait(category="elementsOfFinancialStatements", identifier=name)
      for name in ("asset", "equity", "revenue", "expense")
    }
    session.add_all(traits.values())
    session.flush()
    concepts = {
      "cash": (_element(CASH, "debit", "instant"), "asset"),
      "restricted": (_element(RESTRICTED, "debit", "instant"), "asset"),
      "apic": (
        _element("rs-gaap:AdditionalPaidInCapital", "credit", "instant"),
        "equity",
      ),
      "re": (
        _element("rs-gaap:RetainedEarningsAccumulatedDeficit", "credit", "instant"),
        "equity",
      ),
      "revenue": (_element("rs-gaap:Revenues", "credit", "duration"), "revenue"),
      "expense": (
        _element("rs-gaap:GeneralAndAdministrativeExpense", "debit", "duration"),
        "expense",
      ),
    }
    session.add_all(c for c, _ in concepts.values())
    session.flush()
    for key, (concept, trait) in concepts.items():
      session.add(
        ElementTrait(element_id=concept.id, trait_id=traits[trait].id, is_primary=True)
      )
      source = _element(f"coa:{key}", concept.balance_type, concept.period_type)
      session.add(source)
      session.flush()
      session.add(
        Association(
          structure_id=mapping.id,
          from_element_id=source.id,
          to_element_id=concept.id,
          association_type="mapping",
        )
      )
      ids[key] = source.id
      ids[f"{key}_concept"] = concept.id
    total = _element(TOTAL, "debit", "instant")
    session.add(total)
    session.flush()
    ids["total_concept"] = total.id
    for child in ("cash", "restricted"):
      session.add(
        Association(
          structure_id=calc.id,
          from_element_id=total.id,
          to_element_id=ids[f"{child}_concept"],
          association_type="calculation",
          weight=1,
        )
      )
  return ids


def _post(ids, posting_date, lines):
  """``lines`` are ``(account key, debit dollars, credit dollars)``."""
  with extensions_session(GRAPH) as session:
    entry = Entry(
      entity_id=PARENT_ENTITY_ID,
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
          debit_amount=round(debit * 100),
          credit_amount=round(credit * 100),
          line_order=order,
        )
      )


@pytest.fixture(autouse=True)
def empty_ledger(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
  yield


def _facts(ids, periods):
  with extensions_session(GRAPH) as session:
    return generate_report_facts(
      session,
      "",
      ids["mapping"],
      periods,
      close_target_qname="rs-gaap:RetainedEarningsAccumulatedDeficit",
    ).facts


def _by_qname(facts, period: PeriodSpec) -> dict[str, float]:
  out: dict[str, float] = {}
  for f in facts:
    if (f.period_start, f.period_end) == (period.start, period.end):
      out[f.element_qname] = round(out.get(f.element_qname, 0.0) + f.value, 2)
  return out


def _two_years(ids) -> None:
  """FY2025: a contribution, 200 of it set aside as restricted. FY2026: cash
  revenue, a cash expense, and 100 released from the restricted account."""
  _post(ids, date(2025, 6, 1), [("cash", 1000, 0), ("apic", 0, 1000)])
  _post(ids, date(2025, 6, 2), [("restricted", 200, 0), ("cash", 0, 200)])
  _post(ids, date(2026, 2, 1), [("cash", 500, 0), ("revenue", 0, 500)])
  _post(ids, date(2026, 3, 1), [("expense", 40, 0), ("cash", 0, 40)])
  _post(ids, date(2026, 4, 1), [("cash", 100, 0), ("restricted", 0, 100)])


def _hierarchy(rows: list[FactRow]) -> list[_HierarchyNode]:
  """The Style's tree over ``rows``: each depth-0 row the root of the depth-1
  rows before it."""
  roots: list[_HierarchyNode] = []
  children: list[_HierarchyNode] = []
  for r in rows:
    node = _HierarchyNode(
      element_id=r.element_id,
      qname=r.element_qname,
      name=r.element_name,
      classification="",
      balance_type=r.balance_type,
      is_abstract=False,
      depth=r.depth,
    )
    if r.depth == 0:
      node.children = children
      roots.append(node)
      children = []
    else:
      children.append(node)
  return roots


def test_the_cash_total_is_the_balance_sheets_own_sum_at_each_column_end(tenant):
  """Operating cash plus restricted cash, stated by the calc pass with the
  other subtotals: the number the end-of-period row shows."""
  _two_years(tenant)
  facts = _facts(tenant, [FY26, FY25])
  fy25, fy26 = _by_qname(facts, FY25), _by_qname(facts, FY26)

  assert (fy25[CASH], fy25[RESTRICTED], fy25[TOTAL]) == (800.0, 200.0, 1000.0)
  assert (fy26[CASH], fy26[RESTRICTED], fy26[TOTAL]) == (1360.0, 100.0, 1460.0)


def test_the_earliest_column_opens_on_a_stored_instant(tenant):
  """The day before the first column starts has no column ending on it, so
  the opening cash is stored as an instant; a column opening on another's
  end reads that column's closing instead, and nothing is stored."""
  _two_years(tenant)
  facts = _facts(tenant, [FY26])
  opening = [
    f for f in facts if f.element_qname == TOTAL and f.period_type == "instant"
  ]
  stored = [
    (f.period_start, f.period_end, f.value, (f.derivation or {}).get("computations"))
    for f in opening
    if f.period_end == date(2025, 12, 31)
  ]
  assert stored == [(date(2025, 12, 31), date(2025, 12, 31), 1000.0, ["opening_cash"])]

  facts = _facts(tenant, [FY26, FY25])
  assert not [
    f
    for f in facts
    if f.element_qname == TOTAL
    and f.period_end == date(2025, 12, 31)
    and f.origin == "derived"
  ], "a column ends that day: its closing is the opening, nothing extra is stored"


def test_the_statement_opens_and_closes_on_cash_that_ties(tenant):
  """Rendered, the cash flow reads cash at the beginning of the period + net
  change = cash at the end, on the prior column's closing for the later
  column and on the stored instant for the earliest; the validator passes
  both columns."""
  _two_years(tenant)
  facts = _facts(tenant, [FY26, FY25])
  periods = [FY26, FY25]
  by_period = [_by_qname(facts, p) for p in periods]
  total_id = next(f.element_id for f in facts if f.element_qname == TOTAL)

  rows = [
    FactRow(
      element_id="net_change",
      element_qname=NET_CHANGE,
      element_name="Net change in cash",
      classification=None,
      balance_type="debit",
      values=[460.0, 1000.0],
      is_subtotal=True,
      depth=1,
    ),
    FactRow(
      element_id=total_id,
      element_qname=TOTAL,
      element_name="Cash, cash equivalents and restricted cash",
      classification=None,
      balance_type="debit",
      values=[p.get(TOTAL, 0.0) for p in by_period],
      is_subtotal=True,
      depth=0,
    ),
  ]
  rows = _with_opening_balance(
    rows,
    facts,
    periods,
    _hierarchy(rows),
    roots=CASH_ROLLFORWARD_ROOTS,
    suffixes=(", beginning of period", ", end of period"),
  )

  assert [r.period_role for r in rows] == ["start", None, "end"]
  assert rows[0].element_name.endswith(", beginning of period")
  assert rows[0].values == [1000.0, 0.0]
  assert rows[-1].element_name.endswith(", end of period")
  assert rows[-1].values == [1460.0, 1000.0]
  result = validate_report("cash_flow_statement", rows, period_labels=["2026", "2025"])
  assert (result.status, result.failures) == ("passed", [])
  assert "rollforward_articulates" in result.checks
