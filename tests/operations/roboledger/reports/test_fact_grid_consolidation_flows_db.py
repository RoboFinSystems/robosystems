"""The consolidation entity's flow tags on real Postgres: it moves no cash,
so a tagged line counts on its own, cash-effect signed, and offsets the flow a
member's line produced. An operating entity's tagged line still needs the
cash line, as before."""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.extensions import Entity
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
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity_on

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd13"
ADJUSTMENTS = "ent_test_adjustments"
JAN = PeriodSpec(start=date(2026, 1, 1), end=date(2026, 1, 31), label="Jan")
NOTE_PROCEEDS = "rs-gaap:ProceedsFromRelatedPartyDebt"
CONTRIBUTIONS = "rs-gaap:ProceedsFromContributedCapital"
OTHER_NONCASH = "rs-gaap:OtherNoncashIncomeExpense"


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
      scoped = conn.execution_options(schema_translate_map={None: GRAPH})
      seed_parent_entity_on(scoped)
      scoped.execute(
        Entity.__table__.insert().values(
          id=ADJUSTMENTS,
          name="Consolidation Adjustments",
          is_parent=False,
          parent_entity_id=PARENT_ENTITY_ID,
          source="native",
          entity_role="consolidation",
          consolidation_method="none",
          created_by="test",
        )
      )
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
  """A cash account, a note and a members' equity account mapped to their
  concepts, and two financing flows a line can be tagged with."""
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
    financing = Trait(category="activityType", identifier="financingActivity")
    session.add(financing)
    session.flush()
    revenue = Trait(category="elementsOfFinancialStatements", identifier="revenue")
    session.add(revenue)
    targets = {
      "cash": _element("rs-gaap:Cash", "debit", "instant"),
      "note": _element("rs-gaap:DebtCurrent", "credit", "instant"),
      "equity": _element("rs-gaap:MembersEquity", "credit", "instant"),
      "income": _element("rs-gaap:OtherNonoperatingIncome", "credit", "duration"),
    }
    session.add_all(targets.values())
    session.add(_element(OTHER_NONCASH, "credit", "duration"))
    session.flush()
    session.add(
      ElementTrait(
        element_id=targets["income"].id, trait_id=revenue.id, is_primary=True
      )
    )
    for key, target in targets.items():
      source = _element(f"coa:{key}", target.balance_type, target.period_type)
      session.add(source)
      session.flush()
      session.add(
        Association(
          structure_id=mapping.id,
          from_element_id=source.id,
          to_element_id=target.id,
          association_type="mapping",
        )
      )
      ids[key] = source.id
    for key, qname in (
      ("note_flow", NOTE_PROCEEDS),
      ("contribution_flow", CONTRIBUTIONS),
    ):
      flow = _element(qname, "debit", "duration")
      session.add(flow)
      session.flush()
      session.add(
        ElementTrait(element_id=flow.id, trait_id=financing.id, is_primary=True)
      )
      ids[key] = flow.id
  return ids


def _post(ids, entity_id, lines, posting_date=date(2026, 1, 15)):
  """``lines`` are ``(account key, debit, credit, flow key or None)``."""
  with extensions_session(GRAPH) as session:
    entry = Entry(
      entity_id=entity_id,
      type="standard",
      status="posted",
      posting_date=posting_date,
      memo="test",
      created_by="test",
    )
    session.add(entry)
    session.flush()
    for order, (key, debit, credit, flow) in enumerate(lines, start=1):
      session.add(
        LineItem(
          entry_id=entry.id,
          element_id=ids[key],
          debit_amount=debit,
          credit_amount=credit,
          line_order=order,
          flow_element_id=ids[flow] if flow else None,
        )
      )


@pytest.fixture(autouse=True)
def empty_ledger(tenant):
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM line_items"))
    session.execute(text("DELETE FROM entries"))
  yield


def _flows(ids, entity_id) -> dict[str, float]:
  with extensions_session(GRAPH) as session:
    facts = generate_report_facts(
      session, "", ids["mapping"], [JAN], entity_id=entity_id
    ).facts
  out: dict[str, float] = {}
  for f in facts:
    if f.element_qname in (NOTE_PROCEEDS, CONTRIBUTIONS):
      out[f.element_qname] = out.get(f.element_qname, 0.0) + f.value
  return out


# The elimination of a note the parent lent a member, and of the capital it
# contributed: no cash, each side tagged with the flow the member's own line
# produced.
ELIMINATION = [
  ("note", 100_00, 0, "note_flow"),
  ("equity", 0, 100_00, "contribution_flow"),
]


def test_the_consolidation_entitys_tagged_lines_count_without_a_cash_line(tenant):
  """Cash-effect signed: the debit that reverses the member's note is the
  outflow that nets its proceeds, the credit an inflow."""
  _post(tenant, ADJUSTMENTS, ELIMINATION)
  assert _flows(tenant, ADJUSTMENTS) == {NOTE_PROCEEDS: -100.0, CONTRIBUTIONS: 100.0}


def test_an_operating_entitys_tagged_lines_still_need_the_cash_line(tenant):
  _post(tenant, PARENT_ENTITY_ID, ELIMINATION)
  assert _flows(tenant, PARENT_ENTITY_ID) == {}


def test_a_cash_line_on_the_consolidation_entity_keeps_its_own_sign(tenant):
  """Should it ever move cash, the cash line reads as every entity's does."""
  _post(
    tenant, ADJUSTMENTS, [("cash", 50_00, 0, "note_flow"), ("note", 0, 50_00, None)]
  )
  assert _flows(tenant, ADJUSTMENTS) == {NOTE_PROCEEDS: 50.0}


def test_a_whole_graph_pivot_keeps_the_rule_off(tenant):
  """With no entity named the pivot spans the graph, and only cash lines
  count, as for any entity: the consolidation entity's rule is its own."""
  _post(tenant, ADJUSTMENTS, ELIMINATION)
  assert _flows(tenant, None) == {}


def test_a_tagged_line_beside_income_is_not_offset_as_well(tenant):
  """The parent took a member's distribution as income; the elimination
  reverses the income and restores the capital, tagged with the member's
  distribution. The tag is the capital line's flow, so the income it sits
  beside is not also backed out of operating as a non-cash item."""
  _post(
    tenant,
    ADJUSTMENTS,
    [("income", 70_00, 0, None), ("equity", 0, 70_00, "contribution_flow")],
  )
  with extensions_session(GRAPH) as session:
    facts = generate_report_facts(
      session, "", tenant["mapping"], [JAN], entity_id=ADJUSTMENTS
    ).facts
  values = {f.element_qname: f.value for f in facts if f.period_type == "duration"}
  assert values.get(OTHER_NONCASH, 0.0) == 0.0
  assert values[CONTRIBUTIONS] == 70.0
