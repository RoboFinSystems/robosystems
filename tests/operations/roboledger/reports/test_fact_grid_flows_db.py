"""Investing / financing flows on real Postgres: the explicit pass and the
default-flow fallback share an entry without double-counting or dropping a
line, and a split's untagged parts keep their defaults."""

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
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity_on

pytestmark = pytest.mark.integration

GRAPH = "kgdddddddddddddddd11"
JAN = PeriodSpec(start=date(2026, 1, 1), end=date(2026, 1, 31), label="Jan")

NOTE_PROCEEDS = "rs-gaap:ProceedsFromShortTermDebt"
CONTRIBUTIONS = "rs-gaap:ProceedsFromContributedCapital"


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
  """A cash account, a note and a members' equity account, each mapped to
  its rs-gaap concept; two financing flows, each the default of one of the
  two non-cash concepts."""
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

    targets = {
      "cash": _element("rs-gaap:Cash", "debit", "instant"),
      "note": _element("rs-gaap:DebtCurrent", "credit", "instant"),
      "equity": _element("rs-gaap:MembersEquity", "credit", "instant"),
    }
    session.add_all(targets.values())
    session.flush()
    for key, target in targets.items():
      source = _element(f"coa:{key}", target.balance_type, "instant")
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

    for key, qname, concept in (
      ("note_flow", NOTE_PROCEEDS, "note"),
      ("contribution_flow", CONTRIBUTIONS, "equity"),
    ):
      flow = _element(qname, "debit", "duration")
      session.add(flow)
      session.flush()
      session.add(
        ElementTrait(element_id=flow.id, trait_id=financing.id, is_primary=True)
      )
      # A credit to the concept (its increase) defaults to the inflow.
      session.add(
        Association(
          structure_id=mapping.id,
          from_element_id=flow.id,
          to_element_id=targets[concept].id,
          association_type="derivation",
          weight=1,
        )
      )
      ids[key] = flow.id
  return ids


def _post(ids, lines, posting_date=date(2026, 1, 15)):
  """``lines`` are ``(account key, debit, credit, flow key or None)``."""
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


def _flows(ids) -> dict[str, float]:
  with extensions_session(GRAPH) as session:
    facts = generate_report_facts(session, "", ids["mapping"], [JAN]).facts
  out: dict[str, float] = {}
  for f in facts:
    if f.element_qname in (NOTE_PROCEEDS, CONTRIBUTIONS):
      out[f.element_qname] = out.get(f.element_qname, 0.0) + f.value
  return out


def test_a_split_keeps_the_defaults_of_its_untagged_parts(tenant):
  """One bank line, two parts: the tagged part posts with its own cash
  line and the explicit pass reads it; the untagged part's cash line carries
  no tag, so its account line keeps its default. Both reach the statement."""
  _post(
    tenant,
    [
      ("cash", 100_00, 0, "note_flow"),
      ("note", 0, 100_00, "note_flow"),
      ("cash", 50_00, 0, None),
      ("equity", 0, 50_00, None),
    ],
  )
  assert _flows(tenant) == {NOTE_PROCEEDS: 100.0, CONTRIBUTIONS: 50.0}


def test_a_tagged_cash_line_is_never_attributed_twice(tenant):
  """A journal entry whose cash line carries the flow: the explicit pass
  owns it, and the untagged account line does not add its default on top."""
  _post(tenant, [("cash", 100_00, 0, "note_flow"), ("note", 0, 100_00, None)])
  assert _flows(tenant) == {NOTE_PROCEEDS: 100.0}


def test_an_untagged_entry_takes_the_accounts_default(tenant):
  _post(tenant, [("cash", 70_00, 0, None), ("equity", 0, 70_00, None)])
  assert _flows(tenant) == {CONTRIBUTIONS: 70.0}


def test_an_accrual_moves_no_cash_and_takes_no_flow(tenant):
  _post(tenant, [("note", 30_00, 0, None), ("equity", 0, 30_00, None)])
  assert _flows(tenant) == {}
