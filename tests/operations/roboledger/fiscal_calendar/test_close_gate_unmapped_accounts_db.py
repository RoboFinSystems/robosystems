"""Close refuses a month while an account with postings has no mapping.

Every statement and every consolidating entry reads an account through the
concept its chart mapping gives it, so an unmapped account with postings
leaves its balances out of all of them, silently. A chart brought from
QuickBooks or built by hand is where it happens; nothing lifts the gate —
the account is mapped. Runs against a throwaway schema in the real test
database.
"""

from __future__ import annotations

import os
import uuid
from datetime import date

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import (
  Association,
  Element,
  EntityTaxonomy,
  Structure,
  Taxonomy,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.roboledger.fiscal_calendar import FiscalCalendarService
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  SHADOW_FINDING_BLOCKERS,
  gate_finding_counts,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
)
from tests.ledger_entity import PARENT_ENTITY_ID, entity_account, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg01234567890abcdef"
BLOCKER = CloseableGateResult.UNMAPPED_ACCOUNTS


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_gate_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
  seed_parent_entity(db)
  db.commit()
  db.execute(text(f'SET search_path TO "{schema}"'))
  try:
    fcs = FiscalCalendarService()
    fcs.initialize(db, GRAPH_ID, closed_through="2026-06", actor_id="usr_1")
    db.commit()
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


def _post(session, on: date, lines: list[tuple[str, int]], status: str = "posted"):
  """A landed entry in the parent's books; ``lines`` are (element id, cents),
  debit-positive."""
  entry = Entry(
    entity_id=PARENT_ENTITY_ID, posting_date=on, status=status, created_by="u"
  )
  session.add(entry)
  session.flush()
  for order, (element_id, amount) in enumerate(lines, 1):
    session.add(
      LineItem(
        entry_id=entry.id,
        element_id=element_id,
        debit_amount=max(amount, 0),
        credit_amount=max(-amount, 0),
        line_order=order,
      )
    )
  session.commit()


def _map(session, *element_ids: str, standard: str = "rs-gaap") -> None:
  """A book mapping for the parent's chart, with an arc for each element
  given: the shape `find_entity_mapping` resolves."""
  chart_id = session.execute(
    select(EntityTaxonomy.taxonomy_id).where(
      EntityTaxonomy.entity_id == PARENT_ENTITY_ID,
      EntityTaxonomy.basis == "chart_of_accounts",
    )
  ).scalar_one()
  framework = Taxonomy(
    name=standard,
    taxonomy_type="reporting_standard",
    standard=standard,
    created_by="test",
  )
  session.add(framework)
  session.flush()
  concept = Element(
    qname="rs-gaap:Cash",
    name="Cash",
    taxonomy_id=framework.id,
    element_type="concept",
    is_abstract=False,
    created_by="test",
  )
  mapping = Taxonomy(
    name="mapping",
    taxonomy_type="mapping",
    source_taxonomy_id=chart_id,
    target_taxonomy_id=framework.id,
    created_by="test",
  )
  session.add_all([concept, mapping])
  session.flush()
  structure = Structure(
    name="CoA mapping", block_type="coa_mapping", taxonomy_id=mapping.id
  )
  session.add(structure)
  session.flush()
  for element_id in element_ids:
    session.add(
      Association(
        structure_id=structure.id,
        from_element_id=element_id,
        to_element_id=concept.id,
        association_type="mapping",
      )
    )
  session.commit()


def _gate(session):
  return FiscalCalendarService().closeable_gate(
    session, GRAPH_ID, "2026-07", today=date(2026, 8, 15)
  )


def test_unmapped_accounts_with_postings_block_and_are_named(session):
  cash = entity_account(session, PARENT_ENTITY_ID, "Cash")
  consulting = entity_account(session, PARENT_ENTITY_ID, "Consulting")
  _post(session, date(2026, 7, 10), [(cash, 50_000), (consulting, -50_000)])

  gate = _gate(session)
  assert BLOCKER in gate.blockers
  assert gate.unmapped_account_count == 2
  assert any("Consulting" in item for item in gate.unmapped_account_sample)


def test_mapping_every_posted_account_clears_it_and_half_a_mapping_does_not(
  session,
):
  cash = entity_account(session, PARENT_ENTITY_ID, "Cash")
  consulting = entity_account(session, PARENT_ENTITY_ID, "Consulting")
  _post(session, date(2026, 7, 10), [(cash, 50_000), (consulting, -50_000)])
  _map(session, cash)

  gate = _gate(session)
  assert BLOCKER in gate.blockers
  assert gate.unmapped_account_count == 1
  assert gate.unmapped_account_sample == ["Consulti Consulting"]

  session.add(
    Association(
      structure_id=session.execute(select(Structure.id)).scalar_one(),
      from_element_id=consulting,
      to_element_id=session.execute(
        select(Element.id).where(Element.qname == "rs-gaap:Cash")
      ).scalar_one(),
      association_type="mapping",
    )
  )
  session.commit()
  gate = _gate(session)
  assert BLOCKER not in gate.blockers
  assert gate.unmapped_account_count == 0


def test_postings_after_the_month_or_none_at_all_do_not_block(session):
  later = entity_account(session, PARENT_ENTITY_ID, "Later")
  entity_account(session, PARENT_ENTITY_ID, "Idle")
  _post(session, date(2026, 8, 5), [(later, 1_000), (later, -1_000)])

  gate = _gate(session)
  assert BLOCKER not in gate.blockers
  assert gate.unmapped_account_count == 0


def test_an_earlier_months_posting_still_holds_this_close(session):
  """A balance an unmapped account carried in from a closed month is still
  in no statement of this one."""
  old = entity_account(session, PARENT_ENTITY_ID, "Old")
  _post(session, date(2026, 5, 20), [(old, 1_000), (old, -1_000)])

  assert BLOCKER in _gate(session).blockers


def test_a_parents_chart_from_before_charts_were_linked_is_read(session):
  """A chart linked to no entity is the group parent's; its accounts are
  read as the accounts read reads them, so the gate sees them too."""
  old = entity_account(session, PARENT_ENTITY_ID, "Legacy")
  session.execute(
    text("DELETE FROM entity_taxonomies WHERE entity_id = :e"), {"e": PARENT_ENTITY_ID}
  )
  session.commit()
  _post(session, date(2026, 7, 10), [(old, 1_000), (old, -1_000)])

  gate = _gate(session)
  assert BLOCKER in gate.blockers
  assert gate.unmapped_account_sample == ["Legacy Legacy"]


def test_an_arc_into_another_framework_does_not_count_as_mapped(session):
  cash = entity_account(session, PARENT_ENTITY_ID, "Cash")
  _post(session, date(2026, 7, 10), [(cash, 1_000), (cash, -1_000)])
  _map(session, cash, standard="ifrs")

  assert BLOCKER in _gate(session).blockers


def test_a_shadow_close_records_it_as_a_finding_with_its_count():
  gate = CloseableGateResult(
    is_closeable=False, blockers=[BLOCKER], unmapped_account_count=3
  )
  assert gate_finding_counts(gate) == {BLOCKER: 3}
  assert BLOCKER in SHADOW_FINDING_BLOCKERS
