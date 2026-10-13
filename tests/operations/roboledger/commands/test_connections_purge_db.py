"""Purging a bank feed on disconnect, against a real tenant schema.

A tenant schema is built by ``create_all``, and the model declares no foreign
key behind ``entries.triggered_by_event_id`` or
``transactions.triggered_by_event_id``. Nothing in the database stops the
purge deleting an event that still owns ledger rows, so the purge itself must
not. The case that matters is a voided bank line whose draft is still there:
the close skips that draft only while its event exists and is voided, so
deleting the event alone hands the draft to the next close.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  fire_handler_on_commit,
  update_event_block,
)
from robosystems.operations.roboledger.commands.connections import purge_bank_feed
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  drafts_close_posts,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_feedpurge"
USER = "user_feedpurge"
CONNECTION_ID = "conn_feedpurge"
SEPTEMBER = (date(2026, 9, 1), date(2026, 9, 30))

# Every disconnect that purges a feed: both bank-feed providers run
# `purge_bank_feed` through `purge_bank_feed_connection`.
FEED_SOURCES = ("plaid", "mercury")


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_feedpurge_{uuid.uuid4().hex[:12]}"
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
    yield db
  finally:
    db.rollback()
    db.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


@pytest.fixture(autouse=True)
def _skip_platform_db_checks(monkeypatch):
  """The source and connection registries live in the platform DB; what is
  under test here is the tenant schema."""
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )


def _committed_line(db, source: str, external_id: str) -> Event:
  """A bank line captured by ``source``, classified, and committed: one draft
  entry and its pending transaction, dated 2026-09-14."""
  bank = Element(name="Checking", code="1010", created_by="test")
  expense = Element(name="Software", code="6100", created_by="test")
  db.add_all([bank, expense])
  db.flush()
  event, _ = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="bank_transaction",
      event_category="purchase",
      event_class="economic",
      event_action="transfer",
      resource_type="money",
      source=source,
      external_id=external_id,
      occurred_at=datetime(2026, 9, 14),
      amount=-12_345,
      currency="USD",
      description="SaaS vendor",
      resource_element_id=str(bank.id),
      apply_handlers=False,
      metadata={
        "connection_id": CONNECTION_ID,
        "transaction_id": external_id,
        "account_id": "acc_chk",
        "bank_description": "SAAS VENDOR 0914",
        "classified_element_id": str(expense.id),
      },
    ),
    USER,
    graph_id=GRAPH_ID,
  )
  event.status = "committed"
  fire_handler_on_commit(db, event, USER)
  db.flush()
  return event


def _void(db, event: Event) -> None:
  update_event_block(
    db,
    UpdateEventBlockRequest(event_id=str(event.id), transition_to="voided"),
    USER,
    graph_id=GRAPH_ID,
  )
  db.flush()
  db.refresh(event)
  assert event.status == "voided"


def _dangling(db, model) -> int:
  return db.execute(
    select(func.count())
    .select_from(model)
    .outerjoin(Event, Event.id == model.triggered_by_event_id)
    .where(model.triggered_by_event_id.is_not(None), Event.id.is_(None))
  ).scalar_one()


def _owned(db, model, event_id: str) -> int:
  return db.execute(
    select(func.count())
    .select_from(model)
    .where(model.triggered_by_event_id == event_id)
  ).scalar_one()


@pytest.mark.parametrize("source", FEED_SOURCES)
def test_a_voided_lines_draft_never_reaches_the_close(session, source):
  event = _committed_line(session, source, f"{source}_txn_voided")
  event_id = str(event.id)
  assert _owned(session, Entry, event_id) == 1
  _void(session, event)
  assert drafts_close_posts(session, *SEPTEMBER, entity_id=PARENT_ENTITY_ID).all() == []

  purged = purge_bank_feed(session, source=source, connection_id=CONNECTION_ID)
  session.flush()

  assert drafts_close_posts(session, *SEPTEMBER, entity_id=PARENT_ENTITY_ID).all() == []
  assert _dangling(session, Entry) == 0
  assert _dangling(session, Transaction) == 0
  # The voided line is unposted feed data: it goes, and its leftovers with it.
  assert purged["events_deleted"] == 1
  assert session.get(Event, event_id) is None
  assert session.execute(select(func.count()).select_from(LineItem)).scalar_one() == 0


@pytest.mark.parametrize("source", FEED_SOURCES)
def test_an_event_that_still_owns_landed_rows_is_scrubbed_not_deleted(session, source):
  """Whatever its status says, an event whose entry has posted is history: the
  purge keeps it, as it keeps any posted line, and only scrubs the payload."""
  event = _committed_line(session, source, f"{source}_txn_landed")
  event_id = str(event.id)
  _void(session, event)
  session.execute(
    text("UPDATE entries SET status = 'posted' WHERE triggered_by_event_id = :e"),
    {"e": event_id},
  )

  purged = purge_bank_feed(session, source=source, connection_id=CONNECTION_ID)
  session.flush()

  assert purged["events_deleted"] == 0
  assert purged["events_scrubbed"] == 1
  kept = session.get(Event, event_id)
  assert kept is not None
  assert "bank_description" not in (kept.metadata_ or {})
  assert _owned(session, Entry, event_id) == 1
  assert _dangling(session, Entry) == 0
  assert _dangling(session, Transaction) == 0
