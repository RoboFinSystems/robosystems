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
import threading
import time
import uuid
from datetime import date, datetime

import pytest
from sqlalchemy import create_engine, func, select, text
from sqlalchemy.orm import Session, sessionmaker

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
  EventNotFoundError,
  create_event_block_in_session,
  fire_handler_on_commit,
  update_event_block,
)
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands import connections
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


@pytest.fixture()
def tenant():
  """A tenant schema every connection of the yielded factory lands in, for
  cases that race two transactions. A waiting lock gives up after a few
  seconds, so a race that would hang fails instead."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_feedpurge_{uuid.uuid4().hex[:12]}"
  admin = create_engine(database_url)
  with admin.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  engine = create_engine(
    database_url,
    connect_args={"options": f"-csearch_path={schema} -clock_timeout=8000"},
  )
  factory = sessionmaker(bind=engine)
  with factory() as db:
    ExtensionsBase.metadata.create_all(bind=db.connection())
    seed_parent_entity(db)
    db.commit()
  try:
    yield factory
  finally:
    engine.dispose()
    with admin.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    admin.dispose()


def backend_pid(db) -> int:
  return int(db.execute(text("SELECT pg_backend_pid()")).scalar_one())


def wait_until_blocked(factory, pid: int, timeout: float = 10.0) -> bool:
  """True once backend ``pid`` is waiting on a lock. Each probe is its own
  transaction: ``pg_stat_activity`` is a per-transaction snapshot."""
  deadline = time.monotonic() + timeout
  with factory() as probe:
    while time.monotonic() < deadline:
      waiting = probe.execute(
        text("SELECT wait_event_type FROM pg_stat_activity WHERE pid = :pid"),
        {"pid": pid},
      ).scalar_one_or_none()
      probe.rollback()
      if waiting == "Lock":
        return True
      time.sleep(0.05)
  return False


def held_open(db):
  """``db`` with its ``commit`` deferred: a kernel call that commits its own
  work (``update_event_block``) runs to the end and keeps its row locks until
  the caller ends the transaction with ``Session.commit(db)``."""
  db.commit = db.flush
  return db


class InThread:
  """Run ``work(session)`` on its own session and thread, then commit; the
  session's backend pid is published before the work starts."""

  def __init__(self, factory, work):
    self.factory = factory
    self.work = work
    self.pid: int | None = None
    self.result = None
    self.error: BaseException | None = None
    self._started = threading.Event()
    self._thread = threading.Thread(target=self._run, daemon=True)

  def _run(self):
    db = self.factory()
    try:
      self.pid = backend_pid(db)
      self._started.set()
      self.result = self.work(db)
      db.commit()
    except BaseException as exc:
      db.rollback()
      self.error = exc
    finally:
      self._started.set()
      db.close()

  def start(self) -> InThread:
    self._thread.start()
    assert self._started.wait(10)
    return self

  def join(self) -> None:
    self._thread.join(20)
    assert not self._thread.is_alive(), "the racing transaction never finished"


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


# ── a commit racing the purge ────────────────────────────────────────────────
#
# The purge decides which feed lines are unposted, then deletes them. An
# operator committing one of those lines at the same moment must either land
# first (the purge then sees a committed line and keeps it) or find the line
# gone. Never both: a committed line deleted under its draft hands that draft
# to the close.


def _captured_line(factory, source: str = "plaid") -> str:
  """A classified-but-unposted bank line, committed so other sessions see it."""
  with factory() as db:
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
        external_id=f"{source}_txn_racing",
        occurred_at=datetime(2026, 9, 14),
        amount=-12_345,
        currency="USD",
        description="SaaS vendor",
        resource_element_id=str(bank.id),
        apply_handlers=False,
        metadata={
          "connection_id": CONNECTION_ID,
          "transaction_id": f"{source}_txn_racing",
          "account_id": "acc_chk",
          "bank_description": "SAAS VENDOR 0914",
          "classified_element_id": str(expense.id),
        },
      ),
      USER,
      graph_id=GRAPH_ID,
    )
    event_id = str(event.id)
    db.commit()
  return event_id


def _commit(db, event_id: str):
  return update_event_block(
    db,
    UpdateEventBlockRequest(event_id=event_id, transition_to="committed"),
    USER,
    graph_id=GRAPH_ID,
  )


def _purge(db):
  return purge_bank_feed(db, source="plaid", connection_id=CONNECTION_ID)


def _assert_books_whole(factory, event_id: str, *, committed: bool) -> None:
  """A committed line survives with its one draft, which the close posts; a
  line the purge took leaves nothing behind for the close."""
  with factory() as db:
    event = db.get(Event, event_id)
    posts = drafts_close_posts(db, *SEPTEMBER, entity_id=PARENT_ENTITY_ID).all()
    assert _dangling(db, Entry) == 0
    assert _dangling(db, Transaction) == 0
    if committed:
      assert event is not None, "a committed line was deleted by the purge"
      assert event.status == "committed"
      assert "bank_description" not in (event.metadata_ or {})
      assert [str(e.triggered_by_event_id) for e in posts] == [event_id]
    else:
      assert event is None
      assert posts == []


def test_a_commit_holding_the_line_lands_before_the_purge_decides(tenant):
  event_id = _captured_line(tenant)
  operator = held_open(tenant())
  try:
    _commit(operator, event_id)  # the line's row lock is held from here
    purge = InThread(tenant, _purge).start()
    assert wait_until_blocked(tenant, purge.pid), "the purge never waited"
    Session.commit(operator)
  finally:
    operator.close()
  purge.join()

  assert purge.error is None, purge.error
  assert purge.result["events_deleted"] == 0
  assert purge.result["events_scrubbed"] == 1
  _assert_books_whole(tenant, event_id, committed=True)


def test_a_commit_arriving_while_the_purge_holds_the_line_finds_it_gone(tenant):
  event_id = _captured_line(tenant)
  purger = tenant()
  try:
    purged = _purge(purger)  # held open
    assert purged["events_deleted"] == 1
    operator = InThread(tenant, lambda db: _commit(db, event_id)).start()
    assert wait_until_blocked(tenant, operator.pid), "the commit never waited"
    purger.commit()
  finally:
    purger.close()
  operator.join()

  assert isinstance(operator.error, EventNotFoundError), operator.error
  _assert_books_whole(tenant, event_id, committed=False)


def test_a_commit_between_the_purges_read_and_its_deletes_cannot_land(
  tenant, monkeypatch
):
  """The window between deciding a line is unposted and deleting it: a
  commit there either waits out the purge (and finds the line gone) or, if
  it lands, keeps its line. It must not land and then be deleted."""
  event_id = _captured_line(tenant)
  release = connections._release_unposted_effects
  attempt: dict[str, object] = {}

  def commit_inside_the_window(session, event_ids):
    with tenant() as operator:
      try:
        _commit(operator, event_id)
        operator.commit()
        attempt["landed"] = True
      except RowLockedError:
        attempt["landed"] = False
    return release(session, event_ids)

  monkeypatch.setattr(
    connections, "_release_unposted_effects", commit_inside_the_window
  )
  with tenant() as purger:
    _purge(purger)
    purger.commit()

  assert "landed" in attempt
  _assert_books_whole(tenant, event_id, committed=bool(attempt["landed"]))
  assert attempt["landed"] is False  # the purge holds the line it decided on
