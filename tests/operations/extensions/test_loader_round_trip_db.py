"""QuickBooks round trips of our own write-back, against a real database.

An entry RoboLedger publishes comes back on the next sync as a QuickBooks
JournalEntry. The matcher recognises it by `metadata.qb_external_id`, whatever
the event's source, and never books it twice. An edit the accountant makes to
it in QuickBooks becomes a reconciling item, measured from the last version
the ledger accepted, so a second edit never re-books the first. The same
holds for a QuickBooks-sourced event caught up twice.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import UTC, date, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.reconciling_items import (
  ResolveReconcilingItemRequest,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.event_block.commands import create_event_block_in_session
from robosystems.operations.extensions.loader import OLTPLoader
from robosystems.operations.roboledger.commands.reconciling_items import (
  RestateBlockedError,
  plan_reconciling_item,
  resolve_reconciling_item,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
CONN = "conn_test"
CASH, EXP_A, EXP_B = "35", "60", "61"


@pytest.fixture()
def db():
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  schema = f"ext_rtrip_{uuid.uuid4().hex[:12]}"
  engine = create_engine(url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))
  session = sessionmaker(bind=engine)()
  session.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=session.connection())
  session.commit()
  session.execute(text(f'SET search_path TO "{schema}"'))
  try:
    yield session
  finally:
    session.rollback()
    session.close()
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    engine.dispose()


@pytest.fixture(autouse=True)
def _no_platform(monkeypatch):
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )


def _seed(db) -> dict[str, str]:
  ids = {}
  for ext, code, name in (
    (CASH, "1000", "Checking"),
    (EXP_A, "6100", "Repairs"),
    (EXP_B, "6200", "Software"),
  ):
    element = Element(
      name=name,
      code=code,
      external_id=ext,
      external_source="quickbooks",
      connection_id=CONN,
      created_by="t",
    )
    db.add(element)
    db.flush()
    ids[ext] = str(element.id)
  for name, start, end in (
    ("2026-07", date(2026, 7, 1), date(2026, 7, 31)),
    ("2026-08", date(2026, 8, 1), date(2026, 8, 31)),
  ):
    db.add(
      FiscalPeriod(
        graph_id=GRAPH_ID,
        name=name,
        start_date=start,
        end_date=end,
        period_type="month",
        status="open",
      )
    )
  db.flush()
  return ids


def _dbt(ext_id: str, amount: int, expense: str, token: str, tx_type="JournalEntry"):
  return {
    "transactions": [
      {
        "external_id": ext_id,
        "type": tx_type,
        "amount": amount,
        "date": date(2026, 7, 15),
        "sync_token": token,
        "description": "memo",
      }
    ],
    "entries": [
      {
        "external_id": ext_id,
        "external_transaction_id": ext_id,
        "type": "standard",
        "posting_date": date(2026, 7, 15),
        "memo": "memo",
      }
    ],
    "line_items": [
      {
        "entry_external_id": ext_id,
        "element_external_id": expense,
        "debit_amount": amount,
        "credit_amount": 0,
        "line_order": 1,
      },
      {
        "entry_external_id": ext_id,
        "element_external_id": CASH,
        "debit_amount": 0,
        "credit_amount": amount,
        "line_order": 2,
      },
    ],
  }


def _sync(db, data):
  return OLTPLoader()._capture_transactions_as_events(
    db,
    data,
    source="quickbooks",
    connection_id=CONN,
    created_by="sync",
    now=datetime.now(UTC),
  )


def _net(db, element_id: str) -> int:
  return int(
    db.execute(
      text(
        "SELECT COALESCE(SUM(li.debit_amount - li.credit_amount), 0) "
        "FROM line_items li JOIN entries e ON e.id = li.entry_id "
        "WHERE li.element_id = :el AND e.status = 'posted'"
      ),
      {"el": element_id},
    ).scalar_one()
  )


def _catch_up(db, event_id: str):
  resolve_reconciling_item(
    db,
    ResolveReconcilingItemRequest(
      event_id=event_id,
      disposition="catch_up",
      posting_date=date(2026, 8, 31),
      status="posted",
    ),
    "user",
    graph_id=GRAPH_ID,
  )
  db.flush()


def _written_back_event(db, ids, *, source: str = "manual", publish_flag=None):
  """A RoboLedger draft in the state write-back leaves it once QuickBooks
  accepted it as JournalEntry_77."""
  meta = {
    "posting_date": "2026-07-15",
    "memo": "memo",
    "status": "draft",
    "line_items": [
      {"element_id": ids[EXP_A], "debit_amount": 10000, "credit_amount": 0},
      {"element_id": ids[CASH], "debit_amount": 0, "credit_amount": 10000},
    ],
  }
  if publish_flag is not None:
    meta["publish_to_source"] = publish_flag
  event, _ = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      event_class="economic",
      event_action="transfer",
      source=source,
      occurred_at=datetime(2026, 7, 15),
      amount=10000,
      apply_handlers=True,
      metadata=meta,
    ),
    "user",
    graph_id=GRAPH_ID,
  )
  db.flush()
  entry = db.query(Entry).filter(Entry.triggered_by_event_id == event.id).one()
  entry.status = "posted"
  entry.posted_at = datetime.now(UTC)
  meta = dict(event.metadata_)
  meta["qb_entry_ids"] = {str(entry.id): "JournalEntry_77"}
  meta["qb_external_id"] = "JournalEntry_77"
  event.metadata_ = meta
  event.status = "fulfilled"
  db.flush()
  return event


def test_an_edit_in_quickbooks_to_a_written_back_entry_is_a_reconciling_item(db):
  ids = _seed(db)
  event = _written_back_event(db, ids)
  first = _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0"))
  assert (first.cross_source_matched, first.inserted, first.drift_detected) == (1, 0, 0)

  # The accountant moves it to another account and changes the amount.
  edited = _sync(db, _dbt("JournalEntry_77", 12500, EXP_B, "1"))
  db.refresh(event)
  assert (edited.cross_source_matched, edited.inserted) == (1, 0)
  assert edited.drift_detected == 1
  assert event.payload_drift is True

  plan = plan_reconciling_item(db, str(event.id), graph_id=GRAPH_ID)
  assert {d.element_id: d.delta for d in plan.delta} == {
    ids[EXP_A]: -10000,
    ids[EXP_B]: 12500,
    ids[CASH]: -2500,
  }
  # QuickBooks already holds the change; the local entry is its origin.
  with pytest.raises(RestateBlockedError):
    resolve_reconciling_item(
      db,
      ResolveReconcilingItemRequest(event_id=str(event.id), disposition="restate"),
      "user",
      graph_id=GRAPH_ID,
    )

  _catch_up(db, str(event.id))
  db.refresh(event)
  assert (_net(db, ids[EXP_A]), _net(db, ids[EXP_B])) == (0, 12500)
  # The resolution keeps the event's own identity, so the round trip still
  # matches and the same payload is not raised again.
  assert event.metadata_["qb_external_id"] == "JournalEntry_77"
  again = _sync(db, _dbt("JournalEntry_77", 12500, EXP_B, "1"))
  db.refresh(event)
  assert (again.cross_source_matched, again.inserted, again.drift_detected) == (1, 0, 0)
  assert event.payload_drift is False


def test_a_second_edit_to_a_written_back_entry_posts_only_its_own_change(db):
  ids = _seed(db)
  event = _written_back_event(db, ids)
  _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0"))
  _sync(db, _dbt("JournalEntry_77", 12000, EXP_A, "1"))
  _catch_up(db, str(event.id))
  _sync(db, _dbt("JournalEntry_77", 13000, EXP_A, "2"))
  db.refresh(event)
  assert event.payload_drift is True

  plan = plan_reconciling_item(db, str(event.id), graph_id=GRAPH_ID)
  assert {d.element_id: d.delta for d in plan.delta}[ids[EXP_A]] == 1000
  _catch_up(db, str(event.id))
  assert _net(db, ids[EXP_A]) == 13000


def test_a_quickbooks_bill_edited_twice_nets_its_first_catch_up(db):
  ids = _seed(db)
  assert _sync(db, _dbt("Bill_9", 10000, EXP_A, "0", tx_type="Bill")).handler_dispatched
  event = db.query(Event).filter(Event.external_id == "Bill_9").one()
  _sync(db, _dbt("Bill_9", 12000, EXP_A, "1", tx_type="Bill"))
  _catch_up(db, str(event.id))
  assert _net(db, ids[EXP_A]) == 12000

  _sync(db, _dbt("Bill_9", 13000, EXP_A, "2", tx_type="Bill"))
  plan = plan_reconciling_item(db, str(event.id), graph_id=GRAPH_ID)
  assert {d.element_id: d.delta for d in plan.delta}[ids[EXP_A]] == 1000
  # Restating over a posted catch-up would leave that catch-up counted twice.
  assert any("catch-up" in b for b in plan.restate_blockers)
  _catch_up(db, str(event.id))
  assert _net(db, ids[EXP_A]) == 13000


@pytest.mark.parametrize("source", ["quickbooks", "stripe_sync"])
def test_a_published_event_of_any_source_is_matched(db, source):
  ids = _seed(db)
  _written_back_event(db, ids, source=source, publish_flag=True)
  result = _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0"))
  assert (result.cross_source_matched, result.inserted, result.handler_dispatched) == (
    1,
    0,
    0,
  )
  assert _net(db, ids[EXP_A]) == 10000


def test_a_marker_close_commits_during_a_sync_survives_it(db, monkeypatch):
  """Close records a second entry's QuickBooks id while a sync is matching."""
  import robosystems.operations.extensions.loader as loader_mod

  ids = _seed(db)
  event = _written_back_event(db, ids, source="schedule")
  db.commit()
  schema = db.execute(text("select current_schema()")).scalar_one()
  event_id = str(event.id)
  engine = db.get_bind()
  real_lock = loader_mod._lock_round_trip_events
  fired = []

  def close_commits_first(session, event_ids):
    if not fired:
      fired.append(True)
      with engine.begin() as other:
        other.execute(text(f'SET search_path TO "{schema}"'))
        meta = other.execute(
          text("select metadata from events where id = :i"), {"i": event_id}
        ).scalar_one()
        meta["qb_entry_ids"]["entry_feb"] = "JournalEntry_78"
        meta["qb_external_id"] = "JournalEntry_77,JournalEntry_78"
        other.execute(
          text("update events set metadata = cast(:m as jsonb) where id = :i"),
          {"m": json.dumps(meta), "i": event_id},
        )
    return real_lock(session, event_ids)

  monkeypatch.setattr(loader_mod, "_lock_round_trip_events", close_commits_first)
  _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0"))
  db.commit()

  meta = db.execute(
    text("select metadata from events where id = :i"), {"i": event_id}
  ).scalar_one()
  assert fired
  assert meta["qb_external_id"] == "JournalEntry_77,JournalEntry_78"
  later = _sync(db, _dbt("JournalEntry_78", 10000, EXP_A, "0"))
  assert (later.cross_source_matched, later.inserted) == (1, 0)


def test_a_captured_copy_of_a_round_trip_is_voided_not_booked(db):
  ids = _seed(db)
  _written_back_event(db, ids, source="schedule")
  copy = Event(
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    status="captured",
    source="quickbooks",
    external_id="JournalEntry_77",
    occurred_at=datetime(2026, 7, 15, tzinfo=UTC),
    amount=10000,
    currency="USD",
    metadata_={
      "status": "posted",
      "connection_id": CONN,
      "qb_sync_token": "0",
      "dispatch_error": "closed_period",
      "entries": [
        {
          "external_id": "JournalEntry_77",
          "type": "standard",
          "posting_date": "2026-07-15",
          "memo": "memo",
          "line_items": [
            {
              "element_external_id": EXP_A,
              "debit_amount": 10000,
              "credit_amount": 0,
              "line_order": 1,
            },
            {
              "element_external_id": CASH,
              "debit_amount": 0,
              "credit_amount": 10000,
              "line_order": 2,
            },
          ],
        }
      ],
    },
    created_by="sync",
  )
  db.add(copy)
  db.flush()

  result = _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0"))
  db.refresh(copy)
  assert (result.cross_source_matched, result.handler_dispatched) == (1, 0)
  assert copy.status == "voided"
  assert copy.metadata_["void_reason"] == "round_trip"
  assert _net(db, ids[EXP_A]) == 10000


def test_a_posted_copy_booked_before_the_match_is_raised_to_reverse(db):
  """A sync that ran while close was publishing booked QuickBooks' copy before
  close's marker was committed. The next sync raises it; its catch-up reverses
  the copy."""
  ids = _seed(db)
  copy_result = _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0"))
  assert copy_result.handler_dispatched == 1
  _written_back_event(db, ids)
  assert _net(db, ids[EXP_A]) == 20000

  result = _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0"))
  copy = (
    db.query(Event)
    .filter(Event.source == "quickbooks", Event.external_id == "JournalEntry_77")
    .one()
  )
  assert (result.cross_source_matched, result.drift_detected) == (1, 1)
  assert copy.payload_drift is True
  _catch_up(db, str(copy.id))
  assert _net(db, ids[EXP_A]) == 10000
  # Raised once: a later sync leaves the resolved copy alone.
  assert _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0")).drift_detected == 0


def test_an_edit_reverted_in_quickbooks_clears_its_item(db):
  ids = _seed(db)
  event = _written_back_event(db, ids)
  _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0"))
  _sync(db, _dbt("JournalEntry_77", 12000, EXP_A, "1"))
  db.refresh(event)
  assert event.payload_drift is True

  _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "2"))
  db.refresh(event)
  assert event.payload_drift is False
  assert "drift_payload" not in event.metadata_


def _two_entry_event(db, ids):
  event = _written_back_event(db, ids)
  create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      event_class="economic",
      event_action="transfer",
      source="manual",
      occurred_at=datetime(2026, 7, 15),
      amount=10000,
      apply_handlers=True,
      metadata={
        "posting_date": "2026-07-15",
        "memo": "second",
        "status": "draft",
        "line_items": [
          {"element_id": ids[EXP_A], "debit_amount": 10000, "credit_amount": 0},
          {"element_id": ids[CASH], "debit_amount": 0, "credit_amount": 10000},
        ],
      },
    ),
    "user",
    graph_id=GRAPH_ID,
  )
  db.flush()
  second = db.query(Entry).filter(Entry.memo == "second").one()
  second.triggered_by_event_id = event.id
  second.status = "posted"
  meta = dict(event.metadata_)
  meta["qb_entry_ids"] = {**meta["qb_entry_ids"], str(second.id): "JournalEntry_78"}
  meta["qb_external_id"] = "JournalEntry_77,JournalEntry_78"
  event.metadata_ = meta
  db.flush()
  return event


def test_two_edited_entries_of_one_event_are_raised_one_at_a_time(db):
  ids = _seed(db)
  event = _two_entry_event(db, ids)
  both = {
    key: _dbt("JournalEntry_77", 10000, EXP_A, "0")[key]
    + _dbt("JournalEntry_78", 10000, EXP_A, "0")[key]
    for key in ("transactions", "entries", "line_items")
  }
  _sync(db, both)
  edited = {
    key: _dbt("JournalEntry_77", 11000, EXP_A, "1")[key]
    + _dbt("JournalEntry_78", 12000, EXP_A, "1")[key]
    for key in ("transactions", "entries", "line_items")
  }
  raised = []
  for _ in range(3):
    result = _sync(db, edited)
    db.refresh(event)
    raised.append(
      (result.drift_detected, event.metadata_["drift_payload"]["round_trip"]["qb_id"])
    )
  assert raised == [
    (1, "JournalEntry_77"),
    (0, "JournalEntry_77"),
    (0, "JournalEntry_77"),
  ]

  _catch_up(db, str(event.id))
  _sync(db, edited)
  db.refresh(event)
  assert event.metadata_["drift_payload"]["round_trip"]["qb_id"] == "JournalEntry_78"
  _catch_up(db, str(event.id))
  assert _net(db, ids[EXP_A]) == 23000


def test_a_date_move_alone_raises_nothing(db):
  ids = _seed(db)
  event = _written_back_event(db, ids)
  _sync(db, _dbt("JournalEntry_77", 10000, EXP_A, "0"))
  moved = _dbt("JournalEntry_77", 10000, EXP_A, "1")
  moved["entries"][0]["posting_date"] = date(2026, 8, 3)
  assert _sync(db, moved).drift_detected == 0
  db.refresh(event)
  assert event.payload_drift is False
