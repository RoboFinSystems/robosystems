"""Resolving a reconciling item, against a real database.

The load-bearing claim in this feature is not "the flag flips" — it is that
the item **stays** resolved. The sync decides an event has changed by
comparing its stored metadata against the incoming payload, so a resolution
that clears the flag without replacing the payload lasts exactly until the
next sync. That is the defect being fixed, and it is invisible to a mocked
session: you would be asserting that the resolver wrote what the resolver
wrote.

So these tests run the resolver and then run the **real loader comparison**
over the result. They also exercise the deletes and the join the close gate
depends on, neither of which the in-memory stubs elsewhere in this suite can
model.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, datetime

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
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.commands import create_event_block_in_session
from robosystems.operations.extensions.loader import comparable_payload
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from robosystems.operations.roboledger.commands.reconciling_items import (
  NotAReconcilingItemError,
  RestateBlockedError,
  find_unresolved_reconciling_items,
  plan_reconciling_item,
  resolve_reconciling_item,
)
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  select_writeback_eligible_entries,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
CONNECTION_ID = "conn_test"
PERIOD_START = date(2026, 7, 1)
PERIOD_END = date(2026, 7, 31)
OPEN_START = date(2026, 8, 1)
OPEN_END = date(2026, 8, 31)

# The account the source system moved the expense *from* and *to* — the
# shape every live reconciling item on the reference tenant has: one line's
# account changed, the amount did not.
CASH = "152"
OLD_EXPENSE = "118"
NEW_EXPENSE = "25"
AMOUNT = 5360


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_recon_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema}"'))

  db = sessionmaker(bind=engine)()
  db.execute(text(f'SET search_path TO "{schema}"'))
  ExtensionsBase.metadata.create_all(bind=db.connection())
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
  """Let the extensions-side writes run without a platform database.

  ``create_event_block`` validates that the event's source and any routing
  connection are registered on the graph, and both answers live in the
  platform DB. This suite is about what happens in the tenant schema, so the
  two checks are stubbed rather than dragging a second database into the
  fixture — their own behaviour is covered in
  `tests/operations/event_block/`.
  """
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_event_source",
    lambda source, graph_id: None,
  )
  monkeypatch.setattr(
    "robosystems.operations.event_block.commands._validate_routed_connection",
    lambda metadata, graph_id: None,
  )


def _seed_elements(db) -> dict[str, str]:
  ids: dict[str, str] = {}
  for external_id, code, name in (
    (CASH, "1000", "Checking"),
    (OLD_EXPENSE, "6100", "Cloud Services"),
    (NEW_EXPENSE, "6200", "Cost of Goods Sold"),
  ):
    element = Element(
      name=name,
      code=code,
      external_id=external_id,
      external_source="quickbooks",
      connection_id=CONNECTION_ID,
      created_by="test",
    )
    db.add(element)
    db.flush()
    ids[external_id] = str(element.id)
  return ids


def _seed_periods(db) -> None:
  db.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      name="2026-07",
      start_date=PERIOD_START,
      end_date=PERIOD_END,
      period_type="month",
      status="open",
    )
  )
  db.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      name="2026-08",
      start_date=OPEN_START,
      end_date=OPEN_END,
      period_type="month",
      status="open",
    )
  )
  db.flush()


def _incoming_payload(*, expense_account: str) -> dict:
  """The metadata blob the QuickBooks loader builds for this transaction."""
  return {
    "status": "posted",
    "qb_txn_type": "Expense",
    "qb_doc_number": None,
    "qb_reference_number": None,
    "qb_merchant_name": None,
    "qb_due_date": None,
    "qb_category": None,
    "qb_source_class": "Expense",
    "qb_agent_external_id": None,
    "qb_linked_txns": [],
    "qb_sync_token": None,
    "connection_id": CONNECTION_ID,
    "entries": [
      {
        "external_id": "Expense_1737",
        "type": "standard",
        "posting_date": "2026-07-09",
        "number": None,
        "memo": "QB Expense Expense_1737",
        "line_items": [
          {
            "line_order": 1,
            "description": "Amazon Web Services",
            "debit_amount": 0,
            "credit_amount": AMOUNT,
            "element_external_id": CASH,
          },
          {
            "line_order": 2,
            "description": "Amazon Web Services",
            "debit_amount": AMOUNT,
            "credit_amount": 0,
            "element_external_id": expense_account,
          },
        ],
      }
    ],
  }


def _post_synced_event(db) -> Event:
  """A QuickBooks transaction posted the way the loader posts one."""
  event, _envelope = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="purchase",
      event_class="economic",
      event_action="transfer",
      source="quickbooks",
      external_id="Expense_1737",
      occurred_at=datetime(2026, 7, 9),
      amount=AMOUNT,
      apply_handlers=True,
      metadata=_incoming_payload(expense_account=OLD_EXPENSE),
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  db.flush()
  return event


def _flag_as_reconciling(db, event: Event) -> dict:
  """Flag the event the way the loader's drift branch does."""
  accepted = _incoming_payload(expense_account=NEW_EXPENSE)
  event.payload_drift = True
  event.metadata_ = {
    **dict(event.metadata_ or {}),
    "drift_payload": accepted,
    "drift_detected_at": datetime(2026, 8, 20, 3, 32).isoformat(),
  }
  db.flush()
  return accepted


def _line_nets(db, event_id: str) -> dict[str, int]:
  """Net debit-positive amount per element across the event's live rows."""
  rows = db.execute(
    text("""
      SELECT li.element_id, SUM(li.debit_amount - li.credit_amount) AS net
      FROM line_items li
      JOIN entries e ON e.id = li.entry_id
      WHERE e.triggered_by_event_id = :event_id
      GROUP BY li.element_id
    """),
    {"event_id": event_id},
  ).all()
  return {element_id: int(net) for element_id, net in rows}


def _setup(db, *, july_status: str = "open"):
  elements = _seed_elements(db)
  _seed_periods(db)
  # The event is always posted into an open July — that is the only way it
  # could have got there. Closing comes after, as it does in life.
  event = _post_synced_event(db)
  accepted = _flag_as_reconciling(db, event)
  if july_status != "open":
    db.query(FiscalPeriod).filter(FiscalPeriod.name == "2026-07").update(
      {"status": july_status}, synchronize_session=False
    )
    db.flush()
  return elements, event, accepted


# ───────────────────────────────────────────────────────────────────────────
# The invariant: a resolved item stays resolved
# ───────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("disposition", ["restate", "catch_up", "acknowledge"])
def test_resolution_survives_the_next_sync(session, disposition):
  """Whatever the disposition, the loader stops seeing a difference.

  This is the whole point of the feature. It runs the real comparison the
  sync uses rather than asserting on the flag, because clearing the flag
  alone would pass a weaker test and fail in production a day later.
  """
  _elements, event, accepted = _setup(session)

  resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id),
      disposition=disposition,
      note="handled" if disposition == "acknowledge" else None,
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  session.refresh(event)

  assert event.payload_drift is False
  # The comparison the loader will run on the next sync, against the payload
  # it will send.
  assert comparable_payload(event.metadata_) == comparable_payload(accepted)
  assert find_unresolved_reconciling_items(session, as_of=OPEN_END) == []


def test_disposition_trail_is_recorded_and_not_compared(session):
  """The trail rides on the event without re-triggering the flag."""
  _elements, event, accepted = _setup(session)

  resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id), disposition="acknowledge", note="July alignment JE"
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  session.refresh(event)

  history = event.metadata_["reconciliation_history"]
  assert len(history) == 1
  assert history[0]["disposition"] == "acknowledge"
  assert history[0]["note"] == "July alignment JE"
  assert history[0]["resolved_by"] == "user_test"
  # Present on the row, absent from what the sync compares.
  assert "reconciliation_history" not in comparable_payload(event.metadata_)
  assert comparable_payload(event.metadata_) == comparable_payload(accepted)


def test_resolving_twice_is_refused_and_says_what_happened(session):
  _elements, event, _accepted = _setup(session)
  body = ResolveReconcilingItemRequest(
    event_id=str(event.id), disposition="acknowledge", note="handled"
  )
  resolve_reconciling_item(session, body, "user_test", graph_id=GRAPH_ID)
  session.flush()

  with pytest.raises(NotAReconcilingItemError) as excinfo:
    resolve_reconciling_item(session, body, "user_test", graph_id=GRAPH_ID)

  assert "already resolved" in str(excinfo.value)
  assert excinfo.value.last_disposition["disposition"] == "acknowledge"


# ───────────────────────────────────────────────────────────────────────────
# What each disposition does to the books
# ───────────────────────────────────────────────────────────────────────────


def test_preview_reads_the_difference_without_touching_anything(session):
  elements, event, _accepted = _setup(session)
  before = _line_nets(session, str(event.id))

  plan = plan_reconciling_item(session, str(event.id), graph_id=GRAPH_ID)

  assert plan.default_disposition == "restate"  # July is open
  assert plan.no_gl_effect is False
  moved = {line.element_id: line.delta for line in plan.delta}
  assert moved == {
    elements[OLD_EXPENSE]: -AMOUNT,
    elements[NEW_EXPENSE]: AMOUNT,
  }
  # Cash did not move, so it is not in the delta at all.
  assert elements[CASH] not in moved
  assert _line_nets(session, str(event.id)) == before
  assert event.payload_drift is True


def test_restate_rebuilds_the_entries_on_the_new_account(session):
  elements, event, _accepted = _setup(session)
  original_entry_ids = {
    str(row_id)
    for (row_id,) in session.execute(
      text("SELECT id FROM entries WHERE triggered_by_event_id = :e"),
      {"e": str(event.id)},
    ).all()
  }

  result = resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(event_id=str(event.id), disposition="restate"),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()

  nets = _line_nets(session, str(event.id))
  assert nets[elements[NEW_EXPENSE]] == AMOUNT
  assert elements[OLD_EXPENSE] not in nets
  assert nets[elements[CASH]] == -AMOUNT
  # The old rows are gone rather than sitting alongside the new ones.
  rebuilt = set(result.regenerated.entry_ids)
  assert rebuilt and rebuilt.isdisjoint(original_entry_ids)
  assert session.query(Entry).filter(Entry.id.in_(original_entry_ids)).count() == 0
  assert result.catch_up is None


def test_catch_up_leaves_history_alone_and_posts_the_difference(session):
  elements, event, _accepted = _setup(session)
  before = _line_nets(session, str(event.id))

  result = resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id), disposition="catch_up", posting_date=OPEN_END
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()

  # The original entries are untouched — that is what "prior statements
  # stand" means.
  assert _line_nets(session, str(event.id)) == before

  catch_up_nets = _line_nets(session, result.catch_up.event_id)
  assert catch_up_nets == {
    elements[OLD_EXPENSE]: -AMOUNT,
    elements[NEW_EXPENSE]: AMOUNT,
  }
  catch_up_event = session.get(Event, result.catch_up.event_id)
  assert catch_up_event.source == "system"
  assert catch_up_event.metadata_["publish_to_source"] is False
  assert catch_up_event.metadata_["reconciles_event_id"] == str(event.id)


def test_catch_up_entry_never_publishes_back_to_the_source(session):
  """The entry mirrors a change QuickBooks already has; sending it back
  would apply that change a second time."""
  _elements, event, _accepted = _setup(session)

  result = resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id), disposition="catch_up", posting_date=OPEN_END
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()

  eligible = select_writeback_eligible_entries(session, OPEN_START, OPEN_END)
  assert result.catch_up.entry_id not in {str(entry.id) for entry, _ in eligible}


def test_acknowledge_records_the_reference_and_writes_no_entries(session):
  _elements, event, _accepted = _setup(session)
  reference, _envelope = create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      event_class="economic",
      event_action="transfer",
      source="system",
      occurred_at=datetime(2026, 7, 31),
      apply_handlers=False,
      metadata={"note": "hand-authored alignment"},
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  entries_before = session.query(Entry).count()

  result = resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id),
      disposition="acknowledge",
      note="posted by hand at the July close",
      reference_event_id=str(reference.id),
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()

  assert session.query(Entry).count() == entries_before
  assert result.catch_up is None and result.regenerated is None
  session.refresh(event)
  assert event.metadata_["reconciliation_history"][0]["reference_event_id"] == str(
    reference.id
  )


# ───────────────────────────────────────────────────────────────────────────
# Refusals
# ───────────────────────────────────────────────────────────────────────────


def test_restate_refuses_a_closed_period_and_offers_catch_up(session):
  _elements, event, _accepted = _setup(session, july_status="closed")

  plan = plan_reconciling_item(session, str(event.id), graph_id=GRAPH_ID)
  assert plan.default_disposition == "catch_up"
  assert plan.closed_periods == ["2026-07"]
  assert any("closed period" in blocker for blocker in plan.restate_blockers)

  with pytest.raises((RestateBlockedError, ClosedPeriodError)) as excinfo:
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(event_id=str(event.id), disposition="restate"),
      "user_test",
      graph_id=GRAPH_ID,
    )
  assert "catch_up" in str(excinfo.value) or "Reopen" in str(excinfo.value)


def test_restate_refuses_when_another_event_shares_the_transaction(session):
  """Deleting the transaction would take the other event's entry with it."""
  elements, event, _accepted = _setup(session)
  transaction_id = (
    session.query(Entry.transaction_id)
    .filter(Entry.triggered_by_event_id == str(event.id))
    .scalar()
  )
  intruder = Entry(
    transaction_id=transaction_id,
    posting_date=date(2026, 7, 9),
    memo="unrelated",
    status="posted",
    provenance="manual_entry",
    triggered_by_event_id=None,
    created_by="user_test",
  )
  session.add(intruder)
  session.flush()
  session.add(
    LineItem(
      entry_id=intruder.id,
      element_id=elements[CASH],
      debit_amount=1,
      credit_amount=0,
      line_order=1,
    )
  )
  session.flush()

  with pytest.raises(RestateBlockedError) as excinfo:
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(event_id=str(event.id), disposition="restate"),
      "user_test",
      graph_id=GRAPH_ID,
    )
  assert "share this event's transaction" in str(excinfo.value)


def test_restate_refuses_to_draft_entries_under_a_fulfilled_event(session):
  """A terminal event over draft rows is a state neither side can fix.

  The handler only ever upgrades status to `fulfilled`, so regenerating
  draft entries beneath an already-fulfilled event would leave the two
  permanently disagreeing. Unreachable through QuickBooks — its payloads
  always say posted — but reachable for a source that does not auto-commit.
  """
  _elements, event, _accepted = _setup(session)
  assert event.status == "fulfilled"
  drafting_payload = _incoming_payload(expense_account=NEW_EXPENSE)
  drafting_payload["status"] = "draft"
  event.metadata_ = {**dict(event.metadata_), "drift_payload": drafting_payload}
  session.flush()

  plan = plan_reconciling_item(session, str(event.id), graph_id=GRAPH_ID)
  assert any("fulfilled" in blocker for blocker in plan.restate_blockers)

  with pytest.raises(RestateBlockedError, match="fulfilled"):
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(event_id=str(event.id), disposition="restate"),
      "user_test",
      graph_id=GRAPH_ID,
    )

  # The escape hatch the error names actually works.
  result = resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id), disposition="catch_up", posting_date=OPEN_END
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  assert result.catch_up is not None


def test_a_reflag_between_preview_and_resolve_is_refused(session):
  """Resolving would otherwise accept a payload the operator never saw."""
  _elements, event, _accepted = _setup(session)

  # A sync lands a newer payload after the plan was read but before the
  # resolve takes its lock.
  newer = _incoming_payload(expense_account=CASH)
  event.metadata_ = {
    **dict(event.metadata_),
    "drift_payload": newer,
    "drift_detected_at": datetime(2026, 8, 21, 4, 0).isoformat(),
  }
  session.flush()

  # `resolve` re-plans internally, so simulate the race by pinning the stamp
  # the plan was built from to the older value.
  from robosystems.operations.roboledger.commands import reconciling_items as module

  real_plan = module._plan_with_stamp

  def _stale_plan(*args, **kwargs):
    plan, _stamp = real_plan(*args, **kwargs)
    return plan, datetime(2026, 8, 20, 3, 32).isoformat()

  module._plan_with_stamp = _stale_plan
  try:
    with pytest.raises(RowLockedError, match="re-flagged"):
      resolve_reconciling_item(
        session,
        ResolveReconcilingItemRequest(
          event_id=str(event.id), disposition="acknowledge", note="x"
        ),
        "user_test",
        graph_id=GRAPH_ID,
      )
  finally:
    module._plan_with_stamp = real_plan

  assert event.payload_drift is True


def test_a_z_suffixed_stamp_does_not_read_as_a_reflag(session):
  """The guard compares the stored string, not a round-trip of it.

  `fromisoformat` accepts `Z` and re-serializes it as `+00:00`, so a guard
  that compared parsed-then-formatted values would refuse a `Z`-stamped
  item forever while reporting a race that never happened.
  """
  _elements, event, accepted = _setup(session)
  event.metadata_ = {
    **dict(event.metadata_),
    "drift_detected_at": "2026-08-20T03:32:04.690073Z",
  }
  session.flush()

  resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id), disposition="acknowledge", note="handled"
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  session.refresh(event)

  assert event.payload_drift is False
  assert comparable_payload(event.metadata_) == comparable_payload(accepted)


def test_an_unflagged_event_is_refused(session):
  _elements, _event, _accepted = _setup(session)
  clean = _post_synced_event_with_id(session, "Expense_9999")

  with pytest.raises(NotAReconcilingItemError):
    plan_reconciling_item(session, str(clean.id), graph_id=GRAPH_ID)


def _post_synced_event_with_id(db, external_id: str) -> Event:
  event, _envelope = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="purchase",
      event_class="economic",
      event_action="transfer",
      source="quickbooks",
      external_id=external_id,
      occurred_at=datetime(2026, 7, 10),
      amount=AMOUNT,
      apply_handlers=True,
      metadata=_incoming_payload(expense_account=OLD_EXPENSE),
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  db.flush()
  return event


# ───────────────────────────────────────────────────────────────────────────
# The close gate's lookup
# ───────────────────────────────────────────────────────────────────────────


def test_the_gate_lookup_finds_flagged_events_by_entry_date(session):
  _elements, event, _accepted = _setup(session)

  # July's entries are in scope for a July close and for any later one.
  assert find_unresolved_reconciling_items(session, as_of=PERIOD_END) == [
    (str(event.id), "Expense_1737")
  ]
  assert find_unresolved_reconciling_items(session, as_of=OPEN_END) == [
    (str(event.id), "Expense_1737")
  ]
  # A June close says nothing about a July difference.
  assert find_unresolved_reconciling_items(session, as_of=date(2026, 6, 30)) == []
