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
from robosystems.models.extensions import Entity
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.commands import create_event_block_in_session
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
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
from tests.ledger_entity import PARENT_ENTITY_ID, entity_account, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"


def _previewed(session, event_id: str):
  """The stamp of the preview a resolution is decided on."""
  return plan_reconciling_item(session, event_id, graph_id=GRAPH_ID).drift_detected_at


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
      entity_id=PARENT_ENTITY_ID,
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
      entity_id=PARENT_ENTITY_ID,
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
      expected_drift_detected_at=_previewed(session, str(event.id)),
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
      event_id=str(event.id),
      expected_drift_detected_at=_previewed(session, str(event.id)),
      disposition="acknowledge",
      note="July alignment JE",
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
    event_id=str(event.id),
    expected_drift_detected_at=_previewed(session, str(event.id)),
    disposition="acknowledge",
    note="handled",
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
    ResolveReconcilingItemRequest(
      event_id=str(event.id),
      expected_drift_detected_at=_previewed(session, str(event.id)),
      disposition="restate",
    ),
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
      event_id=str(event.id),
      expected_drift_detected_at=_previewed(session, str(event.id)),
      disposition="catch_up",
      posting_date=OPEN_END,
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
      event_id=str(event.id),
      expected_drift_detected_at=_previewed(session, str(event.id)),
      disposition="catch_up",
      posting_date=OPEN_END,
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()

  eligible = select_writeback_eligible_entries(
    session, OPEN_START, OPEN_END, entity_id=PARENT_ENTITY_ID
  )
  assert result.catch_up.entry_id not in {str(entry.id) for entry, _ in eligible}


def test_acknowledge_records_the_reference_and_writes_no_entries(session):
  elements, event, _accepted = _setup(session)
  reference, _envelope = create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      event_class="economic",
      event_action="transfer",
      source="system",
      occurred_at=datetime(2026, 7, 31),
      apply_handlers=True,
      metadata={
        "posting_date": "2026-07-31",
        "memo": "hand-authored alignment",
        "line_items": [
          {"element_id": elements[NEW_EXPENSE], "debit_amount": AMOUNT},
          {"element_id": elements[OLD_EXPENSE], "credit_amount": AMOUNT},
        ],
      },
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
      expected_drift_detected_at=_previewed(session, str(event.id)),
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


def _alignment_entry(
  session, debit, credit, item_ids, *, entity_id=None, apply_handlers=True
):
  return create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      source="system",
      entity_id=entity_id,
      occurred_at=datetime(2026, 8, 31),
      apply_handlers=apply_handlers,
      metadata={
        "posting_date": "2026-08-31",
        "memo": "Catch-up: reclass made in the source system for July",
        "publish_to_source": False,
        "line_items": [
          {"element_id": debit, "debit_amount": AMOUNT},
          {"element_id": credit, "credit_amount": AMOUNT},
        ],
        "resolves_reconciling_items": item_ids,
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )


def _alignments(session) -> int:
  return (
    session.query(Event).filter(Event.event_type == "journal_entry_recorded").count()
  )


def test_an_alignment_entry_that_names_its_item_acknowledges_it(session):
  elements, event, _accepted = _setup(session)

  alignment, _envelope = _alignment_entry(
    session, elements[NEW_EXPENSE], elements[OLD_EXPENSE], [str(event.id)]
  )
  session.flush()

  session.refresh(event)
  assert event.payload_drift is False
  (trail,) = event.metadata_["reconciliation_history"]
  assert trail["disposition"] == "acknowledge"
  assert trail["reference_event_id"] == str(alignment.id)
  # The item no longer reads as open, so nothing can catch it up again.
  with pytest.raises(NotAReconcilingItemError):
    plan_reconciling_item(session, str(event.id), graph_id=GRAPH_ID)


def test_naming_an_event_that_is_not_a_reconciling_item_refuses_the_entry(session):
  elements = _seed_elements(session)
  _seed_periods(session)
  settled = _post_synced_event(session)

  with pytest.raises(HandlerMetadataValidationError, match="resolves_reconciling"):
    _alignment_entry(
      session, elements[NEW_EXPENSE], elements[OLD_EXPENSE], [str(settled.id)]
    )


def test_one_refused_item_settles_none_of_them(session):
  elements, event, _accepted = _setup(session)
  settled = Event(
    entity_id=PARENT_ENTITY_ID,
    event_type="expense",
    event_category="purchase",
    source="manual",
    status="captured",
    occurred_at=datetime(2026, 7, 20),
    created_by="user_test",
  )
  session.add(settled)
  session.flush()
  before = _alignments(session)

  savepoint = session.begin_nested()
  with pytest.raises(HandlerMetadataValidationError):
    _alignment_entry(
      session,
      elements[NEW_EXPENSE],
      elements[OLD_EXPENSE],
      [str(event.id), str(settled.id)],
    )
  savepoint.rollback()

  session.refresh(event)
  assert event.payload_drift is True
  assert _alignments(session) == before


def test_another_entitys_entry_cannot_settle_the_item(session):
  _elements, event, _accepted = _setup(session)
  session.add(
    Entity(
      id="ent_test_sub",
      name="Sub LLC",
      is_parent=False,
      parent_entity_id=PARENT_ENTITY_ID,
      source="native",
      created_by="test",
    )
  )
  session.flush()
  debit = entity_account(session, "ent_test_sub", "Cost of Goods Sold")
  credit = entity_account(session, "ent_test_sub", "Cloud Services")

  with pytest.raises(HandlerMetadataValidationError, match="this entity's books"):
    _alignment_entry(session, debit, credit, [str(event.id)], entity_id="ent_test_sub")


def test_the_field_is_refused_where_nothing_would_act_on_it(session):
  elements, event, _accepted = _setup(session)

  with pytest.raises(HandlerMetadataValidationError, match="apply_handlers=true"):
    _alignment_entry(
      session,
      elements[NEW_EXPENSE],
      elements[OLD_EXPENSE],
      [str(event.id)],
      apply_handlers=False,
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
      ResolveReconcilingItemRequest(
        event_id=str(event.id),
        expected_drift_detected_at=_previewed(session, str(event.id)),
        disposition="restate",
      ),
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
    entity_id=PARENT_ENTITY_ID,
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
      ResolveReconcilingItemRequest(
        event_id=str(event.id),
        expected_drift_detected_at=_previewed(session, str(event.id)),
        disposition="restate",
      ),
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
      ResolveReconcilingItemRequest(
        event_id=str(event.id),
        expected_drift_detected_at=_previewed(session, str(event.id)),
        disposition="restate",
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )

  # The escape hatch the error names actually works.
  result = resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id),
      expected_drift_detected_at=_previewed(session, str(event.id)),
      disposition="catch_up",
      posting_date=OPEN_END,
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
          event_id=str(event.id),
          expected_drift_detected_at=_previewed(session, str(event.id)),
          disposition="acknowledge",
          note="x",
        ),
        "user_test",
        graph_id=GRAPH_ID,
      )
  finally:
    module._plan_with_stamp = real_plan

  assert event.payload_drift is True


@pytest.mark.parametrize("disposition", ["restate", "catch_up", "acknowledge"])
def test_a_reflag_after_the_preview_is_refused(session, disposition):
  """A sync that lands a newer payload between Review and Settle: the
  resolution was decided on the preview, so it must not settle the newer one."""
  _elements, event, _accepted = _setup(session)
  seen = _previewed(session, str(event.id))

  event.metadata_ = {
    **dict(event.metadata_),
    "drift_payload": _incoming_payload(expense_account=CASH),
    "drift_detected_at": datetime(2026, 8, 21, 4, 0).isoformat(),
  }
  session.flush()

  with pytest.raises(RowLockedError, match="re-flagged"):
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(
        event_id=str(event.id),
        expected_drift_detected_at=seen,
        disposition=disposition,
        posting_date=OPEN_END if disposition == "catch_up" else None,
        note="handled" if disposition == "acknowledge" else None,
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
  session.refresh(event)
  assert event.payload_drift is True


def test_a_resolution_without_a_stamp_still_resolves(session):
  """The stamp is optional for callers that predate it; they keep the check
  for a re-flag landing inside their own request."""
  _elements, event, _accepted = _setup(session)

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
      event_id=str(event.id),
      expected_drift_detected_at=_previewed(session, str(event.id)),
      disposition="acknowledge",
      note="handled",
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


# ───────────────────────────────────────────────────────────────────────────
# Bank-feed lines: restate posts what the bank now says, never the old row
# ───────────────────────────────────────────────────────────────────────────


def _post_bank_line(db, ids: dict[str, str]) -> Event:
  """A Plaid line classified and committed the way the inbox does it, with
  its draft entry posted at close."""
  from robosystems.operations.event_block.commands import fire_handler_on_commit

  event, _envelope = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="bank_transaction",
      event_category="purchase",
      event_class="economic",
      event_action="transfer",
      resource_type="money",
      source="plaid",
      external_id="plaid_txn_t1",
      occurred_at=datetime(2026, 7, 9),
      amount=-1240,
      resource_element_id=ids[CASH],
      apply_handlers=False,
      metadata={
        "connection_id": CONNECTION_ID,
        "transaction_id": "t1",
        "classified_element_id": ids[OLD_EXPENSE],
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  event.status = "committed"
  fire_handler_on_commit(db, event, "user_test")
  db.flush()
  db.query(Entry).filter(Entry.triggered_by_event_id == str(event.id)).update(
    {"status": "posted"}, synchronize_session=False
  )
  db.flush()
  return event


def test_restate_reposts_a_bank_line_at_the_banks_amount_and_date(session):
  from robosystems.adapters.plaid.pipeline.load import (
    PlaidLoadReport,
    reconcile_existing,
  )

  ids = _seed_elements(session)
  _seed_periods(session)
  event = _post_bank_line(session, ids)
  assert _line_nets(session, str(event.id)) == {
    ids[CASH]: -1240,
    ids[OLD_EXPENSE]: 1240,
  }

  # The bank now says $15.00 on the 10th; the feed flags a reconciling item.
  reconcile_existing(
    event,
    {"amount": -1500, "occurred_at": "2026-07-10T00:00:00Z", "metadata": {}},
    PlaidLoadReport(),
  )
  session.flush()
  plan = plan_reconciling_item(session, str(event.id), graph_id=GRAPH_ID)
  assert plan.default_disposition == "restate" and not plan.restate_blockers

  resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id),
      expected_drift_detected_at=_previewed(session, str(event.id)),
      disposition="restate",
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  assert not event.payload_drift
  assert _line_nets(session, str(event.id)) == {
    ids[CASH]: -1500,
    ids[OLD_EXPENSE]: 1500,
  }
  entry = (
    session.query(Entry).filter(Entry.triggered_by_event_id == str(event.id)).one()
  )
  assert entry.posting_date == date(2026, 7, 10)


def test_a_retracted_bank_line_is_reversed_never_restated(session):
  from robosystems.adapters.plaid.pipeline.load import _flag_removed

  ids = _seed_elements(session)
  _seed_periods(session)
  event = _post_bank_line(session, ids)
  _flag_removed(event, ["t1"])
  session.flush()

  plan = plan_reconciling_item(session, str(event.id), graph_id=GRAPH_ID)
  assert plan.default_disposition == "catch_up"
  assert any("retracted" in blocker for blocker in plan.restate_blockers)
  with pytest.raises(RestateBlockedError, match="retracted"):
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(
        event_id=str(event.id),
        expected_drift_detected_at=_previewed(session, str(event.id)),
        disposition="restate",
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
  # The books are exactly as they were.
  assert _line_nets(session, str(event.id)) == {
    ids[CASH]: -1240,
    ids[OLD_EXPENSE]: 1240,
  }

  result = resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(event.id),
      expected_drift_detected_at=_previewed(session, str(event.id)),
      disposition="catch_up",
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  assert {line.element_id: line.delta for line in result.delta} == {
    ids[CASH]: 1240,
    ids[OLD_EXPENSE]: -1240,
  }
  assert result.catch_up is not None
  assert not event.payload_drift


# ───────────────────────────────────────────────────────────────────────────
# An acknowledgement is a booking: an alignment entry that names an item is
# pinned to the preview, counted while it stands, and re-opens the item when
# it is retracted. Driven through the real QuickBooks loader drift branch.
# ───────────────────────────────────────────────────────────────────────────

QB_CASH = "1000"
QB_REPAIRS = "5000"
QB_TXN = "Expense_77"


def _qb_accounts(db) -> dict[str, str]:
  ids: dict[str, str] = {}
  for external_id, code, name in (
    (QB_CASH, "1000", "Checking"),
    (QB_REPAIRS, "6100", "Repairs"),
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
    ids[name] = str(element.id)
  _seed_periods(db)
  return ids


def _qb_expense(amount: int, sync_token: str) -> dict:
  """One QuickBooks expense, as the extract hands it to the loader."""
  return {
    "transactions": [
      {
        "external_id": QB_TXN,
        "external_source": "quickbooks",
        "number": "77",
        "type": "Expense",
        "category": None,
        "amount": amount,
        "currency": "USD",
        "date": date(2026, 7, 9),
        "due_date": None,
        "merchant_name": "Fixit Co",
        "reference_number": None,
        "description": "Repairs",
        "source_id": QB_TXN,
        "status": "posted",
        "sync_token": sync_token,
      }
    ],
    "entries": [
      {
        "external_id": QB_TXN,
        "external_transaction_id": QB_TXN,
        "external_source": "quickbooks",
        "number": "77",
        "type": "standard",
        "posting_date": date(2026, 7, 9),
        "memo": "Repairs",
        "status": "posted",
      }
    ],
    "line_items": [
      {
        "entry_external_id": QB_TXN,
        "element_external_id": QB_REPAIRS,
        "debit_amount": amount,
        "credit_amount": 0,
        "description": "Repairs",
        "line_order": 1,
      },
      {
        "entry_external_id": QB_TXN,
        "element_external_id": QB_CASH,
        "debit_amount": 0,
        "credit_amount": amount,
        "description": "Cash",
        "line_order": 2,
      },
    ],
  }


def _qb_sync(db, amount: int, sync_token: str) -> None:
  from robosystems.operations.extensions.loader import OLTPLoader

  OLTPLoader()._capture_transactions_as_events(
    db,
    _qb_expense(amount, sync_token),
    source="quickbooks",
    connection_id=CONNECTION_ID,
    created_by="sync",
    now=datetime.now(UTC),
  )
  db.flush()


def _qb_item(db) -> Event:
  event = db.query(Event).filter(Event.external_id == QB_TXN).one()
  db.refresh(event)
  return event


def _repairs_delta(db, ids, event_id: str) -> int | None:
  plan = plan_reconciling_item(db, event_id, graph_id=GRAPH_ID)
  deltas = {line.element_id: line.delta for line in plan.delta}
  return deltas.get(ids["Repairs"])


def _repairs_ledger(db, ids) -> int:
  return int(
    db.execute(
      text(
        "SELECT COALESCE(SUM(li.debit_amount - li.credit_amount), 0) "
        "FROM line_items li JOIN entries e ON e.id = li.entry_id "
        "WHERE li.element_id = :element_id AND e.status <> 'reversed' "
        "AND e.reversal_of IS NULL"
      ),
      {"element_id": ids["Repairs"]},
    ).scalar_one()
  )


def _named(item: Event, db, *, stamp: bool = True):
  """How an alignment names the item: with the stamp of the preview it was
  written from, the way the preview hands it over."""
  if not stamp:
    return str(item.id)
  plan = plan_reconciling_item(db, str(item.id), graph_id=GRAPH_ID)
  assert plan.drift_detected_at is not None
  return {
    "event_id": str(item.id),
    "expected_drift_detected_at": plan.drift_detected_at.isoformat(),
  }


def _hand_alignment(db, ids, cents: int, named: list, *, status: str = "draft"):
  event, _envelope = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      source="manual",
      occurred_at=datetime(2026, 8, 31),
      apply_handlers=True,
      metadata={
        "posting_date": "2026-08-31",
        "memo": "Alignment: QuickBooks edit to July repairs",
        "publish_to_source": False,
        "status": status,
        "line_items": [
          {"element_id": ids["Repairs"], "debit_amount": cents},
          {"element_id": ids["Checking"], "credit_amount": cents},
        ],
        "resolves_reconciling_items": named,
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  db.flush()
  return event


def _flagged_at_1500(db) -> tuple[dict[str, str], Event]:
  ids = _qb_accounts(db)
  _qb_sync(db, 1000, "0")
  _qb_sync(db, 1500, "1")
  item = _qb_item(db)
  assert item.payload_drift is True
  return ids, item


# Pinned ───────────────────────────────────────────────────────────────────


def test_an_alignment_from_a_preview_the_item_has_moved_past_is_refused(session):
  """The alignment was written from the +500 preview; a sync re-flagged the
  item at 2,000 before it was recorded. It must not settle the newer payload."""
  ids, item = _flagged_at_1500(session)
  named = _named(item, session)
  _qb_sync(session, 2000, "2")

  savepoint = session.begin_nested()
  with pytest.raises(RowLockedError, match="re-flagged"):
    _hand_alignment(session, ids, 500, [named])
  savepoint.rollback()

  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == 1000


def test_an_alignment_carrying_the_previewed_stamp_settles_the_item(session):
  ids, item = _flagged_at_1500(session)

  alignment = _hand_alignment(session, ids, 500, [_named(item, session)])

  item = _qb_item(session)
  assert item.payload_drift is False
  (record,) = item.metadata_["reconciliation_history"]
  assert record["reference_event_id"] == str(alignment.id)


def test_one_item_named_twice_with_different_stamps_is_refused(session):
  ids, item = _flagged_at_1500(session)
  named = _named(item, session)

  with pytest.raises(HandlerMetadataValidationError, match="twice"):
    _hand_alignment(
      session,
      ids,
      500,
      [named, {**named, "expected_drift_detected_at": "2026-01-01T00:00:00+00:00"}],
    )


# Counted ──────────────────────────────────────────────────────────────────


def test_a_second_edit_after_an_alignment_posts_only_its_own_change(session):
  """RC1: QuickBooks 1,000 → 1,500, settled by a +500 alignment, then 1,800.
  The alignment already booked 500 of it, so the next change is +300."""
  ids, item = _flagged_at_1500(session)
  _hand_alignment(session, ids, 500, [_named(item, session)])

  _qb_sync(session, 1800, "2")
  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == 300

  resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(item.id),
      expected_drift_detected_at=_previewed(session, str(item.id)),
      disposition="catch_up",
      posting_date=OPEN_END,
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  assert _repairs_ledger(session, ids) == 1800


def test_restate_is_blocked_while_an_alignment_levels_the_item(session):
  """Restating rebuilds the event's own entries at the new figure; with the
  alignment still standing, its 500 would be counted twice."""
  ids, item = _flagged_at_1500(session)
  _hand_alignment(session, ids, 500, [_named(item, session)])
  _qb_sync(session, 1800, "2")
  item = _qb_item(session)

  plan = plan_reconciling_item(session, str(item.id), graph_id=GRAPH_ID)
  assert plan.default_disposition == "catch_up"
  assert any("alignment" in blocker for blocker in plan.restate_blockers)


def test_a_plain_acknowledge_naming_its_entry_is_counted_too(session):
  """The REST acknowledge that names the entry which handled the difference
  is the same booking as an alignment that names the item."""
  ids, item = _flagged_at_1500(session)
  alignment = _hand_alignment(session, ids, 500, [])
  resolve_reconciling_item(
    session,
    ResolveReconcilingItemRequest(
      event_id=str(item.id),
      expected_drift_detected_at=_previewed(session, str(item.id)),
      disposition="acknowledge",
      note="booked by hand",
      reference_event_id=str(alignment.id),
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()

  _qb_sync(session, 1800, "2")
  assert _repairs_delta(session, ids, str(_qb_item(session).id)) == 300


def test_a_reverted_edit_after_an_alignment_re_raises_the_difference(session):
  """RC3: the alignment booked +500; QuickBooks then went back to 1,000. The
  re-raised item must ask for the 500 back, not read as no change."""
  ids, item = _flagged_at_1500(session)
  _hand_alignment(session, ids, 500, [_named(item, session)])

  _qb_sync(session, 1000, "2")
  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == -500


def test_an_edit_reverted_before_anyone_settles_it_clears_the_flag(session):
  """P1: 1,000 → 1,500 → 1,000 before anyone settled it. QuickBooks is back
  at the figure the ledger holds, so nothing is owed."""
  _ids, _item = _flagged_at_1500(session)

  _qb_sync(session, 1000, "2")

  item = _qb_item(session)
  assert item.payload_drift is False
  assert "drift_payload" not in item.metadata_
  assert "drift_detected_at" not in item.metadata_
  assert item.metadata_["qb_sync_token"] == "2"


# Re-opens ─────────────────────────────────────────────────────────────────


def _void(db, event_id: str) -> None:
  from robosystems.models.api.event_block import UpdateEventBlockRequest
  from robosystems.operations.event_block.commands import update_event_block

  update_event_block(
    db,
    UpdateEventBlockRequest(event_id=event_id, transition_to="voided"),
    "user_test",
    graph_id=GRAPH_ID,
  )


def test_voiding_an_alignment_reopens_the_items_it_settled(session):
  """RC8: the alignment draft is voided before the close posts it. The item
  owes the books its +500 again, and the next sync does not clear it."""
  ids, item = _flagged_at_1500(session)
  alignment = _hand_alignment(session, ids, 500, [_named(item, session)])
  session.commit()

  _void(session, str(alignment.id))

  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == 500
  assert item.metadata_["reconciliation_history"][-1]["disposition"] == "reopened"

  _qb_sync(session, 1500, "2")
  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == 500


def test_reversing_an_alignment_reopens_the_items_it_settled(session):
  from robosystems.models.api.extensions.journal_entries import (
    ReverseJournalEntryRequest,
  )
  from robosystems.operations.roboledger.commands.journal_entries import (
    reverse_journal_entry,
  )

  ids, item = _flagged_at_1500(session)
  alignment = _hand_alignment(
    session, ids, 500, [_named(item, session)], status="posted"
  )
  (entry,) = session.query(Entry).filter(
    Entry.triggered_by_event_id == str(alignment.id)
  )

  reverse_journal_entry(
    session,
    ReverseJournalEntryRequest(entry_id=str(entry.id), posting_date=OPEN_END),
    "user_test",
  )
  session.flush()

  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == 500


def test_a_reopened_item_stays_owed_through_an_edit_and_its_revert(session):
  """Re-opened at the accepted 1,500 (the ledger holds 1,000). QuickBooks
  then moves to 1,800 and back to 1,500: still 500 owed, never cleared."""
  ids, item = _flagged_at_1500(session)
  alignment = _hand_alignment(session, ids, 500, [_named(item, session)])
  session.commit()
  _void(session, str(alignment.id))

  _qb_sync(session, 1800, "2")
  assert _repairs_delta(session, ids, str(_qb_item(session).id)) == 800
  _qb_sync(session, 1500, "3")

  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == 500


def test_a_preview_taken_before_the_reopen_is_refused(session):
  """The plan changed when the alignment stopped counting; a resolution
  decided on the earlier preview must be previewed again."""
  ids, item = _flagged_at_1500(session)
  alignment = _hand_alignment(session, ids, 500, [_named(item, session)])
  _qb_sync(session, 1800, "2")
  item = _qb_item(session)
  seen = _previewed(session, str(item.id))
  session.commit()
  _void(session, str(alignment.id))

  with pytest.raises(RowLockedError, match="re-flagged"):
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(
        event_id=str(item.id),
        expected_drift_detected_at=seen,
        disposition="catch_up",
        posting_date=OPEN_END,
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
  assert _repairs_delta(session, ids, str(_qb_item(session).id)) == 800


def test_voiding_an_alignment_restores_a_round_trip_items_baseline(session):
  """An entry written back to QuickBooks and edited there is measured from a
  stored baseline; acknowledging moves it, so re-opening must move it back."""
  from robosystems.operations.event_block.qb_writeback import ROUND_TRIP_BASELINE_KEY
  from robosystems.operations.extensions.loader import _round_trip_update

  ids = _qb_accounts(session)
  baseline = {
    "posting_dates": ["2026-07-09"],
    "lines": [[QB_CASH, 0, 1000], [QB_REPAIRS, 1000, 0]],
  }
  edited = {
    "posting_dates": ["2026-07-09"],
    "lines": [[QB_CASH, 0, 1500], [QB_REPAIRS, 1500, 0]],
  }
  written_back = Event(
    entity_id=PARENT_ENTITY_ID,
    event_type="journal_entry_recorded",
    event_category="adjustment",
    source="manual",
    status="fulfilled",
    occurred_at=datetime(2026, 7, 9),
    created_by="user_test",
    payload_drift=True,
    metadata_={
      "qb_external_id": "301",
      "qb_sync_confirmed_at": "2026-07-10T00:00:00+00:00",
      ROUND_TRIP_BASELINE_KEY: {"301": baseline},
      "drift_payload": {
        "round_trip": {
          "qb_id": "301",
          "entry_ids": [],
          "baseline": baseline,
          "accepted": edited,
        },
        "entries": [],
        "connection_id": CONNECTION_ID,
      },
      "drift_detected_at": "2026-08-20T03:32:00+00:00",
    },
  )
  session.add(written_back)
  session.flush()
  assert _repairs_delta(session, ids, str(written_back.id)) == 500

  alignment = _hand_alignment(session, ids, 500, [_named(written_back, session)])
  session.refresh(written_back)
  assert written_back.metadata_[ROUND_TRIP_BASELINE_KEY]["301"] == edited
  session.commit()

  _void(session, str(alignment.id))

  session.refresh(written_back)
  assert written_back.payload_drift is True
  assert written_back.metadata_[ROUND_TRIP_BASELINE_KEY]["301"] == baseline
  assert _repairs_delta(session, ids, str(written_back.id)) == 500

  # QuickBooks still holds the edit: the next sync leaves the item owed.
  qb_entries = [
    {
      "posting_date": date(2026, 7, 9),
      "line_items": [
        {"element_external_id": QB_CASH, "debit_amount": 0, "credit_amount": 1500},
        {"element_external_id": QB_REPAIRS, "debit_amount": 1500, "credit_amount": 0},
      ],
    }
  ]
  _meta, change = _round_trip_update(
    written_back,
    {"301": {"entries": qb_entries}},
    session,
    connection_id=CONNECTION_ID,
    now=datetime.now(UTC),
  )
  assert change != "clear"


# The capture-then-commit door ─────────────────────────────────────────────


def test_an_alignment_committed_from_capture_settles_the_items_it_names(session):
  """RC9: a journal entry captured first and committed with the items named
  in the commit's patch settles them, pinned like the create."""
  from robosystems.models.api.event_block import UpdateEventBlockRequest
  from robosystems.operations.event_block.commands import update_event_block

  ids, item = _flagged_at_1500(session)
  captured, _envelope = create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      source="manual",
      occurred_at=datetime(2026, 8, 31),
      apply_handlers=False,
      metadata={
        "posting_date": "2026-08-31",
        "memo": "Alignment: QuickBooks edit to July repairs",
        "publish_to_source": False,
        "line_items": [
          {"element_id": ids["Repairs"], "debit_amount": 500},
          {"element_id": ids["Checking"], "credit_amount": 500},
        ],
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  named = _named(item, session)
  session.commit()

  update_event_block(
    session,
    UpdateEventBlockRequest(
      event_id=str(captured.id),
      transition_to="committed",
      metadata_patch={"resolves_reconciling_items": [named]},
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )

  item = _qb_item(session)
  assert item.payload_drift is False
  record = item.metadata_["reconciliation_history"][-1]
  assert record["reference_event_id"] == str(captured.id)


def test_naming_items_on_an_update_that_posts_nothing_is_refused(session):
  from robosystems.models.api.event_block import UpdateEventBlockRequest
  from robosystems.operations.event_block.commands import update_event_block

  ids, item = _flagged_at_1500(session)
  alignment = _hand_alignment(session, ids, 500, [])
  session.commit()

  with pytest.raises(HandlerMetadataValidationError, match="resolves_reconciling"):
    update_event_block(
      session,
      UpdateEventBlockRequest(
        event_id=str(alignment.id),
        metadata_patch={"resolves_reconciling_items": [str(item.id)]},
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
  session.rollback()
  assert _qb_item(session).payload_drift is True


# Round 2: every door that books an item's change ─────────────────────────


def _repairs_books(db, ids) -> int:
  """Repairs as the close will leave it: landed entries, plus the drafts of
  events still standing."""
  return int(
    db.execute(
      text(
        "SELECT COALESCE(SUM(li.debit_amount - li.credit_amount), 0) "
        "FROM line_items li JOIN entries e ON e.id = li.entry_id "
        "LEFT JOIN events ev ON ev.id = e.triggered_by_event_id "
        "WHERE li.element_id = :element_id AND (e.status IN ('posted', 'reversed') "
        "OR (e.status = 'draft' AND (ev.id IS NULL "
        "OR ev.status NOT IN ('voided', 'superseded'))))"
      ),
      {"element_id": ids["Repairs"]},
    ).scalar_one()
  )


def _supersede(db, event_id: str, successor_id: str) -> None:
  from robosystems.models.api.event_block import UpdateEventBlockRequest
  from robosystems.operations.event_block.commands import update_event_block

  update_event_block(
    db,
    UpdateEventBlockRequest(
      event_id=event_id,
      transition_to="superseded",
      superseded_by_id=successor_id,
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )


def _plain_entry(db, ids, cents: int, *, metadata: dict | None = None) -> Event:
  """A hand entry that names no item, publishing by its source's default."""
  event, _envelope = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      source="manual",
      occurred_at=datetime(2026, 8, 31),
      apply_handlers=True,
      metadata={
        "posting_date": "2026-08-31",
        "memo": "Corrected alignment",
        "status": "draft",
        "line_items": [
          {"element_id": ids["Repairs"], "debit_amount": cents},
          {"element_id": ids["Checking"], "credit_amount": cents},
        ],
        **(metadata or {}),
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  db.flush()
  return event


_LOCAL = {"publish_to_source": False}


def _publishes_at_close(db, event_id: str) -> bool:
  eligible = select_writeback_eligible_entries(
    db, OPEN_START, OPEN_END, entity_id=PARENT_ENTITY_ID
  )
  return any(str(event.id) == event_id for _entry, event in eligible)


def _catch_up(db, item_id: str, *, status: str = "draft") -> str:
  result = resolve_reconciling_item(
    db,
    ResolveReconcilingItemRequest(
      event_id=item_id,
      expected_drift_detected_at=_previewed(db, item_id),
      disposition="catch_up",
      posting_date=OPEN_END,
      status=status,
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  db.flush()
  assert result.catch_up is not None
  return result.catch_up.event_id


def test_superseding_an_alignment_moves_its_settlement_to_the_successor(session):
  """A-R1-1: the documented correction. The successor books the same +500,
  so the item stays settled, now by the successor, and the books equal
  QuickBooks. The successor settles a QuickBooks change, so it stays local."""
  ids, item = _flagged_at_1500(session)
  first = _hand_alignment(session, ids, 500, [_named(item, session)])
  successor = _plain_entry(session, ids, 500, metadata=_LOCAL)
  session.commit()

  _supersede(session, str(first.id), str(successor.id))

  item = _qb_item(session)
  assert item.payload_drift is False
  record = item.metadata_["reconciliation_history"][-1]
  assert record["disposition"] == "acknowledge"
  assert record["reference_event_id"] == str(successor.id)
  assert record["superseded_event_id"] == str(first.id)
  assert _repairs_books(session, ids) == 1500
  assert not _publishes_at_close(session, str(successor.id))

  # The successor counts toward the next change, as the original did.
  _qb_sync(session, 1800, "2")
  item = _qb_item(session)
  assert _repairs_delta(session, ids, str(item.id)) == 300
  _catch_up(session, str(item.id))
  assert _repairs_books(session, ids) == 1800


def test_voiding_the_successor_reopens_the_item_it_took_over(session):
  ids, item = _flagged_at_1500(session)
  first = _hand_alignment(session, ids, 500, [_named(item, session)])
  successor = _plain_entry(session, ids, 500, metadata=_LOCAL)
  session.commit()
  _supersede(session, str(first.id), str(successor.id))
  session.commit()

  _void(session, str(successor.id))

  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == 500


def test_a_successor_booking_a_different_amount_cannot_take_the_settlement(session):
  """The settlement counts the +500 the item's preview named. A successor
  booking +600 would be counted at a figure it does not book, and re-opening
  would leave it uncounted: the supersede is refused, and nothing moves."""
  ids, item = _flagged_at_1500(session)
  first = _hand_alignment(session, ids, 500, [_named(item, session)])
  successor = _plain_entry(session, ids, 600, metadata=_LOCAL)
  session.commit()

  with pytest.raises(ValueError, match="different amount"):
    _supersede(session, str(first.id), str(successor.id))
  session.rollback()

  item = _qb_item(session)
  assert item.payload_drift is False
  assert session.get(Event, str(first.id)).status != "superseded"


def test_voiding_a_catch_up_reopens_its_item(session):
  """A-R1-2: the catch-up draft is voided at review. The difference it
  booked is owed again, through the next sync too."""
  ids, item = _flagged_at_1500(session)
  catch_up = _catch_up(session, str(item.id))
  session.commit()

  _void(session, catch_up)

  item = _qb_item(session)
  assert item.payload_drift is True
  assert item.metadata_["reconciliation_history"][-1]["disposition"] == "reopened"
  assert _repairs_delta(session, ids, str(item.id)) == 500
  _qb_sync(session, 1500, "2")
  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == 500


def test_reversing_a_posted_catch_up_reopens_its_item(session):
  from robosystems.models.api.extensions.journal_entries import (
    ReverseJournalEntryRequest,
  )
  from robosystems.operations.roboledger.commands.journal_entries import (
    reverse_journal_entry,
  )

  ids, item = _flagged_at_1500(session)
  catch_up = _catch_up(session, str(item.id), status="posted")
  (entry,) = session.query(Entry).filter(Entry.triggered_by_event_id == catch_up)

  reverse_journal_entry(
    session,
    ReverseJournalEntryRequest(entry_id=str(entry.id), posting_date=OPEN_END),
    "user_test",
  )
  session.flush()

  item = _qb_item(session)
  assert item.payload_drift is True
  assert _repairs_delta(session, ids, str(item.id)) == 500
  _catch_up(session, str(item.id))
  assert _repairs_books(session, ids) == 1500


def test_voiding_a_catch_up_restores_a_round_trip_items_baseline(session):
  """A catch-up on an entry written back and edited in QuickBooks moves its
  baseline like an acknowledgement does, so voiding it moves it back."""
  from robosystems.operations.event_block.qb_writeback import ROUND_TRIP_BASELINE_KEY

  ids = _qb_accounts(session)
  baseline = {
    "posting_dates": ["2026-07-09"],
    "lines": [[QB_CASH, 0, 1000], [QB_REPAIRS, 1000, 0]],
  }
  edited = {
    "posting_dates": ["2026-07-09"],
    "lines": [[QB_CASH, 0, 1500], [QB_REPAIRS, 1500, 0]],
  }
  written_back = Event(
    entity_id=PARENT_ENTITY_ID,
    event_type="journal_entry_recorded",
    event_category="adjustment",
    source="manual",
    status="fulfilled",
    occurred_at=datetime(2026, 7, 9),
    created_by="user_test",
    payload_drift=True,
    metadata_={
      "qb_external_id": "302",
      ROUND_TRIP_BASELINE_KEY: {"302": baseline},
      "drift_payload": {
        "round_trip": {
          "qb_id": "302",
          "entry_ids": [],
          "baseline": baseline,
          "accepted": edited,
        },
        "entries": [],
        "connection_id": CONNECTION_ID,
      },
      "drift_detected_at": "2026-08-20T03:32:00+00:00",
    },
  )
  session.add(written_back)
  session.flush()
  catch_up = _catch_up(session, str(written_back.id))
  session.refresh(written_back)
  assert written_back.metadata_[ROUND_TRIP_BASELINE_KEY]["302"] == edited
  session.commit()

  _void(session, catch_up)

  session.refresh(written_back)
  assert written_back.payload_drift is True
  assert written_back.metadata_[ROUND_TRIP_BASELINE_KEY]["302"] == baseline
  assert _repairs_delta(session, ids, str(written_back.id)) == 500


def test_an_alignment_naming_a_quickbooks_item_must_say_it_stays_local(session):
  """A-R1-3 / A-R2-1: QuickBooks already holds the change the alignment books,
  so the close must not write it back. The author says so on the entry; one
  that would publish is refused rather than rewritten."""
  ids, item = _flagged_at_1500(session)

  savepoint = session.begin_nested()
  with pytest.raises(HandlerMetadataValidationError, match="publish_to_source"):
    _plain_entry(
      session,
      ids,
      500,
      metadata={"resolves_reconciling_items": [_named(item, session)]},
    )
  savepoint.rollback()
  assert _qb_item(session).payload_drift is True

  alignment = _plain_entry(
    session,
    ids,
    500,
    metadata={**_LOCAL, "resolves_reconciling_items": [_named(item, session)]},
  )
  assert _qb_item(session).payload_drift is False
  assert not _publishes_at_close(session, str(alignment.id))


def test_an_alignment_committed_from_capture_must_say_it_stays_local(session):
  from robosystems.models.api.event_block import UpdateEventBlockRequest
  from robosystems.operations.event_block.commands import update_event_block

  ids, item = _flagged_at_1500(session)
  captured, _envelope = create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      source="manual",
      occurred_at=datetime(2026, 8, 31),
      apply_handlers=False,
      metadata={
        "posting_date": "2026-08-31",
        "memo": "Alignment",
        "line_items": [
          {"element_id": ids["Repairs"], "debit_amount": 500},
          {"element_id": ids["Checking"], "credit_amount": 500},
        ],
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  named = _named(item, session)
  session.commit()

  def commit(patch: dict) -> None:
    update_event_block(
      session,
      UpdateEventBlockRequest(
        event_id=str(captured.id),
        transition_to="committed",
        metadata_patch={"resolves_reconciling_items": [named], **patch},
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )

  with pytest.raises(HandlerMetadataValidationError, match="publish_to_source"):
    commit({})
  session.rollback()
  assert _qb_item(session).payload_drift is True

  commit(_LOCAL)
  assert _qb_item(session).payload_drift is False
  assert not _publishes_at_close(session, str(captured.id))


def test_an_acknowledgement_naming_an_entry_that_would_publish_is_refused(session):
  """The REST acknowledge naming the entry that booked a QuickBooks change is
  the same settlement: refused while that entry would publish, and the entry
  is left as its author wrote it."""
  ids, item = _flagged_at_1500(session)
  entry = _plain_entry(session, ids, 500)
  assert _publishes_at_close(session, str(entry.id))

  savepoint = session.begin_nested()
  with pytest.raises(ValueError, match="publish_to_source"):
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(
        event_id=str(item.id),
        expected_drift_detected_at=_previewed(session, str(item.id)),
        disposition="acknowledge",
        note="booked by hand",
        reference_event_id=str(entry.id),
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
  savepoint.rollback()

  session.refresh(entry)
  assert "publish_to_source" not in entry.metadata_
  assert _publishes_at_close(session, str(entry.id))
  assert _qb_item(session).payload_drift is True


def test_an_alignment_asking_to_publish_a_quickbooks_change_is_refused(session):
  ids, item = _flagged_at_1500(session)

  savepoint = session.begin_nested()
  with pytest.raises(HandlerMetadataValidationError, match="publish"):
    _plain_entry(
      session,
      ids,
      500,
      metadata={
        "publish_to_source": True,
        "resolves_reconciling_items": [_named(item, session)],
      },
    )
  savepoint.rollback()
  assert _qb_item(session).payload_drift is True


@pytest.mark.parametrize(
  "reference",
  [
    "the_item",
    "its_catch_up",
    "voided",
    "captured",
    "other_entity",
    "another_items_catch_up",
    "another_items_alignment",
    "its_previous_settlement",
  ],
)
def test_an_acknowledgement_must_name_an_entry_that_booked_it(session, reference):
  """A-R1-4 / A-R2-4: a reference is counted as the booking of the difference
  while it stands, so it has to be one: a standing entry on the item's books,
  other than the item, and not one already counted for something else (a
  catch-up, another item's settlement, this item's previous change)."""
  ids, item = _flagged_at_1500(session)
  if reference == "the_item":
    reference_id = str(item.id)
  elif reference == "its_catch_up":
    reference_id = _catch_up(session, str(item.id), status="posted")
    _qb_sync(session, 1800, "2")
  elif reference == "voided":
    voided = _plain_entry(session, ids, 500, metadata={"publish_to_source": False})
    session.commit()
    _void(session, str(voided.id))
    reference_id = str(voided.id)
  elif reference == "another_items_catch_up":
    other = _flagged_other(session)
    reference_id = _catch_up(session, str(other.id), status="posted")
  elif reference == "another_items_alignment":
    other = _flagged_other(session)
    reference_id = str(_hand_alignment(session, ids, 500, [_named(other, session)]).id)
  elif reference == "its_previous_settlement":
    reference_id = str(_hand_alignment(session, ids, 500, [_named(item, session)]).id)
    _qb_sync(session, 1800, "2")
  elif reference == "captured":
    captured, _envelope = create_event_block_in_session(
      session,
      CreateEventBlockRequest(
        event_type="journal_entry_recorded",
        event_category="adjustment",
        source="manual",
        occurred_at=datetime(2026, 8, 31),
        apply_handlers=False,
        metadata={"note": "nothing booked"},
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
    session.flush()
    reference_id = str(captured.id)
  else:
    session.add(
      Entity(
        id="ent_test_sub",
        name="Sub LLC",
        is_parent=False,
        parent_entity_id=PARENT_ENTITY_ID,
        source="native",
        created_by="test",
      )
    )
    session.flush()
    debit = entity_account(session, "ent_test_sub", "Repairs")
    credit = entity_account(session, "ent_test_sub", "Checking")
    foreign, _envelope = create_event_block_in_session(
      session,
      CreateEventBlockRequest(
        event_type="journal_entry_recorded",
        event_category="adjustment",
        source="manual",
        entity_id="ent_test_sub",
        occurred_at=datetime(2026, 8, 31),
        apply_handlers=True,
        metadata={
          "posting_date": "2026-08-31",
          "memo": "Sub's entry",
          "status": "draft",
          "publish_to_source": False,
          "line_items": [
            {"element_id": debit, "debit_amount": 500},
            {"element_id": credit, "credit_amount": 500},
          ],
        },
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
    session.flush()
    reference_id = str(foreign.id)
  item = _qb_item(session)
  assert item.payload_drift is True

  savepoint = session.begin_nested()
  with pytest.raises(ValueError, match="cannot settle"):
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(
        event_id=str(item.id),
        expected_drift_detected_at=_previewed(session, str(item.id)),
        disposition="acknowledge",
        note="handled",
        reference_event_id=reference_id,
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
  savepoint.rollback()
  assert _qb_item(session).payload_drift is True


def test_deleting_one_draft_of_an_alignment_that_settled_items_is_refused(session):
  """A-R1-5: the settlement counts the whole alignment; deleting one of its
  drafts would leave it counted at what it no longer books."""
  from robosystems.models.api.extensions.journal_entries import (
    DeleteJournalEntryRequest,
  )
  from robosystems.operations.roboledger.commands.journal_entries import (
    delete_journal_entry,
  )

  ids, item = _flagged_at_1500(session)
  alignment, _envelope = create_event_block_in_session(
    session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      source="manual",
      occurred_at=datetime(2026, 8, 31),
      apply_handlers=True,
      metadata={
        "status": "draft",
        "publish_to_source": False,
        "entries": [
          {
            "posting_date": "2026-08-31",
            "memo": f"part {cents}",
            "line_items": [
              {"element_id": ids["Repairs"], "debit_amount": cents},
              {"element_id": ids["Checking"], "credit_amount": cents},
            ],
          }
          for cents in (300, 200)
        ],
        "resolves_reconciling_items": [_named(item, session)],
      },
    ),
    "user_test",
    graph_id=GRAPH_ID,
  )
  session.flush()
  first = (
    session.query(Entry)
    .filter(Entry.triggered_by_event_id == str(alignment.id))
    .order_by(Entry.id)
    .first()
  )

  with pytest.raises(ValueError, match="settled"):
    delete_journal_entry(session, DeleteJournalEntryRequest(entry_id=str(first.id)))
  assert _repairs_books(session, ids) == 1500


# Round 3: refuse, don't force ────────────────────────────────────────────

OTHER_TXN = "Expense_88"


def _qb_sync_other(db, amount: int, sync_token: str) -> None:
  """A second QuickBooks expense on the same two accounts."""
  from robosystems.operations.extensions.loader import OLTPLoader

  def other(value):
    if isinstance(value, dict):
      return {key: other(item) for key, item in value.items()}
    if isinstance(value, list):
      return [other(item) for item in value]
    return {QB_TXN: OTHER_TXN, "77": "88"}.get(value, value)

  OLTPLoader()._capture_transactions_as_events(
    db,
    other(_qb_expense(amount, sync_token)),
    source="quickbooks",
    connection_id=CONNECTION_ID,
    created_by="sync",
    now=datetime.now(UTC),
  )
  db.flush()


def _flagged_other(db) -> Event:
  _qb_sync_other(db, 2000, "0")
  _qb_sync_other(db, 2500, "1")
  other = db.query(Event).filter(Event.external_id == OTHER_TXN).one()
  db.refresh(other)
  assert other.payload_drift is True
  return other


def _bundled_lines(db, ids, cents: int) -> list[dict]:
  """The item's change plus a 20,000 legal accrual that has nothing to do
  with it."""
  accrued = Element(name="Accrued Liabilities", code="2100", created_by="test")
  legal = Element(name="Legal Fees", code="6200", created_by="test")
  db.add_all([accrued, legal])
  db.flush()
  return [
    {"element_id": ids["Repairs"], "debit_amount": cents},
    {"element_id": ids["Checking"], "credit_amount": cents},
    {"element_id": str(legal.id), "debit_amount": 20_000},
    {"element_id": str(accrued.id), "credit_amount": 20_000},
  ]


@pytest.mark.parametrize("publish_to_source", [None, False])
def test_an_entry_booking_more_than_the_change_cannot_settle_it(
  session, publish_to_source
):
  """A-R2-1: the month-end entry books the QuickBooks edit and a 20,000
  accrual. Naming it as the item's booking is refused, and the entry is left
  as its author wrote it: the accrual still reaches QuickBooks."""
  ids, item = _flagged_at_1500(session)
  extra = {} if publish_to_source is None else {"publish_to_source": False}
  entry = _plain_entry(
    session,
    ids,
    500,
    metadata={"line_items": _bundled_lines(session, ids, 500), **extra},
  )
  publishes = _publishes_at_close(session, str(entry.id))
  assert publishes is (publish_to_source is None)

  savepoint = session.begin_nested()
  with pytest.raises(ValueError, match="cannot settle"):
    resolve_reconciling_item(
      session,
      ResolveReconcilingItemRequest(
        event_id=str(item.id),
        expected_drift_detected_at=_previewed(session, str(item.id)),
        disposition="acknowledge",
        note="booked in the month-end entry",
        reference_event_id=str(entry.id),
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
  savepoint.rollback()

  session.refresh(entry)
  assert entry.metadata_.get("publish_to_source") is publish_to_source
  assert _publishes_at_close(session, str(entry.id)) is publishes
  assert _qb_item(session).payload_drift is True


def test_an_alignment_booking_more_than_the_change_is_refused(session):
  """A-R2-1 at the alignment door: split the entry instead."""
  ids, item = _flagged_at_1500(session)

  savepoint = session.begin_nested()
  with pytest.raises(HandlerMetadataValidationError, match="more than"):
    _plain_entry(
      session,
      ids,
      500,
      metadata={
        **_LOCAL,
        "line_items": _bundled_lines(session, ids, 500),
        "resolves_reconciling_items": [_named(item, session)],
      },
    )
  savepoint.rollback()
  assert _qb_item(session).payload_drift is True


def test_one_alignment_booking_two_changes_in_full_settles_both(session):
  """The settlement check counts what an entry books against everything it
  settles, so one alignment for two items is still one booking."""
  ids, item = _flagged_at_1500(session)
  other = _flagged_other(session)

  _hand_alignment(session, ids, 1000, [_named(item, session), _named(other, session)])

  assert _qb_item(session).payload_drift is False
  session.refresh(other)
  assert other.payload_drift is False
  _qb_sync(session, 1800, "2")
  assert _repairs_delta(session, ids, str(_qb_item(session).id)) == 300


@pytest.mark.parametrize(
  "successor", ["another_items_alignment", "another_items_catch_up", "books_beyond"]
)
def test_a_successor_already_counted_cannot_take_the_settlement(session, successor):
  """A-R2-3: the successor is counted toward the item from the move on, so it
  has to book the item's difference and nothing it already books for
  something else, or anything beside it."""
  ids, item = _flagged_at_1500(session)
  first = _hand_alignment(session, ids, 500, [_named(item, session)])
  if successor == "another_items_alignment":
    other = _flagged_other(session)
    successor_id = str(_hand_alignment(session, ids, 500, [_named(other, session)]).id)
  elif successor == "another_items_catch_up":
    other = _flagged_other(session)
    successor_id = _catch_up(session, str(other.id))
  else:
    successor_id = str(
      _plain_entry(
        session,
        ids,
        500,
        metadata={**_LOCAL, "line_items": _bundled_lines(session, ids, 500)},
      ).id
    )
  session.commit()

  with pytest.raises(ValueError, match="cannot take that over"):
    _supersede(session, str(first.id), successor_id)
  session.rollback()

  item = _qb_item(session)
  assert item.payload_drift is False
  assert item.metadata_["reconciliation_history"][-1]["reference_event_id"] == str(
    first.id
  )
  assert session.get(Event, str(first.id)).status != "superseded"


def test_a_successor_that_would_publish_cannot_take_a_quickbooks_settlement(session):
  ids, item = _flagged_at_1500(session)
  first = _hand_alignment(session, ids, 500, [_named(item, session)])
  successor = _plain_entry(session, ids, 500)
  session.commit()

  with pytest.raises(ValueError, match="publish_to_source"):
    _supersede(session, str(first.id), str(successor.id))
  session.rollback()

  session.refresh(successor)
  assert "publish_to_source" not in successor.metadata_
  assert session.get(Event, str(first.id)).status != "superseded"


@pytest.mark.parametrize("value", [True, None])
def test_an_entry_that_settled_a_quickbooks_change_cannot_be_set_to_publish(
  session, value
):
  """A-R2-5: the flag is what keeps the change out of QuickBooks, which
  already holds it, so it cannot be patched back while the entry settles."""
  from robosystems.models.api.event_block import UpdateEventBlockRequest
  from robosystems.operations.event_block.commands import update_event_block

  ids, item = _flagged_at_1500(session)
  alignment = _hand_alignment(session, ids, 500, [_named(item, session)])
  session.commit()

  with pytest.raises(ValueError, match="publish"):
    update_event_block(
      session,
      UpdateEventBlockRequest(
        event_id=str(alignment.id), metadata_patch={"publish_to_source": value}
      ),
      "user_test",
      graph_id=GRAPH_ID,
    )
  session.rollback()

  assert not _publishes_at_close(session, str(alignment.id))
  assert _qb_item(session).payload_drift is False


def test_a_moved_round_trip_settlement_keeps_the_decisions_after_it(session):
  """A-R2-2: a round trip acknowledged by A, edited again in QuickBooks and
  caught up. Voiding A is refused (a decision came after it); superseding A
  and voiding the successor is the same retraction and is refused too, with
  a remedy that stays local (A-R2-7)."""
  from robosystems.operations.event_block.qb_writeback import ROUND_TRIP_BASELINE_KEY
  from robosystems.operations.extensions.loader import _round_trip_update
  from robosystems.operations.roboledger.commands.reconciling_items import (
    SettlementNotReopenableError,
  )

  ids = _qb_accounts(session)

  def shape(amount: int) -> dict:
    return {
      "posting_dates": ["2026-07-09"],
      "lines": [[QB_CASH, 0, amount], [QB_REPAIRS, amount, 0]],
    }

  written_back = Event(
    entity_id=PARENT_ENTITY_ID,
    event_type="journal_entry_recorded",
    event_category="adjustment",
    source="manual",
    status="fulfilled",
    occurred_at=datetime(2026, 7, 9),
    created_by="user_test",
    payload_drift=True,
    metadata_={
      "qb_external_id": "303",
      "qb_sync_confirmed_at": "2026-07-10T00:00:00+00:00",
      ROUND_TRIP_BASELINE_KEY: {"303": shape(1000)},
      "drift_payload": {
        "round_trip": {
          "qb_id": "303",
          "entry_ids": [],
          "baseline": shape(1000),
          "accepted": shape(1500),
        },
        "entries": [],
        "connection_id": CONNECTION_ID,
      },
      "drift_detected_at": "2026-08-20T03:32:00+00:00",
    },
  )
  session.add(written_back)
  session.flush()
  first = _hand_alignment(session, ids, 500, [_named(written_back, session)])
  session.commit()

  session.refresh(written_back)
  metadata, change = _round_trip_update(
    written_back,
    {
      "303": {
        "entries": [
          {
            "external_id": "303",
            "memo": "",
            "posting_date": date(2026, 7, 9),
            "line_items": [
              {
                "element_external_id": QB_CASH,
                "debit_amount": 0,
                "credit_amount": 1800,
              },
              {
                "element_external_id": QB_REPAIRS,
                "debit_amount": 1800,
                "credit_amount": 0,
              },
            ],
          }
        ]
      }
    },
    session,
    connection_id=CONNECTION_ID,
    now=datetime.now(UTC),
  )
  assert change == "flag"
  written_back.metadata_ = metadata
  written_back.payload_drift = True
  session.flush()
  _catch_up(session, str(written_back.id))
  session.commit()

  successor = _plain_entry(session, ids, 500, metadata=_LOCAL)
  session.commit()
  _supersede(session, str(first.id), str(successor.id))
  session.commit()

  with pytest.raises(SettlementNotReopenableError, match="publish_to_source: false"):
    _void(session, str(successor.id))
  session.rollback()

  session.refresh(written_back)
  assert written_back.payload_drift is False
  assert written_back.metadata_[ROUND_TRIP_BASELINE_KEY]["303"] == shape(1800)
