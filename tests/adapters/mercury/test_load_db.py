"""The load paths only a real database can prove.

Mercury returns every transaction in the window on every pull, so a line it
already holds comes back: changed, reversed, or as the other leg of a
transfer captured alone. These run the load against ``robosystems_test``.
"""

from __future__ import annotations

from typing import Any

import pytest

from robosystems.adapters.bank_feed.chart import ChartIndex
from robosystems.adapters.mercury.pipeline.load import load_feed
from robosystems.models.api.extensions.reconciling_items import (
  ResolveReconcilingItemRequest,
)
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.commands import fire_handler_on_commit
from robosystems.operations.roboledger.commands.reconciling_items import (
  resolve_reconciling_item,
)
from tests.operations.roboledger.commands.test_reconciling_items_db import (
  CASH,
  GRAPH_ID,
  NEW_EXPENSE,
  OLD_EXPENSE,
  _previewed,
  _seed_elements,
  _seed_periods,
)

pytestmark = pytest.mark.unit

CONNECTION = "conn_mercury_1"
CHECKING, SAVINGS = "acct_chk", "acct_sav"


def _txn(
  txn_id: str,
  account_id: str,
  amount: float,
  day: str,
  *,
  kind: str = "debitCardTransaction",
  counterparty: str = "Acme Supplies",
  status: str = "sent",
) -> dict[str, Any]:
  return {
    "id": txn_id,
    "accountId": account_id,
    "amount": amount,
    "kind": kind,
    "status": status,
    "createdAt": f"{day}T12:00:00Z",
    "postedAt": f"{day}T15:00:00Z",
    "counterpartyName": counterparty,
    "counterpartyId": f"cp_{counterparty.lower().replace(' ', '_')}",
    "bankDescription": counterparty,
  }


def _transfer(txn_id: str, account_id: str, amount: float, day: str, **kw):
  other = (
    "Mercury Savings ••5678" if account_id == CHECKING else "Mercury Checking ••1234"
  )
  return _txn(
    txn_id, account_id, amount, day, kind="internalTransfer", counterparty=other, **kw
  )


def _load(db, elements, transactions: list[dict[str, Any]]):
  raw = {
    "accounts": [
      {"id": CHECKING, "name": "Mercury Checking ••1234", "kind": "checking"},
      {"id": SAVINGS, "name": "Mercury Savings ••5678", "kind": "savings"},
    ],
    "credit_accounts": [],
    "transactions": transactions,
  }
  report = load_feed(
    db,
    graph_id=GRAPH_ID,
    connection_id=CONNECTION,
    created_by="user_test",
    source="mercury",
    raw=raw,
    account_elements={CHECKING: elements[CASH], SAVINGS: elements[NEW_EXPENSE]},
    chart=ChartIndex(),
  )
  db.flush()
  return report


def _event(db, external_id: str) -> Event | None:
  return db.query(Event).filter(Event.external_id == external_id).one_or_none()


def _mercury_events(db) -> list[Event]:
  return db.query(Event).filter(Event.source == "mercury").all()


def _post(db, event: Event, **metadata) -> None:
  event.status = "committed"
  event.metadata_ = {**(event.metadata_ or {}), **metadata}
  db.flush()


# ── retractions ──────────────────────────────────────────────────────────────


def test_a_captured_line_the_bank_reverses_is_deleted(session):
  elements = _seed_elements(session)
  _load(session, elements, [_txn("t1", CHECKING, -120.0, "2026-07-09")])
  assert _event(session, "mercury_txn_t1").status == "captured"

  report = _load(
    session, elements, [_txn("t1", CHECKING, -120.0, "2026-07-09", status="reversed")]
  )

  assert report.events_removed == 1
  assert _event(session, "mercury_txn_t1") is None


def test_a_posted_line_the_bank_reverses_is_flagged_once(session):
  elements = _seed_elements(session)
  _load(session, elements, [_txn("t1", CHECKING, -120.0, "2026-07-09")])
  posted = _event(session, "mercury_txn_t1")
  _post(session, posted)
  reversed_pull = [_txn("t1", CHECKING, -120.0, "2026-07-09", status="reversed")]

  report = _load(session, elements, reversed_pull)

  assert report.reconciling_items == 1 and report.events_removed == 0
  assert posted.status == "committed" and posted.payload_drift is True
  assert posted.metadata_["drift_payload"]["source_removed"] is True
  # Mercury reports the reversal on every pull; it is raised once.
  assert _load(session, elements, reversed_pull).reconciling_items == 0


def test_a_reversed_line_already_in_the_books_is_reversed_out_of_them(session):
  elements = _seed_elements(session)
  _seed_periods(session)
  _load(session, elements, [_txn("t1", CHECKING, -12.40, "2026-07-09")])
  event = _event(session, "mercury_txn_t1")
  _post(session, event, classified_element_id=elements[OLD_EXPENSE])
  fire_handler_on_commit(session, event, "user_test")
  session.flush()
  session.query(Entry).filter(Entry.triggered_by_event_id == str(event.id)).update(
    {"status": "posted"}, synchronize_session=False
  )
  session.flush()

  _load(
    session, elements, [_txn("t1", CHECKING, -12.40, "2026-07-09", status="reversed")]
  )
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
    elements[CASH]: 1240,
    elements[OLD_EXPENSE]: -1240,
  }
  assert not event.payload_drift
  # Resolved, the reversal is not raised again by the next pull.
  reversed_pull = [_txn("t1", CHECKING, -12.40, "2026-07-09", status="reversed")]
  assert _load(session, elements, reversed_pull).reconciling_items == 0


def test_a_posted_pair_with_a_reversed_leg_is_flagged_and_releases_the_other(session):
  elements = _seed_elements(session)
  out_leg = _transfer("x_out", CHECKING, -500.0, "2026-07-09")
  in_leg = _transfer("x_in", SAVINGS, 500.0, "2026-07-09")
  _load(session, elements, [out_leg, in_leg])
  pair = _event(session, "mercury_xfer_x_in")
  _post(session, pair)

  report = _load(session, elements, [{**out_leg, "status": "reversed"}, in_leg])

  assert report.reconciling_items == 1
  assert pair.payload_drift is True and pair.metadata_["legs"] == ["x_out"]
  assert _event(session, "mercury_txn_x_in") is not None
  again = _load(session, elements, [{**out_leg, "status": "reversed"}, in_leg])
  assert again.reconciling_items == 0 and again.events_created == 0


def test_a_pair_with_a_reversed_leg_gives_way_to_the_surviving_leg(session):
  elements = _seed_elements(session)
  out_leg = _transfer("x_out", CHECKING, -500.0, "2026-07-09")
  in_leg = _transfer("x_in", SAVINGS, 500.0, "2026-07-09")
  _load(session, elements, [out_leg, in_leg])
  assert _event(session, "mercury_xfer_x_in").event_type == "internal_transfer"

  report = _load(session, elements, [{**out_leg, "status": "reversed"}, in_leg])

  assert report.events_removed == 1
  assert _event(session, "mercury_xfer_x_in") is None
  survivor = _event(session, "mercury_txn_x_in")
  assert survivor is not None and survivor.amount == 50000


# ── a line that comes back changed ───────────────────────────────────────────


def test_a_captured_line_takes_the_banks_new_amount(session):
  elements = _seed_elements(session)
  _load(session, elements, [_txn("t1", CHECKING, -120.0, "2026-07-09")])

  report = _load(session, elements, [_txn("t1", CHECKING, -130.0, "2026-07-10")])

  event = _event(session, "mercury_txn_t1")
  assert report.events_updated == 1
  assert event.amount == -13000 and event.occurred_at.date().isoformat() == "2026-07-10"


def test_a_posted_line_whose_amount_changes_is_a_reconciling_item(session):
  elements = _seed_elements(session)
  _load(session, elements, [_txn("t1", CHECKING, -120.0, "2026-07-09")])
  posted = _event(session, "mercury_txn_t1")
  _post(session, posted)
  changed = [_txn("t1", CHECKING, -130.0, "2026-07-09")]

  report = _load(session, elements, changed)

  assert report.reconciling_items == 1
  assert posted.amount == -12000  # the books are untouched
  assert posted.metadata_["drift_payload"]["source_amount"] == -13000
  assert _load(session, elements, changed).reconciling_items == 0


# ── transfer legs across pulls ───────────────────────────────────────────────


def test_a_late_leg_merges_with_its_captured_partner(session):
  elements = _seed_elements(session)
  out_leg = _transfer("x_out", CHECKING, -500.0, "2026-07-09")
  # The other side is still pending: only the outflow is captured.
  _load(
    session,
    elements,
    [out_leg, _transfer("x_in", SAVINGS, 500.0, "2026-07-10", status="pending")],
  )
  waiting = _event(session, "mercury_txn_x_out")
  assert waiting is not None
  waiting_id = str(waiting.id)

  report = _load(
    session, elements, [out_leg, _transfer("x_in", SAVINGS, 500.0, "2026-07-10")]
  )

  assert report.transfers_matched == 1
  assert session.get(Event, waiting_id) is None
  pair = _event(session, "mercury_xfer_x_in")
  assert pair.event_type == "internal_transfer" and pair.amount == 50000
  assert pair.metadata_["legs"] == ["x_out", "x_in"]
  assert pair.resource_element_id == elements[NEW_EXPENSE]
  assert pair.metadata_["from_element_id"] == elements[CASH]
  assert pair.occurred_at.date().isoformat() == "2026-07-09"
  assert len(_mercury_events(session)) == 1
  # The next pull finds the pair and leaves it be.
  again = _load(
    session, elements, [out_leg, _transfer("x_in", SAVINGS, 500.0, "2026-07-10")]
  )
  assert again.events_created == 0 and len(_mercury_events(session)) == 1


def test_a_late_leg_dated_before_its_waiting_partner_merges_once(session):
  """The late leg sorts first, so the merge deletes the waiting leg before
  the batch reaches that leg's own line, which must then be left alone."""
  elements = _seed_elements(session)
  in_leg = _transfer("x_in", SAVINGS, 500.0, "2026-07-10")
  _load(
    session,
    elements,
    [_transfer("x_out", CHECKING, -500.0, "2026-07-09", status="pending"), in_leg],
  )
  assert _event(session, "mercury_txn_x_in") is not None

  # The bank moved the waiting leg's date, so its own line would be touched.
  moved = {**in_leg, "postedAt": "2026-07-11T15:00:00Z"}
  report = _load(
    session, elements, [_transfer("x_out", CHECKING, -500.0, "2026-07-09"), moved]
  )

  assert report.transfers_matched == 1 and report.events_updated == 0
  assert _event(session, "mercury_txn_x_in") is None
  assert [e.external_id for e in _mercury_events(session)] == ["mercury_xfer_x_in"]


def test_a_late_leg_whose_partner_is_posted_here_is_voided(session):
  elements = _seed_elements(session)
  out_leg = _transfer("x_out", CHECKING, -500.0, "2026-07-09")
  _load(session, elements, [out_leg])
  posted = _event(session, "mercury_txn_x_out")
  # The operator booked the outflow to the savings account already.
  _post(session, posted, classified_element_id=elements[NEW_EXPENSE])

  report = _load(
    session, elements, [out_leg, _transfer("x_in", SAVINGS, 500.0, "2026-07-10")]
  )

  assert report.legs_voided == 1 and report.transfers_matched == 0
  arrived = _event(session, "mercury_txn_x_in")
  assert arrived.status == "voided"
  assert arrived.metadata_["counterpart_event_id"] == str(posted.id)
  assert posted.status == "committed"


def test_a_late_leg_a_week_off_stays_its_own_line(session):
  elements = _seed_elements(session)
  out_leg = _transfer("x_out", CHECKING, -500.0, "2026-07-01")
  _load(session, elements, [out_leg])

  report = _load(
    session, elements, [out_leg, _transfer("x_in", SAVINGS, 500.0, "2026-07-10")]
  )

  assert report.transfers_matched == 0 and report.events_created == 1
  assert _event(session, "mercury_txn_x_out") is not None
