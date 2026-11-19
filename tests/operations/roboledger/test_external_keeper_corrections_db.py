"""The ledger that keeps an entity's books corrects what it sent: a later
version replaces the entry and a retraction takes it out, on real Postgres."""

from __future__ import annotations

from datetime import date, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import select, text

from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.extensions import Dimension
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction
from robosystems.operations.event_block.commands import (
  DuplicateEventError,
  InvalidEventTransitionError,
  SourceCorrectionError,
  create_event_block_in_session,
  update_event_block,
)
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from tests.ledger_entity import entity_account

pytestmark = pytest.mark.unit

GRAPH_ID = "kg0123456789abcdef0e"
LEDGER = "netsuite-uk"
_COMMANDS = "robosystems.operations.event_block.commands"
_WRITEBACK = "robosystems.operations.roboledger.fiscal_calendar.qb_writeback"


@pytest.fixture()
def mirrored(two_entities):
  """An external ledger keeps the subsidiary's books in shadow; the parent
  keeps its own."""
  t = two_entities
  t.session.info["graph_id"] = GRAPH_ID
  t.session.info["books_keeper_sources"] = {t.parent.id: None, t.sub.id: LEDGER}
  t.accounts = {
    entity.id: (
      entity_account(t.session, entity.id, "Cash"),
      entity_account(t.session, entity.id, "Rent"),
    )
    for entity in (t.parent, t.sub)
  }
  t.session.commit()
  sub = t.sub.id
  with (
    patch(
      f"{_WRITEBACK}.shadow_close_for_entity",
      side_effect=lambda _platform, _graph, entity_id: entity_id == sub,
    ),
    patch(f"{_COMMANDS}._validate_event_source"),
    patch(f"{_COMMANDS}._validate_routed_connection"),
  ):
    yield t


def _send(
  t,
  *,
  amount,
  version=None,
  day=15,
  month=9,
  source=LEDGER,
  entity=None,
  ref="JE-1",
  extra=None,
  dimensions=None,
):
  entity_id = entity or t.sub.id
  cash, rent = t.accounts[entity_id]
  event, _envelope = create_event_block_in_session(
    t.session,
    CreateEventBlockRequest(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      source=source,
      external_id=ref,
      source_version=version,
      occurred_at=datetime(2026, month, day),
      amount=amount,
      apply_handlers=True,
      dimension_ids=dimensions or [],
      metadata={
        "posting_date": date(2026, month, day).isoformat(),
        **(extra or {}),
        "memo": "Rent received",
        "line_items": [
          {"element_id": cash, "debit_amount": amount, "credit_amount": 0},
          {"element_id": rent, "debit_amount": 0, "credit_amount": amount},
        ],
      },
    ),
    "usr_integration",
    graph_id=GRAPH_ID,
    entity_id=entity_id,
  )
  t.session.flush()
  return str(event.id)


def _books(t, event_id):
  """`(status, debit total)` of each of the event's entries, oldest first."""
  rows = t.session.execute(
    select(Entry.id, Entry.status)
    .where(Entry.triggered_by_event_id == event_id)
    .order_by(Entry.created_at, Entry.id)
  ).all()
  return [
    (
      status,
      sum(
        t.session.scalars(
          select(LineItem.debit_amount).where(LineItem.entry_id == entry_id)
        )
      ),
    )
    for entry_id, status in rows
  ]


def _close_september(t):
  t.session.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      entity_id=t.sub.id,
      name="2026-09",
      start_date=date(2026, 9, 1),
      end_date=date(2026, 9, 30),
      period_type="month",
      status="closed",
    )
  )
  t.session.flush()


def test_a_later_version_replaces_the_entry(mirrored):
  first = _send(mirrored, amount=50_000, version=1)
  again = _send(mirrored, amount=62_500, version=2, day=20)

  assert again == first
  assert _books(mirrored, first) == [("superseded", 50_000), ("posted", 62_500)]
  event = mirrored.session.get(Event, first)
  assert event.amount == 62_500
  assert event.metadata_["source_version"] == 2
  assert event.status == "fulfilled"
  old, new = mirrored.session.scalars(
    select(Entry)
    .where(Entry.triggered_by_event_id == first)
    .order_by(Entry.created_at, Entry.id)
  ).all()
  assert old.supersession["op"] == "source_replaced"
  assert old.supersession["replacement_entry_ids"] == [new.id]
  assert new.posting_date == date(2026, 9, 20)
  statuses = set(
    mirrored.session.scalars(
      select(Transaction.status).where(Transaction.triggered_by_event_id == first)
    )
  )
  assert statuses == {"void", "posted"}


@pytest.mark.parametrize("version", [None, 1, 2])
def test_a_repeat_or_an_older_version_is_still_a_duplicate(mirrored, version):
  first = _send(mirrored, amount=50_000, version=2)
  with pytest.raises(DuplicateEventError):
    _send(mirrored, amount=62_500, version=version)
  assert _books(mirrored, first) == [("posted", 50_000)]


def test_a_first_delivery_without_a_version_can_still_be_corrected(mirrored):
  first = _send(mirrored, amount=50_000)
  _send(mirrored, amount=62_500, version=1)
  assert _books(mirrored, first) == [("superseded", 50_000), ("posted", 62_500)]


def test_only_the_books_keeper_corrects_by_sending_again(mirrored):
  # The parent keeps its own books: a registered source there is no ledger.
  parent = mirrored.parent.id
  first = _send(mirrored, amount=50_000, version=1, source="billing", entity=parent)
  with pytest.raises(DuplicateEventError):
    _send(mirrored, amount=62_500, version=2, source="billing", entity=parent)
  assert _books(mirrored, first) == [("draft", 50_000)]


def test_a_stamped_month_is_not_rewritten(mirrored):
  first = _send(mirrored, amount=50_000, version=1)
  _close_september(mirrored)
  with pytest.raises(ClosedPeriodError):
    _send(mirrored, amount=62_500, version=2)
  assert _books(mirrored, first) == [("posted", 50_000)]


def test_an_entry_is_not_moved_into_a_stamped_month(mirrored):
  first = _send(mirrored, amount=50_000, version=1, day=15)
  mirrored.session.add(
    FiscalPeriod(
      graph_id=GRAPH_ID,
      entity_id=mirrored.sub.id,
      name="2026-08",
      start_date=date(2026, 8, 1),
      end_date=date(2026, 8, 31),
      period_type="month",
      status="closed",
    )
  )
  mirrored.session.flush()
  with pytest.raises(ClosedPeriodError):
    _send(mirrored, amount=62_500, version=2, month=8)
  assert _books(mirrored, first) == [("posted", 50_000)]


def test_a_later_versions_dimensions_replace_the_first(mirrored):
  from robosystems.models.extensions.roboledger.dimension_junctions import (
    event_dimensions,
  )

  old, new = (
    Dimension(dimension_type="project", name=name, value=name.lower())
    for name in ("Harbor", "Pier")
  )
  mirrored.session.add_all([old, new])
  mirrored.session.flush()

  def tags(event_id):
    return list(
      mirrored.session.scalars(
        select(event_dimensions.c.dimension_id).where(
          event_dimensions.c.event_id == event_id
        )
      )
    )

  first = _send(mirrored, amount=50_000, version=1, dimensions=[old.id])
  assert tags(first) == [old.id]
  _send(mirrored, amount=62_500, version=2, dimensions=[new.id])
  assert tags(first) == [new.id]
  _send(mirrored, amount=62_500, version=3)
  assert tags(first) == []


def test_a_later_version_settles_no_reconciling_items(mirrored):
  _send(mirrored, amount=50_000, version=1)
  with pytest.raises(SourceCorrectionError, match="reconciling items"):
    _send(
      mirrored,
      amount=62_500,
      version=2,
      extra={"resolves_reconciling_items": []}
      | {"resolves_reconciling_items": [{"event_id": "evt_x"}]},
    )


def test_a_later_version_cannot_move_the_entry_to_another_entity(mirrored):
  _send(mirrored, amount=50_000, version=1)
  with pytest.raises(SourceCorrectionError, match="another entity"):
    _send(mirrored, amount=62_500, version=2, entity=mirrored.parent.id)


def _retract(t, event_id, **kw):
  schema = t.session.execute(text("SELECT current_schema()")).scalar_one()
  try:
    return update_event_block(
      t.session,
      UpdateEventBlockRequest(
        event_id=event_id, transition_to="voided", source_retracted=True, **kw
      ),
      "usr_integration",
      graph_id=GRAPH_ID,
    )
  finally:
    # A commit or rollback hands the connection back to the pool.
    t.session.rollback()
    t.session.execute(text(f'SET search_path TO "{schema}"'))


def test_a_retraction_takes_the_entry_out_and_voids_the_event(mirrored):
  first = _send(mirrored, amount=50_000, version=1)
  mirrored.session.commit()

  envelope = _retract(mirrored, first)

  assert envelope.status == "voided"
  assert _books(mirrored, first) == [("superseded", 50_000)]
  entry = mirrored.session.scalars(
    select(Entry).where(Entry.triggered_by_event_id == first)
  ).one()
  assert entry.supersession["op"] == "source_retracted"
  with pytest.raises(SourceCorrectionError, match="bring it back"):
    _send(mirrored, amount=62_500, version=2)


def test_a_retraction_is_the_books_keepers_alone(mirrored):
  parent = mirrored.parent.id
  first = _send(mirrored, amount=50_000, source="billing", entity=parent)
  mirrored.session.commit()
  with pytest.raises(InvalidEventTransitionError, match="keeps the entity's books"):
    _retract(mirrored, first)


def test_a_retraction_into_a_stamped_month_is_refused(mirrored):
  first = _send(mirrored, amount=50_000, version=1)
  _close_september(mirrored)
  mirrored.session.commit()
  with pytest.raises(ClosedPeriodError):
    _retract(mirrored, first)
  assert _books(mirrored, first) == [("posted", 50_000)]


def test_source_retracted_goes_with_a_void():
  with pytest.raises(ValueError, match="voided"):
    UpdateEventBlockRequest(event_id="evt_1", source_retracted=True)
