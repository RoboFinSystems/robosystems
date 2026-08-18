"""Close-period QB publish durability — real Postgres transactions.

`_publish_drafts_to_qb` POSTs real JournalEntries to QuickBooks and
stamps `Event.metadata['qb_external_id']` — the only dedupe marker —
into the caller's open transaction. If a later failure rolled those
markers back while the QB writes stand, the retried close would
re-publish the same drafts as duplicates in the customer's QuickBooks.
The step must commit the markers before any failure can lose them.

These tests use a real Postgres session: the defect is commit/rollback
ordering, which a MagicMock session structurally cannot observe.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  PeriodCloseService,
  WritebackFailed,
)
from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  WritebackConnection,
)

pytestmark = pytest.mark.unit

GRAPH_ID = "kg01234567890abcdef"
JUNE_START = date(2026, 6, 1)
JUNE_END = date(2026, 6, 30)


@pytest.fixture()
def ext_session():
  """Extensions schema in the test Postgres DB, one throwaway schema per test."""
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_qbpub_{uuid.uuid4().hex[:12]}"
  engine = create_engine(database_url)
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


def _seed_draft(session, memo: str) -> tuple[Entry, Event]:
  event = Event(
    event_type="schedule_entry_due",
    event_category="recognition",
    occurred_at=datetime(2026, 6, 15, tzinfo=UTC),
    source="manual",
    created_by="usr_test",
  )
  session.add(event)
  session.flush()
  entry = Entry(
    posting_date=date(2026, 6, 15),
    status="draft",
    memo=memo,
    created_by="usr_test",
    triggered_by_event_id=event.id,
  )
  session.add(entry)
  session.flush()
  return entry, event


def _mark_published(session, event_id: str, qb_id: str) -> None:
  """What execute_event_block does on success: stamp the dedupe marker."""
  event = session.get(Event, event_id)
  event.metadata_ = {**(event.metadata_ or {}), "qb_external_id": qb_id}
  session.flush()


def _writeback_connection():
  return WritebackConnection(connection_id="conn_qb1", write_policy="qb_authoritative")


def _platform_session_factory():
  factory = MagicMock()
  factory.return_value.__enter__ = MagicMock(return_value=MagicMock())
  factory.return_value.__exit__ = MagicMock(return_value=False)
  return factory


def _publish(session, fake_execute):
  """Run the pre-publish step with the QB boundary faked out."""
  svc = PeriodCloseService()
  with (
    patch(
      "robosystems.operations.roboledger.fiscal_calendar.qb_writeback.resolve_writeback_connection",
      return_value=_writeback_connection(),
    ),
    patch("robosystems.database.SessionFactory", new=_platform_session_factory()),
    patch(
      "robosystems.operations.event_block.commands.execute_event_block",
      side_effect=fake_execute,
    ),
  ):
    svc._publish_drafts_to_qb(
      session, GRAPH_ID, JUNE_START, JUNE_END, actor_id="usr_test"
    )


class TestPublishMarkerDurability:
  def test_markers_survive_rollback_after_partial_failure(self, ext_session):
    """Three publish, one rejected: the three markers must survive the
    close rollback that WritebackFailed triggers."""
    ok_entries = [_seed_draft(ext_session, f"dep {i}") for i in range(3)]
    bad_entry, bad_event = _seed_draft(ext_session, "rejected one")
    ext_session.commit()

    def fake_execute(session, request, created_by, **_kwargs):
      if request.event_id == str(bad_event.id):
        return SimpleNamespace(
          status="pending",
          qb_error={"code": "6000", "message": "period closed in QB"},
        )
      _mark_published(session, request.event_id, f"qb_{request.event_id[-6:]}")
      return SimpleNamespace(status="fulfilled", qb_error=None)

    with pytest.raises(WritebackFailed):
      _publish(ext_session, fake_execute)

    # The caller (extensions_session) rolls back on the domain exception.
    ext_session.rollback()

    for _entry, event in ok_entries:
      persisted = ext_session.get(Event, event.id)
      ext_session.refresh(persisted)
      assert persisted.metadata_.get("qb_external_id"), (
        "published marker was rolled back — retry would duplicate the QB entry"
      )
    ext_session.refresh(ext_session.get(Event, bad_event.id))
    assert not ext_session.get(Event, bad_event.id).metadata_.get("qb_external_id")

  def test_retry_skips_already_published_entries(self, ext_session):
    """The retried close must publish ONLY the previously failed draft."""
    for i in range(3):
      _seed_draft(ext_session, f"dep {i}")
    _bad_entry, bad_event = _seed_draft(ext_session, "rejected one")
    ext_session.commit()

    def first_attempt(session, request, created_by, **_kwargs):
      if request.event_id == str(bad_event.id):
        return SimpleNamespace(status="pending", qb_error={"code": "6000"})
      _mark_published(session, request.event_id, f"qb_{request.event_id[-6:]}")
      return SimpleNamespace(status="fulfilled", qb_error=None)

    with pytest.raises(WritebackFailed):
      _publish(ext_session, first_attempt)
    ext_session.rollback()

    published_on_retry: list[str] = []

    def second_attempt(session, request, created_by, **_kwargs):
      published_on_retry.append(request.event_id)
      _mark_published(session, request.event_id, "qb_retry")
      return SimpleNamespace(status="fulfilled", qb_error=None)

    _publish(ext_session, second_attempt)

    assert published_on_retry == [str(bad_event.id)], (
      f"retry must re-publish only the failed draft, got {published_on_retry}"
    )

  def test_markers_survive_rollback_after_downstream_failure(self, ext_session):
    """All publish fine, then a later close step fails (e.g. statement
    stamping): the rollback must not take the markers with it."""
    entry, event = _seed_draft(ext_session, "june depreciation")
    ext_session.commit()

    def fake_execute(session, request, created_by, **_kwargs):
      _mark_published(session, request.event_id, "qb_ok")
      return SimpleNamespace(status="fulfilled", qb_error=None)

    _publish(ext_session, fake_execute)

    # Simulate StatementStampError later in the close: caller rolls back.
    ext_session.rollback()

    persisted = ext_session.get(Event, event.id)
    ext_session.refresh(persisted)
    assert persisted.metadata_.get("qb_external_id") == "qb_ok"

  def test_lock_timeout_on_one_item_does_not_lose_prior_markers(self, ext_session):
    """A RowLockedError aborts the transaction unless it is isolated in a
    savepoint. The markers for entries that already reached QuickBooks
    must still commit."""
    from robosystems.operations.locking import RowLockedError

    ok_entry, ok_event = _seed_draft(ext_session, "published first")
    _locked_entry, locked_event = _seed_draft(ext_session, "lock timeout")
    ext_session.commit()

    def fake_execute(session, request, created_by, **_kwargs):
      if request.event_id == str(locked_event.id):
        raise RowLockedError("event is being written")
      _mark_published(session, request.event_id, "qb_ok")
      return SimpleNamespace(status="fulfilled", qb_error=None)

    with pytest.raises(WritebackFailed) as exc:
      _publish(ext_session, fake_execute)
    assert exc.value.failed_events[0]["event_id"] == str(locked_event.id)

    ext_session.rollback()

    persisted = ext_session.get(Event, ok_event.id)
    ext_session.refresh(persisted)
    assert persisted.metadata_.get("qb_external_id") == "qb_ok"
    ext_session.refresh(ext_session.get(Event, locked_event.id))
    assert not ext_session.get(Event, locked_event.id).metadata_.get("qb_external_id")

  def test_retracted_event_drafts_are_not_published(self, ext_session):
    """Voiding an event leaves its draft GL rows. Close must not hand
    those leftovers to execute — that would un-void the event and post
    a QB JournalEntry for retracted work."""
    _live_entry, live_event = _seed_draft(ext_session, "still live")
    _void_entry, void_event = _seed_draft(ext_session, "voided leftover")
    void_event.status = "voided"
    _super_entry, super_event = _seed_draft(ext_session, "superseded leftover")
    super_event.status = "superseded"
    ext_session.commit()

    published: list[str] = []

    def fake_execute(session, request, created_by, **_kwargs):
      published.append(request.event_id)
      _mark_published(session, request.event_id, "qb_ok")
      return SimpleNamespace(status="fulfilled", qb_error=None)

    _publish(ext_session, fake_execute)

    assert published == [str(live_event.id)], published
    assert void_event.id not in published
    assert super_event.id not in published
