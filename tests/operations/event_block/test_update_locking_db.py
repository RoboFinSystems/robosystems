"""DB-backed proof of the event row lock.

The MagicMock tests in ``test_commands_update.py`` prove the keyword is passed
and that 55P03 maps to a typed error. They cannot prove the thing that matters:
that the lock is real, that the sync's batch lookup actually blocks an approval,
and that an approval which waits behind a lock re-reads the *committed* row
rather than acting on the snapshot it took before blocking.

Without that, an inbox approval landing during a sync fires the same handler the
sync is about to fire — one event, two sets of GL rows, and a ledger that still
foots and is still wrong.

Runs against the real extensions database (``EXTENSIONS_DATABASE_URL``) with a
throwaway tenant schema built from the model metadata, the same mechanism
``create_tenant_schema`` uses.
"""

from __future__ import annotations

import threading
import time
from datetime import UTC, datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import robosystems.models.extensions  # noqa: F401  (register models on the Base)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, extensions_session
from robosystems.models.api.event_block import UpdateEventBlockRequest
from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.event_block.commands import (
  EventLockedError,
  InvalidEventTransitionError,
  update_event_block,
)

pytestmark = pytest.mark.integration

GRAPH = "kgcccccccccccccccc03"
EVENT_ID = "evt_lock_0001"
EXTERNAL_ID = "QB_TXN_1"
SOURCE = "quickbooks"


def _tenant_tables():
  return [t for t in ExtensionsBase.metadata.sorted_tables if t.schema is None]


@pytest.fixture(scope="module")
def tenant():
  """A real tenant schema, dropped on the way out.

  ``EXTENSIONS_DATABASE_URL`` always resolves to *something* — it falls back to
  a localhost default — so its presence proves nothing. Connect before deciding:
  a developer without the extensions database gets a skip, not a fixture error.
  """
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

  try:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
      conn.execute(text(f"CREATE SCHEMA {GRAPH}"))
      ExtensionsBase.metadata.create_all(
        bind=conn.execution_options(schema_translate_map={None: GRAPH}),
        tables=_tenant_tables(),
      )
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


@pytest.fixture(autouse=True)
def captured_event(tenant):
  """One captured event, reset between cases."""
  with extensions_session(GRAPH) as session:
    session.execute(text("DELETE FROM events"))
    session.add(
      Event(
        id=EVENT_ID,
        # A support-class type with no registered Python handler, so
        # `fire_handler_on_commit` is a no-op. That keeps the assertions about
        # the *lock* — without it, an approval that slips through would fail
        # later on handler metadata and mask which guard actually held.
        event_type="control_check_performed",
        event_category="control",
        event_class="support",
        status="captured",
        occurred_at=datetime(2026, 3, 1, tzinfo=UTC),
        source=SOURCE,
        external_id=EXTERNAL_ID,
        currency="USD",
        metadata_={},
        created_by="usr_seed",
      )
    )
  yield


def _loader_batch_lookup(session):
  """The sync's existing-events lookup, verbatim in shape.

  Mirrors ``_capture_transactions_as_events`` (`extensions/loader.py`) — if that
  query loses ``.with_for_update()``, this test stops blocking and fails.
  """
  return (
    session.query(Event)
    .filter(Event.source == SOURCE, Event.external_id.in_([EXTERNAL_ID]))
    .with_for_update()
    .all()
  )


def test_sync_batch_lock_blocks_an_approval(tenant):
  """The P1 race: the loader locks its batch, so an approval cannot slip in
  between that read and the handler dispatch that follows it. It gets a
  retryable error instead of dispatching a second time."""
  with extensions_session(GRAPH) as sync_session:
    locked = _loader_batch_lookup(sync_session)
    assert [e.id for e in locked] == [EVENT_ID]

    # Sync has not committed — the lock is held. An approval arriving now
    # must not proceed to fire the handler.
    started = time.monotonic()
    with extensions_session(GRAPH) as approval_session:
      with pytest.raises(EventLockedError, match=EVENT_ID):
        update_event_block(
          approval_session,
          UpdateEventBlockRequest(event_id=EVENT_ID, transition_to="committed"),
          created_by="usr_approver",
        )
    waited = time.monotonic() - started

  # It failed by timing out on the lock, not by some unrelated fast path.
  assert waited >= 2.5, f"returned in {waited:.2f}s — did it actually block?"

  with extensions_session(GRAPH) as check:
    assert check.get(Event, EVENT_ID).status == "captured"


def test_approval_that_waits_out_a_lock_sees_the_committed_row(tenant):
  """The lock is only worth taking if the blocked reader re-reads. Under READ
  COMMITTED a blocked ``SELECT ... FOR UPDATE`` returns the row as the winner
  left it, so the loser's transition check runs against current state and
  correctly rejects — rather than acting on the snapshot it took before it
  blocked. (A fresh session matters: ``Session.get`` would otherwise serve an
  identity-map hit and never re-read.)"""
  hold_released = threading.Event()
  failure: list[BaseException] = []

  def winner():
    try:
      with extensions_session(GRAPH) as session:
        session.execute(
          text("SELECT id FROM events WHERE id = :id FOR UPDATE"), {"id": EVENT_ID}
        )
        time.sleep(1.0)
        session.execute(
          text("UPDATE events SET status = 'voided' WHERE id = :id"), {"id": EVENT_ID}
        )
      hold_released.set()
    except BaseException as exc:  # surfaced in the main thread below
      failure.append(exc)
      hold_released.set()

  t = threading.Thread(target=winner)
  t.start()
  try:
    time.sleep(0.2)  # let the winner take the lock first
    with extensions_session(GRAPH) as approval_session:
      # Blocks ~0.8s, well inside the 3s lock_timeout, then re-reads 'voided'
      # — a terminal state with no outbound transitions.
      with pytest.raises(InvalidEventTransitionError, match="terminal"):
        update_event_block(
          approval_session,
          UpdateEventBlockRequest(event_id=EVENT_ID, transition_to="voided"),
          created_by="usr_approver",
        )
  finally:
    t.join(timeout=10)

  assert not failure, f"winner thread failed: {failure[0]!r}"
  assert hold_released.is_set()
