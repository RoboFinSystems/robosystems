"""A landed ledger row is never deleted or rewritten, whatever path writes.

The tenant triggers from migration 0056 refuse to delete a landed entry, line
or transaction, or an event that owns one; they refuse a change to a landed
entry's date, entity, transaction or type, or to a landed line's amounts,
account or entry; and a landed entry's status moves only to ``reversed`` or
``superseded``. Drafts stay free. An entity purge opts out for its own
transaction. Runs against a tenant provisioned the way a graph's is, so the
triggers are the ones a real tenant gets.
"""

from __future__ import annotations

from datetime import date

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.exc import DBAPIError, OperationalError

from robosystems.config import env
from robosystems.db.extensions import extensions_session, provision_tenant_schema
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.models.extensions.roboledger.transaction import Transaction

pytestmark = pytest.mark.integration

GRAPH = "kgeeeeeeeeeeeeeeee56"
ENTITY = "ent_permanent"


@pytest.fixture()
def tenant():
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
  with engine.begin() as conn:
    conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
  try:
    provision_tenant_schema(GRAPH)
    yield
  finally:
    with engine.begin() as conn:
      conn.execute(text(f"DROP SCHEMA IF EXISTS {GRAPH} CASCADE"))
    engine.dispose()


def _write(status: str) -> dict[str, str]:
  """An event with one entry at ``status``, its transaction and two lines."""
  with extensions_session(GRAPH) as session:
    element_id = session.execute(select(Element.id).limit(1)).scalar_one()
    event = Event(
      event_type="journal_entry_recorded",
      event_category="adjustment",
      source="native",
      occurred_at=date(2026, 7, 9),
      amount=100,
      status="committed",
      entity_id=ENTITY,
      created_by="usr_1",
    )
    session.add(event)
    session.flush()
    txn = Transaction(
      entity_id=ENTITY,
      type="journal_entry",
      amount=100,
      date=date(2026, 7, 9),
      status="posted" if status != "draft" else "pending",
      triggered_by_event_id=event.id,
      created_by="usr_1",
    )
    session.add(txn)
    session.flush()
    entry = Entry(
      entity_id=ENTITY,
      transaction_id=txn.id,
      triggered_by_event_id=event.id,
      posting_date=date(2026, 7, 9),
      status=status,
      created_by="usr_1",
    )
    session.add(entry)
    session.flush()
    lines = [
      LineItem(entry_id=entry.id, element_id=element_id, debit_amount=100),
      LineItem(entry_id=entry.id, element_id=element_id, credit_amount=100),
    ]
    session.add_all(lines)
    session.commit()
    return {
      "event": str(event.id),
      "transaction": str(txn.id),
      "entry": str(entry.id),
      "line": str(lines[0].id),
    }


def _refused(sql: str, **params: str) -> None:
  with extensions_session(GRAPH) as session:
    with pytest.raises(DBAPIError, match="ledger row permanent"):
      session.execute(text(sql), params)
    session.rollback()


def _allowed(sql: str, **params: str) -> None:
  with extensions_session(GRAPH) as session:
    session.execute(text(sql), params)
    session.commit()


def test_a_posted_entry_is_never_deleted(tenant):
  rows = _write("posted")
  _refused("DELETE FROM line_items WHERE entry_id = :e", e=rows["entry"])
  _refused("DELETE FROM entries WHERE id = :e", e=rows["entry"])
  _refused("DELETE FROM transactions WHERE id = :t", t=rows["transaction"])
  _refused("DELETE FROM events WHERE id = :v", v=rows["event"])


def test_a_posted_entry_keeps_its_date_and_entity(tenant):
  rows = _write("posted")
  _refused(
    "UPDATE entries SET posting_date = '2026-07-10' WHERE id = :e", e=rows["entry"]
  )
  _refused("UPDATE entries SET entity_id = 'ent_other' WHERE id = :e", e=rows["entry"])
  _allowed("UPDATE entries SET memo = 'annotated' WHERE id = :e", e=rows["entry"])


def test_a_posted_line_keeps_its_amounts_and_account(tenant):
  rows = _write("posted")
  _refused("UPDATE line_items SET debit_amount = 99 WHERE id = :l", l=rows["line"])
  _allowed(
    "UPDATE line_items SET description = 'annotated' WHERE id = :l", l=rows["line"]
  )


def test_status_moves_forward_only(tenant):
  rows = _write("posted")
  _refused("UPDATE entries SET status = 'draft' WHERE id = :e", e=rows["entry"])
  _allowed("UPDATE entries SET status = 'superseded' WHERE id = :e", e=rows["entry"])
  _refused("UPDATE entries SET status = 'posted' WHERE id = :e", e=rows["entry"])
  _refused("DELETE FROM entries WHERE id = :e", e=rows["entry"])


def test_drafts_stay_free(tenant):
  rows = _write("draft")
  _allowed("UPDATE line_items SET debit_amount = 99 WHERE id = :l", l=rows["line"])
  _allowed("DELETE FROM line_items WHERE entry_id = :e", e=rows["entry"])
  _allowed("DELETE FROM entries WHERE id = :e", e=rows["entry"])
  _allowed("DELETE FROM transactions WHERE id = :t", t=rows["transaction"])
  _allowed("DELETE FROM events WHERE id = :v", v=rows["event"])


def test_an_entity_purge_opts_out_for_its_own_transaction(tenant):
  rows = _write("posted")
  with extensions_session(GRAPH) as session:
    session.execute(text("SET LOCAL robosystems.ledger_purge = 'on'"))
    session.execute(
      text("DELETE FROM line_items WHERE entry_id = :e"), {"e": rows["entry"]}
    )
    session.execute(text("DELETE FROM entries WHERE id = :e"), {"e": rows["entry"]})
    session.commit()
  # The opt-out ended with the transaction that set it.
  again = _write("posted")
  _refused("DELETE FROM entries WHERE id = :e", e=again["entry"])
