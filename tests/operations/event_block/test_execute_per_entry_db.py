"""An event whose entries span periods publishes each with its own period.

Real Postgres for the ledger rows; QuickBooks and the platform connection
lookup are faked at their boundaries.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import ExecuteEventBlockRequest
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.commands import execute_event_block

pytestmark = pytest.mark.unit

GRAPH_ID = "kg00000000000000aa"


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_perentry_{uuid.uuid4().hex[:12]}"
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


def _entry(db, event: Event, posting_date: date) -> str:
  entry = Entry(
    posting_date=posting_date,
    status="draft",
    memo=f"accrual {posting_date}",
    created_by="usr_test",
    triggered_by_event_id=event.id,
  )
  db.add(entry)
  db.flush()
  db.add_all(
    [
      LineItem(
        entry_id=entry.id,
        element_id="elem_exp",
        debit_amount=500,
        credit_amount=0,
        line_order=1,
      ),
      LineItem(
        entry_id=entry.id,
        element_id="elem_acc",
        debit_amount=0,
        credit_amount=500,
        line_order=2,
      ),
    ]
  )
  db.flush()
  return str(entry.id)


def _execute(db, event_id: str, entry_ids: list[str], qb_clients=None):
  connection = MagicMock(
    graph_id=GRAPH_ID,
    write_policy="qb_authoritative",
    provider="quickbooks",
    realm_id="9341",
  )
  cred = MagicMock()
  cred.get_credentials.return_value = {"refresh_token": "r"}
  platform = MagicMock()
  platform.__enter__ = MagicMock(return_value=platform)
  platform.__exit__ = MagicMock(return_value=False)
  saved = iter(["101", "102", "103"])

  with (
    patch("robosystems.database.SessionFactory", return_value=platform),
    patch(
      "robosystems.models.core.connection.connection.Connection.get_by_id",
      return_value=connection,
    ),
    patch(
      "robosystems.models.core.connection.connection_credentials.ConnectionCredentials.get_by_connection_id",
      return_value=cred,
    ),
    patch("robosystems.adapters.quickbooks.client.api.QBClient") as qb_client_class,
    patch(
      "robosystems.operations.event_block.qb_writeback._save_with_retry",
      side_effect=lambda *a, **k: next(saved),
    ) as save,
  ):
    result = execute_event_block(
      db,
      ExecuteEventBlockRequest(event_id=event_id, connection_id="conn_qb"),
      created_by="usr_test",
      graph_id=GRAPH_ID,
      acquire_period_fence=False,
      entry_ids=entry_ids,
      qb_clients=qb_clients,
    )
  _execute.clients_built = qb_client_class.call_count  # type: ignore[attr-defined]
  return result, save


def test_each_period_publishes_its_own_entry(session):
  session.add_all(
    [
      Element(
        id="elem_exp",
        name="Expense",
        code="6000",
        balance_type="debit",
        external_id="qb_6000",
      ),
      Element(
        id="elem_acc",
        name="Accrued",
        code="2100",
        balance_type="credit",
        external_id="qb_2100",
      ),
    ]
  )
  event = Event(
    event_type="schedule_entry_due",
    event_category="adjustment",
    occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
    source="schedule",
    status="committed",
    created_by="usr_test",
    metadata_={},
  )
  session.add(event)
  session.flush()
  august = _entry(session, event, date(2026, 8, 31))
  september = _entry(session, event, date(2026, 9, 1))
  session.commit()

  result, save = _execute(session, str(event.id), [august])

  assert result.qb_error is None
  assert save.call_count == 1
  assert session.get(Entry, august).status == "posted"
  assert session.get(Entry, september).status == "draft"
  assert session.get(Event, event.id).status == "committed"

  result, save = _execute(session, str(event.id), [september])

  assert result.qb_error is None
  assert save.call_count == 1
  assert result.qb_entry_ids is not None
  assert set(result.qb_entry_ids) == {august, september}
  reloaded = session.get(Event, event.id)
  assert reloaded.status == "fulfilled"
  assert set(reloaded.metadata_["qb_entry_ids"]) == {august, september}


def test_a_shared_client_cache_builds_one_client(session):
  """Close publishes entry by entry; each build of a client is a token
  refresh, so the run shares one."""
  session.add_all(
    [
      Element(id="elem_exp", name="Expense", code="6000", balance_type="debit"),
      Element(id="elem_acc", name="Accrued", code="2100", balance_type="credit"),
    ]
  )
  event = Event(
    event_type="schedule_entry_due",
    event_category="adjustment",
    occurred_at=datetime(2026, 8, 31, tzinfo=UTC),
    source="schedule",
    status="committed",
    created_by="usr_test",
    metadata_={},
  )
  session.add(event)
  session.flush()
  august = _entry(session, event, date(2026, 8, 31))
  september = _entry(session, event, date(2026, 9, 1))
  session.commit()

  cache: dict = {}
  _execute(session, str(event.id), [august], qb_clients=cache)
  first = _execute.clients_built  # type: ignore[attr-defined]
  _execute(session, str(event.id), [september], qb_clients=cache)
  second = _execute.clients_built  # type: ignore[attr-defined]

  assert (first, second) == (1, 0)
  assert list(cache) == ["conn_qb"]
