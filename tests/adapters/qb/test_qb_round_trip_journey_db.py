"""The QuickBooks round trip, end to end, against real Postgres.

One journey through the write-back lifecycle: close July, which publishes its
draft to QuickBooks → the accountant edits it there → the sync raises it and a
catch-up levels the books → a second edit → a revert → close August. Every
step runs in its own transaction on a real tenant schema and a real platform
connection row; only QuickBooks' HTTP API is faked. Pinned at each step: one
QuickBooks entry per ledger entry, an edit never books a second copy, a revert
is picked up rather than skipped, and the next close publishes only its own
period.
"""

from __future__ import annotations

import json
import os
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any

import duckdb
import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.config import env
from robosystems.db.extensions import ExtensionsBase, bind_search_path
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  ExecuteEventBlockRequest,
)
from robosystems.models.api.extensions.reconciling_items import (
  ResolveReconcilingItemRequest,
)
from robosystems.models.core.connection.connection import Connection
from robosystems.models.core.connection.connection_credentials import (
  ConnectionCredentials,
)
from robosystems.models.extensions.element import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.fiscal_period import FiscalPeriod
from robosystems.operations.event_block.commands import (
  create_event_block_in_session,
  execute_event_block,
)
from robosystems.operations.extensions.loader import OLTPLoader
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from robosystems.operations.roboledger.commands.reconciling_items import (
  plan_reconciling_item,
  resolve_reconciling_item,
)
from robosystems.operations.roboledger.fiscal_calendar import (
  FiscalCalendarService,
  PeriodCloseService,
)
from robosystems.operations.roboledger.fiscal_calendar.close_service import (
  CloseGateFailed,
)
from robosystems.operations.roboledger.fiscal_calendar.service import (
  CloseableGateResult,
)
from robosystems.operations.roboledger.reports.statement_sets import (
  StatementStampResult,
)

pytestmark = pytest.mark.unit

CASH, REPAIRS, SOFTWARE = "35", "60", "61"


@dataclass
class _FakeJournalEntry:
  txn_date: str
  memo: str
  # (QB account id, "Debit" | "Credit", cents)
  lines: list[tuple[str, str, int]]
  sync_token: int = 0


@dataclass
class FakeQuickBooks:
  """QuickBooks' JournalEntry API as write-back and the sync see it.

  ``create_object`` is what ``JournalEntry.save`` calls; a repeated RequestId
  returns the entry it already created, as QuickBooks does.
  """

  entries: dict[str, _FakeJournalEntry] = field(default_factory=dict)
  by_request_id: dict[str, str] = field(default_factory=dict)
  creates: list[str] = field(default_factory=list)

  def create_object(self, qbo_object_name, request_body, request_id=None, params=None):
    assert qbo_object_name == "JournalEntry"
    if request_id in self.by_request_id:
      qb_id = self.by_request_id[request_id]
    else:
      body = json.loads(request_body)
      qb_id = str(100 + len(self.entries))
      self.entries[qb_id] = _FakeJournalEntry(
        txn_date=body["TxnDate"],
        memo=body.get("PrivateNote") or "",
        lines=[
          (
            line["JournalEntryLineDetail"]["AccountRef"]["value"],
            line["JournalEntryLineDetail"]["PostingType"],
            round(float(line["Amount"]) * 100),
          )
          for line in body["Line"]
        ],
      )
      if request_id:
        self.by_request_id[request_id] = qb_id
      self.creates.append(qb_id)
    return {"JournalEntry": {"Id": qb_id, "SyncToken": "0"}}

  def edit(self, qb_id: str, *, expense: str, cents: int) -> None:
    """The accountant rewrites the entry's expense line in QuickBooks."""
    je = self.entries[qb_id]
    je.lines = [
      (expense, "Debit", cents) if posting == "Debit" else (account, posting, cents)
      for account, posting, _ in je.lines
    ]
    je.sync_token += 1

  def journal_report(self, path) -> str:
    """The dbt ledger tables the sync would build from the current entries."""
    con = duckdb.connect(str(path))
    con.execute(
      "CREATE TABLE transactions (external_id VARCHAR, type VARCHAR, "
      "amount BIGINT, currency VARCHAR, date DATE, description VARCHAR, "
      "sync_token VARCHAR)"
    )
    con.execute(
      "CREATE TABLE entries (external_id VARCHAR, external_transaction_id "
      "VARCHAR, type VARCHAR, posting_date DATE, memo VARCHAR)"
    )
    con.execute(
      "CREATE TABLE line_items (entry_external_id VARCHAR, element_external_id "
      "VARCHAR, debit_amount BIGINT, credit_amount BIGINT, description VARCHAR, "
      "line_order INTEGER)"
    )
    for qb_id, je in self.entries.items():
      ext = f"JournalEntry_{qb_id}"
      txn_date = date.fromisoformat(je.txn_date)
      amount = sum(c for _, posting, c in je.lines if posting == "Debit")
      con.execute(
        "INSERT INTO transactions VALUES (?, 'JournalEntry', ?, 'USD', ?, ?, ?)",
        [ext, amount, txn_date, je.memo, str(je.sync_token)],
      )
      con.execute(
        "INSERT INTO entries VALUES (?, ?, 'standard', ?, ?)",
        [ext, ext, txn_date, je.memo],
      )
      for order, (account, posting, cents) in enumerate(je.lines, start=1):
        debit, credit = (cents, 0) if posting == "Debit" else (0, cents)
        con.execute(
          "INSERT INTO line_items VALUES (?, ?, ?, ?, NULL, ?)",
          [ext, account, debit, credit, order],
        )
    con.close()
    return str(path)


@dataclass
class Tenant:
  graph_id: str
  connection_id: str
  user_id: str
  engine: Any
  qb: FakeQuickBooks
  elements: dict[str, str]
  syncs: int = 0

  @contextmanager
  def tx(self):
    """One transaction on the tenant schema, bound the way production binds it."""
    session: Session = sessionmaker(bind=self.engine)()
    bind_search_path(session, self.graph_id, tenant_schema=self.graph_id)
    try:
      yield session
      session.commit()
    except Exception:
      session.rollback()
      raise
    finally:
      session.close()


@pytest.fixture()
def tenant(test_db, test_user, sample_graph, monkeypatch):
  url = os.environ.get("TEST_DATABASE_URL")
  if not url:
    pytest.skip("TEST_DATABASE_URL not configured")
  # The tenant schema is named for the graph, as in production.
  graph_id = str(sample_graph.graph_id)
  engine = create_engine(url)
  with engine.begin() as conn:
    conn.execute(text(f'CREATE SCHEMA "{graph_id}"'))
    conn.execute(text(f'SET search_path TO "{graph_id}"'))
    ExtensionsBase.metadata.create_all(bind=conn)

  monkeypatch.setattr(env, "CONNECTION_CREDENTIALS_KEY", Fernet.generate_key().decode())
  platform = sessionmaker(bind=test_db.get_bind())
  with platform() as p:
    connection = Connection(
      graph_id=graph_id,
      user_id=test_user.id,
      provider="quickbooks",
      status="connected",
      realm_id="9130",
      write_policy="qb_authoritative",
    )
    p.add(connection)
    p.commit()
    connection_id = str(connection.id)
    ConnectionCredentials.create(
      connection_id=connection_id,
      provider="quickbooks",
      user_id=test_user.id,
      credentials={"access_token": "a", "refresh_token": "r"},
      session=p,
    )

  qb = FakeQuickBooks()

  class FakeQBClient:
    def __init__(self, realm_id, qb_credentials, connection_id=None):
      self.client = qb

  monkeypatch.setattr("robosystems.database.SessionFactory", platform)
  monkeypatch.setattr(
    "robosystems.adapters.quickbooks.client.api.QBClient", FakeQBClient
  )

  t = Tenant(
    graph_id=graph_id,
    connection_id=connection_id,
    user_id=str(test_user.id),
    engine=engine,
    qb=qb,
    elements={},
  )

  @contextmanager
  def tenant_session(_graph_id, **_kwargs):
    with t.tx() as session:
      yield session

  monkeypatch.setattr("robosystems.db.extensions.extensions_session", tenant_session)
  monkeypatch.setattr("robosystems.db.extensions.ensure_tenant_schema", lambda g: False)

  with t.tx() as s:
    for ext, code, name in (
      (CASH, "1000", "Checking"),
      (REPAIRS, "6100", "Repairs"),
      (SOFTWARE, "6200", "Software"),
    ):
      element = Element(
        name=name,
        code=code,
        external_id=ext,
        external_source="quickbooks",
        connection_id=connection_id,
        created_by="t",
      )
      s.add(element)
      s.flush()
      t.elements[ext] = str(element.id)
    fcs = FiscalCalendarService()
    fcs.initialize(s, graph_id, closed_through="2026-06", actor_id=t.user_id)
    fcs.ensure_fiscal_periods(
      s,
      graph_id,
      start_period="2026-06",
      end_period="2026-08",
      closed_through="2026-06",
    )

  try:
    yield t
  finally:
    with engine.begin() as conn:
      conn.execute(text(f'DROP SCHEMA "{graph_id}" CASCADE'))
    engine.dispose()


def _draft(t: Tenant, posting_date: date, cents: int, memo: str) -> str:
  """A RoboLedger-authored journal entry, drafted and waiting for close."""
  with t.tx() as s:
    event, _ = create_event_block_in_session(
      s,
      CreateEventBlockRequest(
        event_type="journal_entry_recorded",
        event_category="adjustment",
        event_class="economic",
        event_action="transfer",
        source="manual",
        occurred_at=datetime.combine(posting_date, datetime.min.time()),
        amount=cents,
        apply_handlers=True,
        metadata={
          "posting_date": posting_date.isoformat(),
          "memo": memo,
          "status": "draft",
          "line_items": [
            {
              "element_id": t.elements[REPAIRS],
              "debit_amount": cents,
              "credit_amount": 0,
            },
            {
              "element_id": t.elements[CASH],
              "debit_amount": 0,
              "credit_amount": cents,
            },
          ],
        },
      ),
      t.user_id,
      graph_id=t.graph_id,
    )
    return str(event.id)


def _no_stamp(session, **kwargs):
  return StatementStampResult(stamped=False, note="no_coa_mapping")


def _close(t: Tenant, period: str):
  with t.tx() as s:
    return PeriodCloseService(statement_stamper=_no_stamp).close(
      s,
      t.graph_id,
      period,
      actor_id=t.user_id,
      has_sync_connection=True,
      last_sync_at=datetime.now(UTC),
    )


def _sync(t: Tenant, tmp_path):
  t.syncs += 1
  path = t.qb.journal_report(tmp_path / f"sync_{t.syncs}.duckdb")
  result = OLTPLoader().load(t.graph_id, "quickbooks", t.connection_id, path, "sync")
  # Every QuickBooks entry is one we published: each is matched, none booked.
  assert result.events_cross_source_matched == len(t.qb.entries)
  assert result.events_captured == 0
  return result


def _event(t: Tenant, event_id: str) -> Event:
  with t.tx() as s:
    event = s.get(Event, event_id)
    assert event is not None
    s.expunge(event)
    return event


def _quickbooks_copies(t: Tenant) -> int:
  with t.tx() as s:
    return s.query(Event).filter(Event.source == "quickbooks").count()


def _net(t: Tenant, ext: str) -> int:
  with t.tx() as s:
    return int(
      s.execute(
        text(
          "SELECT COALESCE(SUM(li.debit_amount - li.credit_amount), 0) "
          "FROM line_items li JOIN entries e ON e.id = li.entry_id "
          "WHERE li.element_id = :el AND e.status = 'posted'"
        ),
        {"el": t.elements[ext]},
      ).scalar_one()
    )


def _catch_up(t: Tenant, event_id: str):
  with t.tx() as s:
    return resolve_reconciling_item(
      s,
      ResolveReconcilingItemRequest(
        event_id=event_id, disposition="catch_up", status="posted"
      ),
      t.user_id,
      graph_id=t.graph_id,
    )


def _delta(t: Tenant, event_id: str) -> dict[str, int]:
  by_element = {v: k for k, v in t.elements.items()}
  with t.tx() as s:
    plan = plan_reconciling_item(s, event_id, graph_id=t.graph_id)
  return {by_element[d.element_id]: d.delta for d in plan.delta if d.element_id}


def test_a_quickbooks_round_trip_from_write_back_to_the_next_close(tenant, tmp_path):
  t = tenant
  july = _draft(t, date(2026, 7, 15), 10000, "July repairs")
  august = _draft(t, date(2026, 8, 12), 4000, "August repairs")

  # 1. Close July: its draft, and only its draft, goes to QuickBooks.
  result = _close(t, "2026-07")
  assert result.entries_published_to_qb == 1
  assert len(t.qb.entries) == 1
  [july_qb] = t.qb.entries
  published = _event(t, july)
  assert published.status == "fulfilled"
  assert list(published.metadata_["qb_entry_ids"].values()) == [
    f"JournalEntry_{july_qb}"
  ]
  assert _event(t, august).metadata_.get("qb_entry_ids") is None
  assert _net(t, REPAIRS) == 10000

  # Publishing again is refused by the closed month, and even past the fence
  # (as close's own retry runs) it is a no-op, not a second QuickBooks entry.
  republish = ExecuteEventBlockRequest(event_id=july, connection_id=t.connection_id)
  with pytest.raises(ClosedPeriodError), t.tx() as s:
    execute_event_block(s, republish, t.user_id, graph_id=t.graph_id)
  with t.tx() as s:
    execute_event_block(
      s, republish, t.user_id, graph_id=t.graph_id, acquire_period_fence=False
    )
  assert t.qb.creates == [july_qb]

  # 2. The first sync recognises the round trip and books nothing.
  assert _sync(t, tmp_path).events_drift_detected == 0
  assert _quickbooks_copies(t) == 0
  assert _event(t, july).payload_drift is False

  # 3. The accountant moves it to Software at 125.00 in QuickBooks.
  t.qb.edit(july_qb, expense=SOFTWARE, cents=12500)
  assert _sync(t, tmp_path).events_drift_detected == 1
  assert _quickbooks_copies(t) == 0
  assert _event(t, july).payload_drift is True
  assert _delta(t, july) == {REPAIRS: -10000, SOFTWARE: 12500, CASH: -2500}
  with pytest.raises(CloseGateFailed) as blocked:
    _close(t, "2026-08")
  assert CloseableGateResult.RECONCILING_ITEMS in blocked.value.blockers

  # The catch-up levels the books in the open month, never back into QuickBooks.
  caught = _catch_up(t, july)
  assert caught.catch_up is not None
  assert caught.catch_up.posting_date == date(2026, 8, 31)
  assert (_net(t, REPAIRS), _net(t, SOFTWARE)) == (0, 12500)
  assert _event(t, july).metadata_["qb_external_id"] == f"JournalEntry_{july_qb}"
  assert _sync(t, tmp_path).events_drift_detected == 0
  assert _event(t, july).payload_drift is False

  # 4. A second edit is measured from the version the ledger accepted.
  t.qb.edit(july_qb, expense=SOFTWARE, cents=13000)
  assert _sync(t, tmp_path).events_drift_detected == 1
  assert _event(t, july).payload_drift is True
  assert _delta(t, july) == {SOFTWARE: 500, CASH: -500}

  # 5. Reverted in QuickBooks before anyone resolved it: the item clears.
  t.qb.edit(july_qb, expense=SOFTWARE, cents=12500)
  _sync(t, tmp_path)
  reverted = _event(t, july)
  assert reverted.payload_drift is False
  assert "drift_payload" not in reverted.metadata_

  # Reverted all the way to the original: a real change from what was
  # accepted, so it is raised and caught up, not skipped.
  t.qb.edit(july_qb, expense=REPAIRS, cents=10000)
  assert _sync(t, tmp_path).events_drift_detected == 1
  assert _event(t, july).payload_drift is True
  assert _delta(t, july) == {REPAIRS: 10000, SOFTWARE: -12500, CASH: 2500}
  _catch_up(t, july)
  assert (_net(t, REPAIRS), _net(t, SOFTWARE)) == (10000, 0)
  assert _quickbooks_copies(t) == 0

  # 6. Close August: its own draft publishes; July's entry and the
  # catch-ups stay where they are.
  closed = _close(t, "2026-08")
  assert closed.entries_published_to_qb == 1
  assert len(t.qb.creates) == 2
  august_qb = t.qb.creates[-1]
  assert august_qb != july_qb
  assert list(_event(t, august).metadata_["qb_entry_ids"].values()) == [
    f"JournalEntry_{august_qb}"
  ]
  with t.tx() as s:
    statuses = dict(
      s.query(FiscalPeriod.name, FiscalPeriod.status)
      .filter(FiscalPeriod.name.in_(["2026-07", "2026-08"]))
      .all()
    )
    drafts_left = s.query(Entry).filter(Entry.status == "draft").count()
  assert statuses == {"2026-07": "closed", "2026-08": "closed"}
  assert drafts_left == 0
  assert (_net(t, REPAIRS), _net(t, SOFTWARE), _net(t, CASH)) == (14000, 0, -14000)

  # 7. The sync after the close sees both entries as our own.
  assert _sync(t, tmp_path).events_drift_detected == 0
  assert _quickbooks_copies(t) == 0
  assert _event(t, july).payload_drift is False
  assert _event(t, august).payload_drift is False
