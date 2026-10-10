"""An event citing a stored document, against a real tenant schema.

An invoice, a vendor bill or a receipt is a platform document; the event it
backs names it in `events.document_id`. The name is checked on write (the two
live in different databases), and a document a live event names cannot be
deleted.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from datetime import UTC, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.api.event_block import (
  CreateEventBlockRequest,
  UpdateEventBlockRequest,
)
from robosystems.models.extensions.roboledger import Event
from robosystems.operations.document_service import events_citing
from robosystems.operations.event_block.commands import (
  EventDocumentNotFoundError,
  InvalidEventTransitionError,
  create_event_block_in_session,
  update_event_block,
)
from tests.ledger_entity import PARENT_ENTITY_ID, seed_parent_entity

pytestmark = pytest.mark.unit

GRAPH_ID = "kg_test"
ON_GRAPH = {"doc_invoice", "doc_bill", "doc_other"}
_COMMANDS = "robosystems.operations.event_block.commands"


@pytest.fixture()
def session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_evdoc_{uuid.uuid4().hex[:12]}"
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
def _platform_checks(monkeypatch):
  """Sources and documents live in the platform database; stand them in."""
  monkeypatch.setattr(f"{_COMMANDS}._validate_event_source", lambda s, g: None)
  monkeypatch.setattr(f"{_COMMANDS}._validate_routed_connection", lambda m, g: None)
  monkeypatch.setattr(
    "robosystems.operations.document_service.document_exists",
    lambda graph_id, document_id: graph_id == GRAPH_ID and document_id in ON_GRAPH,
  )


def _create(db, *, document_id=None, event_type="invoice_issued"):
  event, envelope = create_event_block_in_session(
    db,
    CreateEventBlockRequest(
      event_type=event_type,
      event_category="sales",
      occurred_at=datetime(2026, 9, 15, tzinfo=UTC),
      source="manual",
      amount=99_00,
      metadata={"invoice_number": "INV-0042"},
      document_id=document_id,
    ),
    "usr_1",
    graph_id=GRAPH_ID,
  )
  db.commit()
  return str(event.id), envelope


def _update(db, event_id, **fields):
  return update_event_block(
    db, UpdateEventBlockRequest(event_id=event_id, **fields), "usr_1", graph_id=GRAPH_ID
  )


def _document_of(db, event_id):
  db.expire_all()
  return db.execute(select(Event.document_id).where(Event.id == event_id)).scalar()


@contextmanager
def _ledger(db):
  """events_citing reads the graph's own schema; point it at the test one."""

  @contextmanager
  def _session(graph_id, **_kwargs):
    yield db

  with (
    patch("robosystems.db.extensions.tenant_schema_exists", return_value=True),
    patch("robosystems.db.extensions.extensions_session", _session),
  ):
    yield


def test_an_invoice_names_its_pdf(session):
  event_id, envelope = _create(session, document_id="doc_invoice")

  assert envelope.document_id == "doc_invoice"
  assert _document_of(session, event_id) == "doc_invoice"
  # The citation is a column; the payload a sync owns is untouched.
  assert envelope.metadata == {"invoice_number": "INV-0042"}


def test_a_document_off_the_graph_is_refused_and_nothing_is_written(session):
  with pytest.raises(EventDocumentNotFoundError):
    _create(session, document_id="doc_elsewhere")
  session.rollback()

  assert session.execute(select(Event.id)).all() == []


def test_an_event_can_be_given_another_document_or_none(session):
  event_id, _ = _create(session)

  assert _update(session, event_id, document_id="doc_bill").document_id == "doc_bill"
  assert _update(session, event_id, document_id="doc_other").document_id == "doc_other"
  with pytest.raises(EventDocumentNotFoundError):
    _update(session, event_id, document_id="doc_elsewhere")
  session.rollback()
  assert _update(session, event_id, document_id="").document_id is None
  assert _document_of(session, event_id) is None


def test_a_retracted_events_document_cannot_change(session):
  event_id, _ = _create(session, document_id="doc_invoice")
  _update(session, event_id, transition_to="voided")

  with pytest.raises(InvalidEventTransitionError, match="no longer be corrected"):
    _update(session, event_id, document_id="doc_other")


def test_only_live_events_hold_their_document(session):
  """A voided or superseded event no longer rests on its document; every
  other status does, a captured bill and a committed invoice alike."""
  live, _ = _create(session, document_id="doc_invoice")
  committed, _ = _create(session, document_id="doc_invoice")
  # As a posted invoice stands; committing here would run its posting handler.
  session.execute(
    text("UPDATE events SET status = 'committed' WHERE id = :id"), {"id": committed}
  )
  session.commit()
  voided, _ = _create(session, document_id="doc_invoice")
  _update(session, voided, transition_to="voided")
  _create(session, document_id="doc_other")

  with _ledger(session):
    cited = {c.event_id: c.status for c in events_citing(GRAPH_ID, "doc_invoice")}
    uncited = events_citing(GRAPH_ID, "doc_bill")

  assert cited == {live: "captured", committed: "committed"}
  assert uncited == []


def test_a_statement_balance_names_its_document_the_same_way(session):
  from robosystems.operations.roboledger.reconciliations.observations import (
    record_statement_observation,
  )
  from tests.ledger_entity import entity_account

  cash = session.get(
    robosystems.models.extensions.Element,
    entity_account(session, PARENT_ENTITY_ID, "Checking"),
  )
  cash.period_type = "instant"
  cash.balance_type = "debit"
  first = record_statement_observation(
    session,
    element=cash,
    entity_id=PARENT_ENTITY_ID,
    as_of=datetime(2026, 9, 30).date(),
    stated_cents=3_204_88,
    document_id="doc_invoice",
    note=None,
    created_by="usr_1",
  )
  session.commit()

  with _ledger(session):
    assert [c.event_id for c in events_citing(GRAPH_ID, "doc_invoice")] == [
      first.event_id
    ]
    # Recorded again with another statement: the first is superseded and
    # lets its document go.
    record_statement_observation(
      session,
      element=cash,
      entity_id=PARENT_ENTITY_ID,
      as_of=datetime(2026, 9, 30).date(),
      stated_cents=3_204_88,
      document_id="doc_other",
      note=None,
      created_by="usr_1",
    )
    session.commit()
    assert events_citing(GRAPH_ID, "doc_invoice") == []
    assert len(events_citing(GRAPH_ID, "doc_other")) == 1


def test_a_bill_still_in_the_inbox_keeps_its_document(session):
  """Before events could cite documents, only a committed statement balance
  held one; now a captured bill does too, and the refusal names it."""
  from unittest.mock import MagicMock

  from robosystems.models.core.document import Document
  from robosystems.operations.document_service import (
    DocumentInUseError,
    DocumentService,
  )

  bill, _ = _create(session, document_id="doc_bill", event_type="bill_received")
  stored = MagicMock(id="doc_bill", is_file=True)

  with (
    _ledger(session),
    patch.object(Document, "get_by_id_and_graph", return_value=stored),
    pytest.raises(DocumentInUseError, match=bill),
  ):
    DocumentService(MagicMock()).delete_document(GRAPH_ID, "doc_bill")
  stored.delete.assert_not_called()
