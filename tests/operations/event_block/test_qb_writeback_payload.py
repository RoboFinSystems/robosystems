"""What publishes to QuickBooks is the ledger's rows — real Postgres.

An event's ``metadata`` is the capture; its ``Entry`` / ``LineItem`` rows are
what the ledger posts at close. A draft corrected through
`update-journal-entry` moves the rows and leaves the capture behind, so
publishing the capture would put the original in QuickBooks and the
correction in the books. `post_event_to_qb` publishes the rows whenever any
exist and falls back to the capture only for an event that never
materialized rows.
"""

from __future__ import annotations

import os
import uuid
from datetime import UTC, date, datetime
from unittest.mock import patch

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import robosystems.models.extensions  # noqa: F401  (register models on ExtensionsBase)
from robosystems.db.extensions import ExtensionsBase
from robosystems.models.extensions import Element
from robosystems.models.extensions.roboledger.entry import Entry
from robosystems.models.extensions.roboledger.event import Event
from robosystems.models.extensions.roboledger.line_item import LineItem
from robosystems.operations.event_block.qb_writeback import post_event_to_qb

pytestmark = pytest.mark.unit

_MODULE = "robosystems.operations.event_block.qb_writeback"


@pytest.fixture()
def ext_session():
  database_url = os.environ.get("TEST_DATABASE_URL")
  if not database_url:
    pytest.skip("TEST_DATABASE_URL not configured")

  schema = f"ext_qbpay_{uuid.uuid4().hex[:12]}"
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


_CAPTURED_LINES = [
  {"element_id": "elem_cash", "debit_amount": 10_000, "credit_amount": 0},
  {"element_id": "elem_rev", "debit_amount": 0, "credit_amount": 10_000},
]


def _elements(session) -> None:
  session.add_all(
    [
      Element(id="elem_cash", name="Cash", code="1000", balance_type="debit"),
      Element(id="elem_rev", name="Revenue", code="4000", balance_type="credit"),
    ]
  )
  session.flush()


def _event(session, *, with_capture: bool) -> Event:
  _elements(session)
  metadata = {"connection_id": "conn_qb"}
  if with_capture:
    metadata.update(
      {
        "posting_date": "2026-06-15",
        "memo": "as captured",
        "line_items": _CAPTURED_LINES,
      }
    )
  event = Event(
    event_type="journal_entry_recorded",
    event_category="adjustment",
    occurred_at=datetime(2026, 6, 15, tzinfo=UTC),
    source="manual",
    status="committed",
    created_by="usr_test",
    metadata_=metadata,
  )
  session.add(event)
  session.flush()
  return event


def _corrected_rows(session, event: Event) -> Entry:
  """The draft as the operator corrected it: different memo, date, amounts."""
  entry = Entry(
    posting_date=date(2026, 6, 20),
    status="draft",
    memo="as corrected",
    created_by="usr_test",
    triggered_by_event_id=event.id,
  )
  session.add(entry)
  session.flush()
  session.add_all(
    [
      LineItem(
        entry_id=entry.id,
        element_id="elem_cash",
        debit_amount=12_500,
        credit_amount=0,
        line_order=1,
        description="corrected debit",
      ),
      LineItem(
        entry_id=entry.id,
        element_id="elem_rev",
        debit_amount=0,
        credit_amount=12_500,
        line_order=2,
        description="corrected credit",
      ),
    ]
  )
  session.flush()
  return entry


def _publish(session, event: Event) -> list[dict]:
  """Run the publish with the QB boundary faked; return the built payloads."""
  built: list[dict] = []

  def _fake_build(session_, *, posting_date, memo, line_items):
    built.append({"posting_date": posting_date, "memo": memo, "line_items": line_items})
    return object()

  with (
    patch(f"{_MODULE}._build_qb_journal_entry", side_effect=_fake_build),
    patch(f"{_MODULE}._save_with_retry", return_value="77"),
  ):
    ids = post_event_to_qb(session, event, qb_client=object())
  assert ids == ["JournalEntry_77"]
  return built


def test_publishes_the_ledger_rows_not_the_capture(ext_session):
  """Capture and rows both present → the rows are what QuickBooks receives."""
  event = _event(ext_session, with_capture=True)
  entry = _corrected_rows(ext_session, event)
  ext_session.commit()

  (payload,) = _publish(ext_session, event)

  assert payload["posting_date"] == entry.posting_date == date(2026, 6, 20)
  assert payload["memo"] == "as corrected"
  assert [
    (li["element_id"], li["debit_amount"], li["credit_amount"])
    for li in payload["line_items"]
  ] == [
    ("elem_cash", 12_500, 0),
    ("elem_rev", 0, 12_500),
  ]


def test_falls_back_to_the_capture_when_no_rows_exist(ext_session):
  """An event that never materialized rows still publishes its capture."""
  event = _event(ext_session, with_capture=True)
  ext_session.commit()

  (payload,) = _publish(ext_session, event)

  assert payload["memo"] == "as captured"
  assert payload["line_items"] == _CAPTURED_LINES


def test_nothing_to_publish_is_a_rejection(ext_session):
  from robosystems.operations.event_block.qb_writeback import QBWritebackError

  event = _event(ext_session, with_capture=False)
  ext_session.commit()

  with pytest.raises(QBWritebackError) as excinfo:
    _publish(ext_session, event)
  assert excinfo.value.payload["code"] == "no_line_items"
