"""What publishes to QuickBooks is the ledger's rows — real Postgres.

An event's ``metadata`` is the capture; its ``Entry`` / ``LineItem`` rows are
what the ledger posts at close. A draft corrected through
`update-journal-entry` moves the rows and leaves the capture behind, so
publishing the capture would put the original in QuickBooks and the
correction in the books. `post_event_to_qb` publishes the rows, one QuickBooks
JournalEntry per draft entry, and never the capture.
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


def _publish(session, event: Event, **kwargs) -> tuple[list[dict], dict[str, str]]:
  """Run the publish with the QB boundary faked; return payloads and ids."""
  built: list[dict] = []

  def _fake_build(session_, *, posting_date, memo, line_items):
    built.append({"posting_date": posting_date, "memo": memo, "line_items": line_items})
    return object()

  with (
    patch(f"{_MODULE}._build_qb_journal_entry", side_effect=_fake_build),
    patch(f"{_MODULE}._save_with_retry", return_value="77"),
  ):
    ids = post_event_to_qb(session, event, qb_client=object(), **kwargs)
  return built, ids


def test_publishes_the_ledger_rows_not_the_capture(ext_session):
  """Capture and rows both present → the rows are what QuickBooks receives."""
  event = _event(ext_session, with_capture=True)
  entry = _corrected_rows(ext_session, event)
  ext_session.commit()

  (payload,), ids = _publish(ext_session, event)

  assert ids == {entry.id: "JournalEntry_77"}
  assert payload["posting_date"] == entry.posting_date == date(2026, 6, 20)
  assert payload["memo"] == "as corrected"
  assert [
    (li["element_id"], li["debit_amount"], li["credit_amount"])
    for li in payload["line_items"]
  ] == [
    ("elem_cash", 12_500, 0),
    ("elem_rev", 0, 12_500),
  ]


def test_an_event_without_rows_publishes_nothing(ext_session):
  """The capture alone is never published: the ledger holds nothing to mirror."""
  event = _event(ext_session, with_capture=True)
  ext_session.commit()

  built, ids = _publish(ext_session, event)

  assert built == []
  assert ids == {}


def _second_entry(session, event: Event, *, posting_date: date) -> Entry:
  entry = Entry(
    posting_date=posting_date,
    status="draft",
    memo="second",
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
        debit_amount=100,
        credit_amount=0,
        line_order=1,
      ),
      LineItem(
        entry_id=entry.id,
        element_id="elem_rev",
        debit_amount=0,
        credit_amount=100,
        line_order=2,
      ),
    ]
  )
  session.flush()
  return entry


def test_entry_ids_scope_the_publish(ext_session):
  event = _event(ext_session, with_capture=False)
  june = _corrected_rows(ext_session, event)
  _second_entry(ext_session, event, posting_date=date(2026, 7, 1))
  ext_session.commit()

  built, ids = _publish(ext_session, event, entry_ids=[june.id])

  assert [b["memo"] for b in built] == ["as corrected"]
  assert list(ids) == [june.id]


def test_recorded_entries_are_not_posted_again(ext_session):
  event = _event(ext_session, with_capture=False)
  first = _corrected_rows(ext_session, event)
  second = _second_entry(ext_session, event, posting_date=date(2026, 6, 25))
  event.metadata_ = {**event.metadata_, "qb_entry_ids": {first.id: "JournalEntry_1"}}
  ext_session.commit()

  built, ids = _publish(ext_session, event)

  assert [b["memo"] for b in built] == ["second"]
  assert list(ids) == [second.id]


def test_a_rejection_mid_batch_reports_what_landed(ext_session):
  from robosystems.operations.event_block.qb_writeback import QBWritebackError

  event = _event(ext_session, with_capture=False)
  first = _corrected_rows(ext_session, event)
  _second_entry(ext_session, event, posting_date=date(2026, 6, 25))
  ext_session.commit()

  saves = iter(["77", QBWritebackError({"code": "qb_validation_error"})])

  def _save(je, client, request_id, event_id):
    result = next(saves)
    if isinstance(result, Exception):
      raise result
    return result

  with (
    patch(f"{_MODULE}._build_qb_journal_entry", return_value=object()),
    patch(f"{_MODULE}._save_with_retry", side_effect=_save),
    pytest.raises(QBWritebackError) as excinfo,
  ):
    post_event_to_qb(ext_session, event, qb_client=object())

  assert excinfo.value.published == {first.id: "JournalEntry_77"}


def test_a_mapping_error_posts_nothing(ext_session):
  """Every entry is built before the first POST."""
  from robosystems.operations.event_block.qb_writeback import QBWritebackError

  event = _event(ext_session, with_capture=False)
  _corrected_rows(ext_session, event)
  ext_session.get(Element, "elem_cash").external_id = "qb_1"
  ext_session.get(Element, "elem_rev").external_id = "qb_2"
  second = _second_entry(ext_session, event, posting_date=date(2026, 6, 25))
  ext_session.add(
    Element(id="elem_unmapped", name="Other", code="9", balance_type="debit")
  )
  ext_session.flush()
  ext_session.query(LineItem).filter(
    LineItem.entry_id == second.id, LineItem.line_order == 1
  ).update({LineItem.element_id: "elem_unmapped"})
  ext_session.commit()

  with (
    patch(f"{_MODULE}._save_with_retry") as save,
    pytest.raises(QBWritebackError),
  ):
    post_event_to_qb(ext_session, event, qb_client=object())

  save.assert_not_called()
