"""Tests for event block read operations.

Focus is the reconciling-item surface added for post-sync reconciliation:
the envelope must carry ``is_reconciling_item`` off the ORM row (whose
storage column keeps the mechanical name ``payload_drift``), and
``list_event_blocks`` must compose (or omit) the predicate so the
reconciliation worklist is enumerable.
"""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock

from robosystems.models.extensions.roboledger.event import Event
from robosystems.operations.roboledger.reads.event_block import (
  _to_envelope,
  list_event_blocks,
)


def _event(*, payload_drift: bool | None = None) -> Event:
  return Event(
    id="evt_01",
    event_type="journal_entry_recorded",
    event_category="adjustment",
    event_class="economic",
    status="committed",
    occurred_at=datetime(2026, 4, 15, 12, tzinfo=UTC),
    source="quickbooks",
    external_id="JournalEntry_42",
    amount=12345,
    currency="USD",
    metadata_={"qb_txn_type": "JournalEntry"},
    payload_drift=payload_drift,
    created_at=datetime(2026, 4, 16, 9, tzinfo=UTC),
    created_by="system",
  )


def _captured_list_stmt(session: MagicMock) -> str:
  (stmt,) = session.execute.call_args.args
  return str(stmt)


class TestEnvelopeCarriesDrift:
  def test_drifted_row_maps_true(self) -> None:
    envelope = _to_envelope(_event(payload_drift=True), [])
    assert envelope.is_reconciling_item is True

  def test_unset_column_maps_false(self) -> None:
    # A row constructed without the flag (Python-side default is None
    # until flush) must read as not-drifted, never None.
    envelope = _to_envelope(_event(), [])
    assert envelope.is_reconciling_item is False


class TestListDriftFilter:
  def _session(self) -> MagicMock:
    session = MagicMock()
    session.execute.return_value.scalars.return_value.all.return_value = []
    return session

  def test_true_filter_composes_drift_predicate(self) -> None:
    session = self._session()
    list_event_blocks(session, is_reconciling_item=True)
    assert "payload_drift IS true" in _captured_list_stmt(session)

  def test_false_filter_composes_drift_predicate(self) -> None:
    session = self._session()
    list_event_blocks(session, is_reconciling_item=False)
    assert "payload_drift IS false" in _captured_list_stmt(session)

  def test_omitted_filter_leaves_drift_out(self) -> None:
    session = self._session()
    list_event_blocks(session)
    # The column always appears in the SELECT list; only the predicate
    # must be absent.
    assert "payload_drift IS" not in _captured_list_stmt(session)
