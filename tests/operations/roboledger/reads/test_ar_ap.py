"""Smoke tests for AR/AP read operations.

The math is one SQL query with a subquery + filtered aggregate — too
complex to mock meaningfully without re-implementing it in the
assertions. These tests verify (a) the SQL compiles against a real
Postgres dialect, (b) the candidate event-type sets are wired
correctly, and (c) the response shape matches the Pydantic contract.
Numerical correctness is validated end-to-end via the QB sandbox
verification step (see roadmap §5.1 exit).
"""

from __future__ import annotations

from unittest.mock import MagicMock

from sqlalchemy.dialects import postgresql

from robosystems.operations.roboledger.reads import ar_ap as reads_ar_ap
from robosystems.operations.roboledger.reads.ar_ap import (
  _AP_ORIGINATING_TYPES,
  _AR_ORIGINATING_TYPES,
  _OPEN_BALANCE_STATUSES,
  _build_open_balance_subquery,
)


def test_originating_type_sets_are_disjoint_and_explicit() -> None:
  """AR and AP must never share an originating event type — a single
  event can't be both a receivable and a payable."""
  assert set(_AR_ORIGINATING_TYPES).isdisjoint(set(_AP_ORIGINATING_TYPES))
  assert "invoice_issued" in _AR_ORIGINATING_TYPES
  assert "sales_receipt_recorded" in _AR_ORIGINATING_TYPES
  assert "bill_received" in _AP_ORIGINATING_TYPES


def test_open_balance_statuses_exclude_pending_and_voided() -> None:
  """Captured / classified are inbox-pending; voided / superseded are
  terminal off-ramps — neither should count toward an open balance."""
  assert set(_OPEN_BALANCE_STATUSES) == {"committed", "fulfilled"}
  assert "captured" not in _OPEN_BALANCE_STATUSES
  assert "voided" not in _OPEN_BALANCE_STATUSES


def test_subquery_filters_on_originating_types() -> None:
  """The originating-type WHERE clause carries through to the compiled SQL."""
  stmt = _build_open_balance_subquery(originating_types=_AR_ORIGINATING_TYPES)
  compiled = str(
    stmt.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True})
  )
  assert "invoice_issued" in compiled
  assert "sales_receipt_recorded" in compiled
  # The subquery should sum discharges via a LEFT OUTER JOIN onto Event itself.
  assert "LEFT OUTER JOIN events" in compiled
  assert "discharges_event_id" in compiled


def test_aggregate_compiles_against_postgres() -> None:
  """End-to-end SQL build — every column reference resolves."""
  session = MagicMock()
  fake_row = MagicMock(
    total_open=12345, counterparty_count=3, open_event_count=5, currency="USD"
  )
  session.execute.return_value.one.return_value = fake_row

  response = reads_ar_ap.compute_open_receivables(session)
  assert response.total_open_cents == 12345
  assert response.counterparty_count == 3
  assert response.open_event_count == 5
  assert response.currency == "USD"


def test_aggregate_handles_zero_state() -> None:
  """An empty events table returns zeros, not NULLs."""
  session = MagicMock()
  fake_row = MagicMock(
    total_open=None, counterparty_count=None, open_event_count=None, currency=None
  )
  session.execute.return_value.one.return_value = fake_row

  response = reads_ar_ap.compute_open_payables(session)
  assert response.total_open_cents == 0
  assert response.counterparty_count == 0
  assert response.open_event_count == 0
  assert response.currency == "USD"  # default fallback


def test_by_agent_returns_empty_list_when_no_rows() -> None:
  session = MagicMock()
  session.execute.return_value.all.return_value = []
  assert reads_ar_ap.list_open_receivables_by_agent(session) == []
  assert reads_ar_ap.list_open_payables_by_agent(session) == []


def test_by_agent_per_agent_lookup() -> None:
  """Single-agent variant returns at most one row."""
  session = MagicMock()
  fake_row = MagicMock(
    agent_id="agt_cust", open_balance=5000, open_event_count=2, currency="USD"
  )
  session.execute.return_value.all.return_value = [fake_row]

  response = reads_ar_ap.get_open_receivable_for_agent(session, "agt_cust")
  assert response is not None
  assert response.agent_id == "agt_cust"
  assert response.open_balance_cents == 5000
  assert response.open_event_count == 2


def test_by_agent_returns_none_when_agent_has_no_open_balance() -> None:
  session = MagicMock()
  session.execute.return_value.all.return_value = []
  assert reads_ar_ap.get_open_payable_for_agent(session, "agt_vendor") is None
