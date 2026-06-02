"""Unit tests for the period-drafts outbox read (list_period_drafts).

Focus: the QB write-back disposition layered on top of the draft listing
— per-entry `will_publish_to_qb` and the response publish summary —
under the three cases that matter: a write-back connection with an
eligible draft, the same drafts with NO write-back connection (all
local-only), and an ineligible draft (already in QB / synced).

Mock-based: the read does a raw-SQL drafts query (`session.execute`) and
an ORM eligibility query (`session.query(...)`), so the mock session
serves both.
"""

from __future__ import annotations

from datetime import date
from types import SimpleNamespace
from unittest.mock import MagicMock

from robosystems.operations.roboledger.fiscal_calendar.qb_writeback import (
  WritebackConnection,
)
from robosystems.operations.roboledger.reads.period_drafts import list_period_drafts


def _line_row(entry_id: str, line_item_id: str, debit: int, credit: int):
  """One draft line-item row in the shape `_DRAFT_ENTRIES_SQL` returns."""
  return SimpleNamespace(
    entry_id=entry_id,
    posting_date=date(2026, 1, 15),
    entry_type="closing",
    memo=f"memo-{entry_id}",
    provenance="schedule",
    source_structure_id=None,
    source_structure_name=None,
    line_item_id=line_item_id,
    element_id=f"el-{line_item_id}",
    element_code="1000",
    element_name="Cash",
    debit_amount=debit,
    credit_amount=credit,
    line_description=None,
  )


def _two_balanced_entries_session(eligible_entry_ids):
  """Mock session: two balanced draft entries (e1, e2); eligibility query
  returns rows for `eligible_entry_ids`."""
  draft_rows = [
    # e1 — balanced (1000 dr / 1000 cr across two lines)
    _line_row("e1", "li1", 1000, 0),
    _line_row("e1", "li2", 0, 1000),
    # e2 — balanced
    _line_row("e2", "li3", 500, 0),
    _line_row("e2", "li4", 0, 500),
  ]
  eligible_rows = [
    (SimpleNamespace(id=eid), SimpleNamespace(id=f"ev-{eid}"))
    for eid in eligible_entry_ids
  ]
  session = MagicMock()
  session.execute.return_value.fetchall.return_value = draft_rows
  session.query.return_value.join.return_value.filter.return_value.all.return_value = (
    eligible_rows
  )
  return session


def _by_id(resp):
  return {d.entry_id: d for d in resp.drafts}


class TestPeriodDraftsDisposition:
  def test_writeback_connection_marks_eligible_draft(self):
    session = _two_balanced_entries_session(eligible_entry_ids={"e1"})
    writeback = WritebackConnection(
      connection_id="conn-1", write_policy="qb_authoritative"
    )

    resp = list_period_drafts(session, "2026-01", writeback=writeback)

    drafts = _by_id(resp)
    assert resp.draft_count == 2
    assert drafts["e1"].will_publish_to_qb is True
    assert drafts["e2"].will_publish_to_qb is False
    assert resp.qb_publish_count == 1
    assert resp.local_only_count == 1
    assert resp.qb_writeback_connection_id == "conn-1"
    assert resp.qb_write_policy == "qb_authoritative"

  def test_no_writeback_connection_is_all_local_only(self):
    # Even though e1 is eligibility-wise a write-back draft, with no
    # write-back connection NOTHING publishes — close posts locally only.
    session = _two_balanced_entries_session(eligible_entry_ids={"e1"})

    resp = list_period_drafts(session, "2026-01", writeback=None)

    assert all(not d.will_publish_to_qb for d in resp.drafts)
    assert resp.qb_publish_count == 0
    assert resp.local_only_count == 2
    assert resp.qb_writeback_connection_id is None
    assert resp.qb_write_policy is None

  def test_ineligible_draft_not_published(self):
    # No draft is eligible (e.g. already in QB / synced source) → even
    # with a write-back connection, nothing publishes.
    session = _two_balanced_entries_session(eligible_entry_ids=set())
    writeback = WritebackConnection(
      connection_id="conn-1", write_policy="qb_authoritative"
    )

    resp = list_period_drafts(session, "2026-01", writeback=writeback)

    assert all(not d.will_publish_to_qb for d in resp.drafts)
    assert resp.qb_publish_count == 0
    assert resp.local_only_count == 2

  def test_default_writeback_arg_is_local_only(self):
    # Back-compat: callers that don't pass `writeback` get the old
    # behavior plus a clean local-only disposition.
    session = _two_balanced_entries_session(eligible_entry_ids={"e1", "e2"})

    resp = list_period_drafts(session, "2026-01")

    assert resp.qb_publish_count == 0
    assert resp.local_only_count == 2
    assert all(not d.will_publish_to_qb for d in resp.drafts)
