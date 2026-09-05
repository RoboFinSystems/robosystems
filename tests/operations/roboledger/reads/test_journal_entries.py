"""Unit tests for the entry-centric journal read (list_journal_entries).

The case that matters most is the first one: **a posted entry with no
parent transaction is returned.** That shape is why this read exists.
`list_transactions` walks `transactions` and hangs entries off them, so
an entry whose `transaction_id` is NULL — every entry the schedule
engine and the event handlers create — appeared on no list surface in
the product. Nothing exercised a parentless entry, which is how it
shipped invisible; that gap is closed here.

Mock-based, matching `test_period_drafts`: the read issues a count query
and a raw-SQL entry query through `session.execute`, so the mock session
serves both in order.
"""

from __future__ import annotations

from datetime import date, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock

from robosystems.operations.roboledger.reads.journal_entries import (
  list_journal_entries,
)


def _line_row(
  entry_id: str,
  line_item_id: str,
  debit: int,
  credit: int,
  *,
  transaction_id: str | None = None,
  status: str = "posted",
  entry_type: str = "closing",
  provenance: str = "schedule_derived",
  source_structure_id: str | None = None,
  source_structure_name: str | None = None,
  line_order: int = 1,
):
  """One row in the shape the shared entry projection returns."""
  return SimpleNamespace(
    entry_id=entry_id,
    number=None,
    transaction_id=transaction_id,
    posting_date=date(2026, 7, 31),
    entry_type=entry_type,
    status=status,
    memo=f"memo-{entry_id}",
    provenance=provenance,
    source_structure_id=source_structure_id,
    source_structure_name=source_structure_name,
    triggered_by_event_id=None,
    reversal_of=None,
    posted_at=datetime(2026, 8, 1, 12, 0, 0),
    line_item_id=line_item_id,
    element_id=f"el-{line_item_id}",
    element_code="6100",
    element_name="Depreciation Expense",
    debit_amount=debit,
    credit_amount=credit,
    line_description=None,
    line_order=line_order,
  )


def _empty_line_row(entry_id: str):
  """The row shape a LEFT JOIN produces when an entry has no line items:
  the entry columns populated, every line-item column NULL."""
  row = _line_row(entry_id, "unused", 0, 0)
  for field in (
    "line_item_id",
    "element_id",
    "element_code",
    "element_name",
    "debit_amount",
    "credit_amount",
    "line_order",
  ):
    setattr(row, field, None)
  return row


def _session(rows, total=None):
  """Mock session serving the count query then the entry query."""
  session = MagicMock()
  count_result = MagicMock()
  count_result.scalar.return_value = (
    len({r.entry_id for r in rows}) if total is None else total
  )
  rows_result = MagicMock()
  rows_result.fetchall.return_value = rows
  session.execute.side_effect = [count_result, rows_result]
  return session


def _params(session):
  """The bind params the entry query was called with."""
  return session.execute.call_args_list[1].args[1]


class TestParentlessEntriesAreVisible:
  """The defect this read was built to fix."""

  def test_posted_entry_with_no_transaction_is_returned(self):
    rows = [
      _line_row("je_dep", "li1", 4241, 0, transaction_id=None),
      _line_row("je_dep", "li2", 0, 4241, transaction_id=None, line_order=2),
    ]

    resp = list_journal_entries(_session(rows))

    assert len(resp.entries) == 1
    entry = resp.entries[0]
    assert entry.id == "je_dep"
    assert entry.transaction_id is None
    assert entry.status == "posted"
    assert entry.balanced is True

  def test_parentless_and_parented_entries_come_back_together(self):
    rows = [
      _line_row("je_standalone", "li1", 1000, 0, transaction_id=None),
      _line_row("je_standalone", "li2", 0, 1000, transaction_id=None, line_order=2),
      _line_row("je_parented", "li3", 2000, 0, transaction_id="txn_1"),
      _line_row("je_parented", "li4", 0, 2000, transaction_id="txn_1", line_order=2),
    ]

    resp = list_journal_entries(_session(rows))

    by_id = {e.id: e for e in resp.entries}
    assert set(by_id) == {"je_standalone", "je_parented"}
    # transaction_id is the only thing distinguishing them, and it is
    # projected rather than inferred — null means standalone, not missing.
    assert by_id["je_standalone"].transaction_id is None
    assert by_id["je_parented"].transaction_id == "txn_1"


class TestProjection:
  def test_line_items_group_under_their_entry(self):
    rows = [
      _line_row("je_a", "li1", 1000, 0),
      _line_row("je_a", "li2", 0, 1000, line_order=2),
      _line_row("je_b", "li3", 500, 0),
      _line_row("je_b", "li4", 0, 500, line_order=2),
    ]

    resp = list_journal_entries(_session(rows))

    assert [len(e.line_items) for e in resp.entries] == [2, 2]

  def test_amounts_are_dollars_not_cents(self):
    rows = [
      _line_row("je_a", "li1", 4241, 0),
      _line_row("je_a", "li2", 0, 4241, line_order=2),
    ]

    resp = list_journal_entries(_session(rows))

    entry = resp.entries[0]
    assert entry.total_debit == 42.41
    assert entry.total_credit == 42.41
    assert entry.line_items[0].debit_amount == 42.41

  def test_unbalanced_entry_is_flagged(self):
    rows = [
      _line_row("je_bad", "li1", 1000, 0),
      _line_row("je_bad", "li2", 0, 900, line_order=2),
    ]

    resp = list_journal_entries(_session(rows))

    assert resp.entries[0].balanced is False

  def test_source_schedule_name_is_projected(self):
    rows = [
      _line_row(
        "je_dep",
        "li1",
        4241,
        0,
        source_structure_id="struct_1",
        source_structure_name='MacBook Pro 14" (2022) Depreciation',
      ),
      _line_row(
        "je_dep",
        "li2",
        0,
        4241,
        source_structure_id="struct_1",
        source_structure_name='MacBook Pro 14" (2022) Depreciation',
        line_order=2,
      ),
    ]

    resp = list_journal_entries(_session(rows))

    assert (
      resp.entries[0].source_structure_name == 'MacBook Pro 14" (2022) Depreciation'
    )


class TestLineItemJoin:
  """The projection LEFT JOINs line_items on purpose."""

  def test_an_entry_with_no_line_items_still_appears(self):
    # `matched` already picked this entry and it already consumed a LIMIT
    # slot and a count. An INNER JOIN would drop it here, making the page
    # silently shorter than it claims and the total disagree with the rows
    # — the same silent-short-answer failure this whole read exists to fix.
    # Double-entry should make it impossible; a bug elsewhere should not
    # turn into a vanishing row.
    rows = [
      # What a LEFT JOIN yields for an entry with no line items.
      _empty_line_row("je_empty"),
      _line_row("je_ok", "li1", 1000, 0),
      _line_row("je_ok", "li2", 0, 1000, line_order=2),
    ]

    resp = list_journal_entries(_session(rows, total=2))

    by_id = {e.id: e for e in resp.entries}
    assert set(by_id) == {"je_empty", "je_ok"}
    assert by_id["je_empty"].line_items == []
    assert by_id["je_empty"].total_debit == 0.0
    assert by_id["je_empty"].balanced is True
    assert len(by_id["je_ok"].line_items) == 2


class TestFilters:
  def test_filters_are_bound_and_default_to_null(self):
    session = _session([])

    list_journal_entries(session)

    params = _params(session)
    for key in (
      "start_date",
      "end_date",
      "status",
      "type",
      "provenance",
      "transaction_id",
    ):
      assert params[key] is None, f"{key} should default to no filter"

  def test_provenance_filter_is_bound(self):
    session = _session([])

    list_journal_entries(session, provenance="schedule_derived")

    assert _params(session)["provenance"] == "schedule_derived"

  def test_date_range_and_status_are_bound(self):
    session = _session([])

    list_journal_entries(
      session,
      start_date=date(2026, 7, 1),
      end_date=date(2026, 7, 31),
      status="posted",
    )

    params = _params(session)
    assert params["start_date"] == date(2026, 7, 1)
    assert params["end_date"] == date(2026, 7, 31)
    assert params["status"] == "posted"

  def test_transaction_id_filter_is_bound(self):
    session = _session([])

    list_journal_entries(session, transaction_id="txn_1")

    assert _params(session)["transaction_id"] == "txn_1"


class TestPagination:
  def test_pagination_counts_entries_not_line_item_rows(self):
    # Two entries, four line-item rows. The count query answers in
    # entries, and the response must not report four.
    rows = [
      _line_row("je_a", "li1", 1000, 0),
      _line_row("je_a", "li2", 0, 1000, line_order=2),
      _line_row("je_b", "li3", 500, 0),
      _line_row("je_b", "li4", 0, 500, line_order=2),
    ]

    resp = list_journal_entries(_session(rows, total=2))

    assert resp.pagination.total == 2
    assert len(resp.entries) == 2

  def test_limit_and_offset_are_bound(self):
    session = _session([])

    list_journal_entries(session, limit=25, offset=50)

    params = _params(session)
    assert params["limit"] == 25
    assert params["offset"] == 50
