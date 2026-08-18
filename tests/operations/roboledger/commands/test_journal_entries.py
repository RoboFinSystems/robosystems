"""Unit tests for journal entry CRUD commands.

Tests cover:
- validate_and_normalize_lines — balance enforcement, per-line validation
- create_journal_entry — draft/posted status, closed-period gate, session writes
- update_journal_entry — draft-only gate, scalar updates, line replacement,
  unbalanced-before-delete safety, period gate on date change
- delete_journal_entry — draft-only gate, hard delete mechanics
- reverse_journal_entry — posted-only gate, debit↔credit flip, reversal_of FK,
  original marked reversed, closed-period gate on reversal date
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.extensions.journal_entries import (
  CreateJournalEntryRequest,
  DeleteJournalEntryRequest,
  JournalEntryLineItemInput,
  ReverseJournalEntryRequest,
  UpdateJournalEntryRequest,
)
from robosystems.operations.locking import RowLockedError
from robosystems.operations.roboledger.commands._guards import ClosedPeriodError
from robosystems.operations.roboledger.commands.journal_entries import (
  JournalEntryNotDraftError,
  JournalEntryNotFoundError,
  JournalEntryNotPostedError,
  UnbalancedJournalEntryError,
  create_journal_entry,
  delete_journal_entry,
  resolve_flow_element_id,
  reverse_journal_entry,
  update_journal_entry,
  validate_and_normalize_lines,
)

MODULE = "robosystems.operations.roboledger.commands.journal_entries"

_DATE = date(2026, 1, 15)


# ── Helpers ──────────────────────────────────────────────────────────────


def _line(element_id: str = "elem_01", debit: int = 0, credit: int = 0):
  return JournalEntryLineItemInput(
    element_id=element_id,
    debit_amount=debit,
    credit_amount=credit,
  )


def _balanced_lines():
  """Minimal balanced DR/CR pair — 1 000 cents each side."""
  return [
    _line("elem_cash", debit=1000),
    _line("elem_revenue", credit=1000),
  ]


def _mock_entry(
  status: str = "draft",
  entry_id: str = "entry_01",
  transaction_id: str | None = None,
):
  e = MagicMock()
  e.id = entry_id
  e.status = status
  e.transaction_id = transaction_id
  e.type = "standard"
  e.posting_date = _DATE
  e.memo = "Test entry"
  e.provenance = "manual_entry"
  e.reversal_of = None
  e.posted_at = None
  return e


def _session_for_entry(entry):
  """Peek via execute, lock via session.get — the update/delete sequence."""
  session = MagicMock()
  session.execute.return_value.scalar_one_or_none.return_value = entry
  session.get.return_value = entry
  return session


def _mock_line(element_id: str, debit: int, credit: int, order: int):
  li = MagicMock()
  li.id = f"li_{order:02d}"
  li.element_id = element_id
  li.debit_amount = debit
  li.credit_amount = credit
  li.line_order = order
  li.description = None
  return li


def _scalar_exec(value):
  r = MagicMock()
  r.scalar_one_or_none.return_value = value
  return r


def _scalars_exec(values):
  r = MagicMock()
  r.scalars.return_value.all.return_value = values
  return r


# ── validate_and_normalize_lines ────────────────────────────────────────


class TestValidateAndNormalizeLines:
  def test_empty_list_raises(self):
    with pytest.raises(ValueError, match="at least one line item"):
      validate_and_normalize_lines([])

  def test_missing_element_id_raises(self):
    with pytest.raises(ValueError, match="missing element_id"):
      validate_and_normalize_lines([_line(element_id="", debit=100)])

  def test_negative_debit_raises(self):
    with pytest.raises(ValueError, match="non-negative"):
      validate_and_normalize_lines(
        [
          _line("elem_a", debit=-100),
          _line("elem_b", credit=100),
        ]
      )

  def test_negative_credit_raises(self):
    with pytest.raises(ValueError, match="non-negative"):
      validate_and_normalize_lines(
        [
          _line("elem_a", debit=100),
          _line("elem_b", credit=-100),
        ]
      )

  def test_both_debit_and_credit_raises(self):
    with pytest.raises(ValueError, match="cannot have both"):
      validate_and_normalize_lines(
        [
          _line("elem_a", debit=100, credit=100),
        ]
      )

  def test_zero_amounts_raises(self):
    with pytest.raises(ValueError, match="non-zero"):
      validate_and_normalize_lines([_line("elem_a")])  # debit=0, credit=0

  def test_unbalanced_raises_with_amounts(self):
    with pytest.raises(UnbalancedJournalEntryError) as exc_info:
      validate_and_normalize_lines(
        [
          _line("elem_a", debit=1000),
          _line("elem_b", credit=900),
        ]
      )
    assert exc_info.value.total_debit == 1000
    assert exc_info.value.total_credit == 900

  def test_balanced_returns_correct_totals(self):
    lines = [
      _line("elem_a", debit=500),
      _line("elem_b", debit=500),
      _line("elem_c", credit=1000),
    ]
    normalized, total_dr, total_cr = validate_and_normalize_lines(lines)
    assert total_dr == 1000
    assert total_cr == 1000
    assert len(normalized) == 3

  def test_normalized_dict_has_expected_keys(self):
    lines = [_line("elem_a", debit=200), _line("elem_b", credit=200)]
    normalized, _, _ = validate_and_normalize_lines(lines)
    assert normalized[0]["element_id"] == "elem_a"
    assert normalized[0]["debit_amount"] == 200
    assert normalized[0]["credit_amount"] == 0
    assert normalized[1]["debit_amount"] == 0
    assert normalized[1]["credit_amount"] == 200

  def test_many_lines_balance(self):
    lines = [_line(f"elem_{i}", debit=100) for i in range(5)]
    lines += [_line("elem_credit", credit=500)]
    normalized, dr, cr = validate_and_normalize_lines(lines)
    assert dr == cr == 500
    assert len(normalized) == 6


class TestLineMetadataPassthrough:
  """Per-line ``metadata`` rides through the normalization layer; the flow
  tag (``transaction_description_code``) is then promoted to the first-class
  ``LineItem.flow_element_id`` FK at create time and stripped from the stored
  metadata. Other metadata keys are preserved.
  """

  def test_metadata_normalizes_to_dict_when_present(self):
    lines = [
      JournalEntryLineItemInput(
        element_id="elem_cash",
        debit_amount=10000,
        metadata={
          "transaction_description_code": "mini:ProceedsFromInvestmentsByOwner"
        },
      ),
      JournalEntryLineItemInput(
        element_id="elem_paidin",
        credit_amount=10000,
        metadata={"transaction_description_code": "mini:InvestmentsByOwner"},
      ),
    ]
    normalized, _dr, _cr = validate_and_normalize_lines(lines)
    assert (
      normalized[0]["metadata"]["transaction_description_code"]
      == "mini:ProceedsFromInvestmentsByOwner"
    )
    assert (
      normalized[1]["metadata"]["transaction_description_code"]
      == "mini:InvestmentsByOwner"
    )

  def test_metadata_defaults_to_empty_dict_when_absent(self):
    """Backward compatibility: existing callers that don't pass
    metadata get an empty dict, never None — matches LineItem.metadata_
    NOT NULL with default {}."""
    normalized, _dr, _cr = validate_and_normalize_lines(_balanced_lines())
    assert normalized[0]["metadata"] == {}
    assert normalized[1]["metadata"] == {}

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  @patch(f"{MODULE}.resolve_flow_element_id")
  def test_create_journal_entry_promotes_flow_tag(
    self, mock_resolve, _mock_guard, _mock_resp
  ):
    """A flow-tagged line resolves to ``flow_element_id`` and strips the
    legacy ``transaction_description_code`` key from the stored metadata."""
    from robosystems.models.extensions.roboledger.line_item import LineItem

    mock_resolve.side_effect = ["elem_flow_a", "elem_flow_b"]
    session = MagicMock()
    body = CreateJournalEntryRequest(
      posting_date=_DATE,
      memo="Owner investment",
      line_items=[
        JournalEntryLineItemInput(
          element_id="elem_cash",
          debit_amount=10000,
          metadata={
            "transaction_description_code": "mini:ProceedsFromInvestmentsByOwner",
            "source_ref": "JE-201",
          },
        ),
        JournalEntryLineItemInput(
          element_id="elem_paidin",
          credit_amount=10000,
          metadata={"transaction_description_code": "mini:InvestmentsByOwner"},
        ),
      ],
    )
    create_journal_entry(session, body, "usr_1")

    line_adds = [
      call[0][0]
      for call in session.add.call_args_list
      if isinstance(call[0][0], LineItem)
    ]
    assert len(line_adds) == 2
    # Flow tag promoted to the FK.
    assert line_adds[0].flow_element_id == "elem_flow_a"
    assert line_adds[1].flow_element_id == "elem_flow_b"
    # Legacy key stripped from stored metadata; unrelated keys preserved.
    assert "transaction_description_code" not in line_adds[0].metadata_
    assert line_adds[0].metadata_["source_ref"] == "JE-201"
    assert "transaction_description_code" not in line_adds[1].metadata_


class TestResolveFlowElementId:
  """``resolve_flow_element_id`` maps a line's flow tag (a flow-concept qname)
  to its Element id for ``LineItem.flow_element_id``."""

  def test_resolves_qname_to_element_id(self):
    session = MagicMock()
    el_row = MagicMock()
    el_row.fetchone.return_value = ("elem_x",)
    activity_row = MagicMock()
    activity_row.fetchone.return_value = (1,)  # has an activityType trait
    session.execute.side_effect = [el_row, activity_row]
    assert (
      resolve_flow_element_id(
        session, {"transaction_description_code": "mini:CollectionOfReceivables"}
      )
      == "elem_x"
    )

  def test_returns_none_when_tag_absent(self):
    session = MagicMock()
    assert resolve_flow_element_id(session, None) is None
    assert resolve_flow_element_id(session, {}) is None
    assert resolve_flow_element_id(session, {"other": "x"}) is None

  def test_returns_none_when_qname_unresolvable(self):
    session = MagicMock()
    miss = MagicMock()
    miss.fetchone.return_value = None
    session.execute.side_effect = [miss]
    assert (
      resolve_flow_element_id(
        session, {"transaction_description_code": "mini:NoSuchConcept"}
      )
      is None
    )


# ── create_journal_entry ──────────────────────────────────────────────────


def _captured_entry_adds(session):
  """Return Entry objects that were passed to session.add()."""
  from robosystems.models.extensions.roboledger.entry import Entry

  return [
    call[0][0] for call in session.add.call_args_list if isinstance(call[0][0], Entry)
  ]


class TestCreateJournalEntry:
  def _body(self, status: str = "draft"):
    return CreateJournalEntryRequest(
      posting_date=_DATE,
      memo="Test entry",
      line_items=_balanced_lines(),
      status=status,
    )

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_draft_entry_has_no_posted_at(self, mock_guard, mock_resp):
    mock_resp.return_value = MagicMock()
    session = MagicMock()
    create_journal_entry(session, self._body(status="draft"), "usr_1")
    entries = _captured_entry_adds(session)
    assert len(entries) == 1
    assert entries[0].status == "draft"
    assert entries[0].posted_at is None

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_posted_entry_has_posted_at(self, mock_guard, mock_resp):
    mock_resp.return_value = MagicMock()
    session = MagicMock()
    create_journal_entry(session, self._body(status="posted"), "usr_1")
    entries = _captured_entry_adds(session)
    assert len(entries) == 1
    assert entries[0].status == "posted"
    assert entries[0].posted_at is not None

  @patch(
    f"{MODULE}.assert_period_not_closed",
    side_effect=ClosedPeriodError("2026-01", _DATE),
  )
  def test_closed_period_raises_before_write(self, mock_guard):
    session = MagicMock()
    with pytest.raises(ClosedPeriodError):
      create_journal_entry(session, self._body(), "usr_1")
    session.add.assert_not_called()

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_entry_and_line_items_added(self, mock_guard, mock_resp):
    mock_resp.return_value = MagicMock()
    session = MagicMock()
    create_journal_entry(session, self._body(), "usr_1")
    # 1 Transaction (auto-created) + 1 Entry + 2 LineItems = 4 session.add calls
    assert session.add.call_count == 4

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_auto_creates_transaction_when_none_provided(self, mock_guard, mock_resp):
    from robosystems.models.extensions.roboledger.transaction import Transaction

    mock_resp.return_value = MagicMock()
    session = MagicMock()
    create_journal_entry(session, self._body(), "usr_1")
    txn_adds = [
      call[0][0]
      for call in session.add.call_args_list
      if isinstance(call[0][0], Transaction)
    ]
    assert len(txn_adds) == 1
    assert txn_adds[0].type == "journal_entry"
    assert txn_adds[0].source == "native"

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_skips_transaction_creation_when_provided(self, mock_guard, mock_resp):
    from robosystems.models.extensions.roboledger.transaction import Transaction

    mock_resp.return_value = MagicMock()
    session = MagicMock()
    body = CreateJournalEntryRequest(
      posting_date=_DATE,
      memo="Pre-existing txn",
      line_items=_balanced_lines(),
      transaction_id="txn_existing",
    )
    create_journal_entry(session, body, "usr_1")
    txn_adds = [
      call[0][0]
      for call in session.add.call_args_list
      if isinstance(call[0][0], Transaction)
    ]
    assert len(txn_adds) == 0

  @patch(f"{MODULE}.assert_period_not_closed")
  def test_unbalanced_lines_raises_before_any_add(self, mock_guard):
    session = MagicMock()
    body = CreateJournalEntryRequest(
      posting_date=_DATE,
      memo="Unbalanced",
      line_items=[
        _line("elem_a", debit=500),
        _line("elem_b", credit=300),
      ],
    )
    with pytest.raises(UnbalancedJournalEntryError):
      create_journal_entry(session, body, "usr_1")
    session.add.assert_not_called()

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_provenance_is_manual_entry(self, mock_guard, mock_resp):
    mock_resp.return_value = MagicMock()
    session = MagicMock()
    create_journal_entry(session, self._body(), "usr_1")
    entries = _captured_entry_adds(session)
    assert entries[0].provenance == "manual_entry"

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_period_guard_called_with_posting_date(self, mock_guard, mock_resp):
    mock_resp.return_value = MagicMock()
    session = MagicMock()
    create_journal_entry(session, self._body(), "usr_1")
    mock_guard.assert_called_once_with(session, _DATE)


# ── update_journal_entry ──────────────────────────────────────────────────


class TestUpdateJournalEntry:
  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_not_found_raises(self, mock_load):
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    with pytest.raises(JournalEntryNotFoundError) as exc_info:
      update_journal_entry(session, UpdateJournalEntryRequest(entry_id="missing"))
    assert exc_info.value.entry_id == "missing"

  @patch(f"{MODULE}.assert_period_not_closed")
  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_posted_entry_raises(self, mock_load, mock_guard):
    entry = _mock_entry(status="posted")
    with pytest.raises(JournalEntryNotDraftError) as exc_info:
      update_journal_entry(
        _session_for_entry(entry), UpdateJournalEntryRequest(entry_id="entry_01")
      )
    assert exc_info.value.status == "posted"

  @patch(f"{MODULE}.assert_period_not_closed")
  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_reversed_entry_raises(self, mock_load, mock_guard):
    entry = _mock_entry(status="reversed")
    with pytest.raises(JournalEntryNotDraftError) as exc_info:
      update_journal_entry(
        _session_for_entry(entry), UpdateJournalEntryRequest(entry_id="entry_01")
      )
    assert exc_info.value.status == "reversed"

  @patch(f"{MODULE}.assert_period_not_closed")
  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_scalar_field_update(self, mock_load, mock_guard):
    entry = _mock_entry(status="draft")
    entry.memo = "Old memo"
    update_journal_entry(
      _session_for_entry(entry),
      UpdateJournalEntryRequest(entry_id="entry_01", memo="New memo"),
    )
    assert entry.memo == "New memo"

  @patch(f"{MODULE}.assert_period_not_closed")
  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_posting_date_change_checks_period_gate(self, mock_load, mock_guard):
    entry = _mock_entry(status="draft")
    session = _session_for_entry(entry)
    new_date = date(2026, 2, 1)
    update_journal_entry(
      session,
      UpdateJournalEntryRequest(entry_id="entry_01", posting_date=new_date),
    )
    mock_guard.assert_called_once_with(session, _DATE, new_date)

  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_memo_only_update_still_fences_the_existing_period(self, mock_load):
    entry = _mock_entry(status="draft")
    session = _session_for_entry(entry)
    with patch(f"{MODULE}.assert_period_not_closed") as mock_guard:
      update_journal_entry(
        session, UpdateJournalEntryRequest(entry_id="entry_01", memo="Updated")
      )
      mock_guard.assert_called_once_with(session, _DATE)

  @patch(f"{MODULE}.assert_period_not_closed")
  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_line_item_replacement_calls_delete(self, mock_load, mock_guard):
    entry = _mock_entry(status="draft")
    session = _session_for_entry(entry)
    body = UpdateJournalEntryRequest(
      entry_id="entry_01",
      line_items=[
        JournalEntryLineItemInput(element_id="elem_a", debit_amount=400),
        JournalEntryLineItemInput(element_id="elem_b", credit_amount=400),
      ],
    )
    update_journal_entry(session, body)
    # Delete existing line items + add 2 new ones
    session.query.assert_called()
    assert session.add.call_count == 2

  @patch(f"{MODULE}.assert_period_not_closed")
  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_unbalanced_replacement_raises_before_delete(self, mock_load, mock_guard):
    """Validation must run before the delete so a bad batch cannot clobber existing lines."""
    entry = _mock_entry(status="draft")
    session = _session_for_entry(entry)
    body = UpdateJournalEntryRequest(
      entry_id="entry_01",
      line_items=[
        JournalEntryLineItemInput(element_id="elem_a", debit_amount=300),
        JournalEntryLineItemInput(element_id="elem_b", credit_amount=200),
      ],
    )
    with pytest.raises(UnbalancedJournalEntryError):
      update_journal_entry(session, body)
    session.query.assert_not_called()

  @patch(f"{MODULE}.assert_period_not_closed")
  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_locks_the_entry_after_the_period_fence(self, mock_load, mock_guard):
    entry = _mock_entry(status="draft")
    session = _session_for_entry(entry)
    update_journal_entry(
      session, UpdateJournalEntryRequest(entry_id="entry_01", memo="x")
    )
    session.get.assert_called()
    kwargs = session.get.call_args.kwargs
    assert kwargs.get("with_for_update") is True
    assert kwargs.get("populate_existing") is True

  @patch(f"{MODULE}.assert_period_not_closed")
  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_moved_posting_date_is_retryable_not_refenced(self, mock_load, mock_guard):
    """Re-fencing after the entry lock inverts fence→row and can deadlock
    with a close that holds the new period. Retry peeks the current date."""
    peek = _mock_entry(status="draft")
    peek.posting_date = date(2026, 1, 15)
    locked = _mock_entry(status="draft")
    locked.posting_date = date(2026, 3, 1)
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = peek
    session.get.return_value = locked
    with pytest.raises(RowLockedError, match="moved to another period"):
      update_journal_entry(
        session, UpdateJournalEntryRequest(entry_id="entry_01", memo="x")
      )
    mock_guard.assert_called_once_with(session, date(2026, 1, 15))


# ── delete_journal_entry ──────────────────────────────────────────────────


class TestDeleteJournalEntry:
  def test_not_found_raises(self):
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = None
    with pytest.raises(JournalEntryNotFoundError):
      delete_journal_entry(session, DeleteJournalEntryRequest(entry_id="missing"))

  @patch(f"{MODULE}.assert_period_not_closed")
  def test_posted_entry_cannot_be_deleted(self, mock_guard):
    entry = _mock_entry(status="posted")
    with pytest.raises(JournalEntryNotDraftError):
      delete_journal_entry(
        _session_for_entry(entry), DeleteJournalEntryRequest(entry_id="entry_01")
      )

  @patch(f"{MODULE}.assert_period_not_closed")
  def test_reversed_entry_cannot_be_deleted(self, mock_guard):
    entry = _mock_entry(status="reversed")
    with pytest.raises(JournalEntryNotDraftError):
      delete_journal_entry(
        _session_for_entry(entry), DeleteJournalEntryRequest(entry_id="entry_01")
      )

  def test_draft_entry_is_deleted(self):
    entry = _mock_entry(status="draft")
    session = _session_for_entry(entry)
    with patch(f"{MODULE}.assert_period_not_closed") as mock_guard:
      result = delete_journal_entry(
        session, DeleteJournalEntryRequest(entry_id="entry_01")
      )
    mock_guard.assert_called_once_with(session, _DATE)
    assert result == {"deleted": True}
    session.delete.assert_called_once_with(entry)
    session.flush.assert_called()

  @patch(f"{MODULE}.assert_period_not_closed")
  def test_locks_the_entry_after_the_period_fence(self, mock_guard):
    entry = _mock_entry(status="draft")
    session = _session_for_entry(entry)
    delete_journal_entry(session, DeleteJournalEntryRequest(entry_id="entry_01"))
    kwargs = session.get.call_args.kwargs
    assert kwargs.get("with_for_update") is True
    assert kwargs.get("populate_existing") is True

  @patch(f"{MODULE}.assert_period_not_closed")
  def test_moved_posting_date_is_retryable_not_refenced(self, mock_guard):
    peek = _mock_entry(status="draft")
    peek.posting_date = date(2026, 1, 15)
    locked = _mock_entry(status="draft")
    locked.posting_date = date(2026, 3, 1)
    session = MagicMock()
    session.execute.return_value.scalar_one_or_none.return_value = peek
    session.get.return_value = locked
    with pytest.raises(RowLockedError, match="moved to another period"):
      delete_journal_entry(session, DeleteJournalEntryRequest(entry_id="entry_01"))
    mock_guard.assert_called_once_with(session, date(2026, 1, 15))


# ── reverse_journal_entry ─────────────────────────────────────────────────


class TestReverseJournalEntry:
  def _body(self, entry_id: str = "entry_01"):
    return ReverseJournalEntryRequest(entry_id=entry_id, posting_date=_DATE)

  @staticmethod
  def _session(original):
    """Session whose locked load returns `original`.

    The reversal loads the original through `lock_by_id` — `session.get` with
    `with_for_update` and `populate_existing` — rather than a plain select,
    because two concurrent reversals that both read 'posted' both post.
    """
    session = MagicMock()
    session.get.return_value = original
    # The original arrives via the locked `session.get`; `execute` now serves
    # the "does a reversal already exist" check, which these cases want empty.
    session.execute.return_value.scalar_one_or_none.return_value = None
    return session

  def test_not_found_raises(self):
    session = self._session(None)
    with pytest.raises(JournalEntryNotFoundError):
      reverse_journal_entry(session, self._body(), "usr_1")

  def test_draft_entry_cannot_be_reversed(self):
    session = self._session(_mock_entry(status="draft"))
    with pytest.raises(JournalEntryNotPostedError) as exc_info:
      reverse_journal_entry(session, self._body(), "usr_1")
    assert exc_info.value.status == "draft"

  def test_reversed_entry_cannot_be_reversed_again(self):
    session = self._session(_mock_entry(status="reversed"))
    with pytest.raises(JournalEntryNotPostedError):
      reverse_journal_entry(session, self._body(), "usr_1")

  @patch(f"{MODULE}._load_line_items", return_value=[])
  def test_entry_with_no_line_items_raises(self, mock_load):
    session = self._session(_mock_entry(status="posted"))
    with pytest.raises(ValueError, match="no line items"):
      reverse_journal_entry(session, self._body(), "usr_1")

  @patch(
    f"{MODULE}.assert_period_not_closed",
    side_effect=ClosedPeriodError("2026-01", _DATE),
  )
  def test_closed_period_on_reversal_date_raises(self, mock_guard):
    original = _mock_entry(status="posted")
    line = _mock_line("elem_a", debit=100, credit=0, order=1)
    line2 = _mock_line("elem_b", debit=0, credit=100, order=2)
    session = self._session(original)
    # The original now arrives via the locked load, so `execute` serves the
    # bounded wait's `SET LOCAL lock_timeout` and then the line-item read.
    session.execute.side_effect = [
      MagicMock(),  # bounded wait's SET LOCAL lock_timeout
      _scalar_exec(None),  # no existing reversal
      _scalars_exec([line, line2]),
    ]
    with pytest.raises(ClosedPeriodError):
      reverse_journal_entry(session, self._body(), "usr_1")

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_line_items_flipped_debit_to_credit(self, mock_guard, mock_resp):
    """Original DR=1000/CR=0 becomes DR=0/CR=1000 in the reversing entry."""
    from robosystems.models.extensions.roboledger.line_item import LineItem

    mock_resp.return_value = MagicMock()
    original = _mock_entry(status="posted", entry_id="entry_orig")
    line1 = _mock_line("elem_cash", debit=1000, credit=0, order=1)
    line2 = _mock_line("elem_revenue", debit=0, credit=1000, order=2)

    added_objects: list = []
    session = MagicMock()
    session.get.return_value = original
    session.execute.side_effect = [
      MagicMock(),  # bounded wait's SET LOCAL lock_timeout
      _scalar_exec(None),  # no existing reversal
      _scalars_exec([line1, line2]),
      _scalars_exec([]),  # _load_line_items for reversing entry (response build)
    ]
    session.add.side_effect = lambda obj: added_objects.append(obj)

    reverse_journal_entry(session, self._body(), "usr_1")

    reversal_lines = [obj for obj in added_objects if isinstance(obj, LineItem)]
    assert len(reversal_lines) == 2
    # line1 was DR=1000/CR=0 → becomes DR=0/CR=1000
    assert reversal_lines[0].debit_amount == 0
    assert reversal_lines[0].credit_amount == 1000
    # line2 was DR=0/CR=1000 → becomes DR=1000/CR=0
    assert reversal_lines[1].debit_amount == 1000
    assert reversal_lines[1].credit_amount == 0

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_line_items_metadata_preserved_on_reversal(self, mock_guard, mock_resp):
    """Per-line ``metadata_`` (e.g. ``transaction_description_code``)
    must survive the reversal so the rollforward filter engine sees
    the offsetting flow on the reversing period. A lost flow tag
    would break filter aggregation — the reversal'd appear as
    untagged residual instead of the cancelling flow."""
    from robosystems.models.extensions.roboledger.line_item import LineItem

    mock_resp.return_value = MagicMock()
    original = _mock_entry(status="posted", entry_id="entry_orig")
    line1 = _mock_line("elem_cash", debit=1000, credit=0, order=1)
    line1.metadata_ = {
      "transaction_description_code": "mini:ProceedsFromInvestmentsByOwner"
    }
    line2 = _mock_line("elem_paidin", debit=0, credit=1000, order=2)
    line2.metadata_ = {"transaction_description_code": "mini:InvestmentsByOwner"}

    added_objects: list = []
    session = MagicMock()
    session.get.return_value = original
    session.execute.side_effect = [
      MagicMock(),  # bounded wait's SET LOCAL lock_timeout
      _scalar_exec(None),  # no existing reversal
      _scalars_exec([line1, line2]),
      _scalars_exec([]),
    ]
    session.add.side_effect = lambda obj: added_objects.append(obj)

    reverse_journal_entry(session, self._body(), "usr_1")

    reversal_lines = [obj for obj in added_objects if isinstance(obj, LineItem)]
    assert len(reversal_lines) == 2
    # Original DR Cash with ProceedsFromInvestmentsByOwner TDC →
    # reversal CR Cash, SAME TDC. Filter engine sees both lines on
    # the same flow concept; the cancellation is visible to it.
    assert (
      reversal_lines[0].metadata_["transaction_description_code"]
      == "mini:ProceedsFromInvestmentsByOwner"
    )
    assert (
      reversal_lines[1].metadata_["transaction_description_code"]
      == "mini:InvestmentsByOwner"
    )
    # Defensive copy: mutating the reversal metadata_ shouldn't bleed
    # back into the original line's metadata_ (and vice versa).
    reversal_lines[0].metadata_["mutated"] = "yes"
    assert "mutated" not in line1.metadata_

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_reversing_entry_has_reversal_of_and_type(self, mock_guard, mock_resp):
    """Reversing Entry has reversal_of=original.id, type='reversing', status='posted'."""
    from robosystems.models.extensions.roboledger.entry import Entry

    mock_resp.return_value = MagicMock()
    original = _mock_entry(status="posted", entry_id="entry_orig")
    line1 = _mock_line("elem_a", debit=500, credit=0, order=1)
    line2 = _mock_line("elem_b", debit=0, credit=500, order=2)

    added_objects: list = []
    session = MagicMock()
    session.get.return_value = original
    session.execute.side_effect = [
      MagicMock(),  # bounded wait's SET LOCAL lock_timeout
      _scalar_exec(None),  # no existing reversal
      _scalars_exec([line1, line2]),
      _scalars_exec([]),  # _load_line_items for reversing entry
    ]
    session.add.side_effect = lambda obj: added_objects.append(obj)

    reverse_journal_entry(session, self._body(entry_id="entry_orig"), "usr_1")

    entry_adds = [obj for obj in added_objects if isinstance(obj, Entry)]
    assert len(entry_adds) == 1
    reversing = entry_adds[0]
    assert reversing.reversal_of == "entry_orig"
    assert reversing.type == "reversing"
    assert reversing.status == "posted"
    assert reversing.posted_at is not None

  @patch(f"{MODULE}._entry_to_response")
  @patch(f"{MODULE}.assert_period_not_closed")
  def test_original_marked_reversed(self, mock_guard, mock_resp):
    mock_resp.return_value = MagicMock()
    original = _mock_entry(status="posted", entry_id="entry_orig")
    line1 = _mock_line("elem_a", debit=200, credit=0, order=1)
    line2 = _mock_line("elem_b", debit=0, credit=200, order=2)

    session = MagicMock()
    session.get.return_value = original
    session.execute.side_effect = [
      MagicMock(),  # bounded wait's SET LOCAL lock_timeout
      _scalar_exec(None),  # no existing reversal
      _scalars_exec([line1, line2]),
      _scalars_exec([]),  # _load_line_items for reversing entry
    ]
    reverse_journal_entry(session, self._body(), "usr_1")

    assert original.status == "reversed"
