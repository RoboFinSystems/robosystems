"""Tests for the journal_entry_recorded Python handler."""

from __future__ import annotations

from datetime import UTC, date, datetime
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.api.extensions.journal_entries import (
  JournalEntryLineItemInput,
  JournalEntryResponse,
)
from robosystems.operations.event_block.python_handlers.journal_entry_recorded import (
  ElementResolutionError,
  JournalEntryRecordedMetadata,
  dispatch,
  dispatch_preview,
)


def _make_event():
  event = MagicMock()
  event.id = "evt_test"
  event.status = "classified"
  event.source = "manual"
  event.occurred_at = datetime(2026, 3, 31, tzinfo=UTC)
  return event


def _balanced_lines() -> list[JournalEntryLineItemInput]:
  return [
    JournalEntryLineItemInput(element_id="elem_cash", debit_amount=5000),
    JournalEntryLineItemInput(element_id="elem_revenue", credit_amount=5000),
  ]


def _make_metadata(**overrides) -> JournalEntryRecordedMetadata:
  defaults = {
    "posting_date": date(2026, 3, 31),
    "memo": "Sale",
    "line_items": _balanced_lines(),
    "type": "standard",
    "status": "draft",
    "transaction_id": None,
  }
  defaults.update(overrides)
  return JournalEntryRecordedMetadata(**defaults)


def _make_body() -> CreateEventBlockRequest:
  return CreateEventBlockRequest(
    event_type="journal_entry_recorded",
    event_category="adjustment",
    source="native",
    occurred_at=datetime(2026, 3, 31, tzinfo=UTC),
    metadata={
      "posting_date": "2026-03-31",
      "memo": "Sale",
      "line_items": [
        {"element_id": "elem_cash", "debit_amount": 5000, "credit_amount": 0},
        {"element_id": "elem_revenue", "debit_amount": 0, "credit_amount": 5000},
      ],
      "type": "standard",
      "status": "draft",
    },
    apply_handlers=True,
  )


def _fake_response(entry_id="je_new", transaction_id="txn_new") -> JournalEntryResponse:
  return JournalEntryResponse(
    id=entry_id,
    transaction_id=transaction_id,
    type="standard",
    status="draft",
    posting_date=date(2026, 3, 31),
    memo="Sale",
    provenance="manual_entry",
    line_items=[],
    total_debit=5000,
    total_credit=5000,
  )


class TestDispatch:
  def test_dispatch_draft_keeps_classified_status(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata(status="draft")

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
      return_value=_fake_response(),
    ) as mock_create:
      result = dispatch(session, event, metadata, created_by="usr_test")

    mock_create.assert_called_once()
    # Both Entry and Transaction got audit-linked (2 updates)
    assert session.execute.call_count == 2
    # Status not mutated for draft entries
    assert event.status == "classified"
    assert result.entry_ids == ["je_new"]
    assert result.transaction_ids == ["txn_new"]

  def test_dispatch_posted_overrides_to_fulfilled(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata(status="posted")

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
      return_value=_fake_response(),
    ):
      dispatch(session, event, metadata, created_by="usr_test")

    # Dynamic status override
    assert event.status == "fulfilled"

  def test_dispatch_no_transaction_id_in_response_skips_link(self) -> None:
    """If caller supplied a transaction_id, response.transaction_id may be None."""
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    resp = _fake_response(transaction_id=None)
    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
      return_value=resp,
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    # Only Entry got linked (1 update)
    assert session.execute.call_count == 1
    assert result.transaction_ids == []

  def test_dispatch_propagates_closed_period_error(self) -> None:
    session = MagicMock()
    event = _make_event()
    metadata = _make_metadata()

    from robosystems.operations.roboledger.commands._guards import ClosedPeriodError

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
      side_effect=ClosedPeriodError("2026-02", date(2026, 2, 28)),
    ):
      with pytest.raises(ClosedPeriodError):
        dispatch(session, event, metadata, created_by="usr_test")


class TestDispatchPreview:
  def test_preview_balanced_ok(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is True
    assert preview.validation_errors == []
    assert preview.computed_values["total_debit_cents"] == 5000
    assert preview.computed_values["total_credit_cents"] == 5000
    assert preview.computed_values["target_status"] == "classified"

  def test_preview_posted_reports_fulfilled_target_status(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata(status="posted")

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is True
    assert preview.computed_values["target_status"] == "fulfilled"

  def test_preview_unbalanced_fails(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata(
      line_items=[
        JournalEntryLineItemInput(element_id="elem_cash", debit_amount=5000),
        JournalEntryLineItemInput(element_id="elem_revenue", credit_amount=3000),
      ]
    )

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("balance" in err.lower() for err in preview.validation_errors)

  def test_preview_closed_period_fails(self) -> None:
    from robosystems.operations.roboledger.commands._guards import ClosedPeriodError

    session = MagicMock()
    body = _make_body()
    metadata = _make_metadata()

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed",
      side_effect=ClosedPeriodError("2026-02", date(2026, 2, 28)),
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any("closed" in err.lower() for err in preview.validation_errors)


# ── Nested-shape tests (QB-style multi-entry metadata) ─────────────────────


def _nested_metadata(
  *,
  connection_id: str | None = "conn_qb_1",
  status: str = "draft",
  entries: list[dict] | None = None,
) -> JournalEntryRecordedMetadata:
  default_entries = entries or [
    {
      "posting_date": "2026-03-31",
      "memo": "QB Invoice 1001",
      "type": "standard",
      "external_id": "qb_je_001",
      "line_items": [
        {
          "element_external_id": "qba_ar",
          "debit_amount": 5000,
          "credit_amount": 0,
        },
        {
          "element_external_id": "qba_revenue",
          "debit_amount": 0,
          "credit_amount": 5000,
        },
      ],
    },
  ]
  return JournalEntryRecordedMetadata(
    entries=default_entries,
    status=status,
    connection_id=connection_id,
  )


class TestSchemaValidation:
  """One-shape-or-the-other invariant — captures the shape gate that the
  earlier flat-only schema couldn't express."""

  def test_both_shapes_rejected(self) -> None:
    with pytest.raises(ValidationError):
      JournalEntryRecordedMetadata(
        posting_date=date(2026, 3, 31),
        memo="x",
        line_items=_balanced_lines(),
        entries=[
          {
            "posting_date": "2026-03-31",
            "memo": "y",
            "line_items": [
              {"element_id": "a", "debit_amount": 1, "credit_amount": 0},
              {"element_id": "b", "debit_amount": 0, "credit_amount": 1},
            ],
          },
        ],
      )

  def test_neither_shape_rejected(self) -> None:
    with pytest.raises(ValidationError):
      JournalEntryRecordedMetadata()

  def test_flat_shape_missing_posting_date_rejected(self) -> None:
    with pytest.raises(ValidationError):
      JournalEntryRecordedMetadata(
        memo="m",
        line_items=_balanced_lines(),
      )

  def test_flat_shape_under_min_lines_rejected(self) -> None:
    with pytest.raises(ValidationError):
      JournalEntryRecordedMetadata(
        posting_date=date(2026, 3, 31),
        memo="m",
        line_items=[
          {"element_id": "a", "debit_amount": 100, "credit_amount": 0},
        ],
      )

  def test_nested_line_with_both_refs_rejected(self) -> None:
    with pytest.raises(ValidationError):
      JournalEntryRecordedMetadata(
        entries=[
          {
            "posting_date": "2026-03-31",
            "memo": "m",
            "line_items": [
              {
                "element_id": "a",
                "element_external_id": "x",
                "debit_amount": 1,
                "credit_amount": 0,
              },
              {"element_id": "b", "debit_amount": 0, "credit_amount": 1},
            ],
          },
        ],
      )


class TestDispatchNested:
  def _patch_resolution(self, mapping: dict[str, str]):
    """Patch the bulk Element-by-external_id resolution."""
    return patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded._resolve_external_ids",
      return_value=mapping,
    )

  def test_dispatch_single_nested_entry(self) -> None:
    session = MagicMock()
    event = _make_event()
    event.source = "quickbooks"
    metadata = _nested_metadata()

    captured_bodies: list[object] = []

    def _create_je_capture(_session, body, _created_by):
      captured_bodies.append(body)
      return _fake_response(entry_id="je_qb1", transaction_id="txn_qb1")

    with (
      self._patch_resolution(
        {"qba_ar": "elem_qba_ar", "qba_revenue": "elem_qba_revenue"}
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
        side_effect=_create_je_capture,
      ),
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    assert len(captured_bodies) == 1
    body = captured_bodies[0]
    assert body.line_items[0].element_id == "elem_qba_ar"
    assert body.line_items[1].element_id == "elem_qba_revenue"
    assert result.entry_ids == ["je_qb1"]
    assert result.transaction_ids == ["txn_qb1"]

  def test_dispatch_multi_entry_shares_single_transaction(self) -> None:
    session = MagicMock()
    event = _make_event()
    event.source = "quickbooks"
    metadata = _nested_metadata(
      entries=[
        {
          "posting_date": "2026-03-31",
          "memo": "Invoice",
          "line_items": [
            {"element_external_id": "qba_ar", "debit_amount": 5000, "credit_amount": 0},
            {
              "element_external_id": "qba_revenue",
              "debit_amount": 0,
              "credit_amount": 5000,
            },
          ],
        },
        {
          "posting_date": "2026-03-31",
          "memo": "Tax",
          "line_items": [
            {"element_external_id": "qba_ar", "debit_amount": 350, "credit_amount": 0},
            {
              "element_external_id": "qba_tax_payable",
              "debit_amount": 0,
              "credit_amount": 350,
            },
          ],
        },
      ],
    )

    # First create returns a fresh txn id; second create reuses it via
    # the shared_txn_id fold.
    fake_responses = iter(
      [
        _fake_response(entry_id="je_1", transaction_id="txn_shared"),
        _fake_response(entry_id="je_2", transaction_id="txn_shared"),
      ]
    )
    captured_txn_ids: list[str | None] = []

    def _create_je(_session, body, _created_by):
      captured_txn_ids.append(body.transaction_id)
      return next(fake_responses)

    with (
      self._patch_resolution(
        {
          "qba_ar": "elem_qba_ar",
          "qba_revenue": "elem_qba_revenue",
          "qba_tax_payable": "elem_qba_tax_payable",
        }
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
        side_effect=_create_je,
      ),
    ):
      result = dispatch(session, event, metadata, created_by="usr_test")

    # First entry creates the transaction, second reuses the id returned
    # by the first call.
    assert captured_txn_ids[0] is None
    assert captured_txn_ids[1] == "txn_shared"
    # Two entries created; transaction id deduped to one.
    assert result.entry_ids == ["je_1", "je_2"]
    assert result.transaction_ids == ["txn_shared"]

  def test_dispatch_unresolved_external_id_raises(self) -> None:
    session = MagicMock()
    event = _make_event()
    event.source = "quickbooks"
    metadata = _nested_metadata()

    with self._patch_resolution(
      # qba_revenue is missing — simulates a CoA element not yet ingested.
      {"qba_ar": "elem_qba_ar"}
    ):
      with pytest.raises(ElementResolutionError) as exc:
        dispatch(session, event, metadata, created_by="usr_test")

    assert "qba_revenue" in str(exc.value)

  def test_dispatch_collects_all_unresolved_ids_into_one_error(self) -> None:
    """A QB event with N unmapped accounts should error once with N IDs,
    not error N times in sequence as the user maps them one at a time.

    Regression: prior implementation raised on the first miss, forcing
    a one-at-a-time discovery loop. Now the handler reports them all in
    a single error message."""
    session = MagicMock()
    event = _make_event()
    event.source = "quickbooks"
    metadata = _nested_metadata(
      entries=[
        {
          "posting_date": "2026-03-31",
          "memo": "Invoice",
          "line_items": [
            {"element_external_id": "qba_a", "debit_amount": 100, "credit_amount": 0},
            {"element_external_id": "qba_b", "debit_amount": 0, "credit_amount": 100},
          ],
        },
        {
          "posting_date": "2026-03-31",
          "memo": "Tax",
          "line_items": [
            {"element_external_id": "qba_c", "debit_amount": 50, "credit_amount": 0},
            {"element_external_id": "qba_d", "debit_amount": 0, "credit_amount": 50},
          ],
        },
      ],
    )

    # Resolution returns NONE of the IDs — every line is unmapped.
    with self._patch_resolution({}):
      with pytest.raises(ElementResolutionError) as exc:
        dispatch(session, event, metadata, created_by="usr_test")

    msg = str(exc.value)
    # All four IDs surfaced in a single error
    assert "qba_a" in msg
    assert "qba_b" in msg
    assert "qba_c" in msg
    assert "qba_d" in msg
    # Count summary
    assert "4" in msg

  def test_dispatch_dedupes_repeated_unresolved_id_across_entries(self) -> None:
    """Same external_id referenced in two entries should appear once
    in the error (with the first-seen entry index)."""
    session = MagicMock()
    event = _make_event()
    event.source = "quickbooks"
    metadata = _nested_metadata(
      entries=[
        {
          "posting_date": "2026-03-31",
          "memo": "First",
          "line_items": [
            {"element_external_id": "qba_dup", "debit_amount": 100, "credit_amount": 0},
            {
              "element_external_id": "qba_other_a",
              "debit_amount": 0,
              "credit_amount": 100,
            },
          ],
        },
        {
          "posting_date": "2026-03-31",
          "memo": "Second",
          "line_items": [
            {"element_external_id": "qba_dup", "debit_amount": 50, "credit_amount": 0},
            {
              "element_external_id": "qba_other_b",
              "debit_amount": 0,
              "credit_amount": 50,
            },
          ],
        },
      ],
    )

    with self._patch_resolution({}):
      with pytest.raises(ElementResolutionError) as exc:
        dispatch(session, event, metadata, created_by="usr_test")

    msg = str(exc.value)
    # Three distinct IDs (qba_dup deduped), not four
    assert "3" in msg
    # qba_dup reported with the first entry it appeared in (entry 0)
    assert "'qba_dup' (entry 0)" in msg

  def test_dispatch_posted_status_overrides_for_nested(self) -> None:
    session = MagicMock()
    event = _make_event()
    event.source = "quickbooks"
    metadata = _nested_metadata(status="posted")

    with (
      self._patch_resolution(
        {"qba_ar": "elem_qba_ar", "qba_revenue": "elem_qba_revenue"}
      ),
      patch(
        "robosystems.operations.event_block.python_handlers.journal_entry_recorded.create_journal_entry",
        return_value=_fake_response(),
      ),
    ):
      dispatch(session, event, metadata, created_by="usr_test")

    assert event.status == "fulfilled"


class TestDispatchPreviewNested:
  def test_preview_balanced_nested_ok(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _nested_metadata()

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is True
    assert preview.validation_errors == []
    assert preview.computed_values["entry_count"] == 1
    assert preview.computed_values["total_debit_cents"] == 5000

  def test_preview_unbalanced_nested_entry_fails(self) -> None:
    session = MagicMock()
    body = _make_body()
    metadata = _nested_metadata(
      entries=[
        {
          "posting_date": "2026-03-31",
          "memo": "Bad",
          "line_items": [
            {"element_external_id": "qba_ar", "debit_amount": 5000, "credit_amount": 0},
            {
              "element_external_id": "qba_rev",
              "debit_amount": 0,
              "credit_amount": 3000,
            },
          ],
        },
      ],
    )

    with patch(
      "robosystems.operations.event_block.python_handlers.journal_entry_recorded.assert_period_not_closed"
    ):
      preview = dispatch_preview(session, body, metadata)

    assert preview.would_succeed is False
    assert any(
      "Entry 0" in e and "balance" in e.lower() for e in preview.validation_errors
    )
