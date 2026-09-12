"""Tests for the bank-feed handlers: the classify-then-commit half of a feed."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from pydantic import ValidationError

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.operations.event_block.python_handlers.bank_feed import (
  BANK_EVENT_TYPES,
  BANK_FEED_HANDLERS,
  BankEventNotClassifiedError,
  BankFeedMetadata,
  classification_summary,
  contra_allocations,
  dispatch,
  dispatch_preview,
  plan_lines,
)
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
)
from robosystems.operations.event_block.registry import (
  HandlerAmbiguousError,
  HandlerNotFoundError,
)

MODULE = "robosystems.operations.event_block.python_handlers.bank_feed"


def _event(
  event_type: str = "bank_transaction",
  amount: int = -4250,
  resource_element_id: str | None = "elem_card",
  metadata: dict | None = None,
):
  event = MagicMock()
  event.id = "evt_bank"
  event.event_type = event_type
  event.event_category = "purchase"
  event.source = "mercury"
  event.resource_type = "money"
  event.agent_id = "agt_staples"
  event.amount = amount
  event.resource_element_id = resource_element_id
  event.description = "Staples (card)"
  event.occurred_at = datetime(2026, 3, 14, 15, 4, 5, tzinfo=UTC)
  event.effective_at = None
  event.metadata_ = metadata or {}
  return event


def _lines_as_tuples(lines):
  return [(li.element_id, li.debit_amount, li.credit_amount) for li in lines]


@pytest.mark.unit
class TestMetadata:
  def test_open_schema_keeps_the_feed_keys(self):
    meta = BankFeedMetadata.model_validate(
      {"bank_description": "STAPLES", "suggested_element_id": "elem_office"}
    )
    assert meta.suggested_element_id == "elem_office"
    assert meta.model_extra == {"bank_description": "STAPLES"}

  def test_allocation_amount_must_be_positive(self):
    with pytest.raises(ValidationError):
      BankFeedMetadata.model_validate(
        {"classified_allocations": [{"element_id": "e", "amount": 0}]}
      )

  def test_contra_allocations_precedence(self):
    both = BankFeedMetadata(
      classified_element_id="elem_single",
      classified_allocations=[
        {"element_id": "elem_a", "amount": 1000},
        {"element_id": "elem_b", "amount": 3250},
      ],
    )
    assert [a.element_id for a in contra_allocations(both, amount=-4250)] == [
      "elem_a",
      "elem_b",
    ]
    single = BankFeedMetadata(classified_element_id="elem_single")
    assert [a.element_id for a in contra_allocations(single, amount=-4250)] == [
      "elem_single"
    ]
    accepted = BankFeedMetadata(
      accept_suggestion=True, suggested_element_id="elem_sugg"
    )
    assert [a.element_id for a in contra_allocations(accepted, amount=99)] == [
      "elem_sugg"
    ]
    unaccepted = BankFeedMetadata(suggested_element_id="elem_sugg")
    assert contra_allocations(unaccepted, amount=99) is None

  def test_split_must_add_up(self):
    meta = BankFeedMetadata(
      classified_allocations=[{"element_id": "elem_a", "amount": 1000}]
    )
    with pytest.raises(HandlerMetadataValidationError, match="sum to 1000"):
      contra_allocations(meta, amount=-4250)


@pytest.mark.unit
class TestPlanLines:
  def test_money_out_debits_the_contra_and_credits_the_bank(self):
    lines = plan_lines(
      event_type="bank_transaction",
      resource_element_id="elem_card",
      amount=-4250,
      metadata=BankFeedMetadata(classified_element_id="elem_office"),
    )
    assert _lines_as_tuples(lines) == [
      ("elem_office", 4250, 0),
      ("elem_card", 0, 4250),
    ]

  def test_money_in_debits_the_bank_and_credits_the_contra(self):
    lines = plan_lines(
      event_type="bank_transaction",
      resource_element_id="elem_checking",
      amount=9514,
      metadata=BankFeedMetadata(
        accept_suggestion=True, suggested_element_id="elem_revenue"
      ),
    )
    assert _lines_as_tuples(lines) == [
      ("elem_checking", 9514, 0),
      ("elem_revenue", 0, 9514),
    ]

  def test_split_posts_one_line_per_allocation(self):
    lines = plan_lines(
      event_type="bank_transaction",
      resource_element_id="elem_card",
      amount=-4250,
      metadata=BankFeedMetadata(
        classified_allocations=[
          {"element_id": "elem_office", "amount": 4000},
          {"element_id": "elem_tax", "amount": 250},
        ]
      ),
    )
    assert _lines_as_tuples(lines) == [
      ("elem_office", 4000, 0),
      ("elem_tax", 250, 0),
      ("elem_card", 0, 4250),
    ]

  def test_fee_and_external_transfer_share_the_shape(self):
    for event_type in ("bank_fee", "external_transfer"):
      lines = plan_lines(
        event_type=event_type,
        resource_element_id="elem_checking",
        amount=-1500,
        metadata=BankFeedMetadata(classified_element_id="elem_contra"),
      )
      assert _lines_as_tuples(lines) == [
        ("elem_contra", 1500, 0),
        ("elem_checking", 0, 1500),
      ]

  def test_internal_transfer_uses_both_bank_legs(self):
    lines = plan_lines(
      event_type="internal_transfer",
      resource_element_id="elem_savings",
      amount=50000,
      metadata=BankFeedMetadata(
        from_element_id="elem_checking", to_element_id="elem_savings"
      ),
    )
    assert _lines_as_tuples(lines) == [
      ("elem_savings", 50000, 0),
      ("elem_checking", 0, 50000),
    ]

  def test_internal_transfer_missing_a_leg_is_refused(self):
    with pytest.raises(HandlerMetadataValidationError, match="both bank legs"):
      plan_lines(
        event_type="internal_transfer",
        resource_element_id="elem_savings",
        amount=50000,
        metadata=BankFeedMetadata(),
      )

  def test_unclassified_is_none_not_an_error(self):
    assert (
      plan_lines(
        event_type="bank_transaction",
        resource_element_id="elem_card",
        amount=-100,
        metadata=BankFeedMetadata(suggested_element_id="elem_sugg"),
      )
      is None
    )

  def test_no_bank_leg_is_refused(self):
    with pytest.raises(HandlerMetadataValidationError, match="no chart account linked"):
      plan_lines(
        event_type="bank_transaction",
        resource_element_id=None,
        amount=-100,
        metadata=BankFeedMetadata(classified_element_id="elem_x"),
      )

  def test_zero_amount_is_refused(self):
    with pytest.raises(HandlerMetadataValidationError, match="non-zero"):
      plan_lines(
        event_type="bank_transaction",
        resource_element_id="elem_card",
        amount=0,
        metadata=BankFeedMetadata(classified_element_id="elem_x"),
      )

  def test_classifying_to_the_bank_account_itself_is_refused(self):
    with pytest.raises(HandlerMetadataValidationError, match="moved through"):
      plan_lines(
        event_type="bank_transaction",
        resource_element_id="elem_card",
        amount=-100,
        metadata=BankFeedMetadata(classified_element_id="elem_card"),
      )


@pytest.mark.unit
class TestDispatch:
  def test_classified_event_posts_a_draft_in_the_local_lane(self):
    event = _event(metadata={"connection_id": "conn_1"})
    session = MagicMock()
    metadata = BankFeedMetadata(
      classified_element_id="elem_office",
      connection_id="conn_1",
      classified_by="claude",
    )
    with patch(f"{MODULE}.journal_dispatch") as journal:
      journal.return_value = MagicMock(entry_ids=["je_1"], transaction_ids=["txn_1"])
      result = dispatch(session, event, metadata, "usr_1")
    journal.assert_called_once()
    _session, _event_arg, journal_meta, created_by = journal.call_args.args
    assert created_by == "usr_1"
    assert journal_meta.status == "draft"
    assert journal_meta.publish_to_source is False
    assert journal_meta.connection_id == "conn_1"
    assert journal_meta.memo == "Staples (card)"
    assert journal_meta.posting_date.isoformat() == "2026-03-14"
    assert _lines_as_tuples(journal_meta.line_items) == [
      ("elem_office", 4250, 0),
      ("elem_card", 0, 4250),
    ]
    assert result.entry_ids == ["je_1"]

  def test_unclassified_event_with_no_rule_is_refused(self):
    event = _event()
    session = MagicMock()
    with (
      patch(
        f"{MODULE}.resolve_handler",
        side_effect=HandlerNotFoundError("bank_transaction", "purchase"),
      ),
      patch(f"{MODULE}.journal_dispatch") as journal,
    ):
      with pytest.raises(BankEventNotClassifiedError, match="Classify it first"):
        dispatch(
          session, event, BankFeedMetadata(suggested_element_id="elem_sugg"), "usr_1"
        )
    journal.assert_not_called()

  def test_refusal_is_a_handler_validation_error_for_the_router(self):
    assert issubclass(BankEventNotClassifiedError, HandlerMetadataValidationError)

  def test_ambiguous_rules_are_refused_too(self):
    event = _event()
    rules = [MagicMock(name="a", priority=5), MagicMock(name="b", priority=5)]
    ambiguous = HandlerAmbiguousError("bank_transaction", rules)
    with patch(f"{MODULE}.resolve_handler", side_effect=ambiguous):
      with pytest.raises(BankEventNotClassifiedError, match="more than one rule"):
        dispatch(MagicMock(), event, BankFeedMetadata(), "usr_1")

  def test_unclassified_event_posts_through_a_matching_rule(self):
    event = _event()
    session = MagicMock()
    session.get.return_value = MagicMock(agent_type="vendor")
    session.execute.return_value.all.return_value = [("je_rule",)]
    rule = MagicMock(name="stripe-rule")
    rule.name = "Stripe payouts"
    txn = MagicMock()
    txn.id = "txn_rule"
    with (
      patch(f"{MODULE}.resolve_handler", return_value=rule) as resolve,
      patch(f"{MODULE}.apply_handler", return_value=[txn]) as apply,
      patch(f"{MODULE}.journal_dispatch") as journal,
    ):
      result = dispatch(session, event, BankFeedMetadata(), "usr_1")
    journal.assert_not_called()
    resolve.assert_called_once()
    assert resolve.call_args.kwargs["agent_type"] == "vendor"
    assert resolve.call_args.kwargs["source"] == "mercury"
    apply.assert_called_once_with(session, event, rule, created_by="usr_1")
    assert result.transaction_ids == ["txn_rule"]
    assert result.entry_ids == ["je_rule"]

  def test_handlers_registered_for_every_bank_event_type(self):
    from robosystems.operations.event_block.python_handlers.registry import (
      get_python_handler,
    )

    for event_type in BANK_EVENT_TYPES:
      handler = get_python_handler(event_type)
      assert handler is BANK_FEED_HANDLERS[event_type]
      assert handler.metadata_schema is BankFeedMetadata
      assert handler.target_status == "classified"


@pytest.mark.unit
class TestPreview:
  def _body(self, **overrides) -> CreateEventBlockRequest:
    base = {
      "event_type": "bank_transaction",
      "event_category": "purchase",
      "event_class": "economic",
      "resource_type": "money",
      "resource_element_id": "elem_card",
      "occurred_at": datetime(2026, 3, 14, tzinfo=UTC),
      "source": "mercury",
      "external_id": "mercury_txn_1",
      "amount": -4250,
      "description": "Staples (card)",
      "metadata": {"suggested_element_id": "elem_office"},
    }
    base.update(overrides)
    return CreateEventBlockRequest.model_validate(base)

  def test_unclassified_preview_names_the_missing_choice(self):
    preview = dispatch_preview(
      MagicMock(), self._body(), BankFeedMetadata(suggested_element_id="elem_office")
    )
    assert preview.would_succeed is False
    assert "No account chosen" in preview.validation_errors[0]
    assert preview.computed_values["suggested_element_id"] == "elem_office"

  def test_classified_preview_delegates_and_annotates(self):
    body = self._body()
    with patch(f"{MODULE}.journal_dispatch_preview") as journal_preview:
      journal_preview.return_value = MagicMock(
        would_succeed=True, computed_values={"total_debit": 4250}
      )
      preview = dispatch_preview(
        MagicMock(), body, BankFeedMetadata(classified_element_id="elem_office")
      )
    journal_meta = journal_preview.call_args.args[2]
    assert _lines_as_tuples(journal_meta.line_items) == [
      ("elem_office", 4250, 0),
      ("elem_card", 0, 4250),
    ]
    assert preview.would_succeed is True
    assert preview.computed_values["direction"] == "out"
    assert preview.computed_values["contra_element_ids"] == ["elem_office"]
    assert preview.computed_values["total_debit"] == 4250

  def test_bad_split_preview_fails_without_raising(self):
    preview = dispatch_preview(
      MagicMock(),
      self._body(),
      BankFeedMetadata(classified_allocations=[{"element_id": "elem_a", "amount": 1}]),
    )
    assert preview.would_succeed is False
    assert "sum to 1" in preview.validation_errors[0]


@pytest.mark.unit
def test_classification_summary():
  assert classification_summary({}) == "unclassified"
  assert classification_summary({"classified_element_id": "e1"}) == "account e1"
  assert (
    classification_summary({"accept_suggestion": True, "suggested_element_id": "e2"})
    == "suggested account e2"
  )
  assert (
    classification_summary({"classified_allocations": [{}, {}]})
    == "split across 2 accounts"
  )
