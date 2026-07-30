"""Tests for create_event_block and preview_event_block Python-handler dispatch."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.event_block import CreateEventBlockRequest
from robosystems.models.extensions.roboledger.event_handler import EventHandler
from robosystems.operations.event_block.commands import (
  create_event_block,
  preview_event_block,
)
from robosystems.operations.event_block.python_handlers.types import (
  HandlerMetadataValidationError,
  HandlerPreview,
  HandlerResult,
)


def _make_body(**overrides) -> CreateEventBlockRequest:
  defaults = {
    "event_type": "asset_disposed",
    "event_category": "adjustment",
    "source": "manual",
    "occurred_at": datetime(2026, 3, 31, tzinfo=UTC),
    "metadata": {"schedule_id": "struct_schedule"},
    "apply_handlers": True,
  }
  defaults.update(overrides)
  return CreateEventBlockRequest(**defaults)


class TestCreateEventBlockPythonHandlerPath:
  def test_python_handler_wins_over_dsl(self) -> None:
    """When event_type is in the Python registry, DSL resolver is never called."""
    session = MagicMock()
    body = _make_body()

    # Track the added event so we can assign a fake id on flush
    added: list = []
    session.add.side_effect = lambda obj: added.append(obj)

    def fake_flush():
      for obj in added:
        if obj.id is None:
          obj.id = "evt_test"

    session.flush.side_effect = fake_flush

    python_handler = MagicMock()
    python_handler.target_status = "fulfilled"
    python_handler.metadata_schema = MagicMock()
    python_handler.metadata_schema.model_validate.return_value = MagicMock()
    python_handler.dispatch.return_value = HandlerResult(entry_ids=["je_1"])

    with (
      patch(
        "robosystems.operations.event_block.commands.get_python_handler",
        return_value=python_handler,
      ),
      patch(
        "robosystems.operations.event_block.commands.resolve_handler"
      ) as dsl_resolver,
    ):
      envelope = create_event_block(
        session, body, created_by="usr_test", graph_id="kg0123456789abcdef"
      )

    dsl_resolver.assert_not_called()
    python_handler.dispatch.assert_called_once()
    # Event was added with the handler's declared target_status
    session.add.assert_called_once()
    added_event = session.add.call_args.args[0]
    assert added_event.status == "fulfilled"
    # Commit happened once (atomic)
    session.commit.assert_called_once()
    # Envelope reflects the event
    assert envelope.event_type == "asset_disposed"
    assert envelope.status == "fulfilled"

  def test_metadata_validation_error_does_not_persist(self) -> None:
    """Bad metadata → HandlerMetadataValidationError, nothing committed."""
    from pydantic import ValidationError, ValidationInfo  # noqa: F401

    session = MagicMock()
    body = _make_body(metadata={})  # empty metadata → fails schema

    python_handler = MagicMock()

    def raise_validation(data):
      raise ValidationError.from_exception_data(
        title="AssetDisposedMetadata",
        line_errors=[
          {
            "type": "missing",
            "loc": ("schedule_id",),
            "input": {},
          }
        ],
      )

    python_handler.metadata_schema.model_validate.side_effect = raise_validation

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=python_handler,
    ):
      with pytest.raises(HandlerMetadataValidationError):
        create_event_block(
          session, body, created_by="usr_test", graph_id="kg0123456789abcdef"
        )

    # Nothing persisted
    session.add.assert_not_called()
    session.commit.assert_not_called()

  def test_dispatch_failure_rolls_back(self) -> None:
    """If handler.dispatch raises, session.commit is never called."""
    session = MagicMock()
    body = _make_body()

    python_handler = MagicMock()
    python_handler.target_status = "fulfilled"
    python_handler.metadata_schema.model_validate.return_value = MagicMock()
    python_handler.dispatch.side_effect = RuntimeError("boom")

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=python_handler,
    ):
      with pytest.raises(RuntimeError, match="boom"):
        create_event_block(
          session, body, created_by="usr_test", graph_id="kg0123456789abcdef"
        )

    session.commit.assert_not_called()


class TestPreviewEventBlockPythonHandlerPath:
  def test_preview_dispatches_to_python_handler(self) -> None:
    session = MagicMock()
    body = _make_body()

    python_handler = MagicMock()
    python_handler.metadata_schema.model_validate.return_value = MagicMock()
    python_handler.dispatch_preview.return_value = HandlerPreview(
      would_succeed=True,
      planned_entries=[
        {
          "posting_date": "2026-03-31",
          "memo": "Test disposal",
          "entry_type": "closing",
          "line_items": [
            {"element_id": "elem_accum_dep", "debit_amount": 10000, "credit_amount": 0},
            {"element_id": "elem_asset", "debit_amount": 0, "credit_amount": 10000},
          ],
        }
      ],
      computed_values={"nbv_cents": 10000, "gain_loss_cents": 0},
      validation_errors=[],
    )

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=python_handler,
    ):
      resp = preview_event_block(session, body, created_by="usr_test")

    assert resp.would_succeed is True
    assert resp.handler_metadata["nbv_cents"] == 10000
    assert len(resp.planned_transactions) == 1
    assert resp.planned_transactions[0].debit_element_id == "elem_accum_dep"
    assert resp.planned_transactions[0].credit_element_id == "elem_asset"

  def test_preview_handles_external_id_only_lines(self) -> None:
    """QB-captured lines carry element_external_id (element_id=None);
    preview must surface the external id, not blow up Pydantic."""
    session = MagicMock()
    body = _make_body()

    python_handler = MagicMock()
    python_handler.metadata_schema.model_validate.return_value = MagicMock()
    python_handler.dispatch_preview.return_value = HandlerPreview(
      would_succeed=True,
      planned_entries=[
        {
          "posting_date": "2026-02-25",
          "memo": "Invoice_9",
          "entry_type": "standard",
          "line_items": [
            {
              "element_id": None,
              "element_external_id": "84",
              "debit_amount": 10800,
              "credit_amount": 0,
            },
            {
              "element_id": None,
              "element_external_id": "45",
              "debit_amount": 0,
              "credit_amount": 10000,
            },
          ],
        }
      ],
      computed_values={},
      validation_errors=[],
    )

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=python_handler,
    ):
      resp = preview_event_block(session, body, created_by="usr_test")

    assert resp.would_succeed is True
    assert len(resp.planned_transactions) == 1
    assert resp.planned_transactions[0].debit_element_id == "84"
    assert resp.planned_transactions[0].credit_element_id == "45"

  def test_preview_surfaces_validation_errors(self) -> None:
    session = MagicMock()
    body = _make_body()

    python_handler = MagicMock()
    python_handler.metadata_schema.model_validate.return_value = MagicMock()
    python_handler.dispatch_preview.return_value = HandlerPreview(
      would_succeed=False,
      planned_entries=[],
      computed_values={},
      validation_errors=["Schedule not found: struct_missing"],
    )

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=python_handler,
    ):
      resp = preview_event_block(session, body, created_by="usr_test")

    assert resp.would_succeed is False
    assert "struct_missing" in resp.validation_errors[0]

  def test_preview_metadata_validation_error(self) -> None:
    """Bad metadata in preview returns validation errors without raising."""
    from pydantic import ValidationError

    session = MagicMock()
    body = _make_body(metadata={})

    python_handler = MagicMock()
    python_handler.metadata_schema.model_validate.side_effect = (
      ValidationError.from_exception_data(
        title="AssetDisposedMetadata",
        line_errors=[{"type": "missing", "loc": ("schedule_id",), "input": {}}],
      )
    )

    with patch(
      "robosystems.operations.event_block.commands.get_python_handler",
      return_value=python_handler,
    ):
      resp = preview_event_block(session, body, created_by="usr_test")

    assert resp.would_succeed is False
    assert resp.validation_errors  # non-empty


class TestDslHandlerResolution:
  @staticmethod
  def _session_with_event_id() -> MagicMock:
    session = MagicMock()
    added: list = []
    session.add.side_effect = lambda obj: added.append(obj)

    def fake_flush():
      for obj in added:
        if getattr(obj, "id", None) is None and obj.__class__.__name__ == "Event":
          obj.id = "evt_test"

    session.flush.side_effect = fake_flush
    return session

  @staticmethod
  def _handler() -> EventHandler:
    return EventHandler(
      id="hdl_test",
      name="Customer-Specific Handler",
      event_type="invoice_issued",
      transaction_template={"transactions": []},
      priority=10,
      is_active=True,
      origin="tenant",
      created_by="usr_test",
    )

  def test_create_event_block_passes_agent_type_to_dsl_resolver(self) -> None:
    session = self._session_with_event_id()
    session.get.return_value = MagicMock(agent_type="customer")
    body = _make_body(
      event_type="invoice_issued",
      event_category="sales",
      metadata={},
      agent_id="agt_customer",
    )

    with (
      patch(
        "robosystems.operations.event_block.commands.get_python_handler",
        return_value=None,
      ),
      patch(
        "robosystems.operations.event_block.commands.resolve_handler",
        return_value=self._handler(),
      ) as resolver,
      patch("robosystems.operations.event_block.commands.apply_handler"),
    ):
      envelope = create_event_block(
        session, body, created_by="usr_test", graph_id="kg0123456789abcdef"
      )

    assert resolver.call_args.kwargs["agent_type"] == "customer"
    assert envelope.id == "evt_test"
    assert envelope.status == "classified"

  def test_preview_event_block_passes_agent_type_to_dsl_resolver(self) -> None:
    session = MagicMock()
    session.get.return_value = MagicMock(agent_type="vendor")
    body = _make_body(
      event_type="invoice_issued",
      event_category="purchase",
      metadata={},
      agent_id="agt_vendor",
    )

    with (
      patch(
        "robosystems.operations.event_block.commands.get_python_handler",
        return_value=None,
      ),
      patch(
        "robosystems.operations.event_block.commands.resolve_handler",
        return_value=self._handler(),
      ) as resolver,
    ):
      resp = preview_event_block(session, body, created_by="usr_test")

    assert resolver.call_args.kwargs["agent_type"] == "vendor"
    assert resp.would_succeed is True
    assert resp.matched_handler is not None


class TestCreateDualityFields:
  """Stream 1 of event-driven-ledger: create-side flow-through of REA fields."""

  @staticmethod
  def _session_with_event_id() -> MagicMock:
    session = MagicMock()
    added: list = []
    session.add.side_effect = lambda obj: added.append(obj)

    def fake_flush():
      for obj in added:
        if obj.id is None:
          obj.id = "evt_test"

    session.flush.side_effect = fake_flush
    return session

  def test_event_class_defaults_to_economic(self) -> None:
    """Capture-only path defaults event_class to 'economic'."""
    session = self._session_with_event_id()
    body = _make_body(apply_handlers=False, metadata={})

    envelope = create_event_block(
      session, body, created_by="usr_test", graph_id="kg0123456789abcdef"
    )

    added_event = session.add.call_args.args[0]
    assert added_event.event_class == "economic"
    assert envelope.event_class == "economic"

  def test_obligated_by_event_id_round_trips_through_envelope(self) -> None:
    """Forward-materialization link set on create surfaces in the envelope."""
    session = self._session_with_event_id()
    body = _make_body(
      apply_handlers=False,
      metadata={},
      obligated_by_event_id="evt_asset_acquired",
    )

    envelope = create_event_block(
      session, body, created_by="usr_test", graph_id="kg0123456789abcdef"
    )

    added_event = session.add.call_args.args[0]
    assert added_event.obligated_by_event_id == "evt_asset_acquired"
    assert envelope.obligated_by_event_id == "evt_asset_acquired"

  def test_discharges_event_id_round_trips_through_envelope(self) -> None:
    """Settlement link set on create surfaces in the envelope."""
    session = self._session_with_event_id()
    body = _make_body(
      apply_handlers=False,
      metadata={},
      discharges_event_id="evt_invoice_issued",
    )

    envelope = create_event_block(
      session, body, created_by="usr_test", graph_id="kg0123456789abcdef"
    )

    added_event = session.add.call_args.args[0]
    assert added_event.discharges_event_id == "evt_invoice_issued"
    assert envelope.discharges_event_id == "evt_invoice_issued"

  def test_support_event_class_with_support_category(self) -> None:
    """Support events (event_class='support', category='inquiry') flow through."""
    session = self._session_with_event_id()
    body = _make_body(
      event_type="inquiry_logged",
      event_category="inquiry",
      event_class="support",
      apply_handlers=False,
      metadata={},
    )

    envelope = create_event_block(
      session, body, created_by="usr_test", graph_id="kg0123456789abcdef"
    )

    added_event = session.add.call_args.args[0]
    assert added_event.event_class == "support"
    assert added_event.event_category == "inquiry"
    assert envelope.event_class == "support"
    assert envelope.event_category == "inquiry"
