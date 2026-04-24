"""Tests for create_event_block and preview_event_block Python-handler dispatch."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from robosystems.models.api.event_block import CreateEventBlockRequest
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
    "source": "native",
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
      envelope = create_event_block(session, body, created_by="usr_test")

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
        create_event_block(session, body, created_by="usr_test")

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
        create_event_block(session, body, created_by="usr_test")

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
