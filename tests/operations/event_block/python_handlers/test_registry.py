"""Tests for the Event Block Python handler registry."""

from __future__ import annotations

import pytest

from robosystems.operations.event_block.python_handlers import (
  EVENT_BLOCK_PYTHON_REGISTRY,
  get_python_handler,
)
from robosystems.operations.event_block.python_handlers.asset_disposed import (
  ASSET_DISPOSED_HANDLER,
  AssetDisposedMetadata,
)
from robosystems.operations.event_block.python_handlers.journal_entry_recorded import (
  JOURNAL_ENTRY_RECORDED_HANDLER,
)
from robosystems.operations.event_block.python_handlers.journal_entry_reversed import (
  JOURNAL_ENTRY_REVERSED_HANDLER,
)
from robosystems.operations.event_block.python_handlers.schedule_entry_due import (
  SCHEDULE_ENTRY_DUE_HANDLER,
)


def test_asset_disposed_registered() -> None:
  handler = get_python_handler("asset_disposed")
  assert handler is ASSET_DISPOSED_HANDLER


def test_schedule_entry_due_registered() -> None:
  assert get_python_handler("schedule_entry_due") is SCHEDULE_ENTRY_DUE_HANDLER


def test_journal_entry_recorded_registered() -> None:
  assert get_python_handler("journal_entry_recorded") is JOURNAL_ENTRY_RECORDED_HANDLER


def test_journal_entry_reversed_registered() -> None:
  assert get_python_handler("journal_entry_reversed") is JOURNAL_ENTRY_REVERSED_HANDLER


def test_unknown_event_type_returns_none() -> None:
  assert get_python_handler("random_type") is None


def test_qb_source_class_event_types_share_journal_handler() -> None:
  """Phase 2: QB source-class events all dispatch through JOURNAL_ENTRY_RECORDED_HANDLER.
  They differ in inbox display only; on approve they post journal entries via
  the same handler."""
  assert get_python_handler("invoice_issued") is JOURNAL_ENTRY_RECORDED_HANDLER
  assert get_python_handler("bill_received") is JOURNAL_ENTRY_RECORDED_HANDLER
  assert get_python_handler("payment_received") is JOURNAL_ENTRY_RECORDED_HANDLER
  assert get_python_handler("bill_paid") is JOURNAL_ENTRY_RECORDED_HANDLER
  assert get_python_handler("sales_receipt_recorded") is JOURNAL_ENTRY_RECORDED_HANDLER


def test_registry_value_is_frozen() -> None:
  """EventBlockPythonHandler is a frozen dataclass — callers can't mutate it."""
  with pytest.raises(Exception):  # FrozenInstanceError on dataclass
    ASSET_DISPOSED_HANDLER.target_status = "mutated"  # type: ignore[misc]


def test_fulfilled_target_status_handlers() -> None:
  """Terminal events: status transitions straight to fulfilled."""
  assert ASSET_DISPOSED_HANDLER.target_status == "fulfilled"
  assert JOURNAL_ENTRY_REVERSED_HANDLER.target_status == "fulfilled"


def test_classified_target_status_handlers() -> None:
  """These handlers draft entries that close-period posts — target stays classified."""
  assert SCHEDULE_ENTRY_DUE_HANDLER.target_status == "classified"
  assert JOURNAL_ENTRY_RECORDED_HANDLER.target_status == "classified"


def test_asset_disposed_metadata_schema() -> None:
  """Metadata validates schedule_id; proceeds defaults to 0."""
  m = AssetDisposedMetadata(schedule_id="struct_1")
  assert m.schedule_id == "struct_1"
  assert m.proceeds == 0
  assert m.proceeds_element_id is None
  assert m.gain_loss_element_id is None


def test_asset_disposed_metadata_rejects_negative_proceeds() -> None:
  from pydantic import ValidationError

  with pytest.raises(ValidationError):
    AssetDisposedMetadata(schedule_id="struct_1", proceeds=-100)


def test_registry_has_expected_handlers() -> None:
  """Event types dispatched by the Python registry. Phase 2 added three
  QB source-class types (invoice_issued / bill_received / payment_received)
  that share the journal_entry_recorded handler — they post the same way
  on approve, the keys exist so the inbox can filter by source class."""
  assert set(EVENT_BLOCK_PYTHON_REGISTRY.keys()) == {
    "asset_disposed",
    "schedule_created",
    "schedule_entry_due",
    "journal_entry_recorded",
    "journal_entry_reversed",
    "invoice_issued",
    "bill_received",
    "payment_received",
    "bill_paid",
    "sales_receipt_recorded",
  }
