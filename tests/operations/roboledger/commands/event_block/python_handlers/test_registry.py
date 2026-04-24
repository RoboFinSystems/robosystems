"""Tests for the Event Block Python handler registry."""

from __future__ import annotations

import pytest

from robosystems.operations.roboledger.commands.event_block.python_handlers import (
  EVENT_BLOCK_PYTHON_REGISTRY,
  get_python_handler,
)
from robosystems.operations.roboledger.commands.event_block.python_handlers.asset_disposed import (
  ASSET_DISPOSED_HANDLER,
  AssetDisposedMetadata,
)
from robosystems.operations.roboledger.commands.event_block.python_handlers.journal_entry_recorded import (
  JOURNAL_ENTRY_RECORDED_HANDLER,
)
from robosystems.operations.roboledger.commands.event_block.python_handlers.manual_adjustment import (
  MANUAL_ADJUSTMENT_HANDLER,
)
from robosystems.operations.roboledger.commands.event_block.python_handlers.schedule_entry_due import (
  SCHEDULE_ENTRY_DUE_HANDLER,
)
from robosystems.operations.roboledger.commands.event_block.python_handlers.transaction_recorded import (
  TRANSACTION_RECORDED_HANDLER,
)


def test_asset_disposed_registered() -> None:
  handler = get_python_handler("asset_disposed")
  assert handler is ASSET_DISPOSED_HANDLER


def test_schedule_entry_due_registered() -> None:
  assert get_python_handler("schedule_entry_due") is SCHEDULE_ENTRY_DUE_HANDLER


def test_manual_adjustment_registered() -> None:
  assert get_python_handler("manual_adjustment") is MANUAL_ADJUSTMENT_HANDLER


def test_journal_entry_recorded_registered() -> None:
  assert get_python_handler("journal_entry_recorded") is JOURNAL_ENTRY_RECORDED_HANDLER


def test_transaction_recorded_registered() -> None:
  assert get_python_handler("transaction_recorded") is TRANSACTION_RECORDED_HANDLER


def test_unknown_event_type_returns_none() -> None:
  assert get_python_handler("invoice_issued") is None
  assert get_python_handler("random_type") is None


def test_registry_value_is_frozen() -> None:
  """EventBlockPythonHandler is a frozen dataclass — callers can't mutate it."""
  with pytest.raises(Exception):  # FrozenInstanceError on dataclass
    ASSET_DISPOSED_HANDLER.target_status = "mutated"  # type: ignore[misc]


def test_asset_disposed_declares_fulfilled_target_status() -> None:
  assert ASSET_DISPOSED_HANDLER.target_status == "fulfilled"


def test_transaction_recorded_declares_fulfilled_target_status() -> None:
  assert TRANSACTION_RECORDED_HANDLER.target_status == "fulfilled"


def test_classified_target_status_handlers() -> None:
  """These handlers draft entries that close-period posts — target stays classified."""
  assert SCHEDULE_ENTRY_DUE_HANDLER.target_status == "classified"
  assert MANUAL_ADJUSTMENT_HANDLER.target_status == "classified"
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


def test_registry_has_phase_4a_handlers() -> None:
  """Phase 4a collapses the 4 legacy ledger-write ops into event handlers;
  together with the Phase 4b asset_disposed handler, 5 handlers register."""
  assert set(EVENT_BLOCK_PYTHON_REGISTRY.keys()) == {
    "asset_disposed",
    "schedule_entry_due",
    "manual_adjustment",
    "journal_entry_recorded",
    "transaction_recorded",
  }
