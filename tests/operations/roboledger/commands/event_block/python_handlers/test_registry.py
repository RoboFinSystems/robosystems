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


def test_asset_disposed_registered() -> None:
  handler = get_python_handler("asset_disposed")
  assert handler is ASSET_DISPOSED_HANDLER


def test_unknown_event_type_returns_none() -> None:
  assert get_python_handler("invoice_issued") is None
  assert get_python_handler("random_type") is None


def test_registry_value_is_frozen() -> None:
  """EventBlockPythonHandler is a frozen dataclass — callers can't mutate it."""
  with pytest.raises(Exception):  # FrozenInstanceError on dataclass
    ASSET_DISPOSED_HANDLER.target_status = "mutated"  # type: ignore[misc]


def test_asset_disposed_declares_fulfilled_target_status() -> None:
  assert ASSET_DISPOSED_HANDLER.target_status == "fulfilled"


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


def test_registry_has_exactly_one_handler_in_phase_4b() -> None:
  """Phase 4b ships asset_disposed only. Other handlers land in later phases."""
  assert set(EVENT_BLOCK_PYTHON_REGISTRY.keys()) == {"asset_disposed"}
