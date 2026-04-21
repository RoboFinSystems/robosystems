"""Registry tests — SCHEDULE_BLOCK wired, get/list_registered work."""

from __future__ import annotations

import pytest

from robosystems.models.api.extensions.schedules import CreateScheduleRequest
from robosystems.models.api.information_block import ScheduleMechanics
from robosystems.operations.information_block.registry import (
  REGISTRY,
  SCHEDULE_BLOCK,
  get,
  list_registered,
)


class TestRegistry:
  def test_schedule_block_registered(self) -> None:
    assert "schedule" in REGISTRY
    assert REGISTRY["schedule"] is SCHEDULE_BLOCK

  def test_schedule_block_shape(self) -> None:
    assert SCHEDULE_BLOCK.id == "schedule"
    assert SCHEDULE_BLOCK.display_name == "Schedule"
    assert SCHEDULE_BLOCK.display_plural == "Schedules"
    assert SCHEDULE_BLOCK.category == "Close"
    assert SCHEDULE_BLOCK.construction_mode == "declarative"
    assert SCHEDULE_BLOCK.concept_arrangement_default == "roll_forward"
    assert SCHEDULE_BLOCK.member_arrangement_default is None
    # Mechanics + create request models wired to the right Pydantic classes
    assert SCHEDULE_BLOCK.mechanics_schema is ScheduleMechanics
    assert SCHEDULE_BLOCK.create_request_model is CreateScheduleRequest
    # Schedules are tenant-only; not surfaced on the library sentinel
    assert SCHEDULE_BLOCK.surfaces_in_library is False

  def test_get_returns_entry(self) -> None:
    entry = get("schedule")
    assert entry is SCHEDULE_BLOCK

  def test_get_unknown_raises_keyerror(self) -> None:
    with pytest.raises(KeyError) as exc:
      get("balance_sheet")
    assert "balance_sheet" in str(exc.value)

  def test_list_registered_returns_all_entries(self) -> None:
    entries = list_registered()
    # Phase a: only Schedule is registered
    assert SCHEDULE_BLOCK in entries
    assert [e.id for e in entries] == ["schedule"]

  def test_entry_is_frozen(self) -> None:
    """Registry entries are immutable — a typo in handler wiring can't be
    patched out by a caller at runtime."""
    with pytest.raises(Exception):  # FrozenInstanceError (subclass of AttributeError)
      SCHEDULE_BLOCK.category = "Mutated"  # type: ignore[misc]
