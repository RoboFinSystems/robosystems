"""Registry tests — Schedule + statement family wired, get/list work."""

from __future__ import annotations

import pytest

from robosystems.models.api.extensions.schedules import CreateScheduleRequest
from robosystems.models.api.information_block import (
  ScheduleMechanics,
  StatementMechanics,
)
from robosystems.operations.information_block.registry import (
  BALANCE_SHEET_BLOCK,
  CASH_FLOW_STATEMENT_BLOCK,
  EQUITY_STATEMENT_BLOCK,
  INCOME_STATEMENT_BLOCK,
  REGISTRY,
  SCHEDULE_BLOCK,
  get,
  list_registered,
)

STATEMENT_BLOCK_IDS = [
  "balance_sheet",
  "income_statement",
  "cash_flow_statement",
  "equity_statement",
]


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
      get("nonexistent_block_type")
    assert "nonexistent_block_type" in str(exc.value)

  def test_list_registered_returns_all_entries(self) -> None:
    entries = list_registered()
    # Schedule + 4 statement block types + Phase η metric.
    assert [e.id for e in entries] == [
      "schedule",
      *STATEMENT_BLOCK_IDS,
      "metric",
    ]

  def test_entry_is_frozen(self) -> None:
    """Registry entries are immutable — a typo in handler wiring can't be
    patched out by a caller at runtime."""
    with pytest.raises(Exception):  # FrozenInstanceError (subclass of AttributeError)
      SCHEDULE_BLOCK.category = "Mutated"  # type: ignore[misc]


class TestStatementRegistration:
  @pytest.mark.parametrize(
    "block_type,entry",
    [
      ("balance_sheet", BALANCE_SHEET_BLOCK),
      ("income_statement", INCOME_STATEMENT_BLOCK),
      ("cash_flow_statement", CASH_FLOW_STATEMENT_BLOCK),
      ("equity_statement", EQUITY_STATEMENT_BLOCK),
    ],
  )
  def test_statement_block_registered(self, block_type: str, entry) -> None:
    assert block_type in REGISTRY
    assert REGISTRY[block_type] is entry
    assert get(block_type) is entry

  @pytest.mark.parametrize("block_type", STATEMENT_BLOCK_IDS)
  def test_statement_block_shape(self, block_type: str) -> None:
    entry = get(block_type)
    assert entry.id == block_type
    assert entry.category == "Reporting"
    assert entry.construction_mode == "compositional"
    assert entry.concept_arrangement_default == "roll_up"
    assert entry.member_arrangement_default == "aggregation"
    assert entry.mechanics_schema is StatementMechanics
    # All four statement block types surface on the library sentinel
    # because their Structures live in public.structures.
    assert entry.surfaces_in_library is True
