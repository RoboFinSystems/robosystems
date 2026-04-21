"""Information Block type registry.

Single source of truth for every block type the system knows about.
Populated at module import; frozen thereafter. Callers look up entries
by ``id`` string (the block_type discriminator).

Adding a block type: declare a new :class:`BlockTypeRegistryEntry`
literal next to the Schedule one below and add it to the ``REGISTRY``
dict. The entry's handlers live alongside the registration (own module
per block type — ``schedule.py``, ``statement.py``, ...).
"""

from __future__ import annotations

from robosystems.models.api.extensions.schedules import CreateScheduleRequest
from robosystems.models.api.information_block import ScheduleMechanics
from robosystems.operations.information_block import schedule as schedule_handlers
from robosystems.operations.information_block.types import BlockTypeRegistryEntry

# ── Schedule ────────────────────────────────────────────────────────────────

SCHEDULE_BLOCK = BlockTypeRegistryEntry(
  id=schedule_handlers.SCHEDULE_BLOCK_TYPE,
  display_name=schedule_handlers.SCHEDULE_DISPLAY_NAME,
  display_plural="Schedules",
  category=schedule_handlers.SCHEDULE_CATEGORY,
  icon="calendar-clock",
  description=(
    "Pre-generated fact grids for recurring closing entries — "
    "depreciation, amortization, prepaid drawdowns, accruals. Each "
    "in-scope period produces a draft closing entry via the schedule's "
    "entry template."
  ),
  concept_arrangement_default="roll_forward",
  member_arrangement_default=None,
  mechanics_schema=ScheduleMechanics,
  create_request_model=CreateScheduleRequest,
  construction_mode="declarative",
  dispatch_create=schedule_handlers.create,
  dispatch_build_envelope=schedule_handlers.build_envelope,
  # Schedules are tenant-only — they exist against live ledger data.
  # Don't surface them on the library sentinel.
  surfaces_in_library=False,
)

# ── Registry ────────────────────────────────────────────────────────────────

REGISTRY: dict[str, BlockTypeRegistryEntry] = {
  SCHEDULE_BLOCK.id: SCHEDULE_BLOCK,
}


def get(block_type: str) -> BlockTypeRegistryEntry:
  """Return the registry entry for ``block_type`` or raise ``KeyError``.

  Callers that want a 422 on unknown block_type should catch ``KeyError``
  and translate (e.g. into :class:`ValueError`, which the REST/MCP error
  maps already route to 422 / invalid_arguments).
  """
  try:
    return REGISTRY[block_type]
  except KeyError as exc:
    raise KeyError(f"Unknown block_type: {block_type}") from exc


def list_registered() -> list[BlockTypeRegistryEntry]:
  """Return every registered entry in registration order."""
  return list(REGISTRY.values())


__all__ = [
  "REGISTRY",
  "SCHEDULE_BLOCK",
  "get",
  "list_registered",
]
