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

from pydantic import BaseModel, ConfigDict

from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  DeleteScheduleRequest,
  UpdateScheduleRequest,
)
from robosystems.models.api.information_block import (
  ScheduleMechanics,
  StatementMechanics,
)
from robosystems.operations.information_block import schedule as schedule_handlers
from robosystems.operations.information_block.statement import (
  STATEMENT_CATEGORY,
  STATEMENT_DISPLAY,
  make_statement_handlers,
)
from robosystems.operations.information_block.types import BlockTypeRegistryEntry


class _EmptyPayload(BaseModel):
  """Placeholder request model for block types whose dispatch raises.

  Statement block types raise ``NotImplementedError`` in their
  ``dispatch_{create,update,delete}`` handlers — the OpenAPI schema
  stays honest with an empty-but-forbidden-extra-fields shape so
  callers learn "no payload expected, the call will 501 anyway".
  """

  model_config = ConfigDict(extra="forbid")


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
  update_request_model=UpdateScheduleRequest,
  delete_request_model=DeleteScheduleRequest,
  construction_mode="declarative",
  dispatch_create=schedule_handlers.create,
  dispatch_update=schedule_handlers.update,
  dispatch_delete=schedule_handlers.delete,
  dispatch_build_envelope=schedule_handlers.build_envelope,
  # Schedules are tenant-only — they exist against live ledger data.
  # Don't surface them on the library sentinel.
  surfaces_in_library=False,
)


# ── Statements (compositional) ─────────────────────────────────────────────


def _make_statement_entry(block_type: str, icon: str) -> BlockTypeRegistryEntry:
  """Build a registry entry for a statement-family block type.

  All four statement block types share the same construction mode,
  category, Information Model defaults, and dispatch handlers — they
  differ only in their ``block_type`` discriminator and display
  strings. This helper factors the common shape.
  """
  display_name, display_plural = STATEMENT_DISPLAY[block_type]
  handlers = make_statement_handlers(block_type)
  return BlockTypeRegistryEntry(
    id=block_type,
    display_name=display_name,
    display_plural=display_plural,
    category=STATEMENT_CATEGORY,
    icon=icon,
    description=(
      f"{display_name} — library-seeded reporting structure. Facts are "
      f"surfaced from the tenant's most recent Report; created via "
      f"create-report, not create-information-block."
    ),
    concept_arrangement_default="roll_up",
    member_arrangement_default="aggregation",
    mechanics_schema=StatementMechanics,
    create_request_model=_EmptyPayload,
    update_request_model=_EmptyPayload,
    delete_request_model=_EmptyPayload,
    construction_mode="compositional",
    dispatch_create=handlers["create"],
    dispatch_update=handlers["update"],
    dispatch_delete=handlers["delete"],
    dispatch_build_envelope=handlers["build_envelope"],
    # Statement Structures live in public.structures (library-immutable)
    # and should surface on the library sentinel, with facts=[] because
    # reports live in tenant schemas.
    surfaces_in_library=True,
  )


BALANCE_SHEET_BLOCK = _make_statement_entry("balance_sheet", "scale-3d")
INCOME_STATEMENT_BLOCK = _make_statement_entry("income_statement", "trending-up")
CASH_FLOW_STATEMENT_BLOCK = _make_statement_entry("cash_flow_statement", "waves")
EQUITY_STATEMENT_BLOCK = _make_statement_entry("equity_statement", "pie-chart")


# ── Registry ────────────────────────────────────────────────────────────────

REGISTRY: dict[str, BlockTypeRegistryEntry] = {
  SCHEDULE_BLOCK.id: SCHEDULE_BLOCK,
  BALANCE_SHEET_BLOCK.id: BALANCE_SHEET_BLOCK,
  INCOME_STATEMENT_BLOCK.id: INCOME_STATEMENT_BLOCK,
  CASH_FLOW_STATEMENT_BLOCK.id: CASH_FLOW_STATEMENT_BLOCK,
  EQUITY_STATEMENT_BLOCK.id: EQUITY_STATEMENT_BLOCK,
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
  "BALANCE_SHEET_BLOCK",
  "CASH_FLOW_STATEMENT_BLOCK",
  "EQUITY_STATEMENT_BLOCK",
  "INCOME_STATEMENT_BLOCK",
  "REGISTRY",
  "SCHEDULE_BLOCK",
  "get",
  "list_registered",
]
