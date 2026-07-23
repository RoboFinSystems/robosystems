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

from collections.abc import Callable
from typing import Any

from pydantic import BaseModel, ConfigDict

from robosystems.models.api.extensions.forecasts import (
  CreateForecastRequest,
  DeleteForecastRequest,
  UpdateForecastRequest,
)
from robosystems.models.api.extensions.rollforward import (
  CreateRollforwardRequest,
  DeleteRollforwardRequest,
  UpdateRollforwardRequest,
)
from robosystems.models.api.extensions.schedules import (
  CreateScheduleRequest,
  DeleteScheduleRequest,
  UpdateScheduleRequest,
)
from robosystems.models.api.information_block import (
  ForecastMechanics,
  MetricMechanics,
  RollforwardMechanics,
  ScheduleMechanics,
  StatementMechanics,
)
from robosystems.operations.information_block import disclosure as disclosure_handlers
from robosystems.operations.information_block import forecast as forecast_handlers
from robosystems.operations.information_block import metric as metric_handlers
from robosystems.operations.information_block import rollforward as rollforward_handlers
from robosystems.operations.information_block import schedule as schedule_handlers
from robosystems.operations.information_block.statement import (
  STATEMENT_CATEGORY,
  STATEMENT_DISPLAY,
  make_statement_handlers,
)
from robosystems.operations.information_block.types import BlockTypeRegistryEntry

METRIC_BLOCK_TYPE = metric_handlers.METRIC_BLOCK_TYPE
METRIC_DISPLAY_NAME = metric_handlers.METRIC_DISPLAY_NAME
METRIC_CATEGORY = metric_handlers.METRIC_CATEGORY


def make_not_implemented_handler(operation: str, message: str) -> Callable[..., Any]:
  """Build a dispatch handler that raises :class:`NotImplementedError`.

  Used by block types whose ``dispatch_{create,update,delete}`` handler
  is intentionally a stub — either because the construction path lives
  elsewhere (statement family flows through ``create-report``) or because
  the evaluator has not been implemented yet (metric blocks).

  The returned callable accepts and ignores the standard handler
  arguments ``(session, payload, actor)`` so it can be plugged into
  :class:`BlockTypeRegistryEntry` without signature gymnastics.
  """

  def _handler(*_args: Any, **_kwargs: Any) -> str:
    raise NotImplementedError(message)

  _handler.__name__ = f"_not_implemented_{operation}"
  _handler.__qualname__ = _handler.__name__
  return _handler


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
  build_envelope = make_statement_handlers(block_type)
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
    member_arrangement_default="whole_part",
    mechanics_schema=StatementMechanics,
    create_request_model=_EmptyPayload,
    update_request_model=_EmptyPayload,
    delete_request_model=_EmptyPayload,
    construction_mode="compositional",
    dispatch_create=make_not_implemented_handler(
      f"create-{block_type}-block",
      f"Direct construction of {display_name} blocks is not yet "
      "available — they are produced today by `create-report`, which "
      "generates the facts and the envelope becomes queryable via "
      "`informationBlocks(blockType=...)` after the report publishes. "
      "Standalone construction is a planned follow-up: the same "
      "primitive that builds a Schedule block today will extend to "
      "the statement family. Until then, call `create-report` against "
      "the appropriate taxonomy.",
    ),
    dispatch_update=make_not_implemented_handler(
      f"update-{block_type}-block",
      f"Direct in-place updates of {display_name} blocks are not yet "
      "available. The envelope re-surfaces the most recent Report's "
      "facts automatically — call `create-report` (or "
      "`regenerate-report`) to produce new facts. Standalone update "
      "lands when standalone construction does.",
    ),
    dispatch_delete=make_not_implemented_handler(
      f"delete-{block_type}-block",
      f"{display_name} blocks are library-seeded and cannot be deleted "
      "per tenant. To remove the underlying facts, archive the "
      "originating Report via the report APIs (`delete-report`).",
    ),
    dispatch_build_envelope=build_envelope,
    # Statement Structures live in public.structures (library-immutable)
    # and should surface on the library sentinel, with facts=[] because
    # reports live in tenant schemas.
    surfaces_in_library=True,
  )


# ── Rollforward (declarative) ──────────────────────────────────────────────

ROLLFORWARD_BLOCK = BlockTypeRegistryEntry(
  id=rollforward_handlers.ROLLFORWARD_BLOCK_TYPE,
  display_name=rollforward_handlers.ROLLFORWARD_DISPLAY_NAME,
  display_plural="Rollforwards",
  category=rollforward_handlers.ROLLFORWARD_CATEGORY,
  icon="arrow-right-left",
  description=(
    "Filter-based attribution block — decomposes a balance-sheet account's "
    "period change across declared flow concepts (Cash Flow Statement and "
    "Statement of Changes in Equity lines). The author declares the BS "
    "source + a list of attribution filters; the renderer evaluates the "
    "filters against ledger LineItems at read time and emits one fact per "
    "filter per period."
  ),
  concept_arrangement_default="roll_forward",
  member_arrangement_default=None,
  mechanics_schema=RollforwardMechanics,
  create_request_model=CreateRollforwardRequest,
  update_request_model=UpdateRollforwardRequest,
  delete_request_model=DeleteRollforwardRequest,
  construction_mode="declarative",
  dispatch_create=rollforward_handlers.create,
  dispatch_update=rollforward_handlers.update,
  dispatch_delete=rollforward_handlers.delete,
  dispatch_build_envelope=rollforward_handlers.build_envelope,
  # Rollforwards are tenant-authored — they exist against live ledger
  # data and reference tenant-resolved element_ids. Like schedules, they
  # don't surface on the library sentinel.
  surfaces_in_library=False,
)


# ── Forecast (declarative) ─────────────────────────────────────────────────

FORECAST_BLOCK = BlockTypeRegistryEntry(
  id=forecast_handlers.FORECAST_BLOCK_TYPE,
  display_name=forecast_handlers.FORECAST_DISPLAY_NAME,
  display_plural="Forecasts",
  category=forecast_handlers.FORECAST_CATEGORY,
  icon="trending-up-down",
  description=(
    "Authored scenario container (FP&A) — scenario identity, horizon, "
    "and lever assertions on the rs-driver catalog. The block IS the "
    "scenario: compute-forecast walks the driver cascade forward from "
    "the last closed actuals and lands the derived months in the "
    "existing statement/metric block types keyed by this block's "
    "scenario_id (NULL = actuals)."
  ),
  concept_arrangement_default="set",
  member_arrangement_default=None,
  mechanics_schema=ForecastMechanics,
  create_request_model=CreateForecastRequest,
  update_request_model=UpdateForecastRequest,
  delete_request_model=DeleteForecastRequest,
  construction_mode="declarative",
  dispatch_create=forecast_handlers.create,
  dispatch_update=forecast_handlers.update,
  dispatch_delete=forecast_handlers.delete,
  dispatch_build_envelope=forecast_handlers.build_envelope,
  # Forecasts are tenant-authored scenarios against live ledger data —
  # never on the library sentinel.
  surfaces_in_library=False,
)


# ── Statements (compositional) ─────────────────────────────────────────────


BALANCE_SHEET_BLOCK = _make_statement_entry("balance_sheet", "scale-3d")
INCOME_STATEMENT_BLOCK = _make_statement_entry("income_statement", "trending-up")
CASH_FLOW_STATEMENT_BLOCK = _make_statement_entry("cash_flow_statement", "waves")
EQUITY_STATEMENT_BLOCK = _make_statement_entry("equity_statement", "pie-chart")
COMPREHENSIVE_INCOME_BLOCK = _make_statement_entry("comprehensive_income", "activity")


# ── Disclosure notes (compositional) ───────────────────────────────────────

DISCLOSURE_BLOCK = BlockTypeRegistryEntry(
  id=disclosure_handlers.DISCLOSURE_BLOCK_TYPE,
  display_name=disclosure_handlers.DISCLOSURE_DISPLAY_NAME,
  display_plural="Disclosures",
  category=disclosure_handlers.DISCLOSURE_CATEGORY,
  icon="file-text",
  description=(
    "Disclosure note — a reporting structure beyond the statement "
    "family (inventory by category, PP&E by class, debt maturities). "
    "Renders when its concepts receive mapped facts from a Report run; "
    "disclosures are fact-driven, not composed by the Reporting Style. "
    "The structure itself is vocabulary, authored via "
    "create-taxonomy-block, not create-information-block."
  ),
  concept_arrangement_default="roll_up",
  member_arrangement_default=None,
  mechanics_schema=StatementMechanics,
  create_request_model=_EmptyPayload,
  update_request_model=_EmptyPayload,
  delete_request_model=_EmptyPayload,
  construction_mode="compositional",
  dispatch_create=make_not_implemented_handler(
    "create-regulatory-disclosure-block",
    "Disclosure blocks are not created through "
    "create-information-block — the disclosure structure is vocabulary "
    "(elements + presentation/calculation arcs), authored via "
    "`create-taxonomy-block`. Its facts materialize when "
    "`create-report` runs and the mapped ledger reaches the "
    "disclosure's concepts.",
  ),
  dispatch_update=make_not_implemented_handler(
    "update-regulatory-disclosure-block",
    "Disclosure blocks are not updated through "
    "update-information-block — edit the underlying structure via "
    "`update-taxonomy-block`, then `regenerate-report` to refresh "
    "facts.",
  ),
  dispatch_delete=make_not_implemented_handler(
    "delete-regulatory-disclosure-block",
    "Disclosure blocks are not deleted through "
    "delete-information-block — remove the underlying structure via "
    "`delete-taxonomy-block`.",
  ),
  dispatch_build_envelope=disclosure_handlers.build_envelope,
  # Library-seeded disclosure rows are arc-less identity envelopes —
  # build_envelope returns None for them, and the sentinel shouldn't
  # list them either.
  surfaces_in_library=False,
)


# ── Metric (derivative) ────────────────────────────────────────────────────

METRIC_BLOCK = BlockTypeRegistryEntry(
  id=METRIC_BLOCK_TYPE,
  display_name=METRIC_DISPLAY_NAME,
  display_plural="Metrics",
  category=METRIC_CATEGORY,
  icon="gauge",
  description=(
    "Derivative block — computes its facts from one or more source "
    "blocks at read time. Covenant tests, ratios, KPI trend views. "
    "Typed mechanics ship today; the derivation evaluator that "
    "computes facts from source-block FactSets is not yet implemented."
  ),
  concept_arrangement_default="arithmetic",
  member_arrangement_default=None,
  mechanics_schema=MetricMechanics,
  create_request_model=_EmptyPayload,
  update_request_model=_EmptyPayload,
  delete_request_model=_EmptyPayload,
  construction_mode="derivative",
  dispatch_create=make_not_implemented_handler(
    "create-metric-block",
    "Metric block construction is not yet available — the typed "
    "`MetricMechanics` arm ships today (so callers can already query "
    'metric envelopes via `informationBlocks(blockType="metric")`), '
    "but the derivation evaluator that computes facts from source-block "
    "FactSets is not yet implemented; no caller-side workaround is "
    "available.",
  ),
  dispatch_update=make_not_implemented_handler(
    "update-metric-block",
    "Metric block updates are not yet available — pending the "
    "derivation evaluator. See `create-metric-block` for context.",
  ),
  dispatch_delete=make_not_implemented_handler(
    "delete-metric-block",
    "Metric block deletion is not yet available — pending the "
    "derivation evaluator. See `create-metric-block` for context.",
  ),
  dispatch_build_envelope=metric_handlers.build_envelope,
  surfaces_in_library=False,
)


# ── Registry ────────────────────────────────────────────────────────────────

REGISTRY: dict[str, BlockTypeRegistryEntry] = {
  SCHEDULE_BLOCK.id: SCHEDULE_BLOCK,
  ROLLFORWARD_BLOCK.id: ROLLFORWARD_BLOCK,
  FORECAST_BLOCK.id: FORECAST_BLOCK,
  BALANCE_SHEET_BLOCK.id: BALANCE_SHEET_BLOCK,
  INCOME_STATEMENT_BLOCK.id: INCOME_STATEMENT_BLOCK,
  CASH_FLOW_STATEMENT_BLOCK.id: CASH_FLOW_STATEMENT_BLOCK,
  EQUITY_STATEMENT_BLOCK.id: EQUITY_STATEMENT_BLOCK,
  COMPREHENSIVE_INCOME_BLOCK.id: COMPREHENSIVE_INCOME_BLOCK,
  DISCLOSURE_BLOCK.id: DISCLOSURE_BLOCK,
  METRIC_BLOCK.id: METRIC_BLOCK,
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
  "COMPREHENSIVE_INCOME_BLOCK",
  "DISCLOSURE_BLOCK",
  "EQUITY_STATEMENT_BLOCK",
  "FORECAST_BLOCK",
  "INCOME_STATEMENT_BLOCK",
  "METRIC_BLOCK",
  "REGISTRY",
  "ROLLFORWARD_BLOCK",
  "SCHEDULE_BLOCK",
  "get",
  "list_registered",
]
