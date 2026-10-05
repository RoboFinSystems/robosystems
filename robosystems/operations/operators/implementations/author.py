"""AuthorOperator — makes the change a console request asks for (a metric
block, a forecast, a counterparty, a draft report, an inbox classification)
through a narrow set of write tools, on top of the analyst's read surface.
Each write is additive and reversible, or waits on a step only a person
takes.
"""

from __future__ import annotations

from typing import Any

from robosystems.config.operators import OperatorConfig
from robosystems.operations.operators.base import (
  ExecutionProfile,
  OperatorCapability,
  OperatorMode,
  OperatorSpec,
)
from robosystems.operations.operators.implementations.analyst import AnalystOperator
from robosystems.operations.operators.operator_context import (
  OperatorContext,
  ToolAccess,
)
from robosystems.operations.operators.operator_registry import register_operator
from robosystems.operations.operators.tool_loop import WriteGuard, WriteRefusedError

# A backstop against a runaway loop, not the budget: max_credits governs. A
# write plan cut off by a per-mode step cap would leave some writes landed
# and some not.
_STEP_CAP = 25


async def _no_chart_of_accounts(
  arguments: dict[str, Any], tools: ToolAccess
) -> dict[str, Any]:
  # Creating a chart links it as the entity's primary one and demotes the
  # chart the books run on.
  if arguments.get("taxonomy_type") == "chart_of_accounts":
    raise WriteRefusedError(
      "A chart of accounts is set up on the Chart of Accounts page, not from "
      "the console: creating one here would replace the entity's primary chart."
    )
  return arguments


_CLASSIFY_ARGUMENTS = frozenset({"event_id", "transition_to", "metadata_patch"})
_CLASSIFY_METADATA = frozenset(
  {
    "classified_element_id",
    "classified_allocations",
    "accept_suggestion",
    "basis",
    "classified_by",
  }
)


async def _classify_only(
  arguments: dict[str, Any], tools: ToolAccess
) -> dict[str, Any]:
  # Committing a line is the person's approval, so it never happens here.
  from robosystems.operations.event_block.python_handlers.bank_feed import (
    BANK_EVENT_TYPES,
  )

  patch = arguments.get("metadata_patch") or {}
  if (
    arguments.get("transition_to") != "classified"
    or set(arguments) - _CLASSIFY_ARGUMENTS
    or not isinstance(patch, dict)
    or set(patch) - _CLASSIFY_METADATA
  ):
    raise WriteRefusedError(
      "From the console, update-event-block only classifies a bank-feed "
      "line: transition_to 'classified' with a metadata_patch of "
      "classified_element_id, classified_allocations or accept_suggestion, "
      "and a basis. Committing, voiding or editing an event is the user's "
      "step in the Inbox."
    )
  # Any captured event accepts the transition, and a synced one moved to
  # classified is never retried by its sync, so the event itself is read.
  # get-event-block names its argument `id`; update-event-block, `event_id`.
  event = await tools.call_tool(
    "get-event-block", {"id": arguments.get("event_id")}, return_raw=True
  )
  if (
    not isinstance(event, dict)
    or event.get("event_type") not in BANK_EVENT_TYPES
    or event.get("status") != "captured"
  ):
    raise WriteRefusedError(
      "From the console, only a captured bank-feed line can be classified. "
      "This event is not one (or was not found); any other event, and a "
      "line already classified, is handled by the user in the Inbox."
    )
  return {**arguments, "metadata_patch": {**patch, "classified_by": "ai"}}


async def _draft_what_it_promotes(
  arguments: dict[str, Any], tools: ToolAccess
) -> dict[str, Any]:
  # A sweep that only flips status strands the obligations: the close then
  # blocks on entries nobody drafted.
  return {**arguments, "dispatch_handlers": True}


@register_operator("author")
class AuthorOperator(AnalystOperator):
  """Reads like the analyst, and writes through the tools below only."""

  OPERATOR_TYPE = "author"
  LOOP_DESCRIPTION = "Author tool loop"

  # The safety boundary: anything that would need an approval card stays
  # off this list. No delete tools, journal edits, period close/reopen,
  # sync, schema or raw-Cypher writes.
  # Metric structures are authored as taxonomy blocks; update and delete stay
  # off because they can rename or retire existing accounts.
  WRITE_TOOLS = (
    "create-taxonomy-block",
    "create-information-block",
    "update-information-block",
    "assert-metrics",
    "compute-metrics",
    "compute-forecast",
    "create-agent",
    "update-agent",
    "remember",
    # A draft; filing and sharing it are held for a person.
    "create-report",
    # Ledger-bound, so each waits on a person's step: a classified line on
    # its commit in the Inbox, a drafted entry on close-period. The sweep is
    # the background one run on demand, housekeeping included (it voids an
    # orphaned obligation and redrafts a stale draft); it never posts.
    "update-event-block",
    "promote-obligations",
  )

  # The allowlist names tools; these narrow a tool to the arguments it may
  # be called with.
  WRITE_GUARDS: dict[str, WriteGuard] = {
    "create-taxonomy-block": _no_chart_of_accounts,
    "update-event-block": _classify_only,
    "promote-obligations": _draft_what_it_promotes,
  }

  spec = OperatorSpec(
    name="Author Operator",
    description=(
      "Makes the change a request asks for (metric and forecast blocks, "
      "counterparties, memories, draft reports, inbox classifications) "
      "through writes that are reversible or wait on a person's step, and "
      "reports each write it made"
    ),
    capabilities=[OperatorCapability.CUSTOM],
    # Gated on the graph write role in the API and again in the worker.
    read_only=False,
    version="1.0.0",
    requires_credits=True,
    # The cached tools + system prefix, measured on a cold /do in production
    # (70,033 tokens of cache write, 2026-10-04).
    cold_start_tokens=70_000,
    execution_profile={
      OperatorMode.QUICK: ExecutionProfile(
        min_time=5, max_time=30, avg_time=12, tool_calls=6
      ),
      OperatorMode.STANDARD: ExecutionProfile(
        min_time=10, max_time=60, avg_time=25, tool_calls=12
      ),
      OperatorMode.EXTENDED: ExecutionProfile(
        min_time=20, max_time=180, avg_time=60, tool_calls=25
      ),
    },
  )

  def can_handle(self, query: str, context: dict[str, Any] | None = None) -> float:
    # Chosen by name only: auto-routing must never turn a question into a write.
    return 0.0

  def _max_iterations(self, limits: dict[str, Any]) -> int:
    return _STEP_CAP

  @staticmethod
  def _get_max_credits(ctx: OperatorContext) -> float | None:
    """The request's ceiling, else the author's configured default: a write
    run is bounded even when the caller sets nothing."""
    requested = AnalystOperator._get_max_credits(ctx)
    if requested is not None:
      return requested
    return OperatorConfig.get_operator_capabilities("author").get("default_max_credits")

  def _prompt_suffix(self, write_tools: list[str]) -> str:
    if not write_tools:
      return (
        "\n\nAUTHORING: no write tools are available on this graph. Say so, "
        "and describe the change the user would need to make instead."
      )
    names = ", ".join(f"`{t}`" for t in write_tools)
    ledger_rules = ""
    if "update-event-block" in write_tools:
      ledger_rules += (
        "\n- Inbox lines: `update-event-block` here only classifies a "
        "captured bank-feed line (`transition_to: 'classified'` with a "
        "`metadata_patch` of `classified_element_id`, or "
        "`classified_allocations` for a split, or `accept_suggestion: true`, "
        "plus a short `basis`). It never commits, voids or edits a line, and "
        "never touches an event from another source."
      )
    if "create-report" in write_tools:
      ledger_rules += (
        "\n- `create-report` builds a report; it does not file or share it."
      )
    return f"""

AUTHORING (this request may ask you to change the graph, not just read it):
- You can write with these tools only: {names}. Nothing else changes data; Cypher stays read-only.
- Read before you write. Find the real ids (elements, blocks, agents) with the read tools; never invent one. If what the user named does not exist or is ambiguous, stop and ask instead of guessing.
- Make exactly the change asked for. Do not create extra blocks, edit objects the user did not mention, or repeat a write that already succeeded.
- A write tool that returns an error changed nothing: read the message, fix the arguments, and retry at most once.
- When done, say plainly what you created or changed, naming each object, and anything you could not do. If the request was only a question, answer it and write nothing.
- When the request needs something you have no tool for (closing or reopening a period, committing or voiding an inbox line, editing a journal entry, deleting anything, filing or sharing a report), say where it is done: the Closing Book, the Inbox, the Journal, the report's own menu, or an MCP client.{ledger_rules}"""
