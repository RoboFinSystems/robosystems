"""AuthorOperator — makes the change a console request asks for (a metric
block, a forecast, a counterparty) through a narrow set of additive,
reversible write tools, on top of the analyst's read surface.
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
from robosystems.operations.operators.operator_context import OperatorContext
from robosystems.operations.operators.operator_registry import register_operator
from robosystems.operations.operators.tool_loop import WriteGuard, WriteRefusedError

# A backstop against a runaway loop, not the budget: max_credits governs. A
# write plan cut off by a per-mode step cap would leave some writes landed
# and some not.
_STEP_CAP = 25


def _no_chart_of_accounts(arguments: dict[str, Any]) -> dict[str, Any]:
  # Creating a chart links it as the entity's primary one and demotes the
  # chart the books run on.
  if arguments.get("taxonomy_type") == "chart_of_accounts":
    raise WriteRefusedError(
      "A chart of accounts is set up on the Chart of Accounts page, not from "
      "the console: creating one here would replace the entity's primary chart."
    )
  return arguments


@register_operator("author")
class AuthorOperator(AnalystOperator):
  """Reads like the analyst, and writes through the tools below only."""

  OPERATOR_TYPE = "author"
  LOOP_DESCRIPTION = "Author tool loop"

  # The safety boundary: anything that would need an approval card stays
  # off this list. No deletes, journal edits, period close/reopen, sync,
  # schema or raw-Cypher writes.
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
  )

  # The allowlist names tools; these narrow a tool to the arguments it may
  # be called with.
  WRITE_GUARDS: dict[str, WriteGuard] = {
    "create-taxonomy-block": _no_chart_of_accounts,
  }

  spec = OperatorSpec(
    name="Author Operator",
    description=(
      "Makes the change a request asks for (metric and forecast blocks, "
      "counterparties, memories) through additive, reversible writes, and "
      "reports each write it made"
    ),
    capabilities=[OperatorCapability.CUSTOM],
    # Gated on the graph write role in the API and again in the worker.
    read_only=False,
    version="1.0.0",
    requires_credits=True,
    # The cached tools + system prefix measured on a cold /do (~61K tokens).
    cold_start_tokens=62_000,
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
    return f"""

AUTHORING (this request may ask you to change the graph, not just read it):
- You can write with these tools only: {names}. Nothing else changes data; Cypher stays read-only.
- Read before you write. Find the real ids (elements, blocks, agents) with the read tools; never invent one. If what the user named does not exist or is ambiguous, stop and ask instead of guessing.
- Make exactly the change asked for. Do not create extra blocks, edit objects the user did not mention, or repeat a write that already succeeded.
- A write tool that returns an error changed nothing: read the message, fix the arguments, and retry at most once.
- When done, say plainly what you created or changed, naming each object, and anything you could not do. If the request was only a question, answer it and write nothing."""
