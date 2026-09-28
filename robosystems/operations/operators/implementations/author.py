"""AuthorOperator — makes the change a console request asks for (a metric
block, a forecast, a counterparty) through a narrow set of additive,
reversible write tools, on top of the analyst's read surface.
"""

from __future__ import annotations

from typing import Any

from robosystems.operations.operators.base import (
  ExecutionProfile,
  OperatorCapability,
  OperatorMode,
  OperatorSpec,
)
from robosystems.operations.operators.implementations.analyst import AnalystOperator
from robosystems.operations.operators.operator_registry import register_operator

# A backstop against a runaway loop, not the budget: max_credits governs. A
# write plan cut off by a per-mode step cap would leave some writes landed
# and some not.
_STEP_CAP = 25


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
