"""Operator execution context — the services bag injected into Operator.run().

Provides identity, AI access, tool access, and progress reporting.
All services are protocol-based so the same operator code works in API
request context, worker context, or tests.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
  from robosystems.operations.operators.base import OperatorMode
  from robosystems.operations.operators.tracked_ai import TrackedAIClient


@runtime_checkable
class ProgressReporter(Protocol):
  """Protocol for progress reporting — adapted per execution context."""

  async def report(
    self,
    message: str,
    percent: float | None = None,
    details: dict[str, Any] | None = None,
  ) -> None:
    """Emit a progress update."""

  async def is_cancelled(self) -> bool:
    """Check if the operation has been cancelled."""


@runtime_checkable
class ToolAccess(Protocol):
  """Protocol for MCP tool access — adapted per execution context."""

  @property
  def graph_id(self) -> str:
    """The graph ID this tool access is bound to."""

  async def call_tool(
    self,
    tool_name: str,
    arguments: dict[str, Any],
    return_raw: bool = False,
  ) -> Any:
    """Call an MCP tool by name."""

  async def get_tool_schemas(self, names: list[str]) -> list[dict[str, Any]]:
    """Return Anthropic-shaped tool definitions for the named tools.

    Filters to the subset of `names` that are actually available on this
    graph (extension/flag-gated tools are omitted) and returns them as
    `{"name", "description", "input_schema"}` ready to hand to the model.
    """


@dataclass
class OperatorContext:
  """Injected into Operator.run(). Provides all services an operator needs.

  Constructed by execution adapters (api.py, worker.py) with the appropriate
  service implementations for the execution context.

  Attributes:
      graph_id: The graph database identifier.
      user_id: The authenticated user's ID.
      query: The user's query or task description.
      mode: Execution mode (QUICK, STANDARD, EXTENDED).
      history: Conversation history (for multi-turn operators).
      extra: Task-specific parameters (e.g., mapping_id for MappingOperator).
      ai: TrackedAIClient — AI calls with automatic credit tracking.
      tools: ToolAccess — MCP tool access.
      progress: ProgressReporter — progress updates and cancellation.
  """

  # Identity
  graph_id: str
  user_id: str
  query: str
  mode: OperatorMode
  history: list[dict[str, Any]] = field(default_factory=list)
  extra: dict[str, Any] = field(default_factory=dict)

  # Services (injected by the adapter)
  ai: TrackedAIClient | None = None
  tools: ToolAccess | None = None
  progress: ProgressReporter | None = None
