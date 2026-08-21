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

  def get_tool_instance(self, tool_class: type) -> Any:
    """Return an object exposing ``await .execute(arguments)`` for a tool.

    For operators that drive tools imperatively rather than through the
    model-driven loop (MappingOperator). Declared on the protocol because
    both implementations provide it, so an operator calling it never has to
    know which context it is running in. Both must keep implementing it.
    """


@dataclass
class OperatorContext:
  """Everything an operator needs, injected into `Operator.run()`.

  Built by the execution adapters (`adapters/api.py`, `adapters/worker.py`),
  which pick the service implementations that suit their context. `extra`
  carries operator-specific parameters the adapter passed straight through
  (e.g. `mapping_id` for `MappingOperator`).

  The three service fields are optional only so the dataclass can be built
  incrementally — an operator can assume the adapter populated all of them.
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
