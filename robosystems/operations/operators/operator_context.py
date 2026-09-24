"""The services bag injected into `Operator.run()`; protocol-typed so the same
operator runs under either adapter or in tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
  from robosystems.operations.operators.base import OperatorMode
  from robosystems.operations.operators.tracked_ai import TrackedAIClient


@runtime_checkable
class ProgressReporter(Protocol):
  async def report(
    self,
    message: str,
    percent: float | None = None,
    details: dict[str, Any] | None = None,
  ) -> None:
    """Emit a progress update."""

  async def is_cancelled(self) -> bool:
    """Whether the operation has been cancelled."""


@runtime_checkable
class ToolAccess(Protocol):
  @property
  def graph_id(self) -> str:
    """The graph this tool access is bound to."""

  async def call_tool(
    self,
    tool_name: str,
    arguments: dict[str, Any],
    return_raw: bool = False,
  ) -> Any:
    """Call an MCP tool by name."""

  async def get_tool_schemas(self, names: list[str]) -> list[dict[str, Any]]:
    """`names` filtered to what this graph exposes, as
    `{"name", "description", "inputSchema"}`."""

  def get_tool_instance(self, tool_class: type) -> Any:
    """An object exposing ``await .execute(arguments)``, for operators that
    drive tools imperatively (MappingOperator)."""


@dataclass
class OperatorContext:
  """Built by the execution adapters. `extra` carries operator-specific
  parameters (e.g. `mapping_id`). The service fields are optional only for
  construction; an operator can assume all three are set."""

  graph_id: str
  user_id: str
  query: str
  mode: OperatorMode
  history: list[dict[str, Any]] = field(default_factory=list)
  extra: dict[str, Any] = field(default_factory=dict)

  ai: TrackedAIClient | None = None
  tools: ToolAccess | None = None
  progress: ProgressReporter | None = None
