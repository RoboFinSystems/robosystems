"""Base classes for AI Operators.

Write new operators against ``Operator`` / ``OperatorSpec`` / ``OperatorResult``.
``BaseOperator`` / ``OperatorMetadata`` / ``OperatorResponse`` are the older
protocol; no shipped operator extends ``BaseOperator``, but the orchestrator
still answers in ``OperatorResponse``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

from robosystems.logger import logger

if TYPE_CHECKING:
  from robosystems.models.core import User
  from robosystems.operations.operators.operator_context import OperatorContext


# ── Shared enums and primitives ──────────────────────────────────────────────


class OperatorCapability(Enum):
  """Operator capabilities for routing and discovery."""

  FINANCIAL_ANALYSIS = "financial_analysis"
  DEEP_RESEARCH = "deep_research"
  COMPLIANCE = "compliance"
  RAG_SEARCH = "rag_search"
  CUSTOM = "custom"
  ENTITY_ANALYSIS = "entity_analysis"
  TREND_ANALYSIS = "trend_analysis"


class OperatorMode(Enum):
  """Operator execution modes with different performance characteristics."""

  QUICK = "quick"  # Fast, limited tool calls (1-2)
  STANDARD = "standard"  # Balanced performance (3-5 tool calls)
  EXTENDED = "extended"  # Deep research (6+ tool calls)
  STREAMING = "streaming"  # SSE streaming responses


@dataclass
class ExecutionProfile:
  """Execution time profile for an operator mode."""

  min_time: int
  max_time: int
  avg_time: int
  tool_calls: int = 0


@dataclass
class GraphScope:
  """Graphs an operator may run on. None means no restriction on that axis;
  set fields must all match."""

  shared_repo: str | None = None
  schema_extension: str | None = None


def matches_graph_scope(
  scope: GraphScope | None,
  graph_id: str,
  schema_extensions: list[str],
) -> bool:
  """Check if a graph matches the given scope. None scope = matches everything."""
  if scope is None:
    return True
  if scope.shared_repo is not None:
    from robosystems.config.shared_repositories import (
      is_shared_repository_or_subgraph,
      resolve_shared_repository_parent,
    )

    if not is_shared_repository_or_subgraph(graph_id):
      return False
    if resolve_shared_repository_parent(graph_id) != scope.shared_repo:
      return False
  if scope.schema_extension is not None:
    if scope.schema_extension not in schema_extensions:
      return False
  return True


# ── Operator protocol ────────────────────────────────────────────────────────


@dataclass
class OperatorSpec:
  """Declarative operator metadata, set as a class attribute on subclasses.

  Readable without instantiation, so the orchestrator can route without
  constructing operator objects.
  """

  name: str
  description: str
  capabilities: list[OperatorCapability]
  version: str = "1.0.0"
  supported_modes: list[OperatorMode] = field(
    default_factory=lambda: [
      OperatorMode.QUICK,
      OperatorMode.STANDARD,
      OperatorMode.EXTENDED,
    ]
  )
  max_tokens: dict[str, int] = field(
    default_factory=lambda: {"input": 150000, "output": 8000}
  )
  requires_credits: bool = True
  read_only: bool = False
  """Whether this operator only reads from the graph.

  Fail-closed on purpose: the default is `False`, so an operator is treated as
  write-capable — and gated on the graph write role — unless it declares
  otherwise. An operator that gains a write tool later inherits the gate, and
  one that forgets this flag is over-restricted rather than under-protected.
  Set `True` only where the tool allowlist is provably read-only.
  """
  execution_profile: dict[OperatorMode, ExecutionProfile] = field(
    default_factory=lambda: {
      OperatorMode.QUICK: ExecutionProfile(
        min_time=2, max_time=5, avg_time=3, tool_calls=2
      ),
      OperatorMode.STANDARD: ExecutionProfile(
        min_time=5, max_time=15, avg_time=10, tool_calls=5
      ),
      OperatorMode.EXTENDED: ExecutionProfile(
        min_time=30, max_time=120, avg_time=60, tool_calls=20
      ),
    }
  )
  graph_scope: GraphScope | None = None


@dataclass
class OperatorResult:
  """Domain results only; the execution adapter attaches token and credit
  metadata."""

  content: str
  metadata: dict[str, Any] = field(default_factory=dict)
  tools_called: list[str] = field(default_factory=list)
  confidence_score: float | None = None
  requires_followup: bool = False


class Operator(ABC):
  """Stateless operator: declares `spec` as a class attribute and implements
  `run()` against the services in `OperatorContext`."""

  spec: OperatorSpec

  @abstractmethod
  async def run(self, ctx: OperatorContext) -> OperatorResult:
    """Every ``ctx.ai.create_message()`` call bills itself; don't consume
    credits here."""

  def can_handle(self, query: str, context: dict[str, Any] | None = None) -> float:
    """Routing confidence, 0-1."""
    return 0.5


# ── Legacy protocol ──────────────────────────────────────────────────────────


@dataclass
class OperatorMetadata:
  """Capability metadata for a ``BaseOperator``."""

  name: str
  description: str
  capabilities: list[OperatorCapability]
  version: str = "1.0.0"
  supported_modes: list[OperatorMode] = field(
    default_factory=lambda: [
      OperatorMode.QUICK,
      OperatorMode.STANDARD,
      OperatorMode.EXTENDED,
    ]
  )
  max_tokens: dict[str, int] = field(
    default_factory=lambda: {"input": 150000, "output": 8000}
  )
  requires_credits: bool = True
  author: str | None = None
  tags: list[str] = field(default_factory=list)
  execution_profile: dict[OperatorMode, ExecutionProfile] = field(
    default_factory=lambda: {
      OperatorMode.QUICK: ExecutionProfile(
        min_time=2, max_time=5, avg_time=3, tool_calls=2
      ),
      OperatorMode.STANDARD: ExecutionProfile(
        min_time=5, max_time=15, avg_time=10, tool_calls=5
      ),
      OperatorMode.EXTENDED: ExecutionProfile(
        min_time=30, max_time=120, avg_time=60, tool_calls=20
      ),
    }
  )


@dataclass
class OperatorResponse:
  """Response from a ``BaseOperator``, and the orchestrator's answer shape."""

  content: str
  operator_name: str
  mode_used: OperatorMode
  metadata: dict[str, Any] | None = None
  tokens_used: dict[str, int] | None = None
  tools_called: list[str] = field(default_factory=list)
  confidence_score: float | None = None
  requires_followup: bool = False
  error_details: dict[str, Any] | None = None
  execution_time: float | None = None
  timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


class BaseOperator(ABC):
  """Older operator protocol; no shipped operator extends it."""

  def __init__(
    self,
    graph_id: str,
    user: User,
    db_session=None,
  ):
    """Without ``db_session`` credit consumption is skipped, not deferred."""
    self.graph_id = graph_id
    self.user = user
    self.db_session = db_session
    self.logger = logger
    self.total_tokens_used = {"input": 0, "output": 0}
    self.graph_client = None
    self.mcp_tools = None

  @property
  @abstractmethod
  def metadata(self) -> OperatorMetadata:
    pass

  @abstractmethod
  async def analyze(
    self,
    query: str,
    mode: OperatorMode = OperatorMode.STANDARD,
    history: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
    callback: Any | None = None,
  ) -> OperatorResponse:
    """Answer ``query``, optionally reporting progress through ``callback``."""
    pass

  @abstractmethod
  def can_handle(self, query: str, context: dict[str, Any] | None = None) -> float:
    """Return 0-1 confidence that this operator should handle the query."""
    pass

  def supports_mode(self, mode: OperatorMode) -> bool:
    return mode in self.metadata.supported_modes

  def has_capability(self, capability: OperatorCapability) -> bool:
    return capability in self.metadata.capabilities

  async def initialize_tools(self):
    """Open the graph MCP client and build the tool set for this graph."""
    try:
      from robosystems.middleware.mcp import (
        GraphMCPTools,
        create_graph_mcp_client,
      )
      from robosystems.middleware.mcp.tools.manager import resolve_schema_extensions

      self.graph_client = await create_graph_mcp_client(graph_id=self.graph_id)

      schema_extensions = resolve_schema_extensions(self.graph_id)
      self.mcp_tools = GraphMCPTools(
        self.graph_client, schema_extensions=schema_extensions
      )
      self.logger.info(
        f"Initialized MCP tools for operator in graph {self.graph_id} "
        f"(extensions={schema_extensions})"
      )
    except Exception as e:
      self.logger.error(f"Failed to initialize MCP tools: {e!s}")
      raise

  async def close(self):
    """Close the graph MCP client. Safe to call when it was never opened."""
    if self.graph_client:
      try:
        await self.graph_client.close()
        self.logger.debug("Closed Graph client connection")
      except Exception as e:
        self.logger.error(f"Error closing Graph client: {e!s}")

  def track_tokens(self, input_tokens: int, output_tokens: int):
    self.total_tokens_used["input"] += input_tokens
    self.total_tokens_used["output"] += output_tokens

  async def consume_credits(
    self,
    input_tokens: int,
    output_tokens: int,
    model: str = "claude-3-sonnet",
    operation_description: str = "Operator analysis",
  ) -> dict[str, Any] | None:
    """Deduct credits for token usage; None when there is no DB session.

    Never raises: a credit failure is logged and swallowed so it cannot lose an
    already-completed analysis.
    """
    if not self.db_session:
      self.logger.warning(
        f"No DB session for credit consumption (graph={self.graph_id}). "
        f"Tokens used - Input: {input_tokens}, Output: {output_tokens}"
      )
      return None

    try:
      from robosystems.operations.graph.credit_service import CreditService

      credit_service = CreditService(self.db_session)
      result = credit_service.consume_ai_tokens(
        graph_id=self.graph_id,
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        model=model,
        operation_description=operation_description,
        user_id=str(self.user.id),
      )

      if result.get("success"):
        self.logger.info(
          f"Credits consumed for {self.graph_id}: {result.get('credits_consumed', 0)}"
        )
      else:
        self.logger.warning(
          f"Failed to consume credits: {result.get('error', 'Unknown error')}"
        )

      return result

    except Exception as e:
      self.logger.error(
        f"Error consuming credits for graph={self.graph_id} model={model} "
        f"tokens=({input_tokens}/{output_tokens}): {e!s}",
        exc_info=True,
      )
      return None

  def validate_mode(self, mode: OperatorMode) -> None:
    """Raise ValueError when the operator does not support ``mode``."""
    if not self.supports_mode(mode):
      supported = ", ".join(m.value for m in self.metadata.supported_modes)
      raise ValueError(
        f"Operator '{self.metadata.name}' does not support mode '{mode.value}'. "
        f"Supported modes: {supported}"
      )

  def get_mode_limits(self, mode: OperatorMode) -> dict[str, Any]:
    """Per-mode limits (max tools, timeout) from ``OperatorConfig``."""
    from robosystems.config import OperatorConfig

    return OperatorConfig.get_mode_limits(mode.value)

  async def prepare_context(
    self,
    query: str,
    context: dict[str, Any] | None = None,
  ) -> dict[str, Any]:
    """Merge identity, operator, and timestamp fields into ``context``.

    Mutates and returns the caller's dict when one is passed.
    """
    enhanced_context = context or {}

    enhanced_context.update(
      {
        "graph_id": self.graph_id,
        "user_id": str(self.user.id),
        "operator_name": self.metadata.name,
        "timestamp": datetime.now(UTC).isoformat(),
        "capabilities": [c.value for c in self.metadata.capabilities],
      }
    )

    return enhanced_context

  def __repr__(self) -> str:
    return (
      f"<{self.__class__.__name__} "
      f"name='{self.metadata.name}' "
      f"graph_id='{self.graph_id}'>"
    )


def enforce_operator_write_role(
  operator: Operator, graph_id: str, user_id: str
) -> None:
  """Gate a write-capable operator on the caller's graph write role (403).

  The tool-access layer carries no user identity, so the MCP router's per-tool
  write check never runs on this path; this must run before the tool loop.
  No-op when ``spec.read_only``.
  """
  if operator.spec.read_only:
    return

  # Local import: auth dependencies pull in the platform DB stack.
  from robosystems.middleware.auth.dependencies import require_graph_write_role

  require_graph_write_role(user_id, graph_id)


def enforce_operator_graph_scope(operator: Operator, graph_id: str) -> None:
  """403 when the graph is outside the operator's declared ``graph_scope``.

  Paths that bypass orchestrator routing would otherwise start an operator on
  a graph it can't serve (e.g. a ledger operator with no ledger tenant).
  """
  scope = operator.spec.graph_scope
  if scope is None:
    return

  from fastapi import HTTPException

  from robosystems.middleware.mcp.tools.manager import resolve_schema_extensions

  extensions = resolve_schema_extensions(graph_id)
  if not matches_graph_scope(scope, graph_id, extensions):
    raise HTTPException(
      status_code=403,
      detail=(
        f"Operator '{operator.spec.name}' is not available on graph {graph_id}: "
        "the graph is outside the operator's declared scope."
      ),
    )
