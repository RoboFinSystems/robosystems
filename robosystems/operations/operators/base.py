"""Base classes for AI Operators.

Two protocols coexist:

- ``Operator`` / ``OperatorSpec`` / ``OperatorResult`` — the unified protocol.
  Write new operators against this one.
- ``BaseOperator`` / ``OperatorMetadata`` / ``OperatorResponse`` — the older
  protocol. No shipped operator extends ``BaseOperator`` any more
  (``AnalystOperator`` and ``MappingOperator`` are both ``Operator``); it stays
  exported for its own tests, and the orchestrator still answers in
  ``OperatorResponse``.

"Operator" is the AI-executor concept (Claude/MCP), distinct from the REA
``Agent`` (counterparty) modeled in ``models/extensions/roboledger/agent.py``.
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
  """Declares which graphs an operator is allowed to run on.

  None fields mean no restriction on that axis.
  If both fields are set, both conditions must be satisfied.
  """

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


# ── New unified Operator protocol ────────────────────────────────────────────


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
  """What an operator returns. Contains domain results only — no runtime metadata.

  Runtime metadata (tokens used, credits consumed, execution time) is tracked
  by the OperatorContext and attached by the execution adapter.
  """

  content: str
  metadata: dict[str, Any] = field(default_factory=dict)
  tools_called: list[str] = field(default_factory=list)
  confidence_score: float | None = None
  requires_followup: bool = False


class Operator(ABC):
  """Base class for all operators in the unified AI Operator system.

  Operators are stateless domain logic containers. They declare their
  capabilities via `spec` (a class attribute) and implement `run()` which
  receives an `OperatorContext` providing AI, tools, credits, and progress
  reporting.

  Example::

      class MyCoolOperator(Operator):
          spec = OperatorSpec(
              name="Cool Operator",
              description="Does cool things",
              capabilities=[OperatorCapability.CUSTOM],
          )

          async def run(self, ctx: OperatorContext) -> OperatorResult:
              response = await ctx.ai.create_message(...)
              return OperatorResult(content=response.content)
  """

  spec: OperatorSpec  # Must be set by subclass as a class attribute

  @abstractmethod
  async def run(self, ctx: OperatorContext) -> OperatorResult:
    """Execute the operator's logic.

    All services (AI, tools, progress, credits) come from ``ctx``. Credit
    consumption is automatic — every ``ctx.ai.create_message()`` call tracks
    tokens and deducts credits.
    """

  def can_handle(self, query: str, context: dict[str, Any] | None = None) -> float:
    """Return confidence score (0-1) for handling this query.

    Used by the orchestrator for routing. Override for custom logic.
    Default returns 0.5 (neutral confidence).
    """
    return 0.5


# ── Legacy classes (used by orchestrator/routers during migration) ───────────


@dataclass
class OperatorMetadata:
  """Capability metadata for a ``BaseOperator``. New operators use
  :class:`OperatorSpec`.
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
  """Response from a ``BaseOperator``. New operators return
  :class:`OperatorResult`.
  """

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
  """Base class for the older operator protocol. No shipped operator extends
  it — ``AnalystOperator`` and ``MappingOperator`` are both :class:`Operator`,
  which is what new operators inherit from; this stays for its own tests and
  the orchestrator's ``OperatorResponse``.
  """

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
    """Return operator metadata."""
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
    """Check if operator supports the given mode."""
    return mode in self.metadata.supported_modes

  def has_capability(self, capability: OperatorCapability) -> bool:
    """Check if operator has the given capability."""
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
    """Track token usage for the operator."""
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
  """Gate a write-capable operator on the caller's graph write role.

  Operators drive MCP tools through a tool-access layer that carries no user
  identity, so the per-tool write classification the MCP router applies
  (`validate_mcp_access(..., "write")`) never runs on this path. The role has
  to be checked once, before the operator starts its tool-use loop.

  Called from the execution adapters rather than the routers so that every
  entry point inherits it — sync, SSE, queued background, and worker — since
  two of those bypass the orchestrator and call the adapter directly.

  No-ops for operators whose spec sets `read_only=True`; see
  :attr:`OperatorSpec.read_only` for why the default is fail-closed.

  Raises:
      HTTPException: 403 if the user's role on the graph is read-only.
  """
  if operator.spec.read_only:
    return

  # Local import: this module is imported by the operator implementations, and
  # the auth dependencies pull in the platform DB stack.
  from robosystems.middleware.auth.dependencies import require_graph_write_role

  require_graph_write_role(user_id, graph_id)


def enforce_operator_graph_scope(operator: Operator, graph_id: str) -> None:
  """Refuse to run an operator on a graph outside its declared ``graph_scope``.

  The orchestrator applies ``matches_graph_scope`` when it routes, but the
  SSE, background-queue and worker paths call the execution adapters directly
  and skipped it: a mapping operator scoped to ``roboledger`` could be started
  on a graph with no ledger tenant, and its tools would then fail one by one
  against a missing schema. Checked in the adapters, next to the write-role
  gate, so every entry point inherits it. No-op for operators without a scope.

  Raises:
      HTTPException: 403 when the graph does not match the operator's scope.
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
