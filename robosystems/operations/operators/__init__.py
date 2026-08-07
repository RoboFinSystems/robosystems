"""AI Operators — routing, execution adapters, and credit tracking.

## Operator protocol

An operator subclasses `Operator`, declares an `OperatorSpec`, implements
``run(ctx: OperatorContext) -> OperatorResult``, and registers itself with
``@register_operator("name")``. Credit tracking is automatic: every AI call
goes through `TrackedAIClient`, so an operator never consumes credits itself.

## Execution adapters

An operator is stateless — the adapter builds the context around it.

- ``run_operator_api()``: API request context (sync/SSE)
- ``run_operator_worker()``: worker context (background tasks)

`OperatorResult` is the operator-facing return type; `OperatorResponse` is the
API-facing shape. The orchestrator converts between them at the boundary.

## Naming

"Operator" is the AI-executor concept (Claude/MCP), distinct from REA ``Agent``
(counterparty) in ``models/extensions/roboledger/agent.py``.
"""

# Register operator implementations (must import before anything uses the registry)
from robosystems.operations.operators import implementations  # noqa: F401
from robosystems.operations.operators.adapters import (
  run_operator_api,
  run_operator_worker,
)
from robosystems.operations.operators.ai_client import AIClient
from robosystems.operations.operators.base import (
  BaseOperator,
  ExecutionProfile,
  GraphScope,
  Operator,
  OperatorCapability,
  OperatorMetadata,
  OperatorMode,
  OperatorResponse,
  OperatorResult,
  OperatorSpec,
  enforce_operator_write_role,
  matches_graph_scope,
)
from robosystems.operations.operators.credit_consumer import (
  CreditConsumer,
  FactoryCreditConsumer,
  NoOpCreditConsumer,
  SessionCreditConsumer,
)
from robosystems.operations.operators.operator_context import (
  OperatorContext,
  ProgressReporter,
  ToolAccess,
)
from robosystems.operations.operators.operator_registry import (
  get_operator,
  get_operator_class,
  list_operators,
  load_adapter_operators,
  register_operator,
)
from robosystems.operations.operators.orchestrator import (
  OperatorOrchestrator,
  OperatorSelectionCriteria,
  OrchestratorConfig,
  RoutingStrategy,
)
from robosystems.operations.operators.progress import (
  CallbackProgress,
  NoOpProgress,
  OperationManagerProgress,
)
from robosystems.operations.operators.tool_access import (
  DirectToolAccess,
  HttpToolAccess,
)
from robosystems.operations.operators.tracked_ai import TrackedAIClient

# Extension point for adapter-contributed operators; none registered today.
load_adapter_operators()

__all__ = [
  # Runtime services
  "AIClient",
  "BaseOperator",
  "CallbackProgress",
  "CreditConsumer",
  "DirectToolAccess",
  "ExecutionProfile",
  "FactoryCreditConsumer",
  "GraphScope",
  "HttpToolAccess",
  "NoOpCreditConsumer",
  "NoOpProgress",
  "OperationManagerProgress",
  # Operator protocol
  "Operator",
  # Shared enums
  "OperatorCapability",
  "OperatorContext",
  # API response shape
  "OperatorMetadata",
  "OperatorMode",
  # Orchestrator
  "OperatorOrchestrator",
  "OperatorResponse",
  "OperatorResult",
  "OperatorSelectionCriteria",
  "OperatorSpec",
  "OrchestratorConfig",
  "ProgressReporter",
  "RoutingStrategy",
  "SessionCreditConsumer",
  "ToolAccess",
  "TrackedAIClient",
  "enforce_operator_write_role",
  # Registry
  "get_operator",
  "get_operator_class",
  "list_operators",
  "load_adapter_operators",
  "matches_graph_scope",
  "register_operator",
  # Adapters
  "run_operator_api",
  "run_operator_worker",
]
