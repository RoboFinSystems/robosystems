"""AI Operator operations module.

Provides the unified Operator system with dynamic routing, execution adapters,
and automatic credit tracking.

## Operator Protocol

New operators inherit from `Operator` and implement
``run(ctx: OperatorContext) -> OperatorResult``. Credit tracking is automatic
via `TrackedAIClient`. Register with ``@register_operator("name")``.

## Execution Adapters

- ``run_operator_api()``: Runs operators in API request context (sync/SSE)
- ``run_operator_worker()``: Runs operators in worker context (background tasks)

## Legacy

OperatorResponse is kept for API response compatibility. The orchestrator
converts OperatorResult → OperatorResponse at the boundary.

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

# Load adapter-contributed operators (empty for now — extension point)
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
  # Legacy (API compat)
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
