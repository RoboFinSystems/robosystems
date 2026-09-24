"""AI Operators — routing, execution adapters, and credit tracking. See the
README in this directory."""

# Registers the operators; must precede any registry use.
from robosystems.operations.operators import implementations  # noqa: F401
from robosystems.operations.operators.adapters import run_operator_worker
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
  enforce_operator_graph_scope,
  enforce_operator_write_role,
  matches_graph_scope,
)
from robosystems.operations.operators.credit_consumer import (
  CreditConsumer,
  FactoryCreditConsumer,
  NoOpCreditConsumer,
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
  OrchestratorConfig,
  RoutingStrategy,
)
from robosystems.operations.operators.progress import (
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
  "AIClient",
  "BaseOperator",
  "CreditConsumer",
  "DirectToolAccess",
  "ExecutionProfile",
  "FactoryCreditConsumer",
  "GraphScope",
  "HttpToolAccess",
  "NoOpCreditConsumer",
  "NoOpProgress",
  "OperationManagerProgress",
  "Operator",
  "OperatorCapability",
  "OperatorContext",
  "OperatorMetadata",
  "OperatorMode",
  "OperatorOrchestrator",
  "OperatorResponse",
  "OperatorResult",
  "OperatorSpec",
  "OrchestratorConfig",
  "ProgressReporter",
  "RoutingStrategy",
  "ToolAccess",
  "TrackedAIClient",
  "enforce_operator_graph_scope",
  "enforce_operator_write_role",
  "get_operator",
  "get_operator_class",
  "list_operators",
  "load_adapter_operators",
  "matches_graph_scope",
  "register_operator",
  "run_operator_worker",
]
