"""Agent operations module.

Provides the unified agent system with dynamic routing, execution adapters,
and automatic credit tracking.

## Agent Protocol

New agents inherit from `Agent` and implement `run(ctx: AgentContext) -> AgentResult`.
Credit tracking is automatic via `TrackedAIClient`. Register with `@register_agent("name")`.

## Execution Adapters

- `run_agent_api()`: Runs agents in API request context (sync/SSE)
- `run_agent_worker()`: Runs agents in worker context (background tasks)

## Legacy

AgentResponse is kept for API response compatibility. The orchestrator converts
AgentResult → AgentResponse at the boundary.
"""

# Register agent implementations (must import before anything uses the registry)
from robosystems.operations.agents import implementations  # noqa: F401
from robosystems.operations.agents.adapters import run_agent_api, run_agent_worker
from robosystems.operations.agents.agent_context import (
  AgentContext,
  ProgressReporter,
  ToolAccess,
)
from robosystems.operations.agents.agent_registry import (
  get_agent,
  get_agent_class,
  load_adapter_agents,
  register_agent,
)
from robosystems.operations.agents.agent_registry import (
  list_agents as list_v2_agents,
)
from robosystems.operations.agents.ai_client import AIClient
from robosystems.operations.agents.base import (
  Agent,
  AgentCapability,
  AgentMetadata,
  AgentMode,
  AgentResponse,
  AgentResult,
  AgentSpec,
  BaseAgent,
  ExecutionProfile,
  GraphScope,
  matches_graph_scope,
)
from robosystems.operations.agents.credit_consumer import (
  CreditConsumer,
  FactoryCreditConsumer,
  NoOpCreditConsumer,
  SessionCreditConsumer,
)
from robosystems.operations.agents.orchestrator import (
  AgentOrchestrator,
  AgentSelectionCriteria,
  OrchestratorConfig,
  RoutingStrategy,
)
from robosystems.operations.agents.progress import (
  CallbackProgress,
  NoOpProgress,
  OperationManagerProgress,
)
from robosystems.operations.agents.tool_access import (
  DirectToolAccess,
  HttpToolAccess,
)
from robosystems.operations.agents.tracked_ai import TrackedAIClient

# Load adapter-contributed agents (empty for now — extension point)
load_adapter_agents()

__all__ = [
  # Runtime services
  "AIClient",
  # Agent protocol
  "Agent",
  # Shared enums
  "AgentCapability",
  "AgentContext",
  # Legacy (API compat)
  "AgentMetadata",
  "AgentMode",
  # Orchestrator
  "AgentOrchestrator",
  "AgentResponse",
  "AgentResult",
  "AgentSelectionCriteria",
  "AgentSpec",
  "BaseAgent",
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
  "OrchestratorConfig",
  "ProgressReporter",
  "RoutingStrategy",
  "SessionCreditConsumer",
  "ToolAccess",
  "TrackedAIClient",
  # Registry
  "get_agent",
  "get_agent_class",
  "list_v2_agents",
  "load_adapter_agents",
  "matches_graph_scope",
  "register_agent",
  # Adapters
  "run_agent_api",
  "run_agent_worker",
]
