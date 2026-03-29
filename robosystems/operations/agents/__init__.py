"""
Agent operations module.

Provides multiagent system with dynamic routing, RAG, and specialized agents.

## New Unified Agent System

The new Agent protocol (Agent, AgentSpec, AgentResult, AgentContext) provides
a single way to define agents that work in any execution context (API, worker).
Use the new protocol for all new agents.

## Legacy System

The legacy classes (BaseAgent, AgentMetadata, AgentResponse) are used by the
existing orchestrator and routers. They will be removed once the orchestrator
is migrated to the new protocol.
"""

# ── New unified agent system ─────────────────────────────────────────────────

# Register agent implementations
from robosystems.operations.agents import implementations  # noqa: F401
from robosystems.operations.agents.agent_context import (
  AgentContext,
  ProgressReporter,
  ToolAccess,
)
from robosystems.operations.agents.agent_registry import (
  get_agent,
  get_agent_class,
  register_agent,
)
from robosystems.operations.agents.agent_registry import (
  list_agents as list_v2_agents,
)
from robosystems.operations.agents.base import (
  Agent,
  AgentResult,
  AgentSpec,
)
from robosystems.operations.agents.credit_consumer import (
  CreditConsumer,
  FactoryCreditConsumer,
  NoOpCreditConsumer,
  SessionCreditConsumer,
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

# ── Legacy system (orchestrator + routers use these during migration) ────────
# Import concrete agents to register them in legacy registry
from . import cypher_agent
from .ai_client import AIClient
from .base import (
  AgentCapability,
  AgentMetadata,
  AgentMode,
  AgentResponse,
  BaseAgent,
  ExecutionProfile,
)
from .context import (
  ContextEnricher,
  DocumentChunk,
  EmbeddingProvider,
  RAGConfig,
  SearchResult,
)
from .orchestrator import (
  AgentOrchestrator,
  AgentSelectionCriteria,
  OrchestratorConfig,
  RoutingStrategy,
)
from .registry import (
  AgentNotFoundError,
  AgentRegistrationError,
  AgentRegistry,
  DuplicateAgentError,
)

__all__ = [
  # ── Legacy (to be removed) ──
  "AIClient",
  # ── New unified system ──
  "Agent",
  # ── Shared enums ──
  "AgentCapability",
  "AgentContext",
  "AgentMetadata",
  "AgentMode",
  "AgentNotFoundError",
  "AgentOrchestrator",
  "AgentRegistrationError",
  "AgentRegistry",
  "AgentResponse",
  "AgentResult",
  "AgentSelectionCriteria",
  "AgentSpec",
  "BaseAgent",
  "CallbackProgress",
  "ContextEnricher",
  "CreditConsumer",
  "DirectToolAccess",
  "DocumentChunk",
  "DuplicateAgentError",
  "EmbeddingProvider",
  "ExecutionProfile",
  "FactoryCreditConsumer",
  "HttpToolAccess",
  "NoOpCreditConsumer",
  "NoOpProgress",
  "OperationManagerProgress",
  "OrchestratorConfig",
  "ProgressReporter",
  "RAGConfig",
  "RoutingStrategy",
  "SearchResult",
  "SessionCreditConsumer",
  "ToolAccess",
  "TrackedAIClient",
  "cypher_agent",
  "get_agent",
  "get_agent_class",
  "list_v2_agents",
  "register_agent",
]
