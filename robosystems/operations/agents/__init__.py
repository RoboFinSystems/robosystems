"""
Agent operations module.

Provides multiagent system with dynamic routing, RAG, and specialized agents.
"""

from .base import (
  BaseAgent,
  AgentCapability,
  AgentMode,
  AgentMetadata,
  AgentResponse,
)
from .orchestrator import (
  AgentOrchestrator,
  RoutingStrategy,
  AgentSelectionCriteria,
  OrchestratorConfig,
)
from .registry import (
  AgentRegistry,
  AgentRegistrationError,
  DuplicateAgentError,
  AgentNotFoundError,
)
from .context import (
  ContextEnricher,
  RAGConfig,
  EmbeddingProvider,
  DocumentChunk,
  SearchResult,
)

# Import concrete agents to register them
from . import financial
from . import rag

__all__ = [
  # Base
  "BaseAgent",
  "AgentCapability",
  "AgentMode",
  "AgentMetadata",
  "AgentResponse",
  # Orchestrator
  "AgentOrchestrator",
  "RoutingStrategy",
  "AgentSelectionCriteria",
  "OrchestratorConfig",
  # Registry
  "AgentRegistry",
  "AgentRegistrationError",
  "DuplicateAgentError",
  "AgentNotFoundError",
  # Context
  "ContextEnricher",
  "RAGConfig",
  "EmbeddingProvider",
  "DocumentChunk",
  "SearchResult",
  # Modules (for registration)
  "financial",
  "rag",
]
