"""
Agent operations module.

Provides multiagent system with dynamic routing, RAG, and specialized agents.
"""

# Import concrete agents to register them
from . import cypher_agent, financial
from .ai_client import AIClient
from .base import (
  AgentCapability,
  AgentMetadata,
  AgentMode,
  AgentResponse,
  BaseAgent,
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
  # AI Client
  "AIClient",
  "AgentCapability",
  "AgentMetadata",
  "AgentMode",
  "AgentNotFoundError",
  # Orchestrator
  "AgentOrchestrator",
  "AgentRegistrationError",
  # Registry
  "AgentRegistry",
  "AgentResponse",
  "AgentSelectionCriteria",
  # Base
  "BaseAgent",
  # Context
  "ContextEnricher",
  "DocumentChunk",
  "DuplicateAgentError",
  "EmbeddingProvider",
  "OrchestratorConfig",
  "RAGConfig",
  "RoutingStrategy",
  "SearchResult",
  # Modules (for registration)
  "cypher_agent",
  "financial",
]
