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
from .ai_client import AIClient

# Import concrete agents to register them
from . import cypher_agent

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
  # AI Client
  "AIClient",
  # Modules (for registration)
  "cypher_agent",
]
