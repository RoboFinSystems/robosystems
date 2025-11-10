"""
Base agent abstract class and core structures.

Provides the foundation for all agent implementations in the multiagent system.
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from datetime import datetime

from robosystems.models.iam import User
from robosystems.logger import logger


class AgentCapability(Enum):
  """Agent capabilities for routing and discovery."""

  FINANCIAL_ANALYSIS = "financial_analysis"
  DEEP_RESEARCH = "deep_research"
  COMPLIANCE = "compliance"
  RAG_SEARCH = "rag_search"
  CUSTOM = "custom"
  ENTITY_ANALYSIS = "entity_analysis"
  TREND_ANALYSIS = "trend_analysis"


class AgentMode(Enum):
  """Agent execution modes with different performance characteristics."""

  QUICK = "quick"  # Fast, limited tool calls (1-2)
  STANDARD = "standard"  # Balanced performance (3-5 tool calls)
  EXTENDED = "extended"  # Deep research (6+ tool calls)
  STREAMING = "streaming"  # SSE streaming responses


@dataclass
class ExecutionProfile:
  """Execution time profile for an agent mode."""

  min_time: int
  max_time: int
  avg_time: int
  tool_calls: int = 0


@dataclass
class AgentMetadata:
  """Metadata describing an agent's capabilities and configuration."""

  name: str
  description: str
  capabilities: List[AgentCapability]
  version: str = "1.0.0"
  supported_modes: List[AgentMode] = field(
    default_factory=lambda: [
      AgentMode.QUICK,
      AgentMode.STANDARD,
      AgentMode.EXTENDED,
    ]
  )
  max_tokens: Dict[str, int] = field(
    default_factory=lambda: {"input": 150000, "output": 8000}
  )
  requires_credits: bool = True
  author: Optional[str] = None
  tags: List[str] = field(default_factory=list)
  execution_profile: Dict[AgentMode, ExecutionProfile] = field(
    default_factory=lambda: {
      AgentMode.QUICK: ExecutionProfile(
        min_time=2, max_time=5, avg_time=3, tool_calls=2
      ),
      AgentMode.STANDARD: ExecutionProfile(
        min_time=5, max_time=15, avg_time=10, tool_calls=5
      ),
      AgentMode.EXTENDED: ExecutionProfile(
        min_time=30, max_time=120, avg_time=60, tool_calls=20
      ),
    }
  )


@dataclass
class AgentResponse:
  """Standard response structure from agent analysis."""

  content: str
  agent_name: str
  mode_used: AgentMode
  metadata: Optional[Dict[str, Any]] = None
  tokens_used: Optional[Dict[str, int]] = None
  tools_called: List[str] = field(default_factory=list)
  confidence_score: Optional[float] = None
  requires_followup: bool = False
  error_details: Optional[Dict[str, Any]] = None
  execution_time: Optional[float] = None
  timestamp: datetime = field(default_factory=datetime.utcnow)


class BaseAgent(ABC):
  """
  Abstract base class for all agents in the system.

  Provides common functionality and enforces the agent interface.
  """

  def __init__(
    self,
    graph_id: str,
    user: User,
    db_session=None,
  ):
    """
    Initialize the base agent.

    Args:
        graph_id: The graph database identifier
        user: The authenticated user
        db_session: Optional database session for operations
    """
    self.graph_id = graph_id
    self.user = user
    self.db_session = db_session
    self.logger = logger
    self.total_tokens_used = {"input": 0, "output": 0}
    self.kuzu_client = None
    self.mcp_tools = None

  @property
  @abstractmethod
  def metadata(self) -> AgentMetadata:
    """Return agent metadata."""
    pass

  @abstractmethod
  async def analyze(
    self,
    query: str,
    mode: AgentMode = AgentMode.STANDARD,
    history: Optional[List[Dict[str, Any]]] = None,
    context: Optional[Dict[str, Any]] = None,
    callback: Optional[Any] = None,
  ) -> AgentResponse:
    """
    Perform analysis on the query.

    Args:
        query: The user's query to analyze
        mode: Execution mode (quick, standard, extended, streaming)
        history: Conversation history
        context: Additional context for analysis
        callback: Optional callback for progress updates

    Returns:
        AgentResponse with analysis results
    """
    pass

  @abstractmethod
  def can_handle(self, query: str, context: Optional[Dict[str, Any]] = None) -> float:
    """
    Return confidence score (0-1) for handling this query.

    Args:
        query: The query to evaluate
        context: Optional context for evaluation

    Returns:
        Float between 0 and 1 indicating confidence
    """
    pass

  def supports_mode(self, mode: AgentMode) -> bool:
    """Check if agent supports the given mode."""
    return mode in self.metadata.supported_modes

  def has_capability(self, capability: AgentCapability) -> bool:
    """Check if agent has the given capability."""
    return capability in self.metadata.capabilities

  async def initialize_tools(self):
    """Initialize MCP tools for the agent."""
    try:
      from robosystems.middleware.mcp import (
        create_kuzu_mcp_client,
        KuzuMCPTools,
      )

      self.kuzu_client = await create_kuzu_mcp_client(graph_id=self.graph_id)
      self.mcp_tools = KuzuMCPTools(self.kuzu_client)
      self.logger.info(f"Initialized MCP tools for agent in graph {self.graph_id}")
    except Exception as e:
      self.logger.error(f"Failed to initialize MCP tools: {str(e)}")
      raise

  async def close(self):
    """Clean up agent resources."""
    if self.kuzu_client:
      try:
        await self.kuzu_client.close()
        self.logger.debug("Closed Graph client connection")
      except Exception as e:
        self.logger.error(f"Error closing Graph client: {str(e)}")

  def track_tokens(self, input_tokens: int, output_tokens: int):
    """Track token usage for the agent."""
    self.total_tokens_used["input"] += input_tokens
    self.total_tokens_used["output"] += output_tokens

  async def consume_credits(
    self,
    input_tokens: int,
    output_tokens: int,
    model: str = "claude-3-sonnet",
    operation_description: str = "Agent analysis",
  ) -> Optional[Dict[str, Any]]:
    """
    Consume credits based on token usage.

    Args:
        input_tokens: Number of input tokens used
        output_tokens: Number of output tokens generated
        model: AI model used
        operation_description: Description of the operation

    Returns:
        Credit consumption result or None if no session
    """
    if not self.db_session:
      self.logger.debug(
        f"No DB session for credit consumption. "
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
      self.logger.error(f"Error consuming credits: {str(e)}")
      return None

  def validate_mode(self, mode: AgentMode) -> None:
    """
    Validate that the agent supports the requested mode.

    Args:
        mode: The mode to validate

    Raises:
        ValueError: If mode is not supported
    """
    if not self.supports_mode(mode):
      supported = ", ".join(m.value for m in self.metadata.supported_modes)
      raise ValueError(
        f"Agent '{self.metadata.name}' does not support mode '{mode.value}'. "
        f"Supported modes: {supported}"
      )

  def get_mode_limits(self, mode: AgentMode) -> Dict[str, Any]:
    """
    Get operational limits for the specified mode.

    Args:
        mode: The execution mode

    Returns:
        Dict with limits like max_tools, timeout, etc.
    """
    from robosystems.config import AgentConfig

    return AgentConfig.get_mode_limits(mode.value)

  async def prepare_context(
    self,
    query: str,
    context: Optional[Dict[str, Any]] = None,
  ) -> Dict[str, Any]:
    """
    Prepare and enhance context for analysis.

    Args:
        query: The query being analyzed
        context: Initial context

    Returns:
        Enhanced context dictionary
    """
    enhanced_context = context or {}

    # Add standard context elements
    enhanced_context.update(
      {
        "graph_id": self.graph_id,
        "user_id": str(self.user.id),
        "agent_name": self.metadata.name,
        "timestamp": datetime.utcnow().isoformat(),
        "capabilities": [c.value for c in self.metadata.capabilities],
      }
    )

    return enhanced_context

  def __repr__(self) -> str:
    """String representation of the agent."""
    return (
      f"<{self.__class__.__name__} "
      f"name='{self.metadata.name}' "
      f"graph_id='{self.graph_id}'>"
    )
