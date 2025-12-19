"""
Agent orchestrator for routing and coordination.

Handles dynamic agent selection, routing strategies, and multi-agent coordination.
"""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from robosystems.logger import logger
from robosystems.models.iam import User
from robosystems.operations.agents.base import (
  AgentCapability,
  AgentMode,
  AgentResponse,
  BaseAgent,
)
from robosystems.operations.agents.context import ContextEnricher
from robosystems.operations.agents.registry import AgentRegistry


class RoutingStrategy(Enum):
  """Strategies for routing queries to agents."""

  BEST_MATCH = "best_match"  # Select agent with highest confidence
  ROUND_ROBIN = "round_robin"  # Distribute evenly across agents
  CAPABILITY_BASED = "capability_based"  # Match required capabilities
  LOAD_BALANCED = "load_balanced"  # Consider agent load
  ENSEMBLE = "ensemble"  # Use multiple agents and aggregate


@dataclass
class AgentSelectionCriteria:
  """Criteria for selecting agents."""

  min_confidence: float = 0.3
  required_capabilities: list[AgentCapability] = field(default_factory=list)
  preferred_mode: AgentMode | None = None
  max_response_time: float = 60.0
  excluded_agents: list[str] = field(default_factory=list)


@dataclass
class OrchestratorConfig:
  """Configuration for the agent orchestrator."""

  routing_strategy: RoutingStrategy = RoutingStrategy.BEST_MATCH
  enable_rag: bool = False
  enable_caching: bool = False
  enable_fallback: bool = True
  fallback_agent: str | None = None
  max_retries: int = 2
  timeout: float = 60.0
  ensemble_size: int = 3

  def __post_init__(self):
    """Set defaults from AgentConfig if not specified."""
    from robosystems.config import AgentConfig

    if self.fallback_agent is None:
      self.fallback_agent = AgentConfig.ORCHESTRATOR_CONFIG["fallback_agent"]
    if self.enable_rag is None:
      self.enable_rag = AgentConfig.ORCHESTRATOR_CONFIG["enable_rag"]


class AgentOrchestrator:
  """
  Orchestrates agent selection and coordination.

  Handles routing strategies, context enrichment, and multi-agent workflows.
  """

  def __init__(
    self,
    graph_id: str,
    user: User,
    db_session=None,
    config: OrchestratorConfig | None = None,
  ):
    """
    Initialize the orchestrator.

    Args:
        graph_id: Graph database identifier
        user: Authenticated user
        db_session: Optional database session
        config: Orchestrator configuration
    """
    self.graph_id = graph_id
    self.user = user
    self.db_session = db_session
    self.config = config or OrchestratorConfig()
    self.registry = AgentRegistry()
    self.context_enricher = (
      ContextEnricher(graph_id) if self.config.enable_rag else None
    )
    self.logger = logger

    # Metrics tracking
    self._metrics = {
      "total_queries": 0,
      "agent_usage": {},
      "total_response_time": 0.0,
      "cache_hits": 0,
      "cache_misses": 0,
      "errors": 0,
    }

    # Simple in-memory cache
    self._cache = {} if config and config.enable_caching else None
    self._round_robin_index = 0

  async def route_query(
    self,
    query: str,
    agent_type: str | None = None,
    mode: AgentMode = AgentMode.STANDARD,
    history: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
    selection_criteria: AgentSelectionCriteria | None = None,
    force_extended: bool = False,
    stream_callback: Callable | None = None,
    ensemble_size: int | None = None,
  ) -> AgentResponse:
    """
    Route a query to the appropriate agent(s).

    Args:
        query: The user's query
        agent_type: Specific agent type to use (optional)
        mode: Execution mode
        history: Conversation history
        context: Additional context
        selection_criteria: Agent selection criteria
        force_extended: Force extended analysis mode
        stream_callback: Callback for streaming responses
        ensemble_size: Number of agents for ensemble routing

    Returns:
        AgentResponse from the selected agent(s)
    """
    start_time = time.time()
    self._metrics["total_queries"] += 1

    try:
      # Check cache if enabled
      if self._cache is not None and not force_extended:
        cache_key = self._get_cache_key(query, agent_type, mode)
        if cache_key in self._cache:
          self._metrics["cache_hits"] += 1
          cached_response = self._cache[cache_key]
          cached_response.metadata["from_cache"] = True
          return cached_response
        self._metrics["cache_misses"] += 1

      # Enrich context if RAG is enabled
      if self.config.enable_rag and self.context_enricher:
        # Always create a context if None, and explicitly enrich it
        context = context or {}
        context = await self.context_enricher.enrich(query, context)
        context["context_enriched"] = True
      elif context is None:
        context = {}

      # Add history info to context
      if history:
        context["has_history"] = True

      # Route based on strategy
      if agent_type:
        # Explicit agent selection
        response = await self._route_to_specific_agent(
          query, agent_type, mode, history, context, stream_callback
        )
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "explicit"
      elif self.config.routing_strategy == RoutingStrategy.ENSEMBLE:
        # Ensemble routing
        response = await self._ensemble_routing(
          query, mode, history, context, ensemble_size or self.config.ensemble_size
        )
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "ensemble"
      elif self.config.routing_strategy == RoutingStrategy.CAPABILITY_BASED:
        # Capability-based routing
        response = await self._capability_based_routing(
          query, mode, history, context, selection_criteria
        )
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "capability_based"
      elif self.config.routing_strategy == RoutingStrategy.LOAD_BALANCED:
        # Load-balanced routing
        response = await self._load_balanced_routing(query, mode, history, context)
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "load_balanced"
      elif self.config.routing_strategy == RoutingStrategy.ROUND_ROBIN:
        # Round-robin routing
        response = await self._round_robin_routing(query, mode, history, context)
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "round_robin"
      else:
        # Best match routing (default)
        response = await self._best_match_routing(
          query, mode, history, context, selection_criteria
        )
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "best_match"

      # Update metrics
      execution_time = time.time() - start_time
      response.execution_time = execution_time
      self._metrics["total_response_time"] += execution_time

      agent_name = response.agent_name
      if agent_name not in self._metrics["agent_usage"]:
        self._metrics["agent_usage"][agent_name] = {"calls": 0, "total_time": 0.0}
      self._metrics["agent_usage"][agent_name]["calls"] += 1
      self._metrics["agent_usage"][agent_name]["total_time"] += execution_time

      # Cache response if enabled
      if self._cache is not None and not force_extended:
        cache_key = self._get_cache_key(query, agent_type, mode)
        self._cache[cache_key] = response

      return response

    except Exception as e:
      self._metrics["errors"] += 1
      self.logger.error(f"Orchestrator routing error: {e!s}")

      # Return error response
      return AgentResponse(
        content=f"Failed to process query: {e!s}",
        agent_name="orchestrator",
        mode_used=mode,
        error_details={
          "code": "ROUTING_ERROR",
          "message": str(e),
        },
        execution_time=time.time() - start_time,
      )

  async def _route_to_specific_agent(
    self,
    query: str,
    agent_type: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    stream_callback: Callable | None,
  ) -> AgentResponse:
    """Route to a specific agent type."""
    agent = self.registry.get_agent(
      agent_type, self.graph_id, self.user, self.db_session
    )

    if not agent:
      raise ValueError(f"Unknown agent type: {agent_type}")

    return await self._execute_agent(
      agent, query, mode, history, context, stream_callback
    )

  async def _best_match_routing(
    self,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    criteria: AgentSelectionCriteria | None,
  ) -> AgentResponse:
    """Route to the agent with highest confidence."""
    agents = self.registry.get_all_agents(self.graph_id, self.user, self.db_session)
    criteria = criteria or AgentSelectionCriteria()

    # Calculate confidence scores
    scores = {}
    for agent_type, agent in agents.items():
      if agent_type in criteria.excluded_agents:
        continue

      # Check capability requirements
      if criteria.required_capabilities:
        if not all(agent.has_capability(cap) for cap in criteria.required_capabilities):
          continue

      # Check mode support
      if criteria.preferred_mode and not agent.supports_mode(criteria.preferred_mode):
        continue

      # Calculate confidence score
      score = agent.can_handle(query, context)
      scores[agent_type] = score

    # Select best agent
    if not scores:
      # No suitable agents, use fallback
      if self.config.enable_fallback:
        return await self._use_fallback_agent(query, mode, history, context)
      raise ValueError("No suitable agent found for query")

    best_agent_type = max(scores, key=lambda x: scores[x])
    best_score = scores[best_agent_type]

    # Check minimum confidence
    if best_score < criteria.min_confidence:
      if self.config.enable_fallback:
        response = await self._use_fallback_agent(query, mode, history, context)
        if response.metadata is None:
          response.metadata = {}
        response.metadata["used_fallback"] = True
        response.metadata["confidence_scores"] = scores
        return response

    agent = agents[best_agent_type]
    response = await self._execute_agent(agent, query, mode, history, context)
    if response.metadata is None:
      response.metadata = {}
    response.metadata["confidence_scores"] = scores
    response.confidence_score = best_score

    return response

  async def _capability_based_routing(
    self,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    criteria: AgentSelectionCriteria | None,
  ) -> AgentResponse:
    """Route based on required capabilities."""
    criteria = criteria or AgentSelectionCriteria()

    if not criteria.required_capabilities:
      # Fall back to best match if no capabilities specified
      return await self._best_match_routing(query, mode, history, context, criteria)

    agents = self.registry.get_all_agents(self.graph_id, self.user, self.db_session)

    # Find agents with required capabilities
    capable_agents = {}
    for agent_type, agent in agents.items():
      if all(agent.has_capability(cap) for cap in criteria.required_capabilities):
        capable_agents[agent_type] = agent

    if not capable_agents:
      if self.config.enable_fallback:
        return await self._use_fallback_agent(query, mode, history, context)
      raise ValueError(f"No agent with capabilities: {criteria.required_capabilities}")

    # Among capable agents, select best match
    best_agent = None
    best_score = 0.0

    for agent_type, agent in capable_agents.items():
      score = agent.can_handle(query, context)
      if score > best_score:
        best_score = score
        best_agent = agent

    return await self._execute_agent(best_agent, query, mode, history, context)

  async def _ensemble_routing(
    self,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    ensemble_size: int,
  ) -> AgentResponse:
    """Use multiple agents and aggregate responses."""
    agents = self.registry.get_all_agents(self.graph_id, self.user, self.db_session)

    # Get confidence scores and sort
    agent_scores = []
    for agent_type, agent in agents.items():
      score = agent.can_handle(query, context)
      agent_scores.append((agent_type, agent, score))

    agent_scores.sort(key=lambda x: x[2], reverse=True)

    # Select top N agents
    selected_agents = agent_scores[:ensemble_size]

    # Execute agents in parallel
    tasks = []
    for agent_type, agent, score in selected_agents:
      task = self._execute_agent(agent, query, mode, history, context)
      tasks.append(task)

    responses = await asyncio.gather(*tasks, return_exceptions=True)

    # Aggregate responses
    valid_responses = [r for r in responses if not isinstance(r, Exception)]

    if not valid_responses:
      raise ValueError("All ensemble agents failed")

    # Simple aggregation: concatenate responses
    aggregated_content = "\n\n---\n\n".join(
      f"**{r.agent_name}**: {r.content}" for r in valid_responses
    )

    # Combine metadata
    combined_metadata = {
      "ensemble_agents": [r.agent_name for r in valid_responses],
      "individual_metadata": [r.metadata for r in valid_responses],
    }

    return AgentResponse(
      content=aggregated_content,
      agent_name="ensemble",
      mode_used=mode,
      metadata=combined_metadata,
      tokens_used=self._sum_tokens(valid_responses),
    )

  async def _load_balanced_routing(
    self,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> AgentResponse:
    """Route based on agent load."""
    agents = self.registry.get_all_agents(self.graph_id, self.user, self.db_session)

    # Select agent with lowest load
    min_load = float("inf")
    selected_agent = None

    for agent_type, agent in agents.items():
      # Get agent load (calls in last period)
      load = self._metrics["agent_usage"].get(agent_type, {}).get("calls", 0)

      if load < min_load:
        min_load = load
        selected_agent = agent

    if not selected_agent:
      raise ValueError("No agents available")

    return await self._execute_agent(selected_agent, query, mode, history, context)

  async def _round_robin_routing(
    self,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> AgentResponse:
    """Route using round-robin strategy."""
    agents = list(
      self.registry.get_all_agents(self.graph_id, self.user, self.db_session).values()
    )

    if not agents:
      raise ValueError("No agents available")

    # Select next agent in rotation
    agent = agents[self._round_robin_index % len(agents)]
    self._round_robin_index += 1

    return await self._execute_agent(agent, query, mode, history, context)

  async def _use_fallback_agent(
    self,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> AgentResponse:
    """Use the configured fallback agent."""
    agent = self.registry.get_agent(
      self.config.fallback_agent, self.graph_id, self.user, self.db_session
    )

    if not agent:
      raise ValueError(f"Fallback agent '{self.config.fallback_agent}' not found")

    response = await self._execute_agent(agent, query, mode, history, context)
    if response.metadata is None:
      response.metadata = {}
    response.metadata["used_fallback"] = True
    return response

  async def _execute_agent(
    self,
    agent: BaseAgent,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    stream_callback: Callable | None = None,
  ) -> AgentResponse:
    """Execute an agent with timeout and error handling."""
    try:
      # Check if agent requires credits and if we have sufficient balance
      if agent.metadata.requires_credits and self.db_session:
        credit_check = await self._check_credits_for_agent(agent, mode)
        if not credit_check["has_sufficient_credits"]:
          return AgentResponse(
            content=f"Insufficient credits for {agent.metadata.name}. Required: {credit_check['estimated_credits']:.0f}, Available: {credit_check['available_credits']:.0f}",
            agent_name=agent.metadata.name,
            mode_used=mode,
            error_details={
              "code": "INSUFFICIENT_CREDITS",
              "message": "Not enough credits to perform AI analysis",
              "required_credits": credit_check["estimated_credits"],
              "available_credits": credit_check["available_credits"],
            },
            execution_time=0.0,
          )

      # Apply timeout
      response = await asyncio.wait_for(
        agent.analyze(
          query=query,
          mode=mode,
          history=history,
          context=context,
          callback=stream_callback,
        ),
        timeout=self.config.timeout,
      )

      # Copy context flags to metadata
      if response.metadata is None:
        response.metadata = {}

      if context.get("context_enriched"):
        response.metadata["context_enriched"] = True

      if context.get("has_history"):
        response.metadata["has_history"] = True

      return response

    except TimeoutError:
      self.logger.error(f"Agent {agent.metadata.name} timed out")
      response = AgentResponse(
        content="Analysis timed out",
        agent_name=agent.metadata.name,
        mode_used=mode,
        metadata={},
        error_details={
          "code": "TIMEOUT",
          "message": f"Agent timeout after {self.config.timeout}s",
        },
      )

      # Copy context flags to metadata
      if response.metadata and context.get("context_enriched"):
        response.metadata["context_enriched"] = True

      if response.metadata and context.get("has_history"):
        response.metadata["has_history"] = True

      return response
    except Exception as e:
      self.logger.error(f"Agent {agent.metadata.name} failed: {e!s}")
      response = AgentResponse(
        content=f"Agent failed: {e!s}",
        agent_name=agent.metadata.name,
        mode_used=mode,
        metadata={},
        error_details={
          "code": "AGENT_ERROR",
          "message": str(e),
        },
      )

      # Copy context flags to metadata
      if response.metadata and context.get("context_enriched"):
        response.metadata["context_enriched"] = True

      if response.metadata and context.get("has_history"):
        response.metadata["has_history"] = True

      return response

  async def coordinate_agents(
    self,
    query: str,
    agent_sequence: list[str],
    mode: AgentMode = AgentMode.STANDARD,
    history: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
    coordination_type: str = "sequential",
  ) -> AgentResponse:
    """
    Coordinate multiple agents in sequence or parallel.

    Args:
        query: The query to process
        agent_sequence: List of agent types to use
        mode: Execution mode
        history: Conversation history
        context: Additional context
        coordination_type: "sequential" or "parallel"

    Returns:
        Combined agent response
    """
    if coordination_type == "parallel":
      return await self._parallel_coordination(
        query, agent_sequence, mode, history, context
      )
    else:
      return await self._sequential_coordination(
        query, agent_sequence, mode, history, context
      )

  async def _sequential_coordination(
    self,
    query: str,
    agent_sequence: list[str],
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> AgentResponse:
    """Execute agents sequentially, passing output forward."""
    accumulated_content = ""
    accumulated_metadata = {}
    current_context = context.copy() if context else {}

    for agent_type in agent_sequence:
      agent = self.registry.get_agent(
        agent_type, self.graph_id, self.user, self.db_session
      )

      if not agent:
        self.logger.warning(f"Agent {agent_type} not found, skipping")
        continue

      # Add previous agent's output to context
      if accumulated_content:
        current_context["previous_agent_output"] = accumulated_content

      response = await self._execute_agent(agent, query, mode, history, current_context)

      accumulated_content += f"\n\n{response.content}"
      accumulated_metadata[agent_type] = response.metadata

    return AgentResponse(
      content=accumulated_content.strip(),
      agent_name="coordinator",
      mode_used=mode,
      metadata={
        "coordination_type": "sequential",
        "agent_sequence": agent_sequence,
        "agent_metadata": accumulated_metadata,
      },
    )

  async def _parallel_coordination(
    self,
    query: str,
    agent_sequence: list[str],
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> AgentResponse:
    """Execute agents in parallel."""
    tasks = []

    for agent_type in agent_sequence:
      agent = self.registry.get_agent(
        agent_type, self.graph_id, self.user, self.db_session
      )

      if not agent:
        self.logger.warning(f"Agent {agent_type} not found, skipping")
        continue

      task = self._execute_agent(agent, query, mode, history, context)
      tasks.append((agent_type, task))

    # Execute all agents in parallel
    results = []
    for agent_type, task in tasks:
      try:
        response = await task
        results.append((agent_type, response))
      except Exception as e:
        self.logger.error(f"Agent {agent_type} failed: {e!s}")

    # Combine results
    combined_content = ""
    combined_metadata = {}

    for agent_type, response in results:
      combined_content += f"\n\n**{agent_type}**: {response.content}"
      combined_metadata[agent_type] = response.metadata

    return AgentResponse(
      content=combined_content.strip(),
      agent_name="coordinator",
      mode_used=mode,
      metadata={
        "coordination_type": "parallel",
        "agent_sequence": agent_sequence,
        "agent_metadata": combined_metadata,
        "execution_time": sum(r.execution_time or 0 for _, r in results),
      },
    )

  def get_agent_recommendations(
    self, query: str, context: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    """
    Get agent recommendations for a query.

    Args:
        query: The query to analyze
        context: Optional context

    Returns:
        List of agent recommendations with confidence scores
    """
    agents = self.registry.get_all_agents(self.graph_id, self.user, self.db_session)
    recommendations = []

    for agent_type, agent in agents.items():
      confidence = agent.can_handle(query, context)
      recommendations.append(
        {
          "agent_type": agent_type,
          "agent_name": agent.metadata.name,
          "confidence": confidence,
          "capabilities": [c.value for c in agent.metadata.capabilities],
        }
      )

    # Sort by confidence
    recommendations.sort(key=lambda x: x["confidence"], reverse=True)
    return recommendations

  def get_metrics(self) -> dict[str, Any]:
    """Get orchestrator metrics."""
    avg_response_time = (
      self._metrics["total_response_time"] / self._metrics["total_queries"]
      if self._metrics["total_queries"] > 0
      else 0
    )

    return {
      "total_queries": self._metrics["total_queries"],
      "agent_usage": self._metrics["agent_usage"],
      "average_response_time": avg_response_time,
      "cache_hits": self._metrics.get("cache_hits", 0),
      "cache_misses": self._metrics.get("cache_misses", 0),
      "errors": self._metrics["errors"],
    }

  async def _check_credits_for_agent(
    self, agent: BaseAgent, mode: AgentMode
  ) -> dict[str, Any]:
    """
    Check if user has sufficient credits for agent execution.

    Args:
        agent: The agent to execute
        mode: The execution mode

    Returns:
        Dict with credit check results
    """
    from decimal import Decimal

    from robosystems.config.billing.ai import AIBillingConfig
    from robosystems.operations.graph.credit_service import CreditService

    try:
      credit_service = CreditService(self.db_session)

      # Estimate credits needed based on mode and agent type
      # These are conservative estimates based on typical token usage
      estimated_tokens = self._estimate_token_usage(agent, mode)

      # Use Claude Sonnet pricing as default for estimation
      pricing = AIBillingConfig.TOKEN_PRICING.get(
        "anthropic_claude_4_sonnet",
        {
          "input": Decimal("0.003"),
          "output": Decimal("0.015"),
        },
      )

      # Calculate estimated cost
      input_cost = (Decimal(estimated_tokens["input"]) / 1000) * pricing["input"]
      output_cost = (Decimal(estimated_tokens["output"]) / 1000) * pricing["output"]
      estimated_cost = input_cost + output_cost

      # Check credit balance
      credit_check = credit_service.check_credit_balance(
        graph_id=self.graph_id,
        required_credits=estimated_cost,
        user_id=str(self.user.id),
        operation_type="agent_call",
      )

      credit_check["estimated_credits"] = float(estimated_cost)
      credit_check["estimated_tokens"] = estimated_tokens

      return credit_check

    except Exception as e:
      self.logger.warning(f"Credit check failed: {e!s}")
      # On error, allow execution but log warning
      return {
        "has_sufficient_credits": True,
        "estimated_credits": 0,
        "available_credits": 0,
        "warning": f"Credit check failed: {e!s}",
      }

  def _estimate_token_usage(self, agent: BaseAgent, mode: AgentMode) -> dict[str, int]:
    """
    Estimate token usage based on agent type and mode.

    These are conservative estimates to ensure we don't block legitimate usage.
    Actual usage is tracked after execution.
    """
    # Base estimates by mode
    mode_estimates = {
      AgentMode.QUICK: {"input": 2000, "output": 500},
      AgentMode.STANDARD: {"input": 5000, "output": 1500},
      AgentMode.EXTENDED: {"input": 15000, "output": 3000},
      AgentMode.STREAMING: {"input": 8000, "output": 2000},
    }

    # Get base estimate
    estimate = mode_estimates.get(mode, {"input": 5000, "output": 1500})

    # Adjust based on agent type (financial agents typically use more tokens)
    if "financial" in agent.metadata.name.lower():
      estimate["input"] = int(estimate["input"] * 1.5)
      estimate["output"] = int(estimate["output"] * 1.5)

    return estimate

  def _get_cache_key(
    self, query: str, agent_type: str | None, mode: AgentMode
  ) -> str:
    """Generate cache key for a query."""
    return f"{agent_type or 'auto'}:{mode.value}:{hash(query)}"

  def _sum_tokens(self, responses: list[AgentResponse]) -> dict[str, int]:
    """Sum token usage across multiple responses."""
    total = {"input": 0, "output": 0}

    for response in responses:
      if response.tokens_used:
        total["input"] += response.tokens_used.get("input", 0)
        total["output"] += response.tokens_used.get("output", 0)

    return total
