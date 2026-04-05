"""Agent orchestrator for routing and coordination.

Handles dynamic agent selection, routing strategies, and multi-agent coordination.
Uses the unified Agent protocol — agents are stateless, context is injected by adapters.
"""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from robosystems.logger import logger
from robosystems.models.core import User
from robosystems.operations.agents.adapters.api import run_agent_api
from robosystems.operations.agents.agent_registry import get_agent, list_agents
from robosystems.operations.agents.base import (
  Agent,
  AgentCapability,
  AgentMode,
  AgentResult,
  matches_graph_scope,
)


class RoutingStrategy(Enum):
  """Strategies for routing queries to agents."""

  BEST_MATCH = "best_match"
  ROUND_ROBIN = "round_robin"
  CAPABILITY_BASED = "capability_based"
  LOAD_BALANCED = "load_balanced"
  ENSEMBLE = "ensemble"


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
    from robosystems.config import AgentConfig

    if self.fallback_agent is None:
      self.fallback_agent = AgentConfig.ORCHESTRATOR_CONFIG["fallback_agent"]
    if self.enable_rag is None:
      self.enable_rag = AgentConfig.ORCHESTRATOR_CONFIG["enable_rag"]


# ── Legacy AgentResponse (kept for API compatibility) ────────────────────────

from robosystems.operations.agents.base import AgentResponse  # noqa: E402


class AgentOrchestrator:
  """Orchestrates agent selection and coordination.

  Uses the unified Agent protocol. Agents are instantiated without context —
  the adapter (run_agent_api) injects tools, credits, and progress.
  """

  def __init__(
    self,
    graph_id: str,
    user: User,
    db_session=None,
    config: OrchestratorConfig | None = None,
  ):
    self.graph_id = graph_id
    self.user = user
    self.db_session = db_session
    self.config = config or OrchestratorConfig()

    # Metrics tracking
    self._metrics: dict[str, Any] = {
      "total_queries": 0,
      "agent_usage": {},
      "total_response_time": 0.0,
      "cache_hits": 0,
      "cache_misses": 0,
      "errors": 0,
    }

    self._cache: dict[str, AgentResponse] | None = (
      {} if config and config.enable_caching else None
    )
    self._round_robin_index = 0
    self._schema_extensions: list[str] | None = None

  def _get_schema_extensions(self) -> list[str]:
    """Resolve schema extensions for the current graph (cached per instance)."""
    if self._schema_extensions is None:
      from robosystems.middleware.mcp.tools.manager import resolve_schema_extensions

      self._schema_extensions = resolve_schema_extensions(self.graph_id)
    return self._schema_extensions

  def _filter_by_scope(self, agents: dict[str, Agent]) -> dict[str, Agent]:
    """Remove agents whose graph_scope excludes the current graph."""
    extensions = self._get_schema_extensions()
    return {
      agent_type: agent
      for agent_type, agent in agents.items()
      if matches_graph_scope(agent.spec.graph_scope, self.graph_id, extensions)
    }

  def _get_all_agents(self) -> dict[str, Agent]:
    """Get all registered agent instances eligible for the current graph."""
    agents = {}
    for agent_type in list_agents():
      try:
        agents[agent_type] = get_agent(agent_type)
      except Exception as e:
        logger.warning(f"Could not instantiate agent '{agent_type}': {e}")
    return self._filter_by_scope(agents)

  def _result_to_response(
    self,
    result: AgentResult,
    agent_name: str,
    mode: AgentMode,
    execution_time: float | None = None,
  ) -> AgentResponse:
    """Convert AgentResult to AgentResponse for API compatibility."""
    return AgentResponse(
      content=result.content,
      agent_name=agent_name,
      mode_used=mode,
      metadata=result.metadata,
      tokens_used=result.metadata.get("tokens_used"),
      tools_called=result.tools_called,
      confidence_score=result.confidence_score,
      requires_followup=result.requires_followup,
      execution_time=execution_time,
    )

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
    """Route a query to the appropriate agent(s)."""
    start_time = time.time()
    self._metrics["total_queries"] += 1

    try:
      # Check cache
      if self._cache is not None and not force_extended:
        cache_key = self._get_cache_key(query, agent_type, mode)
        if cache_key in self._cache:
          self._metrics["cache_hits"] += 1
          cached = self._cache[cache_key]
          if cached.metadata is None:
            cached.metadata = {}
          cached.metadata["from_cache"] = True
          return cached
        self._metrics["cache_misses"] += 1

      context = context or {}
      if history:
        context["has_history"] = True

      # Route based on strategy
      if agent_type:
        response = await self._route_to_specific_agent(
          query, agent_type, mode, history, context, stream_callback
        )
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "explicit"
      elif self.config.routing_strategy == RoutingStrategy.ENSEMBLE:
        response = await self._ensemble_routing(
          query, mode, history, context, ensemble_size or self.config.ensemble_size
        )
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "ensemble"
      elif self.config.routing_strategy == RoutingStrategy.CAPABILITY_BASED:
        response = await self._capability_based_routing(
          query, mode, history, context, selection_criteria
        )
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "capability_based"
      elif self.config.routing_strategy == RoutingStrategy.LOAD_BALANCED:
        response = await self._load_balanced_routing(query, mode, history, context)
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "load_balanced"
      elif self.config.routing_strategy == RoutingStrategy.ROUND_ROBIN:
        response = await self._round_robin_routing(query, mode, history, context)
        if response.metadata is None:
          response.metadata = {}
        response.metadata["routing_strategy"] = "round_robin"
      else:
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

      # Cache response
      if self._cache is not None and not force_extended:
        cache_key = self._get_cache_key(query, agent_type, mode)
        self._cache[cache_key] = response

      return response

    except Exception as e:
      self._metrics["errors"] += 1
      logger.error(f"Orchestrator routing error: {e!s}")
      return AgentResponse(
        content=f"Failed to process query: {e!s}",
        agent_name="orchestrator",
        mode_used=mode,
        error_details={"code": "ROUTING_ERROR", "message": str(e)},
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
    try:
      agent = get_agent(agent_type)
    except KeyError:
      raise ValueError(f"Unknown agent type: {agent_type}")

    extensions = self._get_schema_extensions()
    if not matches_graph_scope(agent.spec.graph_scope, self.graph_id, extensions):
      raise ValueError(
        f"Agent '{agent_type}' is not available for graph '{self.graph_id}'"
      )

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
    agents = self._get_all_agents()
    criteria = criteria or AgentSelectionCriteria()

    scores = {}
    for agent_type, agent in agents.items():
      if agent_type in criteria.excluded_agents:
        continue
      if criteria.required_capabilities:
        if not all(
          cap in agent.spec.capabilities for cap in criteria.required_capabilities
        ):
          continue
      if (
        criteria.preferred_mode
        and criteria.preferred_mode not in agent.spec.supported_modes
      ):
        continue
      scores[agent_type] = agent.can_handle(query, context)

    if not scores:
      if self.config.enable_fallback:
        return await self._use_fallback_agent(query, mode, history, context)
      raise ValueError("No suitable agent found for query")

    best_agent_type = max(scores, key=lambda x: scores[x])
    best_score = scores[best_agent_type]

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
    criteria = criteria or AgentSelectionCriteria()
    if not criteria.required_capabilities:
      return await self._best_match_routing(query, mode, history, context, criteria)

    agents = self._get_all_agents()
    capable = {
      t: a
      for t, a in agents.items()
      if all(cap in a.spec.capabilities for cap in criteria.required_capabilities)
    }

    if not capable:
      if self.config.enable_fallback:
        return await self._use_fallback_agent(query, mode, history, context)
      raise ValueError(f"No agent with capabilities: {criteria.required_capabilities}")

    best_agent = max(capable.values(), key=lambda a: a.can_handle(query, context))
    return await self._execute_agent(best_agent, query, mode, history, context)

  async def _ensemble_routing(
    self,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    ensemble_size: int,
  ) -> AgentResponse:
    agents = self._get_all_agents()
    scored = [(t, a, a.can_handle(query, context)) for t, a in agents.items()]
    scored.sort(key=lambda x: x[2], reverse=True)
    selected = scored[:ensemble_size]

    tasks = [
      self._execute_agent(a, query, mode, history, context) for _, a, _ in selected
    ]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    valid = [r for r in responses if not isinstance(r, Exception)]

    if not valid:
      raise ValueError("All ensemble agents failed")

    aggregated = "\n\n---\n\n".join(f"**{r.agent_name}**: {r.content}" for r in valid)
    return AgentResponse(
      content=aggregated,
      agent_name="ensemble",
      mode_used=mode,
      metadata={
        "ensemble_agents": [r.agent_name for r in valid],
        "individual_metadata": [r.metadata for r in valid],
      },
      tokens_used=self._sum_tokens(valid),
    )

  async def _load_balanced_routing(
    self,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> AgentResponse:
    agents = self._get_all_agents()
    if not agents:
      raise ValueError("No agents available")

    selected = min(
      agents.values(),
      key=lambda a: self._metrics["agent_usage"].get(a.spec.name, {}).get("calls", 0),
    )
    return await self._execute_agent(selected, query, mode, history, context)

  async def _round_robin_routing(
    self,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> AgentResponse:
    agents = list(self._get_all_agents().values())
    if not agents:
      raise ValueError("No agents available")
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
    try:
      agent = get_agent(self.config.fallback_agent)
    except KeyError:
      raise ValueError(f"Fallback agent '{self.config.fallback_agent}' not found")
    response = await self._execute_agent(agent, query, mode, history, context)
    if response.metadata is None:
      response.metadata = {}
    response.metadata["used_fallback"] = True
    return response

  async def _execute_agent(
    self,
    agent: Agent,
    query: str,
    mode: AgentMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    stream_callback: Callable | None = None,
  ) -> AgentResponse:
    """Execute an agent via the API adapter."""
    try:
      # Credit pre-check
      if agent.spec.requires_credits and self.db_session:
        credit_check = await self._check_credits_for_agent(agent, mode)
        if not credit_check["has_sufficient_credits"]:
          return AgentResponse(
            content=f"Insufficient credits for {agent.spec.name}. "
            f"Required: {credit_check['estimated_credits']:.0f}, "
            f"Available: {credit_check['available_credits']:.0f}",
            agent_name=agent.spec.name,
            mode_used=mode,
            error_details={
              "code": "INSUFFICIENT_CREDITS",
              "message": "Not enough credits to perform AI analysis",
              "required_credits": credit_check["estimated_credits"],
              "available_credits": credit_check["available_credits"],
            },
            execution_time=0.0,
          )

      # Execute via adapter with timeout
      result = await asyncio.wait_for(
        run_agent_api(
          agent=agent,
          graph_id=self.graph_id,
          user=self.user,
          query=query,
          mode=mode,
          db_session=self.db_session,
          history=history,
          context=context,
          callback=stream_callback,
        ),
        timeout=self.config.timeout,
      )

      response = self._result_to_response(result, agent.spec.name, mode)

      if context.get("context_enriched"):
        if response.metadata is None:
          response.metadata = {}
        response.metadata["context_enriched"] = True
      if context.get("has_history"):
        if response.metadata is None:
          response.metadata = {}
        response.metadata["has_history"] = True

      return response

    except TimeoutError:
      logger.error(f"Agent {agent.spec.name} timed out")
      return AgentResponse(
        content="Analysis timed out",
        agent_name=agent.spec.name,
        mode_used=mode,
        metadata={},
        error_details={
          "code": "TIMEOUT",
          "message": f"Agent timeout after {self.config.timeout}s",
        },
      )
    except Exception as e:
      logger.error(f"Agent {agent.spec.name} failed: {e!s}")
      return AgentResponse(
        content=f"Agent failed: {e!s}",
        agent_name=agent.spec.name,
        mode_used=mode,
        metadata={},
        error_details={"code": "AGENT_ERROR", "message": str(e)},
      )

  async def coordinate_agents(
    self,
    query: str,
    agent_sequence: list[str],
    mode: AgentMode = AgentMode.STANDARD,
    history: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
    coordination_type: str = "sequential",
  ) -> AgentResponse:
    if coordination_type == "parallel":
      return await self._parallel_coordination(
        query, agent_sequence, mode, history, context
      )
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
    accumulated_content = ""
    accumulated_metadata: dict[str, Any] = {}
    current_context = context.copy() if context else {}

    for agent_type in agent_sequence:
      try:
        agent = get_agent(agent_type)
      except KeyError:
        logger.warning(f"Agent {agent_type} not found, skipping")
        continue

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
    tasks = []
    for agent_type in agent_sequence:
      try:
        agent = get_agent(agent_type)
        tasks.append(
          (agent_type, self._execute_agent(agent, query, mode, history, context))
        )
      except KeyError:
        logger.warning(f"Agent {agent_type} not found, skipping")

    results = []
    for agent_type, task in tasks:
      try:
        response = await task
        results.append((agent_type, response))
      except Exception as e:
        logger.error(f"Agent {agent_type} failed: {e!s}")

    combined_content = "\n\n".join(f"**{t}**: {r.content}" for t, r in results)
    combined_metadata = {t: r.metadata for t, r in results}

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
    agents = self._get_all_agents()
    recommendations = []
    for agent_type, agent in agents.items():
      confidence = agent.can_handle(query, context)
      recommendations.append(
        {
          "agent_type": agent_type,
          "agent_name": agent.spec.name,
          "confidence": confidence,
          "capabilities": [c.value for c in agent.spec.capabilities],
        }
      )
    recommendations.sort(key=lambda x: x["confidence"], reverse=True)
    return recommendations

  def get_metrics(self) -> dict[str, Any]:
    avg_time = (
      self._metrics["total_response_time"] / self._metrics["total_queries"]
      if self._metrics["total_queries"] > 0
      else 0
    )
    return {
      "total_queries": self._metrics["total_queries"],
      "agent_usage": self._metrics["agent_usage"],
      "average_response_time": avg_time,
      "cache_hits": self._metrics.get("cache_hits", 0),
      "cache_misses": self._metrics.get("cache_misses", 0),
      "errors": self._metrics["errors"],
    }

  async def _check_credits_for_agent(
    self, agent: Agent, mode: AgentMode
  ) -> dict[str, Any]:
    from decimal import Decimal

    from robosystems.config.billing.ai import AIBillingConfig
    from robosystems.operations.graph.credit_service import CreditService

    try:
      credit_service = CreditService(self.db_session)
      estimated_tokens = self._estimate_token_usage(agent, mode)

      pricing = AIBillingConfig.TOKEN_PRICING.get(
        "anthropic_claude_4_sonnet",
        {"input": Decimal("3"), "output": Decimal("15")},
      )

      input_cost = (Decimal(estimated_tokens["input"]) / 1000) * pricing["input"]
      output_cost = (Decimal(estimated_tokens["output"]) / 1000) * pricing["output"]
      estimated_cost = input_cost + output_cost

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
      logger.warning(f"Credit check failed: {e!s}")
      return {
        "has_sufficient_credits": True,
        "estimated_credits": 0,
        "available_credits": 0,
        "warning": f"Credit check failed: {e!s}",
      }

  def _estimate_token_usage(self, agent: Agent, mode: AgentMode) -> dict[str, int]:
    mode_estimates = {
      AgentMode.QUICK: {"input": 2000, "output": 500},
      AgentMode.STANDARD: {"input": 5000, "output": 1500},
      AgentMode.EXTENDED: {"input": 15000, "output": 3000},
      AgentMode.STREAMING: {"input": 8000, "output": 2000},
    }
    estimate = mode_estimates.get(mode, {"input": 5000, "output": 1500})

    if "financial" in agent.spec.name.lower():
      estimate["input"] = int(estimate["input"] * 1.5)
      estimate["output"] = int(estimate["output"] * 1.5)

    return estimate

  def _get_cache_key(self, query: str, agent_type: str | None, mode: AgentMode) -> str:
    return f"{agent_type or 'auto'}:{mode.value}:{hash(query)}"

  def _sum_tokens(self, responses: list[AgentResponse]) -> dict[str, int]:
    total = {"input": 0, "output": 0}
    for response in responses:
      if response.tokens_used:
        total["input"] += response.tokens_used.get("input", 0)
        total["output"] += response.tokens_used.get("output", 0)
    return total
