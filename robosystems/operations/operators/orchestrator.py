"""Operator orchestrator for routing and coordination.

Handles dynamic operator selection, routing strategies, and multi-operator coordination.
Uses the unified Operator protocol — operators are stateless, context is injected by adapters.
"""

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any

from robosystems.logger import logger
from robosystems.models.core import User
from robosystems.operations.operators.adapters.api import run_operator_api
from robosystems.operations.operators.base import (
  Operator,
  OperatorCapability,
  OperatorMode,
  OperatorResult,
  matches_graph_scope,
)
from robosystems.operations.operators.credit_preflight import (
  InsufficientOperatorCreditsError,
)
from robosystems.operations.operators.operator_registry import (
  get_operator,
  list_operators,
)


class RoutingStrategy(Enum):
  """Strategies for routing queries to operators."""

  BEST_MATCH = "best_match"
  ROUND_ROBIN = "round_robin"
  CAPABILITY_BASED = "capability_based"
  LOAD_BALANCED = "load_balanced"
  ENSEMBLE = "ensemble"


@dataclass
class OperatorSelectionCriteria:
  """Criteria for selecting operators."""

  min_confidence: float = 0.3
  required_capabilities: list[OperatorCapability] = field(default_factory=list)
  preferred_mode: OperatorMode | None = None
  max_response_time: float = 60.0
  excluded_operators: list[str] = field(default_factory=list)


@dataclass
class OrchestratorConfig:
  """Configuration for the operator orchestrator."""

  routing_strategy: RoutingStrategy = RoutingStrategy.BEST_MATCH
  enable_rag: bool = False
  enable_caching: bool = False
  enable_fallback: bool = True
  fallback_operator: str | None = None
  max_retries: int = 2
  timeout: float = 60.0
  ensemble_size: int = 3

  def __post_init__(self):
    from robosystems.config import OperatorConfig

    if self.fallback_operator is None:
      self.fallback_operator = OperatorConfig.ORCHESTRATOR_CONFIG["fallback_operator"]
    if self.enable_rag is None:
      self.enable_rag = OperatorConfig.ORCHESTRATOR_CONFIG["enable_rag"]


# ── Legacy OperatorResponse (kept for API compatibility) ────────────────────────

from robosystems.operations.operators.base import OperatorResponse  # noqa: E402


class OperatorOrchestrator:
  """Orchestrates operator selection and coordination.

  Uses the unified Operator protocol. Operators are instantiated without context —
  the adapter (run_operator_api) injects tools, credits, and progress.
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
      "operator_usage": {},
      "total_response_time": 0.0,
      "cache_hits": 0,
      "cache_misses": 0,
      "errors": 0,
    }

    self._cache: dict[str, OperatorResponse] | None = (
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

  def _filter_by_scope(self, operators: dict[str, Operator]) -> dict[str, Operator]:
    """Remove operators whose graph_scope excludes the current graph."""
    extensions = self._get_schema_extensions()
    return {
      operator_type: operator
      for operator_type, operator in operators.items()
      if matches_graph_scope(operator.spec.graph_scope, self.graph_id, extensions)
    }

  def _get_all_operators(self) -> dict[str, Operator]:
    """Get all registered operator instances eligible for the current graph."""
    operators = {}
    for operator_type in list_operators():
      try:
        operators[operator_type] = get_operator(operator_type)
      except Exception as e:
        logger.warning(f"Could not instantiate operator '{operator_type}': {e}")
    return self._filter_by_scope(operators)

  def _result_to_response(
    self,
    result: OperatorResult,
    operator_name: str,
    mode: OperatorMode,
    execution_time: float | None = None,
  ) -> OperatorResponse:
    """Convert OperatorResult to OperatorResponse for API compatibility."""
    return OperatorResponse(
      content=result.content,
      operator_name=operator_name,
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
    operator_type: str | None = None,
    mode: OperatorMode = OperatorMode.STANDARD,
    history: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
    selection_criteria: OperatorSelectionCriteria | None = None,
    force_extended: bool = False,
    stream_callback: Callable | None = None,
    ensemble_size: int | None = None,
  ) -> OperatorResponse:
    """Route a query to the appropriate operator(s)."""
    start_time = time.time()
    self._metrics["total_queries"] += 1

    try:
      # Check cache
      if self._cache is not None and not force_extended:
        cache_key = self._get_cache_key(query, operator_type, mode)
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
      if operator_type:
        response = await self._route_to_specific_operator(
          query, operator_type, mode, history, context, stream_callback
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

      operator_name = response.operator_name
      if operator_name not in self._metrics["operator_usage"]:
        self._metrics["operator_usage"][operator_name] = {"calls": 0, "total_time": 0.0}
      self._metrics["operator_usage"][operator_name]["calls"] += 1
      self._metrics["operator_usage"][operator_name]["total_time"] += execution_time

      # Cache response
      if self._cache is not None and not force_extended:
        cache_key = self._get_cache_key(query, operator_type, mode)
        self._cache[cache_key] = response

      return response

    except Exception as e:
      self._metrics["errors"] += 1
      logger.error(f"Orchestrator routing error: {e!s}")
      return OperatorResponse(
        content=f"Failed to process query: {e!s}",
        operator_name="orchestrator",
        mode_used=mode,
        error_details={"code": "ROUTING_ERROR", "message": str(e)},
        execution_time=time.time() - start_time,
      )

  async def _route_to_specific_operator(
    self,
    query: str,
    operator_type: str,
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    stream_callback: Callable | None,
  ) -> OperatorResponse:
    try:
      operator = get_operator(operator_type)
    except KeyError:
      raise ValueError(f"Unknown operator type: {operator_type}")

    extensions = self._get_schema_extensions()
    if not matches_graph_scope(operator.spec.graph_scope, self.graph_id, extensions):
      raise ValueError(
        f"Operator '{operator_type}' is not available for graph '{self.graph_id}'"
      )

    return await self._execute_operator(
      operator, query, mode, history, context, stream_callback
    )

  async def _best_match_routing(
    self,
    query: str,
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    criteria: OperatorSelectionCriteria | None,
  ) -> OperatorResponse:
    operators = self._get_all_operators()
    criteria = criteria or OperatorSelectionCriteria()

    scores = {}
    for operator_type, operator in operators.items():
      if operator_type in criteria.excluded_operators:
        continue
      if criteria.required_capabilities:
        if not all(
          cap in operator.spec.capabilities for cap in criteria.required_capabilities
        ):
          continue
      if (
        criteria.preferred_mode
        and criteria.preferred_mode not in operator.spec.supported_modes
      ):
        continue
      scores[operator_type] = operator.can_handle(query, context)

    if not scores:
      if self.config.enable_fallback:
        return await self._use_fallback_operator(query, mode, history, context)
      raise ValueError("No suitable operator found for query")

    best_operator_type = max(scores, key=lambda x: scores[x])
    best_score = scores[best_operator_type]

    if best_score < criteria.min_confidence:
      if self.config.enable_fallback:
        response = await self._use_fallback_operator(query, mode, history, context)
        if response.metadata is None:
          response.metadata = {}
        response.metadata["used_fallback"] = True
        response.metadata["confidence_scores"] = scores
        return response

    operator = operators[best_operator_type]
    response = await self._execute_operator(operator, query, mode, history, context)
    if response.metadata is None:
      response.metadata = {}
    response.metadata["confidence_scores"] = scores
    response.confidence_score = best_score
    return response

  async def _capability_based_routing(
    self,
    query: str,
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    criteria: OperatorSelectionCriteria | None,
  ) -> OperatorResponse:
    criteria = criteria or OperatorSelectionCriteria()
    if not criteria.required_capabilities:
      return await self._best_match_routing(query, mode, history, context, criteria)

    operators = self._get_all_operators()
    capable = {
      t: a
      for t, a in operators.items()
      if all(cap in a.spec.capabilities for cap in criteria.required_capabilities)
    }

    if not capable:
      if self.config.enable_fallback:
        return await self._use_fallback_operator(query, mode, history, context)
      raise ValueError(
        f"No operator with capabilities: {criteria.required_capabilities}"
      )

    best_operator = max(capable.values(), key=lambda a: a.can_handle(query, context))
    return await self._execute_operator(best_operator, query, mode, history, context)

  async def _ensemble_routing(
    self,
    query: str,
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    ensemble_size: int,
  ) -> OperatorResponse:
    operators = self._get_all_operators()
    scored = [(t, a, a.can_handle(query, context)) for t, a in operators.items()]
    scored.sort(key=lambda x: x[2], reverse=True)
    selected = scored[:ensemble_size]

    tasks = [
      self._execute_operator(a, query, mode, history, context) for _, a, _ in selected
    ]
    responses = await asyncio.gather(*tasks, return_exceptions=True)
    valid = [r for r in responses if not isinstance(r, Exception)]

    if not valid:
      raise ValueError("All ensemble operators failed")

    aggregated = "\n\n---\n\n".join(
      f"**{r.operator_name}**: {r.content}" for r in valid
    )
    return OperatorResponse(
      content=aggregated,
      operator_name="ensemble",
      mode_used=mode,
      metadata={
        "ensemble_operators": [r.operator_name for r in valid],
        "individual_metadata": [r.metadata for r in valid],
      },
      tokens_used=self._sum_tokens(valid),
    )

  async def _load_balanced_routing(
    self,
    query: str,
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> OperatorResponse:
    operators = self._get_all_operators()
    if not operators:
      raise ValueError("No operators available")

    selected = min(
      operators.values(),
      key=lambda a: (
        self._metrics["operator_usage"].get(a.spec.name, {}).get("calls", 0)
      ),
    )
    return await self._execute_operator(selected, query, mode, history, context)

  async def _round_robin_routing(
    self,
    query: str,
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> OperatorResponse:
    operators = list(self._get_all_operators().values())
    if not operators:
      raise ValueError("No operators available")
    operator = operators[self._round_robin_index % len(operators)]
    self._round_robin_index += 1
    return await self._execute_operator(operator, query, mode, history, context)

  async def _use_fallback_operator(
    self,
    query: str,
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> OperatorResponse:
    try:
      operator = get_operator(self.config.fallback_operator)
    except KeyError:
      raise ValueError(f"Fallback operator '{self.config.fallback_operator}' not found")
    response = await self._execute_operator(operator, query, mode, history, context)
    if response.metadata is None:
      response.metadata = {}
    response.metadata["used_fallback"] = True
    return response

  async def _execute_operator(
    self,
    operator: Operator,
    query: str,
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
    stream_callback: Callable | None = None,
  ) -> OperatorResponse:
    """Execute an operator via the API adapter."""
    try:
      # The credit pre-flight lives in the adapter, not here: the SSE and
      # background-queue strategies call the adapter directly and never build
      # an orchestrator, so a check at this layer covered one path in three.
      # This layer's job is only to render the refusal gracefully for the sync
      # path, which returns a body rather than an error status.
      result = await asyncio.wait_for(
        run_operator_api(
          operator=operator,
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

      response = self._result_to_response(result, operator.spec.name, mode)

      if context.get("context_enriched"):
        if response.metadata is None:
          response.metadata = {}
        response.metadata["context_enriched"] = True
      if context.get("has_history"):
        if response.metadata is None:
          response.metadata = {}
        response.metadata["has_history"] = True

      return response

    except InsufficientOperatorCreditsError as e:
      return OperatorResponse(
        content=str(e),
        operator_name=operator.spec.name,
        mode_used=mode,
        error_details={
          "code": "INSUFFICIENT_CREDITS",
          "message": "Not enough credits to perform AI analysis",
          "required_credits": e.estimated_credits,
          "available_credits": e.available_credits,
        },
        execution_time=0.0,
      )
    except TimeoutError:
      logger.error(f"Operator {operator.spec.name} timed out")
      return OperatorResponse(
        content="Analysis timed out",
        operator_name=operator.spec.name,
        mode_used=mode,
        metadata={},
        error_details={
          "code": "TIMEOUT",
          "message": f"Operator timeout after {self.config.timeout}s",
        },
      )
    except Exception as e:
      logger.error(f"Operator {operator.spec.name} failed: {e!s}")
      return OperatorResponse(
        content=f"Operator failed: {e!s}",
        operator_name=operator.spec.name,
        mode_used=mode,
        metadata={},
        error_details={"code": "AGENT_ERROR", "message": str(e)},
      )

  async def coordinate_operators(
    self,
    query: str,
    operator_sequence: list[str],
    mode: OperatorMode = OperatorMode.STANDARD,
    history: list[dict[str, Any]] | None = None,
    context: dict[str, Any] | None = None,
    coordination_type: str = "sequential",
  ) -> OperatorResponse:
    if coordination_type == "parallel":
      return await self._parallel_coordination(
        query, operator_sequence, mode, history, context
      )
    return await self._sequential_coordination(
      query, operator_sequence, mode, history, context
    )

  async def _sequential_coordination(
    self,
    query: str,
    operator_sequence: list[str],
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> OperatorResponse:
    accumulated_content = ""
    accumulated_metadata: dict[str, Any] = {}
    current_context = context.copy() if context else {}

    for operator_type in operator_sequence:
      try:
        operator = get_operator(operator_type)
      except KeyError:
        logger.warning(f"Operator {operator_type} not found, skipping")
        continue

      if accumulated_content:
        current_context["previous_operator_output"] = accumulated_content

      response = await self._execute_operator(
        operator, query, mode, history, current_context
      )
      accumulated_content += f"\n\n{response.content}"
      accumulated_metadata[operator_type] = response.metadata

    return OperatorResponse(
      content=accumulated_content.strip(),
      operator_name="coordinator",
      mode_used=mode,
      metadata={
        "coordination_type": "sequential",
        "operator_sequence": operator_sequence,
        "operator_metadata": accumulated_metadata,
      },
    )

  async def _parallel_coordination(
    self,
    query: str,
    operator_sequence: list[str],
    mode: OperatorMode,
    history: list[dict[str, Any]] | None,
    context: dict[str, Any],
  ) -> OperatorResponse:
    tasks = []
    for operator_type in operator_sequence:
      try:
        operator = get_operator(operator_type)
        tasks.append(
          (
            operator_type,
            self._execute_operator(operator, query, mode, history, context),
          )
        )
      except KeyError:
        logger.warning(f"Operator {operator_type} not found, skipping")

    results = []
    for operator_type, task in tasks:
      try:
        response = await task
        results.append((operator_type, response))
      except Exception as e:
        logger.error(f"Operator {operator_type} failed: {e!s}")

    combined_content = "\n\n".join(f"**{t}**: {r.content}" for t, r in results)
    combined_metadata = {t: r.metadata for t, r in results}

    return OperatorResponse(
      content=combined_content.strip(),
      operator_name="coordinator",
      mode_used=mode,
      metadata={
        "coordination_type": "parallel",
        "operator_sequence": operator_sequence,
        "operator_metadata": combined_metadata,
        "execution_time": sum(r.execution_time or 0 for _, r in results),
      },
    )

  def get_operator_recommendations(
    self, query: str, context: dict[str, Any] | None = None
  ) -> list[dict[str, Any]]:
    operators = self._get_all_operators()
    recommendations = []
    for operator_type, operator in operators.items():
      confidence = operator.can_handle(query, context)
      recommendations.append(
        {
          "operator_type": operator_type,
          "operator_name": operator.spec.name,
          "confidence": confidence,
          "capabilities": [c.value for c in operator.spec.capabilities],
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
      "operator_usage": self._metrics["operator_usage"],
      "average_response_time": avg_time,
      "cache_hits": self._metrics.get("cache_hits", 0),
      "cache_misses": self._metrics.get("cache_misses", 0),
      "errors": self._metrics["errors"],
    }

  def _get_cache_key(
    self, query: str, operator_type: str | None, mode: OperatorMode
  ) -> str:
    return f"{operator_type or 'auto'}:{mode.value}:{hash(query)}"

  def _sum_tokens(self, responses: list[OperatorResponse]) -> dict[str, int]:
    total = {"input": 0, "output": 0}
    for response in responses:
      if response.tokens_used:
        total["input"] += response.tokens_used.get("input", 0)
        total["output"] += response.tokens_used.get("output", 0)
    return total
