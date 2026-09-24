"""Operator selection: ranks the registered operators in scope for a graph by
their confidence for a query."""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from robosystems.logger import logger
from robosystems.models.core import User
from robosystems.operations.operators.base import (
  Operator,
  matches_graph_scope,
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
class OrchestratorConfig:
  """Per-orchestrator settings; unset fields fall back to `OperatorConfig`."""

  routing_strategy: RoutingStrategy = RoutingStrategy.BEST_MATCH
  enable_rag: bool = False
  enable_fallback: bool = True
  fallback_operator: str | None = None
  max_retries: int = 2
  timeout: float = 60.0
  ensemble_size: int = 3

  def __post_init__(self):
    from robosystems.config import OperatorConfig

    if self.fallback_operator is None:
      self.fallback_operator = OperatorConfig.ORCHESTRATOR_CONFIG["fallback_operator"]


class OperatorOrchestrator:
  """Ranks the operators in scope for one graph and user."""

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

    self._schema_extensions: list[str] | None = None

  def _get_schema_extensions(self) -> list[str]:
    """Cached per instance."""
    if self._schema_extensions is None:
      from robosystems.middleware.mcp.tools.manager import resolve_schema_extensions

      self._schema_extensions = resolve_schema_extensions(self.graph_id)
    return self._schema_extensions

  def _filter_by_scope(self, operators: dict[str, Operator]) -> dict[str, Operator]:
    extensions = self._get_schema_extensions()
    return {
      operator_type: operator
      for operator_type, operator in operators.items()
      if matches_graph_scope(operator.spec.graph_scope, self.graph_id, extensions)
    }

  def _get_all_operators(self) -> dict[str, Operator]:
    operators = {}
    for operator_type in list_operators():
      try:
        operators[operator_type] = get_operator(operator_type)
      except Exception as e:
        logger.warning(f"Could not instantiate operator '{operator_type}': {e}")
    return self._filter_by_scope(operators)

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
