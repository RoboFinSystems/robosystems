"""
Shared utilities for subgraph operations.
"""

from datetime import UTC, datetime

from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from robosystems.config.graph_tier import get_tier_max_subgraphs
from robosystems.config.shared_repositories import is_shared_repository_or_subgraph
from robosystems.logger import log_metric
from robosystems.middleware.graph.utils import (
  construct_subgraph_id,
  generate_unique_subgraph_name,
  parse_subgraph_id,
  validate_subgraph_name,
)
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.models.core.graph import Graph
from robosystems.models.core.graph.graph_user import GraphUser
from robosystems.models.core.user import User
from robosystems.operations.graph.subgraph_service import SubgraphService

# Initialize shared circuit breaker for subgraph operations
circuit_breaker = CircuitBreakerManager()


def get_subgraph_service():
  """Get subgraph service instance."""
  return SubgraphService()


def verify_parent_graph_access(
  graph_id: str, current_user: User, session: Session, required_role: str = "read"
) -> Graph:
  """Return the parent graph after checking the user holds `required_role`
  ('read' or 'admin') on it. Raises 403 on denial, 404 when it is missing."""
  # Enforce graph lifecycle and subscription status (write: creating a subgraph)
  from robosystems.middleware.billing.enforcement import require_graph_access

  parent_graph = require_graph_access(graph_id, session, require_write=True)

  # Block shared repositories from having subgraphs
  if is_shared_repository_or_subgraph(graph_id.lower()):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Shared repositories and their subgraphs cannot have nested subgraphs. "
      "Subgraphs are only available for user-owned graphs.",
    )

  if not GraphUser.user_has_access(current_user.id, graph_id, session):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Access denied to graph {graph_id}",
    )

  if required_role == "admin" and not GraphUser.user_has_admin_access(
    current_user.id, graph_id, session
  ):
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Admin access to parent graph required for this operation",
    )

  return parent_graph


def verify_subgraph_tier_support(parent_graph: Graph):
  """Verify the parent graph's tier allows subgraphs, raising 403 if not."""
  from robosystems.config import env
  from robosystems.logger import logger

  max_subgraphs = get_tier_max_subgraphs(parent_graph.graph_tier)

  logger.info(
    f"Subgraph tier check: tier={parent_graph.graph_tier}, "
    f"max_subgraphs={max_subgraphs}, environment={env.ENVIRONMENT}"
  )

  if max_subgraphs is None or max_subgraphs == 0:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Subgraphs are not available for the {parent_graph.graph_tier} tier. "
      f"Upgrade to a tier that supports subgraphs.",
    )


def verify_parent_graph_active(parent_graph: Graph):
  """Placeholder for a parent-graph liveness check; currently a no-op.

  Callers must not rely on this for authorization — it performs no
  validation and raises nothing.
  """
  pass


def check_subgraph_quota(parent_graph: Graph, session: Session):
  """Check the parent graph's remaining subgraph quota.

  Returns `(current_count, max_allowed, existing_subgraphs)`, raising 403 once
  the quota is exhausted.
  """
  existing_subgraphs = Graph.get_subgraphs(parent_graph.graph_id, session)
  current_count = len(existing_subgraphs)

  # Get max subgraphs from tier configuration
  max_subgraphs = get_tier_max_subgraphs(parent_graph.graph_tier)

  if max_subgraphs is not None and current_count >= max_subgraphs:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Maximum subgraphs limit reached for {parent_graph.graph_tier} tier. "
      f"Current: {current_count}, Maximum: {max_subgraphs}. "
      f"Upgrade tier for higher limits.",
    )

  return current_count, max_subgraphs, existing_subgraphs


def validate_subgraph_name_unique(
  name: str, existing_subgraphs: list, parent_graph_id: str
):
  """Validate the proposed subgraph name's format and uniqueness, raising 400
  if it is malformed or already taken."""
  # Validate name format
  if not validate_subgraph_name(name):
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail="Subgraph name must be alphanumeric and 1-20 characters",
    )

  # Check uniqueness
  existing_names = [sg.subgraph_name for sg in existing_subgraphs]
  if name in existing_names:
    # Try to generate a unique name
    suggested_name = generate_unique_subgraph_name(
      parent_graph_id, name, existing_names
    )
    raise HTTPException(
      status_code=status.HTTP_409_CONFLICT,
      detail=f"Subgraph name '{name}' already exists. "
      f"Suggested alternative: '{suggested_name}'",
    )


def get_subgraph_by_name(
  graph_id: str, subgraph_name: str, session: Session, current_user: User
) -> Graph:
  """Look up a subgraph by parent graph ID and name, raising 404 when it does
  not exist and 400 when the name is malformed."""
  # Construct full subgraph ID
  subgraph_id = construct_subgraph_id(graph_id, subgraph_name)

  # Parse subgraph ID to validate format
  subgraph_info = parse_subgraph_id(subgraph_id)
  if not subgraph_info:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"{subgraph_id} is not a valid subgraph identifier",
    )

  subgraph = Graph.get_by_id(subgraph_id, session)
  if not subgraph or not subgraph.is_subgraph:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Subgraph {subgraph_id} not found",
    )

  return subgraph


def record_operation_start() -> datetime:
  """Record operation start time for metrics."""
  return datetime.now(UTC)


def record_operation_metrics(
  start_time: datetime,
  operation_name: str,
  parent_graph_id: str,
  additional_tags: dict | None = None,
) -> None:
  """Record the duration of a completed subgraph operation."""
  end_time = datetime.now(UTC)
  duration_ms = (end_time - start_time).total_seconds() * 1000

  tags = {"parent_graph": parent_graph_id}
  if additional_tags:
    tags.update(additional_tags)

  log_metric(f"subgraph_{operation_name}_duration", duration_ms, tags)


def handle_circuit_breaker_check(graph_id: str, operation: str) -> None:
  """Check the circuit breaker, raising 503 while it is open."""
  circuit_breaker.check_circuit(graph_id, operation)
