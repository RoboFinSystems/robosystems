"""
Shared utilities for subgraph operations.
"""

from datetime import datetime, timezone
from fastapi import HTTPException, status
from sqlalchemy.orm import Session

from robosystems.models.iam.graph import Graph
from robosystems.models.iam.user import User
from robosystems.models.iam.graph_user import GraphUser
from robosystems.middleware.graph.utils import (
  construct_subgraph_id,
  validate_subgraph_name,
  parse_subgraph_id,
  generate_unique_subgraph_name,
)
from robosystems.middleware.graph.types import GraphTypeRegistry
from robosystems.config.graph_tier import get_tier_max_subgraphs
from robosystems.logger import log_metric
from robosystems.middleware.robustness import CircuitBreakerManager
from robosystems.operations.graph.subgraph_service import SubgraphService

# Initialize shared circuit breaker for subgraph operations
circuit_breaker = CircuitBreakerManager()


def get_subgraph_service():
  """Get subgraph service instance."""
  return SubgraphService()


def verify_parent_graph_access(
  graph_id: str, current_user: User, session: Session, required_role: str = "read"
) -> Graph:
  """
  Verify user has access to parent graph and return the graph.

  Args:
      graph_id: Parent graph ID
      current_user: Current authenticated user
      session: Database session
      required_role: Required role ('read', 'admin')

  Returns:
      Graph object if access is granted

  Raises:
      HTTPException: If access denied or graph not found
  """
  # Block shared repositories from having subgraphs
  if graph_id.lower() in GraphTypeRegistry.SHARED_REPOSITORIES:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Shared repositories cannot have subgraphs. "
      "Subgraphs are only available for user-owned Enterprise and Premium graphs.",
    )

  # Verify parent graph exists
  parent_graph = Graph.get_by_id(graph_id, session)
  if not parent_graph:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Parent graph {graph_id} not found",
    )

  # Get GraphUser for role checking
  user_graph = (
    session.query(GraphUser)
    .filter(GraphUser.user_id == current_user.id, GraphUser.graph_id == graph_id)
    .first()
  )

  if not user_graph:
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail=f"Access denied to graph {graph_id}",
    )

  # Check role requirements
  if required_role == "admin" and user_graph.role != "admin":
    raise HTTPException(
      status_code=status.HTTP_403_FORBIDDEN,
      detail="Admin access to parent graph required for this operation",
    )

  return parent_graph


def verify_subgraph_tier_support(parent_graph: Graph):
  """
  Verify the parent graph tier supports subgraphs.

  Args:
      parent_graph: Parent graph object

  Raises:
      HTTPException: If tier doesn't support subgraphs
  """
  from robosystems.logger import logger
  from robosystems.config import env

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
  """
  Verify parent graph is in active state.

  Args:
      parent_graph: Parent graph object

  Raises:
      HTTPException: If graph is inactive
  """
  pass


def check_subgraph_quota(parent_graph: Graph, session: Session):
  """
  Check if parent graph can create more subgraphs.

  Args:
      parent_graph: Parent graph object
      session: Database session

  Returns:
      tuple: (current_count, max_allowed, existing_subgraphs)

  Raises:
      HTTPException: If quota exceeded
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
  """
  Validate subgraph name is unique and valid.

  Args:
      name: Proposed subgraph name
      existing_subgraphs: List of existing subgraphs
      parent_graph_id: Parent graph ID

  Raises:
      HTTPException: If name is invalid or not unique
  """
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
  """
  Get subgraph by parent graph ID and subgraph name.

  Args:
      graph_id: Parent graph ID
      subgraph_name: Subgraph name
      session: Database session
      current_user: Current user

  Returns:
      Subgraph object

  Raises:
      HTTPException: If subgraph not found or invalid
  """
  # Construct full subgraph ID
  subgraph_id = construct_subgraph_id(graph_id, subgraph_name)

  # Parse subgraph ID to validate format
  subgraph_info = parse_subgraph_id(subgraph_id)
  if not subgraph_info:
    raise HTTPException(
      status_code=status.HTTP_400_BAD_REQUEST,
      detail=f"{subgraph_id} is not a valid subgraph identifier",
    )

  # Get the subgraph
  subgraph = Graph.get_by_id(subgraph_id, session)
  if not subgraph or not subgraph.is_subgraph:
    raise HTTPException(
      status_code=status.HTTP_404_NOT_FOUND,
      detail=f"Subgraph {subgraph_id} not found",
    )

  return subgraph


def record_operation_start() -> datetime:
  """Record operation start time for metrics."""
  return datetime.now(timezone.utc)


def record_operation_metrics(
  start_time: datetime,
  operation_name: str,
  parent_graph_id: str,
  additional_tags: dict | None = None,
) -> None:
  """
  Record operation completion metrics.

  Args:
      start_time: Operation start time
      operation_name: Name of the operation
      parent_graph_id: Parent graph ID
      additional_tags: Additional metric tags
  """
  end_time = datetime.now(timezone.utc)
  duration_ms = (end_time - start_time).total_seconds() * 1000

  tags = {"parent_graph": parent_graph_id}
  if additional_tags:
    tags.update(additional_tags)

  log_metric(f"subgraph_{operation_name}_duration", duration_ms, tags)


def handle_circuit_breaker_check(graph_id: str, operation: str) -> None:
  """
  Check circuit breaker for an operation.

  Args:
      graph_id: Graph ID
      operation: Operation name

  Raises:
      HTTPException: If circuit breaker is open
  """
  circuit_breaker.check_circuit(graph_id, operation)
