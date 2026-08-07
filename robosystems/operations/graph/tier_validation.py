"""Tier change validation for graph upgrade/downgrade operations.

Validates that a graph can safely transition to a new tier by checking
subgraph count and storage capacity against the target tier's limits.
"""

import logging

from fastapi import HTTPException
from sqlalchemy.orm import Session

from robosystems.config.graph_tier import GraphTierConfig, get_tier_max_subgraphs
from robosystems.models.core.graph import Graph

logger = logging.getLogger(__name__)


def validate_subgraph_count(graph_id: str, new_tier: str, db: Session) -> None:
  """Validate that the graph's subgraph count fits the target tier's limit.

  No-op for upgrades (higher tier always has >= subgraphs).

  Args:
      graph_id: Parent graph identifier
      new_tier: Target tier name (e.g., "ladybug-standard")
      db: Database session

  Raises:
      HTTPException: 400 if subgraph count exceeds target tier's max_subgraphs
  """
  new_max = get_tier_max_subgraphs(new_tier)
  if new_max is None:
    return

  subgraphs = Graph.get_subgraphs(graph_id, db)
  current_count = len(subgraphs)

  if current_count > new_max:
    raise HTTPException(
      status_code=400,
      detail=(
        f"Cannot downgrade: {current_count} active subgraphs "
        f"exceeds {new_tier} limit of {new_max}. "
        f"Delete {current_count - new_max} subgraph(s) first."
      ),
    )


async def validate_storage_capacity(
  graph_id: str, current_tier: str, new_tier: str, db: Session
) -> None:
  """Validate that the graph's storage usage fits the target tier's limit.

  No-op for upgrades (higher tier always has >= storage).

  Args:
      graph_id: Parent graph identifier
      current_tier: Current tier name (for storage lookup)
      new_tier: Target tier name
      db: Database session

  Raises:
      HTTPException: 400 if storage usage exceeds target tier's limit
  """
  new_limit_gb = GraphTierConfig.get_instance_storage_limit_gb(new_tier)
  current_limit_gb = GraphTierConfig.get_instance_storage_limit_gb(current_tier)

  if new_limit_gb >= current_limit_gb:
    return

  from robosystems.middleware.graph.ingestion_limits import IngestionLimitChecker

  storage_info = await IngestionLimitChecker.check_instance_storage(
    db, graph_id, current_tier
  )
  # Judged on durable bytes: a blue-green `-wip`/`-prev` artifact is reclaimed
  # by the next successful build and should not pin a graph to a larger tier.
  total_gb = storage_info.get("enforced_storage_gb")

  # Fail closed: approving a downgrade without knowing the usage could land
  # a graph over its new, smaller cap the moment the change completes.
  if total_gb is None:
    raise HTTPException(
      status_code=503,
      detail=(
        "Storage usage could not be verified for downgrade validation; retry shortly."
      ),
    )

  if total_gb > new_limit_gb:
    raise HTTPException(
      status_code=400,
      detail=(
        f"Cannot downgrade: {total_gb:.1f} GB storage "
        f"exceeds {new_tier} limit of {new_limit_gb:.0f} GB. "
        f"Reduce data first."
      ),
    )
