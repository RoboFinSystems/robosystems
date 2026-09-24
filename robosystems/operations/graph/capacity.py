"""Tier capacity as the sale paths (checkout and ``change-tier``) see it: refuse
the sale unless a healthy writer has a free slot right now."""

from __future__ import annotations

from robosystems.config import env
from robosystems.logger import logger


async def tier_capacity_status(tier: str) -> str:
  """``ready`` when a writer for ``tier`` has a free slot; otherwise
  ``at_capacity``. ``scalable`` (no slot, ASG below max) counts as
  ``at_capacity`` because nothing on the sale paths raises desired capacity.
  Any failure to determine capacity reads as ``at_capacity``.
  """
  try:
    from robosystems.middleware.graph.allocation_manager import (
      LadybugAllocationManager,
    )
    from robosystems.middleware.graph.types import GraphTier

    manager = LadybugAllocationManager(environment=env.ENVIRONMENT)
    status_value = await manager.check_tier_capacity(GraphTier(tier))
  except Exception as e:
    logger.warning(f"Could not determine capacity for tier {tier}: {e}")
    return "at_capacity"
  return "ready" if status_value == "ready" else "at_capacity"
