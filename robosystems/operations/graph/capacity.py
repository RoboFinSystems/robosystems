"""Tier capacity as the sale paths see it.

Both places the platform commits money against a writer slot — checkout and
``change-tier`` — ask the same question first: is there a healthy writer on
the target tier with a free slot *right now*? Nothing on either path raises
desired capacity on purpose (the high tiers are provisioned on request), so
``scalable`` is not good enough, and any failure to determine capacity reads
as none: refuse the sale rather than collect against a slot that may not
exist.
"""

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
